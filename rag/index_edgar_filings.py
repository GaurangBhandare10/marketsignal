"""
Index SEC 8-K filing text into ChromaDB for RAG queries.

Usage:
    python rag/index_edgar_filings.py
    python rag/index_edgar_filings.py --tickers META TSLA  # specific tickers only
"""

import os
import re
import time
import argparse
import logging

import requests
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

EDGAR_BASE  = "https://www.sec.gov"
USER_AGENT  = os.environ.get("EDGAR_USER_AGENT", "MarketSignal/1.0 admin@example.com")
HEADERS     = {"User-Agent": USER_AGENT, "Accept": "text/html,application/json"}


def get_snowflake_filings(tickers: list[str] | None = None) -> list[dict]:
    """Fetch 8-K filing metadata from Snowflake."""
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ.get("SNOWFLAKE_ROLE", "MARKETSIGNAL_ROLE"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "MARKETSIGNAL"),
        schema="RAW",
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    )
    cur = conn.cursor()

    ticker_filter = ""
    if tickers:
        quoted = ", ".join(f"'{t.upper()}'" for t in tickers)
        ticker_filter = f"AND UPPER(ticker) IN ({quoted})"

    cur.execute(f"""
        SELECT ticker, cik, file_date, file_name
        FROM raw.edgar_filings
        WHERE form_type = '8-K'
          AND file_name IS NOT NULL
          {ticker_filter}
        ORDER BY file_date DESC
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    log.info("Found %d 8-K filings in Snowflake", len(rows))
    return [{"ticker": r[0], "cik": r[1], "date": r[2], "accession": str(r[3])} for r in rows]


def fetch_filing_text(cik: str, file_date: str) -> str:
    """Re-fetch EDGAR submissions for the CIK to get the real accession number,
    then download and strip the primary 8-K document."""
    sub_url = f"https://data.sec.gov/submissions/CIK{str(cik).zfill(10)}.json"
    resp = requests.get(sub_url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    time.sleep(0.15)

    data = resp.json()
    recent = data.get("filings", {}).get("recent", {})
    forms      = recent.get("form", [])
    dates      = recent.get("filingDate", [])
    accessions = recent.get("accessionNumber", [])
    docs       = recent.get("primaryDocument", [])

    # Find the 8-K filed on the given date
    for form, date, acc, doc in zip(forms, dates, accessions, docs):
        if form in ("8-K", "8-K/A") and str(date) == str(file_date):
            acc_clean = acc.replace("-", "")
            doc_url = f"{EDGAR_BASE}/Archives/edgar/data/{cik}/{acc_clean}/{doc}"
            log.info("  → downloading %s", doc_url)
            doc_resp = requests.get(doc_url, headers=HEADERS, timeout=60)
            if doc_resp.status_code != 200:
                log.warning("  → HTTP %s for %s", doc_resp.status_code, doc_url)
                return ""
            time.sleep(0.15)
            # Strip HTML tags and collapse whitespace
            text = re.sub(r"<[^>]+>", " ", doc_resp.text)
            text = re.sub(r"&nbsp;", " ", text)
            text = re.sub(r"&#\d+;", " ", text)
            text = re.sub(r"\s{2,}", " ", text).strip()
            return text

    log.warning("  → No 8-K found for CIK %s on %s in submissions API", cik, file_date)
    return ""


def main(tickers: list[str] | None = None):
    # Import here so the script works from any directory
    import sys
    sys.path.insert(0, os.path.dirname(__file__))
    from rag_pipeline import index_filing, get_chroma_collection

    collection = get_chroma_collection()
    existing_ids = set(collection.get()["ids"])
    log.info("ChromaDB already has %d chunks", len(existing_ids))

    filings = get_snowflake_filings(tickers)
    if not filings:
        log.warning("No filings found in Snowflake. Run the edgar_daily DAG first.")
        return

    indexed = 0
    skipped = 0
    for f in filings:
        key = f"{f['ticker']}_{f['date']}_0"   # first chunk id
        if key in existing_ids:
            log.info("Already indexed %s / %s — skipping", f['ticker'], f['date'])
            skipped += 1
            continue

        log.info("Fetching %s / %s  (CIK %s)", f['ticker'], f['date'], f['cik'])
        text = fetch_filing_text(f['cik'], f['date'])
        if not text or len(text) < 200:
            log.warning("  → No usable text found, skipping")
            skipped += 1
            continue

        log.info("  → %d characters — indexing into ChromaDB", len(text))
        index_filing(f['ticker'], str(f['date']), text)
        indexed += 1

    log.info("Done. Indexed: %d, Skipped: %d", indexed, skipped)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tickers", nargs="*", help="Ticker(s) to index (default: all in Snowflake)")
    args = parser.parse_args()
    main(tickers=args.tickers)
