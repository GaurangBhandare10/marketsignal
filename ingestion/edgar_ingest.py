"""
SEC EDGAR Ingestion — MarketSignal
Fetches company facts (financial data) and 8-K filing metadata from EDGAR APIs.

Usage:
    python edgar_ingest.py --tickers AAPL MSFT TSLA

Docs: https://efts.sec.gov/LATEST/search-index?q=%228-K%22&dateRange=custom
"""

import os
import json
import time
import logging
import argparse
from datetime import datetime, timedelta

import requests
import boto3
from botocore.client import Config
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── EDGAR constants ────────────────────────────────────────────────────────────
EDGAR_BASE = "https://data.sec.gov"
EFTS_BASE = "https://efts.sec.gov"
USER_AGENT = os.environ["EDGAR_USER_AGENT"]   # required by SEC fair-use policy
HEADERS = {"User-Agent": USER_AGENT, "Accept": "application/json"}

# ── MinIO / S3 client ──────────────────────────────────────────────────────────
def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
        config=Config(signature_version="s3v4"),
    )


def upload_to_minio(s3, data: dict, key: str):
    bucket = os.environ["MINIO_BUCKET"]
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(data),
        ContentType="application/json",
    )
    log.info("Uploaded s3://%s/%s", bucket, key)


# ── EDGAR API helpers ──────────────────────────────────────────────────────────
@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=30))
def fetch_company_facts(cik: str) -> dict:
    """Fetch all financial facts for a company (XBRL data)."""
    url = f"{EDGAR_BASE}/api/xbrl/companyfacts/CIK{cik.zfill(10)}.json"
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    time.sleep(0.15)  # EDGAR rate limit: ~10 req/s
    return resp.json()


@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=30))
def fetch_8k_filings(ticker: str, days_back: int = 90) -> list[dict]:
    """Search EFTS for recent 8-K filings using CIK (exact company match)."""
    cik = get_cik(ticker)
    all_filings = []

    while True:
        # Submissions API returns exact company filings by CIK — no false matches
        sub_url = f"{EDGAR_BASE}/submissions/CIK{cik.zfill(10)}.json"
        resp = requests.get(sub_url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        time.sleep(0.15)

        data = resp.json()
        recent = data.get("filings", {}).get("recent", {})
        forms = recent.get("form", [])
        dates = recent.get("filingDate", [])
        accessions = recent.get("accessionNumber", [])
        descriptions = recent.get("primaryDocument", [])

        cutoff = (datetime.utcnow() - timedelta(days=days_back)).strftime("%Y-%m-%d")
        for form, date, acc, doc in zip(forms, dates, accessions, descriptions):
            if form in ("8-K", "8-K/A") and date >= cutoff:
                all_filings.append({
                    "ticker": ticker,
                    "cik": cik,
                    "form_type": form,
                    "file_date": date,
                    "period_of_report": date,
                    "display_names": data.get("name", ticker),
                    "file_name": doc,
                })
        break  # submissions API returns all at once

    log.info("%s — %d 8-K filings found (last %d days)", ticker, len(all_filings), days_back)
    return all_filings
# ── CIK lookup ─────────────────────────────────────────────────────────────────
_cik_map: dict[str, str] | None = None

def get_cik(ticker: str) -> str:
    global _cik_map
    if _cik_map is None:
        # company_tickers_exchange.json is the current working endpoint
        resp = requests.get(
            "https://www.sec.gov/files/company_tickers.json", headers=HEADERS, timeout=30
        )
        resp.raise_for_status()
        data = resp.json()
        # format: {0: {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."}, ...}
        _cik_map = {v["ticker"]: str(v["cik_str"]) for v in data.values()}
    cik = _cik_map.get(ticker.upper())
    if not cik:
        raise ValueError(f"CIK not found for ticker: {ticker}")
    return cik


# ── Main ───────────────────────────────────────────────────────────────────────
def ingest_ticker(s3, ticker: str):
    log.info("Ingesting %s …", ticker)
    cik = get_cik(ticker)

    # 1. Company facts
    facts = fetch_company_facts(cik)
    upload_to_minio(
        s3, facts,
        f"edgar/company_facts/{ticker}/{datetime.utcnow().strftime('%Y/%m/%d')}/facts.json",
    )

    # 2. 8-K filings metadata
    filings = fetch_8k_filings(ticker)
    upload_to_minio(
        s3, {"ticker": ticker, "cik": cik, "filings": filings},
        f"edgar/filings_8k/{ticker}/{datetime.utcnow().strftime('%Y/%m/%d')}/8k_filings.json",
    )
    log.info("%s — %d 8-K filings found", ticker, len(filings))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tickers", nargs="+", required=True)
    args = parser.parse_args()

    s3 = get_s3_client()
    for ticker in args.tickers:
        ingest_ticker(s3, ticker.upper())
