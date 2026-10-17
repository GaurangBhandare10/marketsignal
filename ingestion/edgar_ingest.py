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
    """Search EFTS for recent 8-K filings for a ticker."""
    date_from = (datetime.utcnow() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    url = (
        f"{EFTS_BASE}/LATEST/search-index"
        f"?q=%228-K%22&dateRange=custom&startdt={date_from}"
        f"&entity={ticker}&forms=8-K"
    )
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    time.sleep(0.15)
    hits = resp.json().get("hits", {}).get("hits", [])
    return [h["_source"] for h in hits]


# ── CIK lookup ─────────────────────────────────────────────────────────────────
_cik_map: dict[str, str] | None = None

def get_cik(ticker: str) -> str:
    global _cik_map
    if _cik_map is None:
        resp = requests.get(
            f"{EDGAR_BASE}/files/company_tickers.json", headers=HEADERS, timeout=30
        )
        resp.raise_for_status()
        data = resp.json()
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
