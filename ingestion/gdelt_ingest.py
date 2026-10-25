"""
GDELT Ingestion — MarketSignal
Downloads GDELT 2.0 GKG (Global Knowledge Graph) 15-minute CSV files,
filters to financial news events, and loads to MinIO.

Usage:
    python gdelt_ingest.py                 # latest 15-min file
    python gdelt_ingest.py --backfill 7   # backfill last N days
"""

import os
import io
import csv
import gzip
import time
import logging
import argparse
from datetime import datetime, timedelta
from urllib.request import urlopen

import boto3
import pandas as pd
from botocore.client import Config
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── GDELT constants ────────────────────────────────────────────────────────────
GDELT_MASTER_URL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"
GDELT_BASE_URL   = "http://data.gdeltproject.org/gdeltv2/"

# Columns we care about (GKG column indices, 0-based)
# Full schema: https://blog.gdeltproject.org/gdelt-2-0-our-global-world-in-realtime/
KEEP_COLUMNS = [
    "GKGRECORDID", "DATE", "SourceCollectionIdentifier",
    "SourceCommonName", "DocumentIdentifier",
    "Organizations", "Persons", "Locations",
    "Themes", "Tone",
]

# Themes that indicate financial/corporate news
FINANCE_THEMES = {
    "ECON_STOCKMARKET", "ECON_BANKRUPTCY", "ECON_MERGER",
    "CORP_EARNINGS", "CORP_FRAUD", "CORP_REGULATORY",
    "GOV_INVESTIGATE", "ECON_TRADE",
}


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
        config=Config(signature_version="s3v4"),
    )


def fetch_latest_gkg_url() -> str:
    with urlopen(GDELT_MASTER_URL, timeout=30) as resp:
        lines = resp.read().decode().strip().splitlines()
    # lastupdate.txt has 3 lines: events, mentions, gkg
    gkg_line = [l for l in lines if ".gkg." in l][0]
    return gkg_line.split()[-1]   # URL is the last field


def download_and_filter_gkg(url: str) -> pd.DataFrame:
    log.info("Downloading %s", url)
    with urlopen(url, timeout=120) as resp:
        raw = resp.read()

    # GKG files are tab-separated, sometimes gzipped
    if url.endswith(".gz"):
        raw = gzip.decompress(raw)

    df = pd.read_csv(
        io.StringIO(raw.decode("utf-8", errors="replace")),
        sep="\t",
        header=None,
        low_memory=False,
        on_bad_lines="skip",
    )

    # GKG v2 has 27 columns; map by position for the ones we need
    col_positions = {
        "GKGRECORDID": 0, "DATE": 1, "SourceCollectionIdentifier": 2,
        "SourceCommonName": 3, "DocumentIdentifier": 4,
        "Themes": 7, "Locations": 9, "Persons": 11, "Organizations": 13,
        "Tone": 15,   # V2Tone: "avg,pos,neg,polarity,..." — we take avg only
    }
    df = df.rename(columns={v: k for k, v in col_positions.items()})
    df = df[[c for c in KEEP_COLUMNS if c in df.columns]]

    # Parse Tone: first comma-separated value is the overall average tone (float)
    if "Tone" in df.columns:
        df["Tone"] = pd.to_numeric(
            df["Tone"].astype(str).str.split(",").str[0],
            errors="coerce",
        )

    # Filter: keep rows that mention at least one finance theme
    if "Themes" in df.columns:
        df = df[
            df["Themes"].fillna("").apply(
                lambda t: any(theme in t for theme in FINANCE_THEMES)
            )
        ]

    log.info("Filtered to %d rows", len(df))
    return df


def upload_to_minio(s3, df: pd.DataFrame, timestamp: str):
    bucket = os.environ["MINIO_BUCKET"]
    key = f"gdelt/gkg/{timestamp[:8]}/{timestamp}.csv"
    csv_bytes = df.to_csv(index=False).encode()
    s3.put_object(Bucket=bucket, Key=key, Body=csv_bytes, ContentType="text/csv")
    log.info("Uploaded s3://%s/%s (%d rows)", bucket, key, len(df))


def ingest_latest(s3):
    url = fetch_latest_gkg_url()
    timestamp = url.split("/")[-1].split(".")[0]   # e.g. 20241015123000
    df = download_and_filter_gkg(url)
    upload_to_minio(s3, df, timestamp)


def backfill(s3, days: int):
    """Backfill by fetching the 15-min files list from GDELT master file index."""
    log.info("Backfill mode: %d days", days)
    cutoff = datetime.utcnow() - timedelta(days=days)
    master_url = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
    with urlopen(master_url, timeout=120) as resp:
        lines = resp.read().decode().strip().splitlines()

    gkg_lines = [l for l in lines if ".gkg." in l and not l.endswith(".md5")]
    log.info("Found %d total GKG files in master list", len(gkg_lines))

    for line in gkg_lines:
        parts = line.split()
        url = parts[-1]
        ts_str = url.split("/")[-1].split(".")[0]
        try:
            ts = datetime.strptime(ts_str, "%Y%m%d%H%M%S")
        except ValueError:
            continue
        if ts < cutoff:
            continue
        try:
            df = download_and_filter_gkg(url)
            upload_to_minio(s3, df, ts_str)
        except Exception as exc:
            log.warning("Failed %s: %s", url, exc)
        time.sleep(0.5)   # be polite to GDELT servers


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--backfill", type=int, default=0,
                        help="Number of days to backfill (0 = latest file only)")
    args = parser.parse_args()

    s3 = get_s3_client()
    if args.backfill > 0:
        backfill(s3, args.backfill)
    else:
        ingest_latest(s3)
