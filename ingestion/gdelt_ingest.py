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
import zipfile
import logging
import argparse
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from urllib.request import urlopen
from urllib.error import HTTPError

import boto3
import pandas as pd
import snowflake.connector
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
    try:
        with urlopen(url, timeout=120) as resp:
            raw = resp.read()
    except HTTPError as e:
        if e.code == 404:
            # lastupdate.txt races ahead of the actual file upload by ~1 min
            log.warning("GDELT file not yet available (404): %s — skipping.", url)
            return pd.DataFrame()
        raise

    # GKG files are tab-separated, delivered as .zip or .gz
    if url.endswith(".zip"):
        with zipfile.ZipFile(io.BytesIO(raw)) as zf:
            raw = zf.read(zf.namelist()[0])   # extract the single CSV inside
    elif url.endswith(".gz"):
        raw = gzip.decompress(raw)

    df = pd.read_csv(
        io.StringIO(raw.decode("utf-8", errors="replace")),
        sep="\t",
        header=None,
        on_bad_lines="skip",
        engine="python",       # C parser overflows on GDELT's very long theme/org fields
        quoting=csv.QUOTE_NONE,  # GKG files are unquoted tab-separated; skip quote parsing
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

    # Replace pandas 'nan' strings with actual NaN so they upload as NULL
    df = df.replace("nan", pd.NA)

    log.info("Filtered to %d rows", len(df))
    return df


def upload_to_minio(s3, df: pd.DataFrame, timestamp: str):
    bucket = os.environ["MINIO_BUCKET"]
    key = f"gdelt/gkg/{timestamp[:8]}/{timestamp}.csv"
    csv_bytes = df.to_csv(index=False).encode()
    s3.put_object(Bucket=bucket, Key=key, Body=csv_bytes, ContentType="text/csv")
    log.info("Uploaded s3://%s/%s (%d rows)", bucket, key, len(df))


def ingest_latest(s3) -> str | None:
    """Returns the MinIO key on success, None if the file wasn't available yet."""
    url = fetch_latest_gkg_url()
    timestamp = url.split("/")[-1].split(".")[0]   # e.g. 20241015123000
    df = download_and_filter_gkg(url)
    if df.empty:
        log.info("Nothing to upload — GDELT file not yet available.")
        return None
    upload_to_minio(s3, df, timestamp)
    return f"gdelt/gkg/{timestamp[:8]}/{timestamp}.csv"


def _get_existing_keys(s3, days: int) -> set:
    """Return the set of MinIO keys already uploaded (to skip on resume)."""
    bucket = os.environ["MINIO_BUCKET"]
    existing = set()
    cutoff_date = (datetime.utcnow() - timedelta(days=days)).strftime("%Y%m%d")
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix="gdelt/gkg/"):
        for obj in page.get("Contents", []):
            # key format: gdelt/gkg/YYYYMMDD/YYYYMMDDHHMMSS.csv
            date_part = obj["Key"].split("/")[2]  # YYYYMMDD folder
            if date_part >= cutoff_date:
                ts = obj["Key"].split("/")[-1].replace(".csv", "")
                existing.add(ts)
    log.info("Found %d files already in MinIO — will skip these.", len(existing))
    return existing


def _process_one(url: str, ts_str: str, counter: list, total: int, lock: threading.Lock) -> str:
    """Download, filter and upload one GDELT file. Returns ts_str on success, '' on skip/fail."""
    s3 = get_s3_client()   # each thread needs its own client (not thread-safe)
    try:
        df = download_and_filter_gkg(url)
        if not df.empty:
            upload_to_minio(s3, df, ts_str)
        with lock:
            counter[0] += 1
            done = counter[0]
        if done % 50 == 0 or done == total:
            log.info("Progress: %d / %d files processed", done, total)
        return ts_str
    except Exception as exc:
        log.warning("Failed %s: %s", url, exc)
        with lock:
            counter[0] += 1
        return ""


def backfill(s3, days: int, workers: int = 6):
    """Backfill by fetching the 15-min files list from GDELT master file index.
    Runs in parallel (workers threads) and skips files already in MinIO.
    """
    log.info("Backfill mode: %d days, %d parallel workers", days, workers)
    cutoff = datetime.utcnow() - timedelta(days=days)

    existing = _get_existing_keys(s3, days)

    master_url = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
    with urlopen(master_url, timeout=120) as resp:
        lines = resp.read().decode().strip().splitlines()

    gkg_lines = [l for l in lines if ".gkg." in l and not l.endswith(".md5")]
    log.info("Found %d total GKG files in master list", len(gkg_lines))

    # Build work list: filter by date and skip already-uploaded files
    tasks = []
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
        if ts_str in existing:
            continue
        tasks.append((url, ts_str))

    log.info("%d files to download (%d already done, skipping those)", len(tasks), len(existing))

    if not tasks:
        log.info("Nothing to do — all files already uploaded.")
        return

    counter = [0]
    lock = threading.Lock()
    total = len(tasks)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(_process_one, url, ts_str, counter, total, lock): ts_str
            for url, ts_str in tasks
        }
        for f in as_completed(futures):
            pass   # progress is logged inside _process_one


def _get_snowflake_conn():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ.get("SNOWFLAKE_ROLE", "MARKETSIGNAL_ROLE"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "MARKETSIGNAL"),
        schema="RAW",
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    )


def _read_minio_csv(s3, bucket: str, key: str) -> pd.DataFrame:
    """Download one CSV from MinIO and return rows ready for Snowflake insertion."""
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    df = pd.read_csv(io.BytesIO(body))
    if df.empty:
        return df
    if "DATE" in df.columns:
        df["capture_time"] = pd.to_datetime(
            df["DATE"].astype(str), format="%Y%m%d%H%M%S", errors="coerce"
        ).dt.strftime("%Y-%m-%d %H:%M:%S")
    df = df.rename(columns={
        "DocumentIdentifier": "source_url",
        "Themes":             "themes",
        "Tone":               "tone",
        "Organizations":      "organizations",
    })
    return df


def load_minio_to_snowflake(days: int, workers: int = 6, batch_size: int = 20):
    """Read all MinIO backfill files and insert into raw.gdelt_gkg.
    Skips dates already fully loaded; reloads today in case the DAG ran mid-day.
    """
    s3 = get_s3_client()
    bucket = os.environ["MINIO_BUCKET"]
    cutoff = datetime.utcnow() - timedelta(days=days)
    cutoff_str = cutoff.strftime("%Y%m%d")

    # List all MinIO files in the date range
    paginator = s3.get_paginator("list_objects_v2")
    all_keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix="gdelt/gkg/"):
        for obj in page.get("Contents", []):
            date_folder = obj["Key"].split("/")[2]
            if date_folder >= cutoff_str:
                all_keys.append(obj["Key"])
    log.info("Found %d MinIO files in the last %d days", len(all_keys), days)

    # Find dates already loaded in Snowflake so we can skip them
    conn = _get_snowflake_conn()
    cur = conn.cursor()
    cur.execute(f"""
        SELECT DISTINCT TO_VARCHAR(TO_DATE(capture_time), 'YYYYMMDD')
        FROM raw.gdelt_gkg
        WHERE capture_time >= '{cutoff.strftime("%Y-%m-%d")}'
    """)
    existing_dates = {row[0] for row in cur.fetchall()}
    log.info("Dates already in Snowflake (skipping): %s", sorted(existing_dates))

    keys_to_load = [k for k in all_keys if k.split("/")[2] not in existing_dates]
    log.info("%d files to load, %d skipped (already in Snowflake)",
             len(keys_to_load), len(all_keys) - len(keys_to_load))

    if not keys_to_load:
        log.info("Nothing to load — Snowflake is up to date.")
        conn.close()
        return

    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw.gdelt_gkg (
            capture_time  TIMESTAMP_NTZ,
            source_url    VARCHAR(2000),
            themes        VARCHAR(16777216),
            tone          FLOAT,
            organizations VARCHAR(16777216),
            ingested_at   TIMESTAMP_NTZ
        )
    """)

    ingested_at = datetime.utcnow().isoformat()
    total = len(keys_to_load)
    done = 0

    for i in range(0, total, batch_size):
        batch_keys = keys_to_load[i : i + batch_size]

        # Download batch from MinIO in parallel
        rows = []
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(_read_minio_csv, s3, bucket, k): k for k in batch_keys}
            for fut in as_completed(futures):
                df = fut.result()
                if df.empty:
                    continue
                for _, row in df.iterrows():
                    ct = row.get("capture_time")
                    su = row.get("source_url")
                    # Use pd.isna() — plain `not val` is False for float('nan') since NaN is truthy
                    if pd.isna(ct) or pd.isna(su) or not str(su).strip():
                        continue
                    def _s(v):
                        """Convert to string, returning '' for any NaN/None."""
                        try:
                            return "" if pd.isna(v) else str(v)
                        except (TypeError, ValueError):
                            return str(v)
                    tone_val = row.get("tone")
                    try:
                        tone_val = None if pd.isna(tone_val) else float(tone_val)
                    except (TypeError, ValueError):
                        tone_val = None
                    rows.append((
                        _s(ct),
                        _s(su),
                        _s(row.get("themes")),
                        tone_val,
                        _s(row.get("organizations")),
                        ingested_at,
                    ))

        if rows:
            cur.executemany(
                """
                INSERT INTO raw.gdelt_gkg
                    (capture_time, source_url, themes, tone, organizations, ingested_at)
                VALUES (%s::TIMESTAMP_NTZ, %s, %s, %s, %s, %s::TIMESTAMP_NTZ)
                """,
                rows,
            )
            conn.commit()

        done += len(batch_keys)
        log.info("Snowflake load: %d / %d files, %d rows this batch", done, total, len(rows))

    cur.close()
    conn.close()
    log.info("Snowflake backfill complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--backfill", type=int, default=0,
                        help="Number of days to backfill to MinIO (0 = latest file only)")
    parser.add_argument("--load-snowflake", type=int, default=0,
                        help="Load MinIO backfill files into Snowflake for last N days")
    parser.add_argument("--workers", type=int, default=6,
                        help="Parallel download threads (default: 6)")
    args = parser.parse_args()

    s3 = get_s3_client()
    if args.load_snowflake > 0:
        load_minio_to_snowflake(args.load_snowflake, workers=args.workers)
    elif args.backfill > 0:
        backfill(s3, args.backfill, workers=args.workers)
    else:
        ingest_latest(s3)
