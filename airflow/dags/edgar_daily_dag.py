"""
Airflow DAG — edgar_daily
Runs every day at 06:00 UTC.
Ingests SEC EDGAR company facts + 8-K filings for the watchlist tickers.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

# ── Secrets come from Airflow Variables (set via UI or CLI, never hardcoded) ──
# airflow variables set EDGAR_USER_AGENT "MarketSignal/1.0 your@email.com"
# airflow variables set WATCHLIST_TICKERS '["AAPL","MSFT","TSLA","NVDA","META"]'

sys.path.insert(0, "/opt/airflow/ingestion")   # path set in docker-compose volume

default_args = {
    "owner": "marketsignal",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}


def _get_tickers() -> list[str]:
    import json
    return json.loads(Variable.get("WATCHLIST_TICKERS", default_var='["AAPL","MSFT","TSLA"]'))


def _ingest_edgar(**context):
    import os
    import boto3
    from botocore.client import Config
    from edgar_ingest import ingest_ticker

    os.environ.setdefault("EDGAR_USER_AGENT", Variable.get("EDGAR_USER_AGENT"))
    os.environ.setdefault("MINIO_ENDPOINT",   Variable.get("MINIO_ENDPOINT"))
    os.environ.setdefault("MINIO_ACCESS_KEY", Variable.get("MINIO_ACCESS_KEY"))
    os.environ.setdefault("MINIO_SECRET_KEY", Variable.get("MINIO_SECRET_KEY"))
    os.environ.setdefault("MINIO_BUCKET",     Variable.get("MINIO_BUCKET", default_var="marketsignal-raw"))

    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
        config=Config(signature_version="s3v4"),
    )

    for ticker in _get_tickers():
        ingest_ticker(s3, ticker)


def _load_to_snowflake(**context):
    """Read today's EDGAR JSON files from MinIO and INSERT into raw.edgar_filings."""
    import json
    import logging
    import boto3
    import snowflake.connector
    from botocore.client import Config
    from datetime import datetime, timezone

    log = logging.getLogger(__name__)

    # ── MinIO ──────────────────────────────────────────────────────────────────
    s3 = boto3.client(
        "s3",
        endpoint_url=Variable.get("MINIO_ENDPOINT"),
        aws_access_key_id=Variable.get("MINIO_ACCESS_KEY"),
        aws_secret_access_key=Variable.get("MINIO_SECRET_KEY"),
        config=Config(signature_version="s3v4"),
    )
    bucket = Variable.get("MINIO_BUCKET", default_var="marketsignal-raw")
    today = datetime.utcnow().strftime("%Y/%m/%d")
    ingested_at = datetime.now(timezone.utc).isoformat()

    # ── Collect rows from all tickers ──────────────────────────────────────────
    rows: list[tuple] = []
    for ticker in _get_tickers():
        prefix = f"edgar/filings_8k/{ticker}/{today}/"
        try:
            resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
            for obj in resp.get("Contents", []):
                body = s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read()
                data = json.loads(body)
                for filing in data.get("filings", []):
                    rows.append((
                        str(filing.get("ticker", ticker)),
                        str(filing.get("cik", "")),
                        filing.get("file_date"),          # 'YYYY-MM-DD' str or None
                        str(filing.get("form_type", "8-K")),
                        str(filing.get("display_names", "")),
                        str(filing.get("file_name", "")),
                        filing.get("period_of_report"),   # 'YYYY-MM-DD' str or None
                        ingested_at,
                    ))
        except Exception as exc:
            log.warning("Skipping %s: %s", ticker, exc)

    if not rows:
        log.info("No filings to load for %s.", today)
        return

    # ── Snowflake ──────────────────────────────────────────────────────────────
    conn = snowflake.connector.connect(
        account=Variable.get("SNOWFLAKE_ACCOUNT"),
        user=Variable.get("SNOWFLAKE_USER"),
        password=Variable.get("SNOWFLAKE_PASSWORD"),
        role=Variable.get("SNOWFLAKE_ROLE", default_var="MARKETSIGNAL_ROLE"),
        database=Variable.get("SNOWFLAKE_DATABASE", default_var="MARKETSIGNAL"),
        schema="RAW",
        warehouse=Variable.get("SNOWFLAKE_WAREHOUSE"),
    )
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw.edgar_filings (
            ticker           VARCHAR(20),
            cik              VARCHAR(20),
            file_date        DATE,
            form_type        VARCHAR(20),
            display_names    VARCHAR(1000),
            file_name        VARCHAR(200),
            period_of_report DATE,
            ingested_at      TIMESTAMP_TZ
        )
    """)

    # Bronze layer = append-only; dbt incremental handles dedup via surrogate key
    cur.executemany(
        """
        INSERT INTO raw.edgar_filings
            (ticker, cik, file_date, form_type, display_names,
             file_name, period_of_report, ingested_at)
        VALUES (%s, %s, %s::DATE, %s, %s, %s, %s::DATE, %s::TIMESTAMP_TZ)
        """,
        rows,
    )

    conn.commit()
    cur.close()
    conn.close()
    log.info("Loaded %d rows into raw.edgar_filings", len(rows))


with DAG(
    dag_id="edgar_daily",
    schedule_interval="0 6 * * *",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    default_args=default_args,
    tags=["marketsignal", "edgar"],
    description="Daily SEC EDGAR ingestion for watchlist tickers",
) as dag:

    ingest = PythonOperator(
        task_id="fetch_and_stage_edgar",
        python_callable=_ingest_edgar,
    )

    load_snowflake = PythonOperator(
        task_id="load_to_snowflake",
        python_callable=_load_to_snowflake,
    )

    ingest >> load_snowflake
