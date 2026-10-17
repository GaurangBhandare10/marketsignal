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
    """Copy staged JSON files from MinIO into Snowflake raw schema via Snowpipe / COPY INTO."""
    # TODO (Week 2): implement Snowflake COPY INTO from internal stage
    pass


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
