"""
Airflow DAG — gdelt_15min
Runs every 15 minutes.
Downloads the latest GDELT GKG file, filters to financial news, loads to MinIO.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow/ingestion")

default_args = {
    "owner": "marketsignal",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}


def _ingest_gdelt(**context):
    import os
    import boto3
    from botocore.client import Config
    from gdelt_ingest import ingest_latest

    # Force-set from Airflow Variables — env_file pre-populates with localhost values
    os.environ["MINIO_ENDPOINT"]   = Variable.get("MINIO_ENDPOINT")
    os.environ["MINIO_ACCESS_KEY"] = Variable.get("MINIO_ACCESS_KEY")
    os.environ["MINIO_SECRET_KEY"] = Variable.get("MINIO_SECRET_KEY")
    os.environ["MINIO_BUCKET"]     = Variable.get("MINIO_BUCKET", default_var="marketsignal-raw")

    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
        config=Config(signature_version="s3v4"),
    )
    minio_key = ingest_latest(s3)   # None if GDELT file not yet available (404)
    # Push key so load task knows which file to read; None = nothing to load
    context["ti"].xcom_push(key="minio_key", value=minio_key)


def _load_gdelt_to_snowflake(**context):
    """Read the just-ingested GDELT CSV from MinIO and INSERT into raw.gdelt_gkg."""
    import io
    import logging
    import boto3
    import pandas as pd
    import snowflake.connector
    from botocore.client import Config
    from datetime import datetime

    log = logging.getLogger(__name__)

    minio_key = context["ti"].xcom_pull(key="minio_key", task_ids="fetch_gdelt_gkg")
    if not minio_key:
        log.info("No minio_key in XCom — nothing to load.")
        return

    # ── MinIO ──────────────────────────────────────────────────────────────────
    s3 = boto3.client(
        "s3",
        endpoint_url=Variable.get("MINIO_ENDPOINT"),
        aws_access_key_id=Variable.get("MINIO_ACCESS_KEY"),
        aws_secret_access_key=Variable.get("MINIO_SECRET_KEY"),
        config=Config(signature_version="s3v4"),
    )
    bucket = Variable.get("MINIO_BUCKET", default_var="marketsignal-raw")

    body = s3.get_object(Bucket=bucket, Key=minio_key)["Body"].read()
    df = pd.read_csv(io.BytesIO(body))

    if df.empty:
        log.info("Empty GDELT file — nothing to load.")
        return

    ingested_at = datetime.utcnow().isoformat()  # TIMESTAMP_NTZ — no tz suffix

    # Map CSV columns → raw.gdelt_gkg columns
    # DATE is YYYYMMDDHHMMSS integer; parse to timestamp
    df["capture_time"] = pd.to_datetime(
        df["DATE"].astype(str), format="%Y%m%d%H%M%S", errors="coerce"
    ).dt.strftime("%Y-%m-%d %H:%M:%S")

    df = df.rename(columns={
        "DocumentIdentifier": "source_url",
        "Themes":             "themes",
        "Tone":               "tone",
        "Organizations":      "organizations",
    })

    rows = [
        (
            row.get("capture_time"),
            str(row.get("source_url", "") or ""),
            str(row.get("themes", "") or ""),
            row.get("tone"),           # float or None
            str(row.get("organizations", "") or ""),
            ingested_at,
        )
        for _, row in df.iterrows()
        if row.get("capture_time") and row.get("source_url")
    ]

    if not rows:
        log.info("No valid rows after mapping.")
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
        CREATE TABLE IF NOT EXISTS raw.gdelt_gkg (
            capture_time  TIMESTAMP_NTZ,
            source_url    VARCHAR(2000),
            themes        VARCHAR(16777216),
            tone          FLOAT,
            organizations VARCHAR(16777216),
            ingested_at   TIMESTAMP_NTZ
        )
    """)

    cur.executemany(
        """
        INSERT INTO raw.gdelt_gkg
            (capture_time, source_url, themes, tone, organizations, ingested_at)
        VALUES (%s::TIMESTAMP_NTZ, %s, %s, %s, %s, %s::TIMESTAMP_NTZ)
        """,
        rows,
    )

    conn.commit()
    cur.close()
    conn.close()
    log.info("Loaded %d rows into raw.gdelt_gkg", len(rows))


with DAG(
    dag_id="gdelt_15min",
    schedule_interval="*/15 * * * *",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    default_args=default_args,
    tags=["marketsignal", "gdelt"],
    description="15-minute GDELT GKG ingestion → MinIO → Snowflake raw",
) as dag:

    ingest = PythonOperator(
        task_id="fetch_gdelt_gkg",
        python_callable=_ingest_gdelt,
    )

    load_snowflake = PythonOperator(
        task_id="load_to_snowflake",
        python_callable=_load_gdelt_to_snowflake,
    )

    ingest >> load_snowflake
