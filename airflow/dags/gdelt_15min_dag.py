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
    ingest_latest(s3)


with DAG(
    dag_id="gdelt_15min",
    schedule_interval="*/15 * * * *",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    default_args=default_args,
    tags=["marketsignal", "gdelt"],
    description="15-minute GDELT GKG ingestion",
) as dag:

    ingest = PythonOperator(
        task_id="fetch_gdelt_gkg",
        python_callable=_ingest_gdelt,
    )
