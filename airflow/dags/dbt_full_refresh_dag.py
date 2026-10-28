"""
Airflow DAG — dbt_full_refresh
Runs every Sunday at 02:00 UTC.
Full refresh of all dbt models (staging → dims → facts → aggregates).
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "marketsignal",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
}

# dbt project is mounted at /opt/airflow/dbt in docker-compose
DBT_DIR = "/opt/airflow/dbt/marketsignal"
DBT_BIN = "/home/airflow/.local/bin/dbt"  # BashOperator PATH omits ~/.local/bin

with DAG(
    dag_id="dbt_full_refresh",
    schedule_interval="0 2 * * *",   # daily 02:00 UTC
    start_date=datetime(2026, 10, 1),
    catchup=False,
    default_args=default_args,
    tags=["marketsignal", "dbt"],
    description="Weekly full refresh of all dbt models",
) as dag:

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"cd {DBT_DIR} && {DBT_BIN} seed --profiles-dir . --full-refresh",
        env={
            "SNOWFLAKE_ACCOUNT":   "{{ var.value.SNOWFLAKE_ACCOUNT }}",
            "SNOWFLAKE_USER":      "{{ var.value.SNOWFLAKE_USER }}",
            "SNOWFLAKE_PASSWORD":  "{{ var.value.SNOWFLAKE_PASSWORD }}",
            "SNOWFLAKE_ROLE":      "{{ var.value.SNOWFLAKE_ROLE }}",
            "SNOWFLAKE_DATABASE":  "{{ var.value.SNOWFLAKE_DATABASE }}",
            "SNOWFLAKE_WAREHOUSE": "{{ var.value.SNOWFLAKE_WAREHOUSE }}",
        },
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && {DBT_BIN} run --profiles-dir . --full-refresh",
        env={
            "SNOWFLAKE_ACCOUNT":   "{{ var.value.SNOWFLAKE_ACCOUNT }}",
            "SNOWFLAKE_USER":      "{{ var.value.SNOWFLAKE_USER }}",
            "SNOWFLAKE_PASSWORD":  "{{ var.value.SNOWFLAKE_PASSWORD }}",
            "SNOWFLAKE_ROLE":      "{{ var.value.SNOWFLAKE_ROLE }}",
            "SNOWFLAKE_DATABASE":  "{{ var.value.SNOWFLAKE_DATABASE }}",
            "SNOWFLAKE_WAREHOUSE": "{{ var.value.SNOWFLAKE_WAREHOUSE }}",
        },
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && {DBT_BIN} test --profiles-dir .",
        env={
            "SNOWFLAKE_ACCOUNT":   "{{ var.value.SNOWFLAKE_ACCOUNT }}",
            "SNOWFLAKE_USER":      "{{ var.value.SNOWFLAKE_USER }}",
            "SNOWFLAKE_PASSWORD":  "{{ var.value.SNOWFLAKE_PASSWORD }}",
            "SNOWFLAKE_ROLE":      "{{ var.value.SNOWFLAKE_ROLE }}",
            "SNOWFLAKE_DATABASE":  "{{ var.value.SNOWFLAKE_DATABASE }}",
            "SNOWFLAKE_WAREHOUSE": "{{ var.value.SNOWFLAKE_WAREHOUSE }}",
        },
    )

    dbt_seed >> dbt_run >> dbt_test
