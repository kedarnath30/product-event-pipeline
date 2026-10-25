"""Airflow DAG: hourly micro-batch ingest -> dbt build (tests gate) -> DQ monitor.

The landing queue is the hand-off point: collectors write hourly NDJSON files,
this DAG drains whatever has arrived since the last run. The ingester's
checkpoint table makes every task safe to retry, and staging is incremental on
load time, so late mobile events land in whichever run loads them.

`dbt build` runs models *and* tests together: a failing test (bad volume,
stale data, quarantine rate > 5%, broken funnel ordering) fails the task and
blocks the DQ report / downstream consumers.

Set PEP_PROJECT_DIR to the repo checkout on the Airflow workers.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

PROJECT_DIR = os.environ.get("PEP_PROJECT_DIR", os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
DB_PATH = os.environ.get("PEP_DUCKDB_PATH", os.path.join(PROJECT_DIR, "warehouse", "events.duckdb"))
ENV = {"PYTHONPATH": PROJECT_DIR, "PEP_DUCKDB_PATH": DB_PATH, "PATH": os.environ.get("PATH", "")}

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}


def _ingest(data_interval_end=None, **_):
    import sys
    sys.path.insert(0, PROJECT_DIR)
    from pathlib import Path

    from src import ingester

    until = data_interval_end.strftime("%Y-%m-%dT%H") if data_interval_end else None
    return ingester.run(db=Path(DB_PATH), until=until, verbose=False)


def _dq(**_):
    import sys
    sys.path.insert(0, PROJECT_DIR)
    from pathlib import Path

    from src import dq_monitor

    out = dq_monitor.run(db=Path(DB_PATH))
    critical = [a for a in out["alerts"] if a["severity"] == "critical"]
    return {"alerts": len(out["alerts"]), "critical": len(critical)}


with DAG(
    dag_id="product_event_pipeline",
    description="Segment-style events: validate + ingest micro-batches, dbt build, data-quality monitor",
    start_date=datetime(2026, 9, 1),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,  # one writer at a time on the DuckDB file
    default_args=default_args,
    tags=["events", "dbt", "data-quality"],
) as dag:
    ingest = PythonOperator(task_id="ingest_micro_batches", python_callable=_ingest)

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            "cd {{ params.project }}/dbt && dbt build --profiles-dir . "
            "--vars '{\"pipeline_as_of\": \"{{ data_interval_end.strftime('%Y-%m-%d %H:%M:%S') }}\"}'"
        ),
        params={"project": PROJECT_DIR},
        env=ENV,
        append_env=True,
    )

    dq = PythonOperator(task_id="dq_monitor", python_callable=_dq)

    ingest >> dbt_build >> dq
