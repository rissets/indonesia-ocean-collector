"""Airflow DAGs for PIPP catch data collection."""

from __future__ import annotations

from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

COLLECTOR_DIR = "/home/rissets/indonesia-ocean-collector"
ENV_PREFIX = f"cd {COLLECTOR_DIR} && set -a && . {COLLECTOR_DIR}/.env && set +a"
PYTHON = f"{COLLECTOR_DIR}/.venv/bin/python"

DEFAULT_ARGS = {
    "owner": "maritime-data",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def _month_ranges(start: date, end: date) -> list[tuple[str, str]]:
    cur = date(start.year, start.month, 1)
    ranges = []
    while cur <= end:
        nxt = date(cur.year + (cur.month // 12), (cur.month % 12) + 1, 1)
        ranges.append((cur.isoformat(), min(nxt - timedelta(days=1), end).isoformat()))
        cur = nxt
    return ranges


with DAG(
    dag_id="pipp_tangkapan_sync",
    description="Daily PIPP tangkapan sync for yesterday's landing data",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 5, 21),
    schedule_interval="0 1 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["pipp", "tangkapan", "maritime"],
) as sync_dag:
    BashOperator(
        task_id="sync_yesterday",
        bash_command=(
            f"{ENV_PREFIX} && "
            f"{PYTHON} -m collectors.pipp_collector "
            "--days 7 --batch-size 7 --workers 3 --log-level INFO"
        ),
        execution_timeout=timedelta(minutes=45),
    )


with DAG(
    dag_id="pipp_tangkapan_backfill",
    description="Monthly PIPP backfill from 2021-01-01 to today",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2021, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    tags=["pipp", "tangkapan", "maritime", "backfill"],
) as backfill_dag:
    for start, end in _month_ranges(date(2021, 1, 1), date.today()):
        BashOperator(
            task_id=f"collect_{start[:7].replace('-', '_')}",
            bash_command=(
                f"{ENV_PREFIX} && "
                f"{PYTHON} -m collectors.pipp_collector "
                f"--start {start} --end {end} --batch-size 7 --workers 4 --no-postprocess --log-level INFO"
            ),
            execution_timeout=timedelta(hours=2),
        )
