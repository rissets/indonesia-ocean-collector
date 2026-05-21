"""
Airflow DAG: PIPP Tangkapan Collector

Schedules:
  - Daily run: collects yesterday's data (incremental)
  - Backfill: triggered manually with start_date / end_date params for historical batching

The DAG runs on the maritime-master VM via SSHOperator, executing the collector
inside the indonesia-ocean-collector virtualenv.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator

# ---------------------------------------------------------------------------
# Default args
# ---------------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner": "maritime-data",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

# Path to the collector on the remote VM
COLLECTOR_DIR = "/home/rissets/indonesia-ocean-collector"
VENV_PYTHON = f"{COLLECTOR_DIR}/.venv/bin/python"

# ---------------------------------------------------------------------------
# Daily incremental DAG
# ---------------------------------------------------------------------------
with DAG(
    dag_id="pipp_tangkapan_daily",
    description="Collect yesterday PIPP tangkapan data into master_tangkapan_pipp",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 5, 1),
    schedule_interval="0 2 * * *",  # 02:00 UTC daily
    catchup=False,
    max_active_runs=1,
    tags=["pipp", "tangkapan", "maritime"],
) as daily_dag:

    collect_yesterday = SSHOperator(
        task_id="collect_pipp_yesterday",
        ssh_conn_id="maritime_master_ssh",
        # Collect yesterday; --days 1 means last 1 day relative to today
        command=(
            f"cd {COLLECTOR_DIR} && "
            f"{VENV_PYTHON} -m collectors.pipp_collector "
            "--days 1 "
            "--batch-size 1 "
            "--log-level INFO "
            "2>&1"
        ),
        cmd_timeout=600,
        conn_timeout=30,
    )

    verify_count = SSHOperator(
        task_id="verify_row_count",
        ssh_conn_id="maritime_master_ssh",
        command=(
            "PGPASSWORD='@Maritime210526' psql -h 127.0.0.1 -U maritime-os -d maritime-os "
            "-c \"SELECT COUNT(*), MAX(tanggal_bongkar) FROM master_tangkapan_pipp;\" "
            "2>&1"
        ),
        cmd_timeout=60,
        conn_timeout=30,
    )

    collect_yesterday >> verify_count


# ---------------------------------------------------------------------------
# Backfill / batch DAG (triggered manually)
# ---------------------------------------------------------------------------
with DAG(
    dag_id="pipp_tangkapan_backfill",
    description="Backfill PIPP tangkapan data for a custom date range",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,  # manual trigger only
    catchup=False,
    max_active_runs=1,
    tags=["pipp", "tangkapan", "maritime", "backfill"],
    params={
        "start_date": Param(
            default="2026-01-01",
            type="string",
            description="Start date inclusive (YYYY-MM-DD)",
        ),
        "end_date": Param(
            default="2026-05-20",
            type="string",
            description="End date inclusive (YYYY-MM-DD)",
        ),
        "batch_size": Param(
            default=30,
            type="integer",
            description="Commit every N days",
        ),
    },
) as backfill_dag:

    def _build_backfill_command(**context) -> str:
        params = context["params"]
        start = params["start_date"]
        end = params["end_date"]
        batch = params["batch_size"]
        return (
            f"cd {COLLECTOR_DIR} && "
            f"{VENV_PYTHON} -m collectors.pipp_collector "
            f"--start {start} "
            f"--end {end} "
            f"--batch-size {batch} "
            "--log-level INFO "
            "2>&1"
        )

    run_backfill = SSHOperator(
        task_id="run_pipp_backfill",
        ssh_conn_id="maritime_master_ssh",
        command=_build_backfill_command,
        cmd_timeout=7200,  # 2 hours for large historical ranges
        conn_timeout=30,
    )

    verify_backfill = SSHOperator(
        task_id="verify_backfill_count",
        ssh_conn_id="maritime_master_ssh",
        command=(
            "PGPASSWORD='@Maritime210526' psql -h 127.0.0.1 -U maritime-os -d maritime-os "
            "-c \""
            "SELECT COUNT(*) as total, "
            "COUNT(kode_ikan) as has_kode_ikan, "
            "COUNT(lama_trip) as has_lama_trip, "
            "COUNT(wpp_tangkap) as has_wpp, "
            "COUNT(nomor_bkp) as has_nomor_bkp "
            "FROM master_tangkapan_pipp;"
            "\" 2>&1"
        ),
        cmd_timeout=60,
        conn_timeout=30,
    )

    run_backfill >> verify_backfill
