"""
Airflow DAG: KKP Datamart Collector

Schedules:
  - Daily run (kkp_datamart_daily): collects all vessels + tracking, runs at 01:00 UTC
  - Backfill / full re-collect (kkp_datamart_backfill): manual trigger, supports
    --max-vessels and --no-enrich flags for targeted runs

The DAG runs on the maritime-master VM via SSHOperator, executing the collector
inside the indonesia-ocean-collector virtualenv.

Airflow connection required:
  conn_id: maritime_master_ssh  (SSH connection to maritime-master VM)
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.providers.ssh.operators.ssh import SSHOperator

# ---------------------------------------------------------------------------
# Shared config
# ---------------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner": "maritime-data",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
}

COLLECTOR_DIR = "/home/rissets/indonesia-ocean-collector"
VENV_PYTHON   = f"{COLLECTOR_DIR}/.venv/bin/python"
PSQL_CMD      = "PGPASSWORD='@Maritime210526' psql -h 127.0.0.1 -U maritime-os -d maritime-os"


# ---------------------------------------------------------------------------
# Daily DAG — full collect: all vessels + tracking
# ---------------------------------------------------------------------------
with DAG(
    dag_id="kkp_datamart_daily",
    description="Daily KKP Datamart collect: all kapal registry + vessel tracking",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 5, 21),
    schedule_interval="0 1 * * *",  # 01:00 UTC daily
    catchup=False,
    max_active_runs=1,
    tags=["kkp", "kapal", "tracking", "maritime"],
) as daily_dag:

    collect_kapal = SSHOperator(
        task_id="collect_kkp_kapal_and_tracking",
        ssh_conn_id="maritime_master_ssh",
        command=(
            f"cd {COLLECTOR_DIR} && "
            f"{VENV_PYTHON} -m collectors.kkp_datamart_collector "
            "--interval 30 "
            "--batch-size 500 "
            "--log-level INFO "
            "2>&1"
        ),
        cmd_timeout=7200,  # 2 hours — full fleet can be large
        conn_timeout=30,
    )

    verify_counts = SSHOperator(
        task_id="verify_row_counts",
        ssh_conn_id="maritime_master_ssh",
        command=(
            f"{PSQL_CMD} -c \""
            "SELECT "
            "  (SELECT COUNT(*) FROM master_kapal) AS kapal_total, "
            "  (SELECT COUNT(*) FROM master_kapal WHERE tanda_selar IS NOT NULL) AS kapal_enriched, "
            "  (SELECT COUNT(*) FROM master_vessel_tracking) AS tracking_total, "
            "  (SELECT MAX(timestamp) FROM master_vessel_tracking) AS latest_ping;"
            "\" 2>&1"
        ),
        cmd_timeout=60,
        conn_timeout=30,
    )

    collect_kapal >> verify_counts


# ---------------------------------------------------------------------------
# Backfill / targeted re-collect DAG (manual trigger)
# ---------------------------------------------------------------------------
with DAG(
    dag_id="kkp_datamart_backfill",
    description="Manual KKP Datamart re-collect with configurable scope",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,  # manual trigger only
    catchup=False,
    max_active_runs=1,
    tags=["kkp", "kapal", "tracking", "maritime", "backfill"],
    params={
        "max_vessels": Param(
            default=0,
            type="integer",
            description="Limit tracking to first N vessels (0 = all)",
        ),
        "batch_size": Param(
            default=500,
            type="integer",
            description="Commit every N tracking rows",
        ),
        "tracking_interval": Param(
            default=30,
            type="integer",
            description="Location interval in minutes",
        ),
        "skip_enrich": Param(
            default=False,
            type="boolean",
            description="Skip data-kapal detail enrichment (faster, fewer columns)",
        ),
    },
) as backfill_dag:

    def _build_backfill_cmd(**context) -> str:
        p = context["params"]
        max_v   = int(p.get("max_vessels", 0))
        batch   = int(p.get("batch_size", 500))
        intv    = int(p.get("tracking_interval", 30))
        no_enr  = bool(p.get("skip_enrich", False))

        cmd = (
            f"cd {COLLECTOR_DIR} && "
            f"{VENV_PYTHON} -m collectors.kkp_datamart_collector "
            f"--interval {intv} "
            f"--batch-size {batch} "
            "--log-level INFO "
        )
        if max_v > 0:
            cmd += f"--max-vessels {max_v} "
        if no_enr:
            cmd += "--no-enrich "
        cmd += "2>&1"
        return cmd

    run_backfill = SSHOperator(
        task_id="run_kkp_backfill",
        ssh_conn_id="maritime_master_ssh",
        command=_build_backfill_cmd,
        cmd_timeout=14400,  # 4 hours for full historical re-collect
        conn_timeout=30,
    )

    verify_backfill = SSHOperator(
        task_id="verify_backfill_counts",
        ssh_conn_id="maritime_master_ssh",
        command=(
            f"{PSQL_CMD} -c \""
            "SELECT "
            "  (SELECT COUNT(*) FROM master_kapal) AS kapal_total, "
            "  (SELECT COUNT(*) FROM master_kapal WHERE pemilik IS NOT NULL) AS has_pemilik, "
            "  (SELECT COUNT(*) FROM master_kapal WHERE alat_tangkap IS NOT NULL) AS has_alat_tangkap, "
            "  (SELECT COUNT(*) FROM master_kapal WHERE tanda_selar IS NOT NULL) AS has_tanda_selar, "
            "  (SELECT COUNT(*) FROM master_vessel_tracking) AS tracking_total, "
            "  (SELECT COUNT(*) FROM master_vessel_tracking WHERE direction IS NOT NULL) AS has_direction, "
            "  (SELECT MAX(timestamp) FROM master_vessel_tracking) AS latest_ping;"
            "\" 2>&1"
        ),
        cmd_timeout=60,
        conn_timeout=30,
    )

    run_backfill >> verify_backfill
