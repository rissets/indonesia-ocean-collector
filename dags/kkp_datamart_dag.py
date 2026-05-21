"""
Airflow DAGs: KKP Datamart Collector

Three DAGs:
  1. kkp_datamart_initial_load  — manual trigger, TRUNCATE + full historical batch
                                   (batch size 1000, all vessels from the beginning)
  2. kkp_datamart_sync          — cron 0 1 * * * (01:00 UTC daily), incremental sync
                                   (no truncate, upsert only, batch size 1000)
  3. kkp_datamart_recollect     — manual trigger with params, for ad-hoc re-runs

Airflow connection required:
  conn_id: maritime_master_ssh  (SSH to maritime-master VM)
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.providers.ssh.operators.ssh import SSHOperator

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

VERIFY_SQL = (
    "SELECT "
    "  (SELECT COUNT(*) FROM master_kapal) AS kapal_total, "
    "  (SELECT COUNT(*) FROM master_kapal WHERE tanda_selar IS NOT NULL AND tanda_selar != '-') AS kapal_enriched, "
    "  (SELECT COUNT(*) FROM master_kapal WHERE pemilik IS NOT NULL AND pemilik != '-') AS has_pemilik, "
    "  (SELECT COUNT(*) FROM master_kapal WHERE alat_tangkap IS NOT NULL AND alat_tangkap != '-') AS has_alat_tangkap, "
    "  (SELECT COUNT(*) FROM master_vessel_tracking) AS tracking_total, "
    "  (SELECT COUNT(*) FROM master_vessel_tracking WHERE direction IS NOT NULL) AS has_direction, "
    "  (SELECT MAX(timestamp) FROM master_vessel_tracking) AS latest_ping;"
)


# ---------------------------------------------------------------------------
# DAG 1: Initial full historical load (manual trigger)
# TRUNCATE both tables, then batch-collect everything from the beginning
# batch_size=1000 per commit
# ---------------------------------------------------------------------------
with DAG(
    dag_id="kkp_datamart_initial_load",
    description="Full historical load: TRUNCATE + collect all kapal & tracking, batch=1000",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 5, 21),
    schedule_interval=None,  # manual trigger only
    catchup=False,
    max_active_runs=1,
    tags=["kkp", "kapal", "tracking", "maritime", "initial-load"],
) as initial_dag:

    truncate = SSHOperator(
        task_id="truncate_tables",
        ssh_conn_id="maritime_master_ssh",
        command=(
            f"{PSQL_CMD} -c \""
            "TRUNCATE TABLE master_vessel_tracking; "
            "TRUNCATE TABLE master_kapal CASCADE;"
            "\" 2>&1"
        ),
        cmd_timeout=120,
        conn_timeout=30,
    )

    collect_all = SSHOperator(
        task_id="collect_all_kapal_and_tracking",
        ssh_conn_id="maritime_master_ssh",
        command=(
            f"cd {COLLECTOR_DIR} && "
            f"{VENV_PYTHON} -m collectors.kkp_datamart_collector "
            "--interval 30 "
            "--batch-size 1000 "
            "--log-level INFO "
            "2>&1"
        ),
        cmd_timeout=14400,  # 4 hours — full fleet initial load
        conn_timeout=30,
    )

    verify_initial = SSHOperator(
        task_id="verify_counts",
        ssh_conn_id="maritime_master_ssh",
        command=f"{PSQL_CMD} -c \"{VERIFY_SQL}\" 2>&1",
        cmd_timeout=60,
        conn_timeout=30,
    )

    truncate >> collect_all >> verify_initial


# ---------------------------------------------------------------------------
# DAG 2: Hourly sync at 01:00 UTC (cron job)
# No truncate — upsert/ON CONFLICT to keep data fresh
# batch_size=1000 per commit
# ---------------------------------------------------------------------------
with DAG(
    dag_id="kkp_datamart_sync",
    description="Daily sync at 01:00 UTC: upsert all kapal & tracking, batch=1000",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 5, 22),
    schedule_interval="0 1 * * *",  # 01:00 UTC every day
    catchup=False,
    max_active_runs=1,
    tags=["kkp", "kapal", "tracking", "maritime", "sync"],
) as sync_dag:

    sync_collect = SSHOperator(
        task_id="sync_kapal_and_tracking",
        ssh_conn_id="maritime_master_ssh",
        command=(
            f"cd {COLLECTOR_DIR} && "
            f"{VENV_PYTHON} -m collectors.kkp_datamart_collector "
            "--interval 30 "
            "--batch-size 1000 "
            "--log-level INFO "
            "2>&1"
        ),
        cmd_timeout=7200,  # 2 hours
        conn_timeout=30,
    )

    sync_verify = SSHOperator(
        task_id="verify_counts",
        ssh_conn_id="maritime_master_ssh",
        command=f"{PSQL_CMD} -c \"{VERIFY_SQL}\" 2>&1",
        cmd_timeout=60,
        conn_timeout=30,
    )

    sync_collect >> sync_verify


# ---------------------------------------------------------------------------
# DAG 3: Ad-hoc re-collect with params (manual trigger)
# ---------------------------------------------------------------------------
with DAG(
    dag_id="kkp_datamart_recollect",
    description="Ad-hoc re-collect: optional TRUNCATE then collect with custom params",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 5, 21),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["kkp", "kapal", "tracking", "maritime", "recollect"],
    params={
        "batch_size": Param(
            default=1000,
            type="integer",
            description="Commit every N tracking rows (default 1000)",
        ),
        "tracking_interval": Param(
            default=30,
            type="integer",
            description="Location interval in minutes",
        ),
        "truncate_first": Param(
            default=False,
            type="boolean",
            description="TRUNCATE tables before collecting",
        ),
        "skip_enrich": Param(
            default=False,
            type="boolean",
            description="Skip data-kapal detail enrichment (faster, fewer columns)",
        ),
    },
) as recollect_dag:

    def _truncate_cmd(**context) -> str:
        if context["params"].get("truncate_first", False):
            return (
                f"{PSQL_CMD} -c \""
                "TRUNCATE TABLE master_vessel_tracking; "
                "TRUNCATE TABLE master_kapal CASCADE;"
                "\" 2>&1"
            )
        return "echo 'truncate_first=false, skipping truncate'"

    maybe_truncate = SSHOperator(
        task_id="maybe_truncate_tables",
        ssh_conn_id="maritime_master_ssh",
        command=_truncate_cmd,
        cmd_timeout=120,
        conn_timeout=30,
    )

    def _collect_cmd(**context) -> str:
        p = context["params"]
        batch  = int(p.get("batch_size", 1000))
        intv   = int(p.get("tracking_interval", 30))
        no_enr = bool(p.get("skip_enrich", False))

        cmd = (
            f"cd {COLLECTOR_DIR} && "
            f"{VENV_PYTHON} -m collectors.kkp_datamart_collector "
            f"--interval {intv} "
            f"--batch-size {batch} "
            "--log-level INFO "
        )
        if no_enr:
            cmd += "--no-enrich "
        cmd += "2>&1"
        return cmd

    run_recollect = SSHOperator(
        task_id="collect_all",
        ssh_conn_id="maritime_master_ssh",
        command=_collect_cmd,
        cmd_timeout=14400,
        conn_timeout=30,
    )

    verify_recollect = SSHOperator(
        task_id="verify_counts",
        ssh_conn_id="maritime_master_ssh",
        command=f"{PSQL_CMD} -c \"{VERIFY_SQL}\" 2>&1",
        cmd_timeout=60,
        conn_timeout=30,
    )

    maybe_truncate >> run_recollect >> verify_recollect
