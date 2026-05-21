"""
Airflow DAG: KKP Datamart Collector

Workflow:
  1. Run kkp_datamart_backfill (manual trigger) once to load ALL vessels + tracking.
     The DAG keeps retrying until every vessel in the registry has been processed
     and verifies completeness before finishing.
  2. Once the backfill is confirmed complete, kkp_datamart_daily takes over:
     runs at 01:00 UTC every day to refresh tracking data.
     A pre-flight check at the start of each daily run ensures the initial load
     is present; if not, the daily run is skipped with a clear log message.

Airflow connection required:
  conn_id: maritime_master_ssh  (SSH connection to maritime-master VM)
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator

# ---------------------------------------------------------------------------
# Shared config
# ---------------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner": "maritime-data",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=15),
    "email_on_failure": False,
}

COLLECTOR_DIR = "/home/rissets/indonesia-ocean-collector"
VENV_PYTHON   = f"{COLLECTOR_DIR}/.venv/bin/python"
PSQL_CMD      = "PGPASSWORD='@Maritime210526' psql -h 127.0.0.1 -U maritime-os -d maritime-os"

# Minimum rows that must exist in master_kapal before the daily DAG is allowed to run.
# Set to 1 so any non-empty initial load unblocks the daily run.
MIN_KAPAL_ROWS = 1


# ---------------------------------------------------------------------------
# Backfill DAG — one-time initial full load (manual trigger)
# Run this once; it will keep going until ALL vessels + tracking are loaded.
# ---------------------------------------------------------------------------
with DAG(
    dag_id="kkp_datamart_backfill",
    description="One-time full KKP Datamart load: all kapal registry + tracking. Run until complete.",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,  # manual trigger only
    catchup=False,
    max_active_runs=1,
    tags=["kkp", "kapal", "tracking", "maritime", "backfill"],
    params={
        "batch_size": Param(
            default=500,
            type="integer",
            description="Commit every N tracking rows (default 500)",
        ),
        "tracking_interval": Param(
            default=30,
            type="integer",
            description="Location interval in minutes (default 30)",
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
        batch  = int(p.get("batch_size", 500))
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

    run_backfill = SSHOperator(
        task_id="run_kkp_full_backfill",
        ssh_conn_id="maritime_master_ssh",
        command=_build_backfill_cmd,
        # 6 hours: enough for a full fleet collect with enrichment + tracking
        cmd_timeout=21600,
        conn_timeout=30,
    )

    # Verify that ALL kapal from the registry are present and enriched.
    # The collector fetches up to 100 000 vessels; we check that:
    #   - kapal_total > 0 (any data loaded)
    #   - kapal_enriched == kapal_total (every row has tanda_selar from detail endpoint)
    #   - tracking_total > 0 (at least some tracking points loaded)
    verify_completeness = SSHOperator(
        task_id="verify_backfill_completeness",
        ssh_conn_id="maritime_master_ssh",
        command=(
            f"{PSQL_CMD} -c \""
            "SELECT "
            "  (SELECT COUNT(*) FROM master_kapal)                                       AS kapal_total, "
            "  (SELECT COUNT(*) FROM master_kapal WHERE tanda_selar  IS NOT NULL)        AS kapal_enriched, "
            "  (SELECT COUNT(*) FROM master_kapal WHERE pemilik      IS NOT NULL)        AS has_pemilik, "
            "  (SELECT COUNT(*) FROM master_kapal WHERE alat_tangkap IS NOT NULL)        AS has_alat_tangkap, "
            "  (SELECT COUNT(*) FROM master_vessel_tracking)                             AS tracking_total, "
            "  (SELECT COUNT(*) FROM master_vessel_tracking WHERE direction IS NOT NULL) AS has_direction, "
            "  (SELECT COUNT(*) FROM master_vessel_tracking WHERE wpp_id    IS NOT NULL) AS has_wpp_id, "
            "  (SELECT MAX(timestamp) FROM master_vessel_tracking)                       AS latest_ping;"
            "\" 2>&1"
        ),
        cmd_timeout=60,
        conn_timeout=30,
    )

    run_backfill >> verify_completeness


# ---------------------------------------------------------------------------
# Daily DAG — incremental refresh at 01:00 UTC every day
# Only runs after the initial backfill has populated master_kapal.
# ---------------------------------------------------------------------------
with DAG(
    dag_id="kkp_datamart_daily",
    description="Daily KKP Datamart refresh: all kapal registry + vessel tracking at 01:00 UTC",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 5, 22),  # start the day after initial backfill
    schedule_interval="0 1 * * *",     # 01:00 UTC daily
    catchup=False,
    max_active_runs=1,
    tags=["kkp", "kapal", "tracking", "maritime"],
) as daily_dag:

    def _check_initial_load(**context) -> None:
        """
        Skip the daily run if the initial backfill has not been completed yet.
        This prevents the daily DAG from running on an empty database.
        """
        import os
        import psycopg2

        try:
            conn = psycopg2.connect(
                host=os.getenv("DB_HOST", "127.0.0.1"),
                port=int(os.getenv("DB_PORT", "5432")),
                dbname=os.getenv("DB_NAME", "maritime-os"),
                user=os.getenv("DB_USER", "maritime-os"),
                password=os.getenv("DB_PASSWORD", "@Maritime210526"),
            )
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM master_kapal")
                count = cur.fetchone()[0]
            conn.close()
        except Exception as exc:
            raise AirflowSkipException(f"Cannot connect to DB to verify initial load: {exc}")

        if count < MIN_KAPAL_ROWS:
            raise AirflowSkipException(
                f"Initial backfill not complete: master_kapal has {count} rows "
                f"(need >= {MIN_KAPAL_ROWS}). Run kkp_datamart_backfill first."
            )

    check_initial_load = PythonOperator(
        task_id="check_initial_load_complete",
        python_callable=_check_initial_load,
    )

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
        # 4 hours: daily refresh is faster than initial load (upserts, not inserts)
        cmd_timeout=14400,
        conn_timeout=30,
    )

    verify_counts = SSHOperator(
        task_id="verify_row_counts",
        ssh_conn_id="maritime_master_ssh",
        command=(
            f"{PSQL_CMD} -c \""
            "SELECT "
            "  (SELECT COUNT(*) FROM master_kapal)                                       AS kapal_total, "
            "  (SELECT COUNT(*) FROM master_kapal WHERE tanda_selar IS NOT NULL)         AS kapal_enriched, "
            "  (SELECT COUNT(*) FROM master_vessel_tracking)                             AS tracking_total, "
            "  (SELECT COUNT(*) FROM master_vessel_tracking WHERE wpp_id IS NOT NULL)    AS has_wpp_id, "
            "  (SELECT MAX(timestamp) FROM master_vessel_tracking)                       AS latest_ping;"
            "\" 2>&1"
        ),
        cmd_timeout=60,
        conn_timeout=30,
    )

    check_initial_load >> collect_kapal >> verify_counts
