"""
Airflow DAG: KKP Datamart Master Kapal Collector

Runs vessel enrichment from KKP Insight Datamart into `master_kapal`.
The daily DAG only enriches rows with placeholder/missing values; the manual
backfill DAG can refresh the full search index.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "maritime-data",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

COLLECTOR_DIR = "/home/rissets/indonesia-ocean-collector"
VENV_PYTHON = f"{COLLECTOR_DIR}/.venv/bin/python"
ENV_PREFIX = f"set -a && . {COLLECTOR_DIR}/.env && set +a"
TRACKING_ENV = "KKP_TRACKING_TIMEOUT=8 KKP_TRACKING_RETRIES=1 KKP_TRACKING_CHUNK_TIMEOUT=45"
TRACKING_LOCK = "/tmp/kkp_vessel_tracking.lock"


with DAG(
    dag_id="kkp_datamart_kapal_enrichment_daily",
    description="Enrich master_kapal placeholder fields from KKP Insight Datamart",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 5, 21),
    schedule_interval="30 2 * * *",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    tags=["kkp", "datamart", "kapal", "master"],
) as daily_dag:

    enrich_missing_kapal = BashOperator(
        task_id="enrich_missing_master_kapal",
        bash_command=(
            f"cd {COLLECTOR_DIR} && "
            f"{ENV_PREFIX} && "
            f"{VENV_PYTHON} -m collectors.kkp_datamart_collector "
            "--only-missing "
            "--batch-size 50 "
            "--sleep 0.03 "
            "--log-level INFO "
            "2>&1"
        ),
        execution_timeout=timedelta(hours=4),
    )

    verify_master_kapal_quality = BashOperator(
        task_id="verify_master_kapal_quality",
        bash_command=(
            f"{ENV_PREFIX} && "
            "PGPASSWORD=\"$DB_PASSWORD\" psql -h \"$DB_HOST\" -p \"$DB_PORT\" "
            "-U \"$DB_USER\" -d \"$DB_NAME\" -c \""
            "SELECT COUNT(*) total, "
            "COUNT(*) FILTER (WHERE NULLIF(TRIM(tanda_selar),'') IS NOT NULL AND TRIM(tanda_selar) <> '-') tanda_ok, "
            "COUNT(*) FILTER (WHERE ukuran_kapal IS NOT NULL AND ukuran_kapal > 0) ukuran_ok, "
            "COUNT(*) FILTER (WHERE NULLIF(TRIM(pemilik),'') IS NOT NULL AND TRIM(pemilik) <> '-') pemilik_ok, "
            "COUNT(*) FILTER (WHERE NULLIF(TRIM(alat_tangkap),'') IS NOT NULL AND TRIM(alat_tangkap) <> '-') alat_ok, "
            "COUNT(*) FILTER (WHERE kekuatan_mesin IS NOT NULL AND kekuatan_mesin > 0) mesin_ok, "
            "COUNT(*) FILTER (WHERE NULLIF(TRIM(wilayah_tangkap),'') IS NOT NULL AND TRIM(wilayah_tangkap) <> '-') wilayah_ok, "
            "COUNT(*) FILTER (WHERE NULLIF(TRIM(pelabuhan_pangkalan),'') IS NOT NULL AND TRIM(pelabuhan_pangkalan) <> '-') pelabuhan_ok "
            "FROM master_kapal;"
            "\" 2>&1"
        ),
        execution_timeout=timedelta(minutes=2),
    )

    enrich_missing_kapal >> verify_master_kapal_quality


with DAG(
    dag_id="kkp_datamart_kapal_refresh",
    description="Manual refresh of KKP Datamart vessel index/detail into master_kapal",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 5, 21),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["kkp", "datamart", "kapal", "backfill"],
    params={
        "limit": Param(default=100000, type="integer", description="Search endpoint limit"),
        "max_vessels": Param(default=0, type="integer", description="0 means no cap"),
        "only_missing": Param(default=False, type="boolean", description="Only rows with placeholder values"),
        "sleep": Param(default=0.05, type="number", description="Delay between API detail calls"),
    },
) as refresh_dag:

    refresh_kapal = BashOperator(
        task_id="refresh_master_kapal",
        bash_command=(
            f"cd {COLLECTOR_DIR} && "
            f"{ENV_PREFIX} && "
            "MAX_VESSELS=\"{{ params.max_vessels }}\"; "
            "ONLY_MISSING=\"{{ params.only_missing }}\"; "
            "CMD=\""
            f"{VENV_PYTHON} -m collectors.kkp_datamart_collector "
            "--limit {{ params.limit }} "
            "--sleep {{ params.sleep }} "
            "--log-level INFO\"; "
            "if [ \"$ONLY_MISSING\" = \"True\" ] || [ \"$ONLY_MISSING\" = \"true\" ]; then CMD=\"$CMD --only-missing\"; fi; "
            "if [ \"$MAX_VESSELS\" != \"0\" ]; then CMD=\"$CMD --max-vessels $MAX_VESSELS\"; fi; "
            "$CMD 2>&1"
        ),
        execution_timeout=timedelta(hours=4),
    )


with DAG(
    dag_id="kkp_vessel_tracking_sync_daily",
    description="Daily sync of vessel tracking from KKP Datamart into master_vessel_tracking",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 5, 22),
    schedule_interval="15 1 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["kkp", "datamart", "vessel", "tracking", "daily"],
) as tracking_daily_dag:

    sync_tracking_daily = BashOperator(
        task_id="sync_tracking_daily",
        bash_command=(
            f"cd {COLLECTOR_DIR} && "
            f"{ENV_PREFIX} && "
            "CMD=\""
            f"{TRACKING_ENV} "
            f"{VENV_PYTHON} -m collectors.kkp_datamart_collector "
            "--tracking-only-from-master "
            "--no-enrich --no-license-enrich "
            "--tracking-intervals 1440 "
            "--batch-size 10 "
            "--workers 1 "
            "--sleep 0 "
            "--log-level INFO\"; "
            f"flock {TRACKING_LOCK} bash -lc \"$CMD\" 2>&1"
        ),
        execution_timeout=timedelta(hours=4),
    )


with DAG(
    dag_id="kkp_vessel_tracking_backfill",
    description="Manual tracking backfill using wide KKP intervals to fetch earliest available history",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 5, 22),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    params={
        "tracking_intervals": Param(
            default="525600",
            type="string",
            description="Comma-separated intervals in minutes. 525600 is the widest KKP interval observed to respond reliably.",
        ),
        "max_vessels": Param(default=0, type="integer", description="0 means no cap"),
        "sleep": Param(default=0.01, type="number", description="Delay per vessel in seconds"),
    },
    tags=["kkp", "datamart", "vessel", "tracking", "backfill"],
) as tracking_backfill_dag:

    for shard_index in range(2):
        BashOperator(
            task_id=f"backfill_tracking_shard_{shard_index}",
            bash_command=(
                f"cd {COLLECTOR_DIR} && "
                f"{ENV_PREFIX} && "
                "MAX_VESSELS=\"{{ params.max_vessels }}\"; "
                "CMD=\""
                f"{TRACKING_ENV} "
                f"{VENV_PYTHON} -m collectors.kkp_datamart_collector "
                "--tracking-only-from-master "
                "--no-enrich --no-license-enrich "
                "--tracking-intervals '{{ params.tracking_intervals }}' "
                "--batch-size 10 "
                "--workers 1 "
                f"--shard-index {shard_index} "
                "--shard-count 2 "
                "--sleep {{ params.sleep }} "
                "--log-level INFO\"; "
                "if [ \"$MAX_VESSELS\" != \"0\" ]; then CMD=\"$CMD --max-vessels $MAX_VESSELS\"; fi; "
                "bash -lc \"$CMD\" 2>&1"
            ),
            execution_timeout=timedelta(hours=24),
        )
