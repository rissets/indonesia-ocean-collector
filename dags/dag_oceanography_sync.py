"""
Airflow DAG: Sinkronisasi harian master_oceanography — setiap hari jam 01:00 UTC.

Mengambil data kemarin dari semua sumber (ERDDAP, CMEMS, Open-Meteo) dan
upsert ke master_oceanography. Idempoten — aman dijalankan ulang.
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

logger = logging.getLogger(__name__)

BATCH_SIZE = 1000
RAW_DIR = str(_REPO_ROOT / "data" / "raw")


def _sync_yesterday(**context) -> dict:
    """Collect yesterday's data from all sources and upsert into master_oceanography."""
    from dotenv import load_dotenv
    load_dotenv(_REPO_ROOT / ".env")

    from collectors import erddap_collector, cmems_collector
    from collectors.openmeteo_collector import collect as openmeteo_collect
    from db.writer import upsert_dataframe

    # Use Airflow logical date if available, otherwise yesterday UTC
    execution_date = context.get("logical_date") or context.get("execution_date")
    if execution_date:
        target_date = execution_date.strftime("%Y-%m-%d")
    else:
        target_date = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

    logger.info("Syncing data for date: %s", target_date)
    results = {"target_date": target_date}

    # ---- ERDDAP ----
    try:
        sst_df = erddap_collector.collect_sst(
            target_date, target_date, max_records=BATCH_SIZE, raw_dir=RAW_DIR, stride=2
        )
        chl_df = erddap_collector.collect_chlorophyll(
            target_date, target_date, max_records=BATCH_SIZE, raw_dir=RAW_DIR, stride=2
        )
        ssh_df = erddap_collector.collect_ssh_currents(
            target_date, target_date, max_records=BATCH_SIZE, raw_dir=RAW_DIR, stride=1
        )

        if not sst_df.empty:
            sst_df = sst_df.rename(columns={"time": "tanggal", "sst_celsius": "sst"})
            sst_df["suhu_permukaan"] = sst_df["sst"]
            sst_df["sumber_data"] = "NOAA_ERDDAP"

            if not chl_df.empty:
                chl_df = chl_df.rename(columns={"time": "tanggal", "chlorophyll_mgm3": "klorofil"})
                sst_df = sst_df.merge(
                    chl_df[["tanggal", "latitude", "longitude", "klorofil"]],
                    on=["tanggal", "latitude", "longitude"],
                    how="left",
                )
            if not ssh_df.empty:
                ssh_df = ssh_df.rename(columns={
                    "time": "tanggal",
                    "ssh_m": "ssh",
                    "u_current_ms": "arus_laut_u",
                    "v_current_ms": "arus_laut_v",
                })
                sst_df = sst_df.merge(
                    ssh_df[["tanggal", "latitude", "longitude", "ssh", "arus_laut_u", "arus_laut_v"]],
                    on=["tanggal", "latitude", "longitude"],
                    how="left",
                )
            n = upsert_dataframe(sst_df, "NOAA_ERDDAP")
            results["erddap_rows"] = n
    except Exception as exc:
        logger.error("ERDDAP sync failed for %s: %s", target_date, exc)
        results["erddap_error"] = str(exc)

    # ---- CMEMS ----
    try:
        cmems_df = cmems_collector.collect_all(
            target_date, target_date, max_records=BATCH_SIZE, raw_dir=RAW_DIR
        )
        if not cmems_df.empty:
            cmems_df = cmems_df.rename(columns={
                "time": "tanggal",
                "sst_celsius": "sst",
                "chlorophyll_mgm3": "klorofil",
                "ssh_m": "ssh",
                "u_current_ms": "arus_laut_u",
                "v_current_ms": "arus_laut_v",
            })
            cmems_df["suhu_permukaan"] = cmems_df.get("sst", None)
            cmems_df["sumber_data"] = "CMEMS"
            n = upsert_dataframe(cmems_df, "CMEMS")
            results["cmems_rows"] = n
    except Exception as exc:
        logger.error("CMEMS sync failed for %s: %s", target_date, exc)
        results["cmems_error"] = str(exc)

    # ---- Open-Meteo ----
    try:
        om_df = openmeteo_collect(
            target_date, target_date, max_records=BATCH_SIZE, raw_dir=RAW_DIR
        )
        if not om_df.empty:
            n = upsert_dataframe(om_df, "Open-Meteo")
            results["openmeteo_rows"] = n
    except Exception as exc:
        logger.error("Open-Meteo sync failed for %s: %s", target_date, exc)
        results["openmeteo_error"] = str(exc)

    logger.info("Sync complete for %s: %s", target_date, results)
    return results


default_args = {
    "owner": "data-engineer",
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=1),
    "email_on_failure": False,
}

with DAG(
    dag_id="oceanography_sync",
    description="Sinkronisasi harian master_oceanography — setiap hari jam 01:00 UTC",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="0 1 * * *",  # setiap hari jam 01:00 UTC
    catchup=False,
    max_active_runs=1,
    tags=["oceanography", "sync", "maritime"],
) as dag:

    sync_task = PythonOperator(
        task_id="sync_yesterday",
        python_callable=_sync_yesterday,
        doc_md=(
            "Ambil data kemarin dari ERDDAP + CMEMS + Open-Meteo "
            "dan upsert ke master_oceanography (batch=1000)."
        ),
    )
