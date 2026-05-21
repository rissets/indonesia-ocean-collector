"""
Airflow DAG: Backfill master_oceanography dari 2021-01-01 sampai hari ini.

Strategi:
- Satu TaskGroup per bulan (2021-01 s/d bulan berjalan)
- Setiap bulan: collect ERDDAP + CMEMS + Open-Meteo, upsert ke master_oceanography
- Batch size 1000 records per source per bulan
- Idempoten: upsert ON CONFLICT, aman dijalankan ulang
- Paralel: max_active_tasks=4 agar tidak membebani API

Jalankan sekali untuk backfill historis. Setelah selesai, dag_oceanography_sync.py
mengambil alih untuk sinkronisasi harian.
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Tambahkan root repo ke sys.path agar bisa import collectors/db
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

logger = logging.getLogger(__name__)

BACKFILL_START = "2021-01-01"
BATCH_SIZE = 1000
RAW_DIR = str(_REPO_ROOT / "data" / "raw")


def _month_list(start: str, end: str) -> list[tuple[str, str]]:
    """Return list of (month_start, month_end) tuples from start to end inclusive."""
    months = []
    cur = datetime.strptime(start, "%Y-%m-%d").replace(day=1)
    end_dt = datetime.strptime(end, "%Y-%m-%d").replace(day=1)
    while cur <= end_dt:
        # last day of month
        if cur.month == 12:
            last = cur.replace(day=31)
        else:
            last = (cur.replace(month=cur.month + 1, day=1) - timedelta(days=1))
        months.append((cur.strftime("%Y-%m-%d"), last.strftime("%Y-%m-%d")))
        if cur.month == 12:
            cur = cur.replace(year=cur.year + 1, month=1)
        else:
            cur = cur.replace(month=cur.month + 1)
    return months


def _collect_and_upsert_month(month_start: str, month_end: str, **context) -> dict:
    """Collect all sources for one month and upsert into master_oceanography."""
    from dotenv import load_dotenv
    load_dotenv(_REPO_ROOT / ".env")

    from collectors import erddap_collector, cmems_collector
    from collectors.openmeteo_collector import collect as openmeteo_collect
    from db.writer import upsert_dataframe

    results = {}

    # ---- ERDDAP ----
    try:
        sst_df = erddap_collector.collect_sst(
            month_start, month_end, max_records=BATCH_SIZE, raw_dir=RAW_DIR, stride=2
        )
        chl_df = erddap_collector.collect_chlorophyll(
            month_start, month_end, max_records=BATCH_SIZE, raw_dir=RAW_DIR, stride=2
        )
        ssh_df = erddap_collector.collect_ssh_currents(
            month_start, month_end, max_records=BATCH_SIZE, raw_dir=RAW_DIR, stride=1
        )

        import pandas as pd
        import math

        # Normalise ERDDAP SST
        if not sst_df.empty:
            sst_df = sst_df.rename(columns={"time": "tanggal", "sst_celsius": "sst"})
            sst_df["suhu_permukaan"] = sst_df["sst"]
            sst_df["sumber_data"] = "NOAA_ERDDAP"
            # Merge chlorophyll into SST frame where possible
            if not chl_df.empty:
                chl_df = chl_df.rename(columns={"time": "tanggal", "chlorophyll_mgm3": "klorofil"})
                sst_df = sst_df.merge(
                    chl_df[["tanggal", "latitude", "longitude", "klorofil"]],
                    on=["tanggal", "latitude", "longitude"],
                    how="left",
                )
            # Merge SSH/currents
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
            results["erddap"] = n
            logger.info("ERDDAP %s–%s: %d rows upserted", month_start, month_end, n)
    except Exception as exc:
        logger.error("ERDDAP failed for %s–%s: %s", month_start, month_end, exc)
        results["erddap_error"] = str(exc)

    # ---- CMEMS ----
    try:
        cmems_df = cmems_collector.collect_all(
            month_start, month_end, max_records=BATCH_SIZE, raw_dir=RAW_DIR
        )
        if not cmems_df.empty:
            cmems_df = cmems_df.rename(columns={
                "time": "tanggal",
                "sst_celsius": "sst",
                "chlorophyll_mgm3": "klorofil",
                "ssh_m": "ssh",
                "u_current_ms": "arus_laut_u",
                "v_current_ms": "arus_laut_v",
                "salinity_psu": "salinitas",
            })
            cmems_df["suhu_permukaan"] = cmems_df.get("sst", None)
            cmems_df["sumber_data"] = "CMEMS"
            n = upsert_dataframe(cmems_df, "CMEMS")
            results["cmems"] = n
            logger.info("CMEMS %s–%s: %d rows upserted", month_start, month_end, n)
    except Exception as exc:
        logger.error("CMEMS failed for %s–%s: %s", month_start, month_end, exc)
        results["cmems_error"] = str(exc)

    # ---- Open-Meteo ----
    try:
        om_df = openmeteo_collect(
            month_start, month_end, max_records=BATCH_SIZE, raw_dir=RAW_DIR
        )
        if not om_df.empty:
            n = upsert_dataframe(om_df, "Open-Meteo")
            results["openmeteo"] = n
            logger.info("Open-Meteo %s–%s: %d rows upserted", month_start, month_end, n)
    except Exception as exc:
        logger.error("Open-Meteo failed for %s–%s: %s", month_start, month_end, exc)
        results["openmeteo_error"] = str(exc)

    return results


# ---------------------------------------------------------------------------
# Build DAG — one task per month
# ---------------------------------------------------------------------------

today = datetime.utcnow().strftime("%Y-%m-%d")
months = _month_list(BACKFILL_START, today)

default_args = {
    "owner": "data-engineer",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="oceanography_backfill",
    description="Backfill master_oceanography dari 2021-01-01 sampai hari ini (batch 1000/bulan)",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,  # triggered manually — bukan recurring
    catchup=False,
    max_active_tasks=4,
    tags=["oceanography", "backfill", "maritime"],
) as dag:

    prev_task = None
    for month_start, month_end in months:
        task_id = f"collect_{month_start[:7].replace('-', '_')}"

        task = PythonOperator(
            task_id=task_id,
            python_callable=_collect_and_upsert_month,
            op_kwargs={"month_start": month_start, "month_end": month_end},
            doc_md=f"Collect & upsert {month_start} → {month_end} (batch={BATCH_SIZE})",
        )

        # Serial execution: setiap bulan menunggu bulan sebelumnya selesai
        # agar tidak membebani API secara bersamaan
        if prev_task is not None:
            prev_task >> task
        prev_task = task
