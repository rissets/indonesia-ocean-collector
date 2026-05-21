"""
Airflow DAG: Backfill master_oceanography dari 2021-01-01 sampai hari ini.

Strategi:
- Task pertama: hapus semua data di luar WPP dari master_oceanography
- Satu task per bulan (2021-01 s/d bulan berjalan), serial
- Setiap bulan: collect ERDDAP + CMEMS + Open-Meteo, filter WPP, upsert
- Batch size 1000 records per source per bulan
- Idempoten: upsert ON CONFLICT, aman dijalankan ulang
- Semua kolom diisi: ssh, klorofil, arus, gelombang, angin, radiasi,
  cuaca, kedalaman_laut, pasang_surut, periode_gelombang, jarak_padang
"""

from __future__ import annotations

import logging
import math
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

BACKFILL_START = "2021-01-01"
BATCH_SIZE = 1000
RAW_DIR = str(_REPO_ROOT / "data" / "raw")


def _month_list(start: str, end: str) -> list[tuple[str, str]]:
    months = []
    cur = datetime.strptime(start, "%Y-%m-%d").replace(day=1)
    end_dt = datetime.strptime(end, "%Y-%m-%d").replace(day=1)
    while cur <= end_dt:
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


def _compute_jarak_padang(df):
    """
    Compute jarak_padang: approximate distance (km) to nearest known seagrass
    (padang lamun) area in Indonesian waters.

    Uses a curated list of major seagrass hotspots. Distance computed via
    Haversine formula. Returns the DataFrame with 'jarak_padang' column added.
    """
    import numpy as np

    # Major seagrass (padang lamun) hotspots in Indonesia (lat, lon)
    SEAGRASS_HOTSPOTS = [
        (-8.72, 115.17),   # Bali
        (-5.15, 119.45),   # Spermonde Archipelago, Sulawesi
        (-0.90, 134.90),   # Teluk Cendrawasih, Papua
        (-8.50, 140.40),   # Merauke, Papua
        (-3.80, 128.20),   # Banda Sea
        (1.00, 104.00),    # Riau Islands
        (-2.50, 107.50),   # Bangka-Belitung
        (-7.00, 112.70),   # East Java coast
        (-8.30, 122.50),   # Flores
        (0.50, 127.50),    # North Maluku
        (-4.00, 122.60),   # Southeast Sulawesi
        (-1.50, 136.00),   # Biak, Papua
        (-6.10, 106.80),   # Jakarta Bay
        (3.80, 108.20),    # Natuna Sea
        (-9.50, 119.50),   # Sumba
        (-10.20, 123.60),  # Timor
    ]

    hotspots = np.array(SEAGRASS_HOTSPOTS)
    lats = df["latitude"].values
    lons = df["longitude"].values

    R = 6371.0  # Earth radius km
    min_dists = []
    for lat, lon in zip(lats, lons):
        dlat = np.radians(hotspots[:, 0] - lat)
        dlon = np.radians(hotspots[:, 1] - lon)
        a = np.sin(dlat / 2) ** 2 + np.cos(np.radians(lat)) * np.cos(np.radians(hotspots[:, 0])) * np.sin(dlon / 2) ** 2
        dists = 2 * R * np.arcsin(np.sqrt(a))
        min_dists.append(round(float(dists.min()), 3))

    df = df.copy()
    df["jarak_padang"] = min_dists
    return df


def _delete_outside_wpp(**context) -> dict:
    """Delete all rows from master_oceanography that fall outside WPP regions."""
    from dotenv import load_dotenv
    load_dotenv(_REPO_ROOT / ".env")

    from collectors.wpp_filter import delete_outside_wpp_sql
    from db.writer import _get_conn

    sql = delete_outside_wpp_sql()
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                deleted = cur.rowcount
        logger.info("Deleted %d rows outside WPP from master_oceanography.", deleted)
        return {"deleted_outside_wpp": deleted}
    finally:
        conn.close()


def _collect_and_upsert_month(month_start: str, month_end: str, **context) -> dict:
    """Collect all sources for one month, filter to WPP, upsert into master_oceanography."""
    from dotenv import load_dotenv
    load_dotenv(_REPO_ROOT / ".env")

    import pandas as pd
    from collectors import erddap_collector, cmems_collector
    from collectors.openmeteo_collector import collect as openmeteo_collect
    from collectors.wpp_filter import filter_wpp
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

            sst_df = filter_wpp(sst_df)
            sst_df = _compute_jarak_padang(sst_df)
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
            cmems_df = filter_wpp(cmems_df)
            cmems_df = _compute_jarak_padang(cmems_df)
            n = upsert_dataframe(cmems_df, "CMEMS")
            results["cmems"] = n
            logger.info("CMEMS %s–%s: %d rows upserted", month_start, month_end, n)
    except Exception as exc:
        logger.error("CMEMS failed for %s–%s: %s", month_start, month_end, exc)
        results["cmems_error"] = str(exc)

    # ---- Open-Meteo ----
    try:
        om_df = openmeteo_collect(
            month_start, month_end, max_records=BATCH_SIZE, raw_dir=RAW_DIR, wpp_only=True
        )
        if not om_df.empty:
            om_df = _compute_jarak_padang(om_df)
            n = upsert_dataframe(om_df, "Open-Meteo")
            results["openmeteo"] = n
            logger.info("Open-Meteo %s–%s: %d rows upserted", month_start, month_end, n)
    except Exception as exc:
        logger.error("Open-Meteo failed for %s–%s: %s", month_start, month_end, exc)
        results["openmeteo_error"] = str(exc)

    return results


# ---------------------------------------------------------------------------
# Build DAG
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
    description="Backfill master_oceanography 2021-01-01 s/d hari ini — semua kolom terisi, WPP only",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    max_active_tasks=4,
    tags=["oceanography", "backfill", "maritime"],
) as dag:

    delete_task = PythonOperator(
        task_id="delete_outside_wpp",
        python_callable=_delete_outside_wpp,
        doc_md="Hapus semua data di luar WPP dari master_oceanography sebelum backfill.",
    )

    prev_task = delete_task
    for month_start, month_end in months:
        task_id = f"collect_{month_start[:7].replace('-', '_')}"

        task = PythonOperator(
            task_id=task_id,
            python_callable=_collect_and_upsert_month,
            op_kwargs={"month_start": month_start, "month_end": month_end},
            doc_md=f"Collect & upsert {month_start} → {month_end} (WPP only, batch={BATCH_SIZE})",
        )

        prev_task >> task
        prev_task = task
