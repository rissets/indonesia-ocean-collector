"""
Airflow DAG: Backfill master_oceanography dari 2021-01-01 sampai hari ini.

Strategi continuous batching:
- 3 task paralel: satu per source (ERDDAP, CMEMS, Open-Meteo)
- Setiap task loop internal: collect batch → upsert → langsung batch berikutnya
- Tidak ada jeda antar batch — selesai satu langsung mulai berikutnya
- Batch size: 1000 records per 30-hari window
- Idempoten: upsert ON CONFLICT, aman dijalankan ulang
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

BACKFILL_START = "2021-01-01"
BATCH_SIZE = 1000
RAW_DIR = str(_REPO_ROOT / "data" / "raw")


def _date_batches(start: str, end: str, days_per_batch: int = 30) -> list[tuple[str, str]]:
    """Split date range into consecutive windows of `days_per_batch` days."""
    batches = []
    cur = datetime.strptime(start, "%Y-%m-%d")
    end_dt = datetime.strptime(end, "%Y-%m-%d")
    while cur <= end_dt:
        batch_end = min(cur + timedelta(days=days_per_batch - 1), end_dt)
        batches.append((cur.strftime("%Y-%m-%d"), batch_end.strftime("%Y-%m-%d")))
        cur = batch_end + timedelta(days=1)
    return batches


def _backfill_erddap(**context) -> dict:
    """Continuous batch backfill from NOAA ERDDAP (SST + chlorophyll + SSH/currents)."""
    from dotenv import load_dotenv
    load_dotenv(_REPO_ROOT / ".env")

    from collectors import erddap_collector
    from db.writer import upsert_dataframe

    today = datetime.utcnow().strftime("%Y-%m-%d")
    batches = _date_batches(BACKFILL_START, today, days_per_batch=30)
    total = 0

    for batch_start, batch_end in batches:
        try:
            sst_df = erddap_collector.collect_sst(
                batch_start, batch_end, max_records=BATCH_SIZE, raw_dir=RAW_DIR, stride=2
            )
            chl_df = erddap_collector.collect_chlorophyll(
                batch_start, batch_end, max_records=BATCH_SIZE, raw_dir=RAW_DIR, stride=2
            )
            ssh_df = erddap_collector.collect_ssh_currents(
                batch_start, batch_end, max_records=BATCH_SIZE, raw_dir=RAW_DIR, stride=1
            )

            if sst_df.empty:
                logger.warning("ERDDAP SST empty for %s–%s, skipping", batch_start, batch_end)
                continue

            df = sst_df.rename(columns={"time": "tanggal", "sst_celsius": "sst"})
            df["suhu_permukaan"] = df["sst"]
            df["sumber_data"] = "NOAA_ERDDAP"

            if not chl_df.empty:
                chl = chl_df.rename(columns={"time": "tanggal", "chlorophyll_mgm3": "klorofil"})
                df = df.merge(
                    chl[["tanggal", "latitude", "longitude", "klorofil"]],
                    on=["tanggal", "latitude", "longitude"],
                    how="left",
                )

            if not ssh_df.empty:
                ssh = ssh_df.rename(columns={
                    "time": "tanggal",
                    "ssh_m": "ssh",
                    "u_current_ms": "arus_laut_u",
                    "v_current_ms": "arus_laut_v",
                })
                df = df.merge(
                    ssh[["tanggal", "latitude", "longitude", "ssh", "arus_laut_u", "arus_laut_v"]],
                    on=["tanggal", "latitude", "longitude"],
                    how="left",
                )

            n = upsert_dataframe(df, "NOAA_ERDDAP")
            total += n
            logger.info("ERDDAP batch %s–%s: %d rows upserted (total=%d)", batch_start, batch_end, n, total)

        except Exception as exc:
            logger.error("ERDDAP batch %s–%s failed: %s", batch_start, batch_end, exc)

    logger.info("ERDDAP backfill complete: %d total rows upserted", total)
    return {"source": "NOAA_ERDDAP", "total_rows": total}


def _backfill_cmems(**context) -> dict:
    """Continuous batch backfill from Copernicus Marine (CMEMS)."""
    from dotenv import load_dotenv
    load_dotenv(_REPO_ROOT / ".env")

    from collectors import cmems_collector
    from db.writer import upsert_dataframe

    today = datetime.utcnow().strftime("%Y-%m-%d")
    batches = _date_batches(BACKFILL_START, today, days_per_batch=30)
    total = 0

    for batch_start, batch_end in batches:
        try:
            df = cmems_collector.collect_all(
                batch_start, batch_end, max_records=BATCH_SIZE, raw_dir=RAW_DIR
            )
            if df.empty:
                logger.warning("CMEMS empty for %s–%s, skipping", batch_start, batch_end)
                continue

            df = df.rename(columns={
                "time": "tanggal",
                "sst_celsius": "sst",
                "chlorophyll_mgm3": "klorofil",
                "ssh_m": "ssh",
                "u_current_ms": "arus_laut_u",
                "v_current_ms": "arus_laut_v",
                "salinity_psu": "salinitas",
            })
            df["suhu_permukaan"] = df.get("sst", None)
            df["sumber_data"] = "CMEMS"

            n = upsert_dataframe(df, "CMEMS")
            total += n
            logger.info("CMEMS batch %s–%s: %d rows upserted (total=%d)", batch_start, batch_end, n, total)

        except Exception as exc:
            logger.error("CMEMS batch %s–%s failed: %s", batch_start, batch_end, exc)

    logger.info("CMEMS backfill complete: %d total rows upserted", total)
    return {"source": "CMEMS", "total_rows": total}


def _backfill_openmeteo(**context) -> dict:
    """Continuous batch backfill from Open-Meteo (waves + wind + radiation)."""
    from dotenv import load_dotenv
    load_dotenv(_REPO_ROOT / ".env")

    from collectors.openmeteo_collector import collect as openmeteo_collect
    from db.writer import upsert_dataframe

    today = datetime.utcnow().strftime("%Y-%m-%d")
    # Open-Meteo free tier: max 92-day window per request — use 30-day batches
    batches = _date_batches(BACKFILL_START, today, days_per_batch=30)
    total = 0

    for batch_start, batch_end in batches:
        try:
            df = openmeteo_collect(
                batch_start, batch_end, max_records=BATCH_SIZE, raw_dir=RAW_DIR
            )
            if df.empty:
                logger.warning("Open-Meteo empty for %s–%s, skipping", batch_start, batch_end)
                continue

            n = upsert_dataframe(df, "Open-Meteo")
            total += n
            logger.info("Open-Meteo batch %s–%s: %d rows upserted (total=%d)", batch_start, batch_end, n, total)

        except Exception as exc:
            logger.error("Open-Meteo batch %s–%s failed: %s", batch_start, batch_end, exc)

    logger.info("Open-Meteo backfill complete: %d total rows upserted", total)
    return {"source": "Open-Meteo", "total_rows": total}


default_args = {
    "owner": "data-engineer",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=12),
}

with DAG(
    dag_id="oceanography_backfill",
    description="Backfill master_oceanography 2021-01-01 s/d hari ini — continuous batching per source",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,  # triggered manually
    catchup=False,
    max_active_tasks=3,  # 3 sources run in parallel
    tags=["oceanography", "backfill", "maritime"],
) as dag:

    erddap_task = PythonOperator(
        task_id="backfill_erddap",
        python_callable=_backfill_erddap,
        doc_md="Continuous batch backfill NOAA ERDDAP → master_oceanography",
    )

    cmems_task = PythonOperator(
        task_id="backfill_cmems",
        python_callable=_backfill_cmems,
        doc_md="Continuous batch backfill CMEMS → master_oceanography",
    )

    openmeteo_task = PythonOperator(
        task_id="backfill_openmeteo",
        python_callable=_backfill_openmeteo,
        doc_md="Continuous batch backfill Open-Meteo → master_oceanography",
    )

    # All 3 sources run in parallel — no dependency between them
    [erddap_task, cmems_task, openmeteo_task]
