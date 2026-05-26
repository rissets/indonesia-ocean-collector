"""
Airflow DAG: Backfill master_oceanography dari 2021-01-01 sampai hari ini.

Strategi:
- Task pertama: hapus semua data di luar WPP dari master_oceanography
- Satu task per bulan (2021-01 s/d bulan berjalan), serial
- Setiap bulan: collect ERDDAP + CMEMS + Open-Meteo, filter WPP, upsert
- Batch size 5000 records per source per bulan
- Idempoten: upsert ON CONFLICT, aman dijalankan ulang
- Semua kolom diisi: ssh, klorofil, arus, gelombang, angin, radiasi,
  cuaca, kedalaman_laut, pasang_surut, periode_gelombang, jarak_padang
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

_REPO_ROOT = Path(os.environ.get("OCEAN_COLLECTOR_ROOT", "/home/rissets/indonesia-ocean-collector"))
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

logger = logging.getLogger(__name__)

BACKFILL_START = "2021-01-01"
BATCH_SIZE = 2500
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


def _enrich_month_window(month_start: str, month_end: str) -> int:
    """
    Enrich sparse rows by sharing available values across sources on the same
    date and nearby grid cell (rounded 0.1 degrees).
    """
    from db.writer import _get_conn

    # Pass-1: fill from same-day, same 0.5-degree cell.
    day_sql = """
    WITH day_bucket AS (
        SELECT
            tanggal,
            round(latitude::numeric * 2) / 2  AS lat_key,
            round(longitude::numeric * 2) / 2 AS lon_key,
            max(suhu_permukaan) FILTER (WHERE suhu_permukaan IS NOT NULL) AS suhu_permukaan,
            max(sst) FILTER (WHERE sst IS NOT NULL) AS sst,
            max(ssh) FILTER (WHERE ssh IS NOT NULL) AS ssh,
            max(klorofil) FILTER (WHERE klorofil IS NOT NULL) AS klorofil,
            max(arus_laut_u) FILTER (WHERE arus_laut_u IS NOT NULL) AS arus_laut_u,
            max(arus_laut_v) FILTER (WHERE arus_laut_v IS NOT NULL) AS arus_laut_v,
            max(kecepatan_arus) FILTER (WHERE kecepatan_arus IS NOT NULL) AS kecepatan_arus,
            max(tinggi_gelombang) FILTER (WHERE tinggi_gelombang IS NOT NULL) AS tinggi_gelombang,
            max(periode_gelombang) FILTER (WHERE periode_gelombang IS NOT NULL) AS periode_gelombang,
            max(kecepatan_angin) FILTER (WHERE kecepatan_angin IS NOT NULL) AS kecepatan_angin,
            max(arah_angin) FILTER (WHERE arah_angin IS NOT NULL) AS arah_angin,
            max(radiasi_matahari) FILTER (WHERE radiasi_matahari IS NOT NULL) AS radiasi_matahari,
            max(kedalaman_laut) FILTER (WHERE kedalaman_laut IS NOT NULL) AS kedalaman_laut,
            max(jarak_padang) FILTER (WHERE jarak_padang IS NOT NULL) AS jarak_padang,
            max(pasang_surut) FILTER (WHERE pasang_surut IS NOT NULL) AS pasang_surut,
            max(cuaca) FILTER (WHERE cuaca IS NOT NULL) AS cuaca
        FROM master_oceanography
        WHERE tanggal BETWEEN %(start)s AND %(end)s
        GROUP BY 1, 2, 3
    )
    UPDATE master_oceanography t
    SET
        suhu_permukaan   = COALESCE(t.suhu_permukaan, b.suhu_permukaan),
        sst              = COALESCE(t.sst, b.sst),
        ssh              = COALESCE(t.ssh, b.ssh),
        klorofil         = COALESCE(t.klorofil, b.klorofil),
        arus_laut_u      = COALESCE(t.arus_laut_u, b.arus_laut_u),
        arus_laut_v      = COALESCE(t.arus_laut_v, b.arus_laut_v),
        kecepatan_arus   = COALESCE(
            t.kecepatan_arus,
            b.kecepatan_arus,
            CASE
                WHEN COALESCE(t.arus_laut_u, b.arus_laut_u) IS NOT NULL
                 AND COALESCE(t.arus_laut_v, b.arus_laut_v) IS NOT NULL
                THEN sqrt(
                    power(COALESCE(t.arus_laut_u, b.arus_laut_u), 2) +
                    power(COALESCE(t.arus_laut_v, b.arus_laut_v), 2)
                )
                ELSE NULL
            END
        ),
        tinggi_gelombang = COALESCE(t.tinggi_gelombang, b.tinggi_gelombang),
        periode_gelombang= COALESCE(t.periode_gelombang, b.periode_gelombang),
        kecepatan_angin  = COALESCE(t.kecepatan_angin, b.kecepatan_angin),
        arah_angin       = COALESCE(t.arah_angin, b.arah_angin),
        radiasi_matahari = COALESCE(t.radiasi_matahari, b.radiasi_matahari),
        kedalaman_laut   = COALESCE(t.kedalaman_laut, b.kedalaman_laut),
        jarak_padang     = COALESCE(t.jarak_padang, b.jarak_padang),
        pasang_surut     = COALESCE(t.pasang_surut, b.pasang_surut, t.ssh, b.ssh),
        cuaca            = COALESCE(t.cuaca, b.cuaca),
        updated_at       = NOW()
    FROM day_bucket b
    WHERE t.tanggal = b.tanggal
      AND round(t.latitude::numeric * 2) / 2 = b.lat_key
      AND round(t.longitude::numeric * 2) / 2 = b.lon_key
      AND t.tanggal BETWEEN %(start)s AND %(end)s
      AND (
          t.suhu_permukaan IS NULL OR t.sst IS NULL OR t.ssh IS NULL OR t.klorofil IS NULL
          OR t.arus_laut_u IS NULL OR t.arus_laut_v IS NULL OR t.kecepatan_arus IS NULL
          OR t.tinggi_gelombang IS NULL OR t.periode_gelombang IS NULL
          OR t.kecepatan_angin IS NULL OR t.arah_angin IS NULL
          OR t.radiasi_matahari IS NULL OR t.kedalaman_laut IS NULL
          OR t.jarak_padang IS NULL OR t.pasang_surut IS NULL OR t.cuaca IS NULL
      )
    """

    # Pass-2: propagate monthly variables (SST/chlorophyll/current) to all days
    # in the same month and 0.5-degree cell.
    month_sql = """
    WITH month_bucket AS (
        SELECT
            date_trunc('month', tanggal)::date AS month_key,
            round(latitude::numeric * 2) / 2  AS lat_key,
            round(longitude::numeric * 2) / 2 AS lon_key,
            max(suhu_permukaan) FILTER (WHERE suhu_permukaan IS NOT NULL) AS suhu_permukaan,
            max(sst) FILTER (WHERE sst IS NOT NULL) AS sst,
            max(ssh) FILTER (WHERE ssh IS NOT NULL) AS ssh,
            max(klorofil) FILTER (WHERE klorofil IS NOT NULL) AS klorofil,
            max(arus_laut_u) FILTER (WHERE arus_laut_u IS NOT NULL) AS arus_laut_u,
            max(arus_laut_v) FILTER (WHERE arus_laut_v IS NOT NULL) AS arus_laut_v,
            max(kecepatan_arus) FILTER (WHERE kecepatan_arus IS NOT NULL) AS kecepatan_arus
        FROM master_oceanography
        WHERE tanggal BETWEEN %(start)s AND %(end)s
        GROUP BY 1, 2, 3
    )
    UPDATE master_oceanography t
    SET
        suhu_permukaan = COALESCE(t.suhu_permukaan, m.suhu_permukaan),
        sst            = COALESCE(t.sst, m.sst),
        ssh            = COALESCE(t.ssh, m.ssh),
        klorofil       = COALESCE(t.klorofil, m.klorofil),
        arus_laut_u    = COALESCE(t.arus_laut_u, m.arus_laut_u),
        arus_laut_v    = COALESCE(t.arus_laut_v, m.arus_laut_v),
        kecepatan_arus = COALESCE(
            t.kecepatan_arus,
            m.kecepatan_arus,
            CASE
                WHEN COALESCE(t.arus_laut_u, m.arus_laut_u) IS NOT NULL
                 AND COALESCE(t.arus_laut_v, m.arus_laut_v) IS NOT NULL
                THEN sqrt(
                    power(COALESCE(t.arus_laut_u, m.arus_laut_u), 2) +
                    power(COALESCE(t.arus_laut_v, m.arus_laut_v), 2)
                )
                ELSE NULL
            END
        ),
        pasang_surut   = COALESCE(t.pasang_surut, t.ssh, m.ssh),
        updated_at     = NOW()
    FROM month_bucket m
    WHERE date_trunc('month', t.tanggal)::date = m.month_key
      AND round(t.latitude::numeric * 2) / 2 = m.lat_key
      AND round(t.longitude::numeric * 2) / 2 = m.lon_key
      AND t.tanggal BETWEEN %(start)s AND %(end)s
      AND (
          t.suhu_permukaan IS NULL OR t.sst IS NULL OR t.ssh IS NULL OR t.klorofil IS NULL
          OR t.arus_laut_u IS NULL OR t.arus_laut_v IS NULL OR t.kecepatan_arus IS NULL
          OR t.pasang_surut IS NULL
      )
    """

    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(day_sql, {"start": month_start, "end": month_end})
                day_updated = max(cur.rowcount, 0)
                cur.execute(month_sql, {"start": month_start, "end": month_end})
                month_updated = max(cur.rowcount, 0)
                return day_updated + month_updated
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
            month_start, month_end, max_records=max(1000, BATCH_SIZE // 2), raw_dir=RAW_DIR
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

    try:
        enriched = _enrich_month_window(month_start, month_end)
        results["enriched"] = enriched
        logger.info("Enriched %s–%s rows: %d", month_start, month_end, enriched)
    except Exception as exc:
        logger.error("Enrichment failed for %s–%s: %s", month_start, month_end, exc)
        results["enrich_error"] = str(exc)

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
    "execution_timeout": timedelta(hours=4),
}

with DAG(
    dag_id="oceanography_backfill",
    description="Backfill master_oceanography 2021-01-01 s/d hari ini — semua kolom terisi, WPP only",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    max_active_tasks=1,
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
