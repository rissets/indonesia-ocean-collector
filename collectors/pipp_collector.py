"""
PIPP (Pusat Informasi Pelabuhan Perikanan) Tangkapan Collector.

Fetches daily catch/landing data per vessel from the PIPP API and upserts
into the master_tangkapan_pipp PostgreSQL table.

Usage:
    python -m collectors.pipp_collector --start 2026-01-01 --end 2026-05-20
    python -m collectors.pipp_collector --days 30
"""

from __future__ import annotations

import argparse
import logging
import os
import time
from datetime import date, datetime, timedelta
from typing import Any

import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

PIPP_API_URL = (
    "https://pipp.kkp.go.id/pnbp-sdap/api.php"
    "?action=produksi_per_kapal_plus_harga_new4"
    "&pass=timon@20220307"
    "&start={date}"
    "&end={date}"
)

SOURCE_NAME = "PIPP"
MAX_RETRIES = 3
RETRY_BACKOFF = 2.0  # seconds, doubles each retry


def _get_db_conn() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "217.217.252.155"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "maritime-os"),
        user=os.getenv("DB_USER", "maritime-os"),
        password=os.getenv("DB_PASSWORD", "@Maritime210526"),
        connect_timeout=10,
    )


def _fetch_pipp_day(day: str) -> list[dict[str, Any]]:
    """Fetch PIPP data for a single date (YYYY-MM-DD). Returns list of activity records."""
    url = PIPP_API_URL.format(date=day)
    delay = RETRY_BACKOFF
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            payload = resp.json()
            if payload.get("status") != "sukses":
                logger.warning("PIPP non-sukses for %s: %s", day, payload.get("error"))
                return []
            return payload.get("data") or []
        except (requests.RequestException, ValueError) as exc:
            logger.warning("PIPP fetch attempt %d/%d for %s failed: %s", attempt, MAX_RETRIES, day, exc)
            if attempt < MAX_RETRIES:
                time.sleep(delay)
                delay *= 2
    logger.error("PIPP fetch exhausted retries for %s", day)
    return []


def _map_wpp(wpp_str: str | None) -> str | None:
    """Normalise free-text WPP to canonical WPP-7xx code."""
    if not wpp_str:
        return None
    mapping = {
        "laut jawa": "WPP-712",
        "selat makassar": "WPP-713",
        "laut flores": "WPP-713",
        "laut bali": "WPP-713",
        "teluk bone": "WPP-713",
        "laut banda": "WPP-714",
        "laut maluku": "WPP-715",
        "laut sulawesi": "WPP-716",
        "laut natuna": "WPP-711",
        "selat karimata": "WPP-711",
        "samudera hindia selatan": "WPP-573",
        "samudera hindia barat": "WPP-572",
        "selat malaka": "WPP-571",
        "laut arafuru": "WPP-718",
        "laut aru": "WPP-718",
        "laut timor": "WPP-718",
        "teluk cendrawasih": "WPP-717",
        "samudera pasifik": "WPP-717",
    }
    lower = wpp_str.lower()
    for key, code in mapping.items():
        if key in lower:
            return code
    return wpp_str[:20] if wpp_str else None


def _flatten_records(activities: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Flatten nested API response into one row per (vessel activity × fish species).
    Each activity has a `hasil` list; we emit one row per hasil entry.
    """
    rows = []
    for act in activities:
        hasil_list = act.get("hasil") or []
        if not hasil_list:
            # Still record the activity with null fish fields
            hasil_list = [{}]

        # Compute total catch weight across all species for this activity
        total_kg = sum(
            float(h.get("jml_ikan") or 0) for h in act.get("hasil") or []
        )

        base = {
            "nama_kapal": (act.get("nama_kapal") or "").strip() or None,
            "nomor_bkp": str(act.get("nomor_buku_kapal") or "").strip() or None,
            "tanggal_bongkar": act.get("tgl_bongkar") or act.get("tgl_aktivitas"),
            "pelabuhan": (act.get("pelabuhan_kedatangan") or "").strip() or None,
            "pelabuhan_kode": str(act.get("id_pelabuhan_dss_kedatangan") or "").strip() or None,
            "total_tangkapan": total_kg if total_kg > 0 else None,
            "wpp_tangkap": _map_wpp(act.get("wpp_operasi") or act.get("wpp_pelabuhan")),
            "sumber_data": SOURCE_NAME,
        }

        for h in hasil_list:
            jml = float(h.get("jml_ikan") or 0)
            harga = float(h.get("harga_produsen") or 0)
            row = {
                **base,
                "jenis_ikan": (h.get("nama_jenis_ikan") or "").strip() or None,
                "kode_ikan": None,  # not provided by API
                "berat_per_jenis": jml if jml > 0 else None,
                "nilai_tangkapan": round(jml * harga, 2) if jml and harga else None,
                "harga_per_kg": harga if harga > 0 else None,
                "trip_ke": None,
                "lama_trip": None,
                "jumlah_abk": None,
            }
            rows.append(row)
    return rows


UPSERT_SQL = """
INSERT INTO master_tangkapan_pipp (
    nama_kapal, nomor_bkp, tanggal_bongkar, pelabuhan, pelabuhan_kode,
    total_tangkapan, jenis_ikan, kode_ikan, berat_per_jenis,
    nilai_tangkapan, harga_per_kg, wpp_tangkap,
    trip_ke, lama_trip, jumlah_abk, sumber_data,
    created_at, updated_at
)
VALUES (
    %(nama_kapal)s, %(nomor_bkp)s, %(tanggal_bongkar)s, %(pelabuhan)s, %(pelabuhan_kode)s,
    %(total_tangkapan)s, %(jenis_ikan)s, %(kode_ikan)s, %(berat_per_jenis)s,
    %(nilai_tangkapan)s, %(harga_per_kg)s, %(wpp_tangkap)s,
    %(trip_ke)s, %(lama_trip)s, %(jumlah_abk)s, %(sumber_data)s,
    NOW(), NOW()
)
ON CONFLICT DO NOTHING
"""

# Deduplication: treat (nama_kapal, tanggal_bongkar, jenis_ikan) as the natural key.
# We check existence before insert to avoid duplicates across runs.
EXISTS_SQL = """
SELECT 1 FROM master_tangkapan_pipp
WHERE nama_kapal = %(nama_kapal)s
  AND tanggal_bongkar = %(tanggal_bongkar)s
  AND jenis_ikan IS NOT DISTINCT FROM %(jenis_ikan)s
LIMIT 1
"""

UPDATE_SQL = """
UPDATE master_tangkapan_pipp SET
    nomor_bkp       = %(nomor_bkp)s,
    pelabuhan       = %(pelabuhan)s,
    pelabuhan_kode  = %(pelabuhan_kode)s,
    total_tangkapan = %(total_tangkapan)s,
    berat_per_jenis = %(berat_per_jenis)s,
    nilai_tangkapan = %(nilai_tangkapan)s,
    harga_per_kg    = %(harga_per_kg)s,
    wpp_tangkap     = %(wpp_tangkap)s,
    sumber_data     = %(sumber_data)s,
    updated_at      = NOW()
WHERE nama_kapal       = %(nama_kapal)s
  AND tanggal_bongkar  = %(tanggal_bongkar)s
  AND jenis_ikan IS NOT DISTINCT FROM %(jenis_ikan)s
"""


def _upsert_rows(conn: psycopg2.extensions.connection, rows: list[dict[str, Any]]) -> tuple[int, int]:
    """Upsert rows; returns (inserted, updated) counts."""
    inserted = updated = 0
    with conn.cursor() as cur:
        for row in rows:
            cur.execute(EXISTS_SQL, row)
            if cur.fetchone():
                cur.execute(UPDATE_SQL, row)
                updated += 1
            else:
                cur.execute(UPSERT_SQL, row)
                inserted += 1
    conn.commit()
    return inserted, updated


def collect(start_date: str, end_date: str) -> dict[str, int]:
    """
    Main entry point. Fetches PIPP data for [start_date, end_date] inclusive
    and upserts into master_tangkapan_pipp.

    Returns summary dict with total_fetched, inserted, updated, skipped_days.
    """
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()

    if start > end:
        raise ValueError(f"start_date {start_date} is after end_date {end_date}")

    total_fetched = total_inserted = total_updated = skipped_days = 0

    conn = _get_db_conn()
    try:
        current = start
        while current <= end:
            day_str = current.strftime("%Y-%m-%d")
            logger.info("Fetching PIPP data for %s …", day_str)

            activities = _fetch_pipp_day(day_str)
            if not activities:
                logger.info("  No data for %s", day_str)
                skipped_days += 1
                current += timedelta(days=1)
                continue

            rows = _flatten_records(activities)
            total_fetched += len(rows)

            ins, upd = _upsert_rows(conn, rows)
            total_inserted += ins
            total_updated += upd
            logger.info("  %s: %d activities → %d rows (ins=%d upd=%d)", day_str, len(activities), len(rows), ins, upd)

            current += timedelta(days=1)
            # Polite delay between API calls
            time.sleep(0.5)
    finally:
        conn.close()

    summary = {
        "total_fetched": total_fetched,
        "inserted": total_inserted,
        "updated": total_updated,
        "skipped_days": skipped_days,
    }
    logger.info("Done. %s", summary)
    return summary


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect PIPP tangkapan data into master_tangkapan_pipp")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--start", metavar="YYYY-MM-DD", help="Start date (inclusive)")
    group.add_argument("--days", type=int, default=30, help="Collect last N days (default: 30)")
    parser.add_argument("--end", metavar="YYYY-MM-DD", help="End date (inclusive, default: today)")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    end_date = args.end or date.today().strftime("%Y-%m-%d")
    if args.start:
        start_date = args.start
    else:
        start_date = (date.today() - timedelta(days=args.days)).strftime("%Y-%m-%d")

    logger.info("PIPP collector: %s → %s", start_date, end_date)
    summary = collect(start_date, end_date)
    print(f"Completed: {summary}")
