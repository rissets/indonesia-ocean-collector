"""
KKP Datamart Collector.

Fetches vessel registry, tracking, and catch data from KKP Insight API
and inserts into PostgreSQL tables: master_kapal, master_vessel_tracking.

Requires in .env:
  KKP_DATAMART_TOKEN=<JWT>
  DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any, Optional

import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

KKP_BASE_URL = "https://insight.kkp.go.id/datamart/api"
REQUEST_TIMEOUT = 30
RATE_LIMIT_DELAY = 0.5  # seconds between per-vessel requests
MAX_RETRIES = 3
RETRY_BACKOFF = 2.0


# ---------------------------------------------------------------------------
# Auth / HTTP helpers
# ---------------------------------------------------------------------------

def _token() -> str:
    t = os.getenv("KKP_DATAMART_TOKEN", "")
    if not t:
        raise EnvironmentError("KKP_DATAMART_TOKEN not set in environment")
    return t


def _headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {_token()}"}


def _get(url: str, params: Optional[dict] = None) -> Optional[dict]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, params=params, headers=_headers(), timeout=REQUEST_TIMEOUT)
            if resp.status_code == 429:
                wait = RETRY_BACKOFF ** attempt
                logger.warning("Rate limited on %s — waiting %.1fs", url, wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError as exc:
            logger.error("HTTP error [attempt %d/%d] %s: %s", attempt, MAX_RETRIES, url, exc)
            if attempt == MAX_RETRIES:
                return None
            time.sleep(RETRY_BACKOFF)
        except requests.exceptions.RequestException as exc:
            logger.error("Request failed [attempt %d/%d] %s: %s", attempt, MAX_RETRIES, url, exc)
            if attempt == MAX_RETRIES:
                return None
            time.sleep(RETRY_BACKOFF)
    return None


# ---------------------------------------------------------------------------
# DB connection
# ---------------------------------------------------------------------------

def _db_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "maritime-os"),
        user=os.getenv("DB_USER", "maritime-os"),
        password=os.getenv("DB_PASSWORD", ""),
    )


# ---------------------------------------------------------------------------
# API fetchers
# ---------------------------------------------------------------------------

def fetch_all_kapal(page: int = 1, limit: int = 100000) -> list[dict]:
    """Fetch full vessel registry from KKP search-kapal-bkp endpoint."""
    url = f"{KKP_BASE_URL}/kapal/search-kapal-bkp"
    params = {
        "page": page,
        "limit": limit,
        "sort_by": "nama_kapal",
        "sort_order": "asc",
    }
    data = _get(url, params)
    if data is None:
        logger.error("Failed to fetch kapal list")
        return []

    # API may wrap results in data/rows/items key
    if isinstance(data, list):
        return data
    for key in ("data", "rows", "items", "result", "kapal"):
        if key in data and isinstance(data[key], list):
            return data[key]

    logger.warning("Unexpected kapal list response shape: %s", list(data.keys()))
    return []


def fetch_vessel_tracking(nomor_bkp: str, interval: int = 30) -> list[dict]:
    """Fetch location interval data for a single vessel."""
    url = f"{KKP_BASE_URL}/kapal/data-kapal-lokasi-interval"
    data = _get(url, {"nomor_bkp": nomor_bkp, "interval": interval})
    if data is None:
        return []
    if isinstance(data, list):
        return data
    for key in ("data", "rows", "items", "result", "lokasi"):
        if key in data and isinstance(data[key], list):
            return data[key]
    return []


def fetch_kapal_detail(transmitter_no: str) -> Optional[dict]:
    """Fetch detailed vessel info by transmitter number."""
    url = f"{KKP_BASE_URL}/kapal/data-kapal"
    data = _get(url, {"transmitter_no": transmitter_no})
    if data is None:
        return None
    if isinstance(data, dict):
        for key in ("data", "result", "kapal"):
            if key in data and isinstance(data[key], dict):
                return data[key]
        return data
    if isinstance(data, list) and data:
        return data[0]
    return None


def fetch_tangkapan(transmitter_no: str) -> list[dict]:
    """Fetch catch data per vessel."""
    url = f"{KKP_BASE_URL}/kapal/data-kapal-tangkapan-interval-pelabuhan"
    data = _get(url, {"transmitter_no": transmitter_no})
    if data is None:
        return []
    if isinstance(data, list):
        return data
    for key in ("data", "rows", "items", "result", "tangkapan"):
        if key in data and isinstance(data[key], list):
            return data[key]
    return []


# ---------------------------------------------------------------------------
# Value helpers
# ---------------------------------------------------------------------------

def _safe_decimal(val: Any) -> Optional[float]:
    if val is None or val == "":
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _safe_str(val: Any, maxlen: int = 150) -> Optional[str]:
    if val is None:
        return None
    return str(val)[:maxlen]


# ---------------------------------------------------------------------------
# DB upsert helpers
# ---------------------------------------------------------------------------

def upsert_master_kapal(conn, kapal_rows: list[dict]) -> int:
    sql = """
        INSERT INTO master_kapal (
            nama_kapal, nomor_bkp, tanda_selar, ukuran_kapal,
            no_transmitter, pemilik, alat_tangkap, kekuatan_mesin,
            merek_mesin, wilayah_tangkap, pelabuhan_pangkalan, aktif
        ) VALUES (
            %(nama_kapal)s, %(nomor_bkp)s, %(tanda_selar)s, %(ukuran_kapal)s,
            %(no_transmitter)s, %(pemilik)s, %(alat_tangkap)s, %(kekuatan_mesin)s,
            %(merek_mesin)s, %(wilayah_tangkap)s, %(pelabuhan_pangkalan)s, %(aktif)s
        )
        ON CONFLICT (nomor_bkp) DO UPDATE SET
            nama_kapal          = EXCLUDED.nama_kapal,
            tanda_selar         = EXCLUDED.tanda_selar,
            ukuran_kapal        = EXCLUDED.ukuran_kapal,
            no_transmitter      = EXCLUDED.no_transmitter,
            pemilik             = EXCLUDED.pemilik,
            alat_tangkap        = EXCLUDED.alat_tangkap,
            kekuatan_mesin      = EXCLUDED.kekuatan_mesin,
            merek_mesin         = EXCLUDED.merek_mesin,
            wilayah_tangkap     = EXCLUDED.wilayah_tangkap,
            pelabuhan_pangkalan = EXCLUDED.pelabuhan_pangkalan,
            aktif               = EXCLUDED.aktif,
            updated_at          = NOW()
    """
    inserted = 0
    with conn.cursor() as cur:
        for row in kapal_rows:
            try:
                cur.execute(sql, row)
                inserted += 1
            except Exception as exc:
                logger.warning("Skipping kapal row (nomor_bkp=%s): %s", row.get("nomor_bkp"), exc)
                conn.rollback()
                continue
        conn.commit()
    return inserted


def upsert_vessel_tracking(conn, tracking_rows: list[dict]) -> int:
    sql = """
        INSERT INTO master_vessel_tracking (
            nama_kapal, nomor_bkp, transmitter_no, mmsi,
            latitude, longitude, direction, speed,
            timestamp, status_kapal, source
        ) VALUES (
            %(nama_kapal)s, %(nomor_bkp)s, %(transmitter_no)s, %(mmsi)s,
            %(latitude)s, %(longitude)s, %(direction)s, %(speed)s,
            %(timestamp)s, %(status_kapal)s, %(source)s
        )
        ON CONFLICT DO NOTHING
    """
    inserted = 0
    with conn.cursor() as cur:
        for row in tracking_rows:
            try:
                cur.execute(sql, row)
                inserted += 1
            except Exception as exc:
                logger.warning("Skipping tracking row: %s", exc)
                conn.rollback()
                continue
        conn.commit()
    return inserted


# ---------------------------------------------------------------------------
# Row mappers
# ---------------------------------------------------------------------------

def _map_kapal_row(raw: dict) -> dict:
    return {
        "nama_kapal":          _safe_str(raw.get("nama_kapal") or raw.get("name"), 150),
        "nomor_bkp":           _safe_str(raw.get("nomor_bkp") or raw.get("no_bkp") or raw.get("bkp"), 50),
        "tanda_selar":         _safe_str(raw.get("tanda_selar") or raw.get("selar"), 100),
        "ukuran_kapal":        _safe_decimal(raw.get("ukuran_kapal") or raw.get("gt") or raw.get("gross_tonnage")),
        "no_transmitter":      _safe_str(raw.get("no_transmitter") or raw.get("transmitter_no") or raw.get("transmitter"), 100),
        "pemilik":             _safe_str(raw.get("pemilik") or raw.get("owner"), 150),
        "alat_tangkap":        _safe_str(raw.get("alat_tangkap") or raw.get("fishing_gear"), 100),
        "kekuatan_mesin":      _safe_decimal(raw.get("kekuatan_mesin") or raw.get("engine_power")),
        "merek_mesin":         _safe_str(raw.get("merek_mesin") or raw.get("engine_brand"), 100),
        "wilayah_tangkap":     _safe_str(raw.get("wilayah_tangkap") or raw.get("fishing_area"), 100),
        "pelabuhan_pangkalan": _safe_str(raw.get("pelabuhan_pangkalan") or raw.get("home_port") or raw.get("pelabuhan"), 150),
        "aktif":               bool(raw.get("aktif", True)),
    }


def _map_tracking_row(raw: dict, nomor_bkp: str, nama_kapal: str) -> dict:
    # Timestamp may come as various field names
    ts = (
        raw.get("timestamp") or raw.get("waktu") or raw.get("time")
        or raw.get("created_at") or raw.get("date_time")
    )
    return {
        "nama_kapal":    _safe_str(raw.get("nama_kapal") or nama_kapal, 100),
        "nomor_bkp":     _safe_str(raw.get("nomor_bkp") or nomor_bkp, 50),
        "transmitter_no": _safe_str(raw.get("transmitter_no") or raw.get("no_transmitter"), 100),
        "mmsi":          _safe_str(raw.get("mmsi"), 20),
        "latitude":      _safe_decimal(raw.get("latitude") or raw.get("lat")),
        "longitude":     _safe_decimal(raw.get("longitude") or raw.get("lon") or raw.get("lng")),
        "direction":     _safe_decimal(raw.get("direction") or raw.get("heading") or raw.get("course")),
        "speed":         _safe_decimal(raw.get("speed") or raw.get("kecepatan")),
        "timestamp":     ts,
        "status_kapal":  _safe_str(raw.get("status_kapal") or raw.get("status"), 30),
        "source":        "KKP_VMS",
    }


# ---------------------------------------------------------------------------
# Main collection entry points
# ---------------------------------------------------------------------------

def collect_master_kapal(conn) -> tuple[int, list[dict]]:
    """
    Fetch all kapal from KKP and upsert into master_kapal.
    Returns (inserted_count, kapal_list) — kapal_list used by downstream collectors.
    """
    logger.info("Fetching kapal list from KKP Datamart ...")
    raw_list = fetch_all_kapal()
    if not raw_list:
        logger.warning("No kapal returned from KKP API")
        return 0, []

    logger.info("Fetched %d kapal records", len(raw_list))
    rows = [_map_kapal_row(r) for r in raw_list]
    # Drop rows with no nomor_bkp — can't upsert without the unique key
    rows = [r for r in rows if r["nomor_bkp"]]
    inserted = upsert_master_kapal(conn, rows)
    logger.info("master_kapal: %d/%d rows upserted", inserted, len(rows))
    return inserted, raw_list


def collect_vessel_tracking(conn, kapal_list: list[dict], interval: int = 30, max_vessels: Optional[int] = None) -> int:
    """
    Iterate kapal list, fetch tracking data per vessel, upsert into master_vessel_tracking.
    """
    total_inserted = 0
    vessels = kapal_list[:max_vessels] if max_vessels else kapal_list

    for i, kapal in enumerate(vessels, 1):
        nomor_bkp = (kapal.get("nomor_bkp") or kapal.get("no_bkp") or "").strip()
        nama_kapal = (kapal.get("nama_kapal") or kapal.get("name") or "").strip()

        if not nomor_bkp:
            continue

        logger.info("[%d/%d] Fetching tracking for %s (%s)", i, len(vessels), nama_kapal, nomor_bkp)
        raw_tracks = fetch_vessel_tracking(nomor_bkp, interval=interval)

        if raw_tracks:
            tracking_rows = [_map_tracking_row(t, nomor_bkp, nama_kapal) for t in raw_tracks]
            # Only insert rows that have lat/lon
            tracking_rows = [r for r in tracking_rows if r["latitude"] is not None and r["longitude"] is not None]
            n = upsert_vessel_tracking(conn, tracking_rows)
            total_inserted += n
            logger.info("  → %d tracking points inserted", n)
        else:
            logger.debug("  → no tracking data for %s", nomor_bkp)

        time.sleep(RATE_LIMIT_DELAY)

    logger.info("master_vessel_tracking: %d total rows inserted", total_inserted)
    return total_inserted


def run(
    tracking_interval: int = 30,
    max_vessels: Optional[int] = None,
) -> dict[str, int]:
    """
    Full collection run: kapal registry + vessel tracking.

    Parameters
    ----------
    tracking_interval : int   Interval in minutes for location data (default 30)
    max_vessels       : int   Limit vessel tracking to first N vessels (None = all)

    Returns
    -------
    dict with keys: kapal_inserted, tracking_inserted
    """
    logger.info("=" * 60)
    logger.info("KKP Datamart Collector starting")
    logger.info("  tracking_interval : %d min", tracking_interval)
    logger.info("  max_vessels       : %s", max_vessels or "all")
    logger.info("=" * 60)

    conn = _db_conn()
    try:
        kapal_inserted, kapal_list = collect_master_kapal(conn)
        tracking_inserted = collect_vessel_tracking(
            conn, kapal_list,
            interval=tracking_interval,
            max_vessels=max_vessels,
        )
    finally:
        conn.close()

    result = {
        "kapal_inserted": kapal_inserted,
        "tracking_inserted": tracking_inserted,
    }
    logger.info("KKP Datamart Collector done: %s", result)
    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="KKP Datamart Collector")
    parser.add_argument("--interval", type=int, default=30, help="Tracking interval in minutes (default: 30)")
    parser.add_argument("--max-vessels", type=int, default=None, help="Limit tracking to first N vessels")
    args = parser.parse_args()

    run(tracking_interval=args.interval, max_vessels=args.max_vessels)
