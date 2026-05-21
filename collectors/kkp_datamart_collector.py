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
RATE_LIMIT_DELAY = 0.3  # seconds between per-vessel requests
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


def _get(url: str, params: Optional[dict] = None) -> Optional[Any]:
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

def fetch_all_kapal() -> list[dict]:
    """
    Fetch full vessel registry from KKP search-kapal-bkp endpoint.

    Response shape:
      {"data": [{"nomor_buku_kapal": "...", "transmitter_no": "...", "nama_kapal": "..."}, ...],
       "pagination": {"total_page": N, "total_count": N, "current_page": 1}}
    """
    url = f"{KKP_BASE_URL}/kapal/search-kapal-bkp"
    # Fetch all in one shot — API supports limit up to 100000
    params = {"page": 1, "limit": 100000, "sort_by": "nama_kapal", "sort_order": "asc"}
    data = _get(url, params)
    if data is None:
        logger.error("Failed to fetch kapal list")
        return []
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and "data" in data:
        return data["data"]
    logger.warning("Unexpected kapal list response shape: %s", list(data.keys()) if isinstance(data, dict) else type(data))
    return []


def fetch_vessel_tracking(nomor_bkp: str, interval: int = 30) -> list[dict]:
    """
    Fetch location interval data for a single vessel.

    Response shape: GeoJSON FeatureCollection.
    Point features carry: speed, heading, ping_time, nomor_bkp in properties;
    coordinates are [lon, lat].
    """
    url = f"{KKP_BASE_URL}/kapal/data-kapal-lokasi-interval"
    data = _get(url, {"nomor_bkp": nomor_bkp, "interval": interval})
    if data is None:
        return []

    # Parse GeoJSON FeatureCollection — extract only Point features (individual pings)
    if isinstance(data, dict) and data.get("type") == "FeatureCollection":
        points = []
        for feature in data.get("features", []):
            geom = feature.get("geometry", {})
            if geom.get("type") == "Point":
                props = feature.get("properties", {})
                coords = geom.get("coordinates", [None, None])
                points.append({
                    "longitude": coords[0] if len(coords) > 0 else None,
                    "latitude":  coords[1] if len(coords) > 1 else None,
                    "speed":     props.get("speed"),
                    "heading":   props.get("heading"),
                    "ping_time": props.get("ping_time"),
                    "nomor_bkp": props.get("nomor_bkp", nomor_bkp),
                })
        return points

    # Fallback: plain list
    if isinstance(data, list):
        return data
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
    """
    Upsert into master_kapal. Conflict key: nomor_bkp.
    The KKP search endpoint only returns 3 fields (nomor_buku_kapal,
    transmitter_no, nama_kapal); remaining columns are left NULL and
    can be enriched later via fetch_kapal_detail.
    """
    sql = """
        INSERT INTO master_kapal (
            nama_kapal, nomor_bkp, no_transmitter, aktif
        ) VALUES (
            %(nama_kapal)s, %(nomor_bkp)s, %(no_transmitter)s, %(aktif)s
        )
        ON CONFLICT (nomor_bkp) DO UPDATE SET
            nama_kapal     = EXCLUDED.nama_kapal,
            no_transmitter = COALESCE(EXCLUDED.no_transmitter, master_kapal.no_transmitter),
            aktif          = EXCLUDED.aktif,
            updated_at     = NOW()
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
            nama_kapal, nomor_bkp, transmitter_no,
            latitude, longitude, direction, speed,
            timestamp, source
        ) VALUES (
            %(nama_kapal)s, %(nomor_bkp)s, %(transmitter_no)s,
            %(latitude)s, %(longitude)s, %(direction)s, %(speed)s,
            %(timestamp)s, %(source)s
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
    """
    Map KKP search-kapal-bkp record to master_kapal columns.
    API fields: nomor_buku_kapal, transmitter_no, nama_kapal
    """
    return {
        "nama_kapal":    _safe_str(raw.get("nama_kapal"), 150),
        # KKP uses nomor_buku_kapal as the BKP identifier
        "nomor_bkp":     _safe_str(raw.get("nomor_buku_kapal") or raw.get("nomor_bkp") or raw.get("no_bkp"), 50),
        "no_transmitter": _safe_str(raw.get("transmitter_no") or raw.get("no_transmitter"), 100),
        "aktif":         True,
    }


def _map_tracking_row(raw: dict, nomor_bkp: str, nama_kapal: str, transmitter_no: str) -> dict:
    return {
        "nama_kapal":     _safe_str(nama_kapal, 100),
        "nomor_bkp":      _safe_str(nomor_bkp, 50),
        "transmitter_no": _safe_str(transmitter_no, 100),
        "latitude":       _safe_decimal(raw.get("latitude")),
        "longitude":      _safe_decimal(raw.get("longitude")),
        "direction":      _safe_decimal(raw.get("heading") or raw.get("direction")),
        "speed":          _safe_decimal(raw.get("speed")),
        "timestamp":      raw.get("ping_time") or raw.get("timestamp") or raw.get("waktu"),
        "source":         "KKP_VMS",
    }


# ---------------------------------------------------------------------------
# Main collection entry points
# ---------------------------------------------------------------------------

def collect_master_kapal(conn) -> tuple[int, list[dict]]:
    """
    Fetch all kapal from KKP and upsert into master_kapal.
    Returns (inserted_count, raw_kapal_list).
    """
    logger.info("Fetching kapal list from KKP Datamart ...")
    raw_list = fetch_all_kapal()
    if not raw_list:
        logger.warning("No kapal returned from KKP API")
        return 0, []

    logger.info("Fetched %d kapal records", len(raw_list))
    rows = [_map_kapal_row(r) for r in raw_list]
    rows = [r for r in rows if r["nomor_bkp"]]  # drop rows without BKP key
    logger.info("Rows with valid nomor_bkp: %d", len(rows))
    inserted = upsert_master_kapal(conn, rows)
    logger.info("master_kapal: %d/%d rows upserted", inserted, len(rows))
    return inserted, raw_list


def collect_vessel_tracking(
    conn,
    kapal_list: list[dict],
    interval: int = 30,
    max_vessels: Optional[int] = None,
) -> int:
    """
    Iterate kapal list, fetch tracking data per vessel, upsert into master_vessel_tracking.
    """
    total_inserted = 0
    vessels = kapal_list[:max_vessels] if max_vessels else kapal_list

    for i, kapal in enumerate(vessels, 1):
        nomor_bkp      = str(kapal.get("nomor_buku_kapal") or kapal.get("nomor_bkp") or "").strip()
        nama_kapal     = str(kapal.get("nama_kapal") or "").strip()
        transmitter_no = str(kapal.get("transmitter_no") or "").strip()

        if not nomor_bkp:
            continue

        logger.info("[%d/%d] Tracking %s (%s)", i, len(vessels), nama_kapal, nomor_bkp)
        raw_tracks = fetch_vessel_tracking(nomor_bkp, interval=interval)

        if raw_tracks:
            tracking_rows = [
                _map_tracking_row(t, nomor_bkp, nama_kapal, transmitter_no)
                for t in raw_tracks
            ]
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
