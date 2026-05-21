"""
KKP Datamart Collector.

Fetches vessel registry, tracking, and catch data from KKP Insight API
and inserts into PostgreSQL tables: master_kapal, master_vessel_tracking.

Endpoints used:
  1. search-kapal-bkp       → master_kapal (basic registry, all vessels)
  2. data-kapal             → master_kapal (enriched detail per vessel)
  3. data-kapal-lokasi-interval → master_vessel_tracking (GPS pings)

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
DEFAULT_BATCH_SIZE = 500  # commit every N tracking rows


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

    Response: {"data": [{"nomor_buku_kapal": "...", "transmitter_no": "...", "nama_kapal": "..."}, ...]}
    """
    url = f"{KKP_BASE_URL}/kapal/search-kapal-bkp"
    params = {"page": 1, "limit": 100000, "sort_by": "nama_kapal", "sort_order": "asc"}
    data = _get(url, params)
    if data is None:
        logger.error("Failed to fetch kapal list")
        return []
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and "data" in data:
        return data["data"]
    logger.warning("Unexpected kapal list response shape: %s", type(data))
    return []


def fetch_vessel_tracking(nomor_bkp: str, interval: int = 30) -> list[dict]:
    """
    Fetch location interval data for a single vessel.

    Response: GeoJSON FeatureCollection.
    - LineString feature: summary with average_speed, last_heading, last_ping
    - Point features: individual pings with speed, heading, ping_time, nomor_bkp
    """
    url = f"{KKP_BASE_URL}/kapal/data-kapal-lokasi-interval"
    data = _get(url, {"nomor_bkp": nomor_bkp, "interval": interval})
    if data is None:
        return []

    if not (isinstance(data, dict) and data.get("type") == "FeatureCollection"):
        if isinstance(data, list):
            return data
        return []

    points = []
    for feature in data.get("features", []):
        geom = feature.get("geometry", {})
        props = feature.get("properties", {})

        if geom.get("type") == "Point":
            coords = geom.get("coordinates", [None, None])
            points.append({
                "longitude": coords[0] if len(coords) > 0 else None,
                "latitude":  coords[1] if len(coords) > 1 else None,
                "speed":     props.get("speed"),
                "direction": props.get("heading"),
                "ping_time": props.get("ping_time"),
                "nomor_bkp": props.get("nomor_bkp", nomor_bkp),
            })

    return points


def fetch_kapal_detail(transmitter_no: str) -> Optional[dict]:
    """
    Fetch full vessel detail by transmitter number.

    Returns the rich data-kapal record with fields:
    no_bkp, nama_kapal, tanda_selar, ukuran_gt, nomor_transmitter,
    pemilik_kapal, alat_tangkap, jenis_alat_tangkap, kekuatan_mesin,
    merk_mesin, nama_wpp, nama_pelabuhan_pangkalan, ping_time, etc.
    """
    url = f"{KKP_BASE_URL}/kapal/data-kapal"
    data = _get(url, {"transmitter_no": transmitter_no})
    if data is None:
        return None
    if isinstance(data, dict):
        # Unwrap {"data": {...}, "last_updated": "..."}
        if "data" in data and isinstance(data["data"], dict):
            return data["data"]
        for key in ("result", "kapal"):
            if key in data and isinstance(data[key], dict):
                return data[key]
        return data
    if isinstance(data, list) and data:
        return data[0]
    return None


def fetch_tangkapan(transmitter_no: str) -> list[dict]:
    """Fetch catch data per vessel from tangkapan-interval-pelabuhan endpoint."""
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
    s = str(val).strip()
    return s[:maxlen] if s else None


# ---------------------------------------------------------------------------
# Row mappers
# ---------------------------------------------------------------------------

def _map_kapal_basic(raw: dict) -> dict:
    """Map search-kapal-bkp record (3 fields only) to master_kapal columns."""
    return {
        "nama_kapal":          _safe_str(raw.get("nama_kapal"), 150),
        "nomor_bkp":           _safe_str(raw.get("nomor_buku_kapal") or raw.get("nomor_bkp") or raw.get("no_bkp"), 50),
        "no_transmitter":      _safe_str(raw.get("transmitter_no") or raw.get("no_transmitter") or raw.get("nomor_transmitter"), 100),
        "tanda_selar":         None,
        "ukuran_kapal":        None,
        "pemilik":             None,
        "alat_tangkap":        None,
        "kekuatan_mesin":      None,
        "merek_mesin":         None,
        "wilayah_tangkap":     None,
        "pelabuhan_pangkalan": None,
        "aktif":               True,
    }


def _map_kapal_detail(detail: dict) -> dict:
    """
    Map data-kapal response to master_kapal columns.

    API fields available:
      no_bkp, nama_kapal, tanda_selar, ukuran_gt, nomor_transmitter,
      pemilik_kapal, alat_tangkap / jenis_alat_tangkap, kekuatan_mesin,
      merk_mesin, nama_wpp, nama_pelabuhan_pangkalan, ping_time
    """
    # Combine alat_tangkap code + jenis name for a readable value
    alat = _safe_str(detail.get("jenis_alat_tangkap") or detail.get("alat_tangkap"), 100)

    # wilayah_tangkap: use nama_wpp (comma-separated WPP names)
    wilayah = _safe_str(detail.get("nama_wpp"), 100)

    return {
        "nama_kapal":          _safe_str(detail.get("nama_kapal"), 150),
        "nomor_bkp":           _safe_str(detail.get("no_bkp") or detail.get("nomor_bkp") or detail.get("nomor_buku_kapal"), 50),
        "no_transmitter":      _safe_str(detail.get("nomor_transmitter") or detail.get("transmitter_no"), 100),
        "tanda_selar":         _safe_str(detail.get("tanda_selar"), 100),
        "ukuran_kapal":        _safe_decimal(detail.get("ukuran_gt")),
        "pemilik":             _safe_str(detail.get("pemilik_kapal"), 150),
        "alat_tangkap":        alat,
        "kekuatan_mesin":      _safe_decimal(detail.get("kekuatan_mesin")),
        "merek_mesin":         _safe_str(detail.get("merk_mesin"), 100),
        "wilayah_tangkap":     wilayah,
        "pelabuhan_pangkalan": _safe_str(detail.get("nama_pelabuhan_pangkalan"), 150),
        "aktif":               True,
    }


def _map_tracking_row(raw: dict, nomor_bkp: str, nama_kapal: str, transmitter_no: str) -> dict:
    return {
        "nama_kapal":     _safe_str(nama_kapal, 100),
        "nomor_bkp":      _safe_str(nomor_bkp, 50),
        "transmitter_no": _safe_str(transmitter_no, 100),
        "mmsi":           None,
        "latitude":       _safe_decimal(raw.get("latitude")),
        "longitude":      _safe_decimal(raw.get("longitude")),
        "direction":      _safe_decimal(raw.get("direction") or raw.get("heading")),
        "speed":          _safe_decimal(raw.get("speed")),
        "timestamp":      raw.get("ping_time") or raw.get("timestamp") or raw.get("waktu"),
        "status_kapal":   None,
        "source":         "KKP_VMS",
    }


# ---------------------------------------------------------------------------
# DB upsert helpers
# ---------------------------------------------------------------------------

def upsert_master_kapal(conn, kapal_rows: list[dict]) -> int:
    """
    Upsert into master_kapal. Conflict key: nomor_bkp.
    Uses COALESCE so enriched detail fields don't overwrite with NULL
    when called from the basic search pass.
    """
    sql = """
        INSERT INTO master_kapal (
            nama_kapal, nomor_bkp, no_transmitter,
            tanda_selar, ukuran_kapal, pemilik, alat_tangkap,
            kekuatan_mesin, merek_mesin, wilayah_tangkap, pelabuhan_pangkalan,
            aktif
        ) VALUES (
            %(nama_kapal)s, %(nomor_bkp)s, %(no_transmitter)s,
            %(tanda_selar)s, %(ukuran_kapal)s, %(pemilik)s, %(alat_tangkap)s,
            %(kekuatan_mesin)s, %(merek_mesin)s, %(wilayah_tangkap)s, %(pelabuhan_pangkalan)s,
            %(aktif)s
        )
        ON CONFLICT (nomor_bkp) DO UPDATE SET
            nama_kapal          = EXCLUDED.nama_kapal,
            no_transmitter      = COALESCE(EXCLUDED.no_transmitter,      master_kapal.no_transmitter),
            tanda_selar         = COALESCE(EXCLUDED.tanda_selar,         master_kapal.tanda_selar),
            ukuran_kapal        = COALESCE(EXCLUDED.ukuran_kapal,        master_kapal.ukuran_kapal),
            pemilik             = COALESCE(EXCLUDED.pemilik,             master_kapal.pemilik),
            alat_tangkap        = COALESCE(EXCLUDED.alat_tangkap,        master_kapal.alat_tangkap),
            kekuatan_mesin      = COALESCE(EXCLUDED.kekuatan_mesin,      master_kapal.kekuatan_mesin),
            merek_mesin         = COALESCE(EXCLUDED.merek_mesin,         master_kapal.merek_mesin),
            wilayah_tangkap     = COALESCE(EXCLUDED.wilayah_tangkap,     master_kapal.wilayah_tangkap),
            pelabuhan_pangkalan = COALESCE(EXCLUDED.pelabuhan_pangkalan, master_kapal.pelabuhan_pangkalan),
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


def upsert_vessel_tracking_batch(conn, tracking_rows: list[dict]) -> int:
    """
    Batch-insert tracking rows.
    Deduplicates in-memory on (nomor_bkp, timestamp) before inserting so we
    don't rely on a DB unique constraint that may not exist.
    """
    if not tracking_rows:
        return 0

    # Deduplicate: keep last occurrence of each (nomor_bkp, timestamp) pair
    seen: dict[tuple, dict] = {}
    for row in tracking_rows:
        key = (row.get("nomor_bkp"), row.get("timestamp"))
        seen[key] = row
    deduped = list(seen.values())

    sql = """
        INSERT INTO master_vessel_tracking (
            nama_kapal, nomor_bkp, transmitter_no,
            mmsi, latitude, longitude, direction, speed,
            timestamp, status_kapal, source
        ) VALUES (
            %(nama_kapal)s, %(nomor_bkp)s, %(transmitter_no)s,
            %(mmsi)s, %(latitude)s, %(longitude)s, %(direction)s, %(speed)s,
            %(timestamp)s, %(status_kapal)s, %(source)s
        )
        ON CONFLICT DO NOTHING
    """
    inserted = 0
    with conn.cursor() as cur:
        for row in deduped:
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
# Main collection entry points
# ---------------------------------------------------------------------------

def collect_master_kapal(conn, enrich_detail: bool = True) -> tuple[int, list[dict]]:
    """
    Phase 1: fetch all kapal from search endpoint → upsert basic rows.
    Phase 2 (optional): fetch data-kapal detail per vessel → enrich master_kapal.

    Returns (total_upserted, raw_kapal_list).
    """
    logger.info("Fetching kapal list from KKP Datamart ...")
    raw_list = fetch_all_kapal()
    if not raw_list:
        logger.warning("No kapal returned from KKP API")
        return 0, []

    logger.info("Fetched %d kapal records", len(raw_list))

    # Phase 1: basic upsert (3 fields from search endpoint)
    basic_rows = [_map_kapal_basic(r) for r in raw_list]
    basic_rows = [r for r in basic_rows if r["nomor_bkp"]]
    logger.info("Rows with valid nomor_bkp: %d", len(basic_rows))
    upserted = upsert_master_kapal(conn, basic_rows)
    logger.info("master_kapal basic pass: %d/%d rows upserted", upserted, len(basic_rows))

    if not enrich_detail:
        return upserted, raw_list

    # Phase 2: enrich with full detail from data-kapal endpoint
    logger.info("Enriching master_kapal with data-kapal detail ...")
    enriched = 0
    for i, kapal in enumerate(raw_list, 1):
        transmitter_no = str(kapal.get("transmitter_no") or "").strip()
        if not transmitter_no:
            continue

        detail = fetch_kapal_detail(transmitter_no)
        if detail:
            detail_row = _map_kapal_detail(detail)
            if detail_row.get("nomor_bkp"):
                n = upsert_master_kapal(conn, [detail_row])
                enriched += n

        if i % 100 == 0:
            logger.info("  Enriched %d/%d vessels", i, len(raw_list))
        time.sleep(RATE_LIMIT_DELAY)

    logger.info("master_kapal enrichment: %d vessels updated", enriched)
    return upserted, raw_list


def collect_vessel_tracking(
    conn,
    kapal_list: list[dict],
    interval: int = 30,
    max_vessels: Optional[int] = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> int:
    """
    Iterate kapal list, fetch tracking data per vessel, batch-insert into master_vessel_tracking.
    Commits every `batch_size` rows for memory efficiency.
    """
    total_inserted = 0
    pending: list[dict] = []
    vessels = kapal_list[:max_vessels] if max_vessels else kapal_list

    for i, kapal in enumerate(vessels, 1):
        nomor_bkp      = str(kapal.get("nomor_buku_kapal") or kapal.get("nomor_bkp") or "").strip()
        nama_kapal     = str(kapal.get("nama_kapal") or "").strip()
        transmitter_no = str(kapal.get("transmitter_no") or "").strip()

        if not nomor_bkp:
            continue

        logger.info("[%d/%d] Tracking %s (bkp=%s tx=%s)", i, len(vessels), nama_kapal, nomor_bkp, transmitter_no)
        raw_tracks = fetch_vessel_tracking(nomor_bkp, interval=interval)

        if raw_tracks:
            rows = [
                _map_tracking_row(t, nomor_bkp, nama_kapal, transmitter_no)
                for t in raw_tracks
            ]
            rows = [r for r in rows if r["latitude"] is not None and r["longitude"] is not None]
            pending.extend(rows)
            logger.debug("  → %d valid tracking points queued", len(rows))

        # Flush batch
        if len(pending) >= batch_size:
            n = upsert_vessel_tracking_batch(conn, pending)
            total_inserted += n
            logger.info("  Batch committed: %d rows (total so far: %d)", n, total_inserted)
            pending = []

        time.sleep(RATE_LIMIT_DELAY)

    # Flush remainder
    if pending:
        n = upsert_vessel_tracking_batch(conn, pending)
        total_inserted += n
        logger.info("  Final batch committed: %d rows", n)

    logger.info("master_vessel_tracking: %d total rows inserted", total_inserted)
    return total_inserted


def run(
    tracking_interval: int = 30,
    max_vessels: Optional[int] = None,
    enrich_detail: bool = True,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> dict[str, int]:
    """
    Full collection run: kapal registry (+ optional detail enrichment) + vessel tracking.

    Parameters
    ----------
    tracking_interval : int   Interval in minutes for location data (default 30)
    max_vessels       : int   Limit vessel tracking to first N vessels (None = all)
    enrich_detail     : bool  Fetch data-kapal detail to fill all master_kapal columns (default True)
    batch_size        : int   Commit tracking rows every N records (default 500)
    """
    logger.info("=" * 60)
    logger.info("KKP Datamart Collector starting")
    logger.info("  tracking_interval : %d min", tracking_interval)
    logger.info("  max_vessels       : %s", max_vessels or "all")
    logger.info("  enrich_detail     : %s", enrich_detail)
    logger.info("  batch_size        : %d", batch_size)
    logger.info("=" * 60)

    conn = _db_conn()
    try:
        kapal_upserted, kapal_list = collect_master_kapal(conn, enrich_detail=enrich_detail)
        tracking_inserted = collect_vessel_tracking(
            conn, kapal_list,
            interval=tracking_interval,
            max_vessels=max_vessels,
            batch_size=batch_size,
        )
    finally:
        conn.close()

    result = {
        "kapal_upserted":     kapal_upserted,
        "tracking_inserted":  tracking_inserted,
    }
    logger.info("KKP Datamart Collector done: %s", result)
    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="KKP Datamart Collector")
    parser.add_argument("--interval",    type=int,  default=30,   help="Tracking interval in minutes (default: 30)")
    parser.add_argument("--max-vessels", type=int,  default=None, help="Limit tracking to first N vessels")
    parser.add_argument("--batch-size",  type=int,  default=DEFAULT_BATCH_SIZE, help=f"Commit every N tracking rows (default: {DEFAULT_BATCH_SIZE})")
    parser.add_argument("--no-enrich",   action="store_true",     help="Skip data-kapal detail enrichment (faster, fewer columns)")
    parser.add_argument("--log-level",   default="INFO",          help="Logging level (default: INFO)")
    args = parser.parse_args()

    logging.getLogger().setLevel(getattr(logging, args.log_level.upper(), logging.INFO))

    try:
        result = run(
            tracking_interval=args.interval,
            max_vessels=args.max_vessels,
            enrich_detail=not args.no_enrich,
            batch_size=args.batch_size,
        )
        # Exit non-zero if nothing was collected at all (signals Airflow to retry)
        if result["kapal_upserted"] == 0:
            logger.error("No kapal rows upserted — possible API failure. Exiting with error.")
            sys.exit(1)
    except Exception as exc:
        logger.exception("Collector failed: %s", exc)
        sys.exit(1)
