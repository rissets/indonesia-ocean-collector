"""
KKP Insight Datamart collector.

Collects vessel master data from KKP Datamart and upserts it into
`master_kapal`. The search endpoint only returns the vessel key fields, so
this collector enriches each vessel through `/data-kapal`.

Usage:
    KKP_DATAMART_TOKEN=... python3 -m collectors.kkp_datamart_collector --limit 100000 --interval 30
    python3 -m collectors.kkp_datamart_collector --only-missing --max-vessels 500
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError, as_completed
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv
from db.wpp import get_wpp_id_for_point

load_dotenv()

logger = logging.getLogger(__name__)

BASE_URL = os.getenv("KKP_DATAMART_BASE_URL", "https://insight.kkp.go.id/datamart/api")
SOURCE_NAME = "KKP_DATAMART"
MAX_RETRIES = 3
RETRY_BACKOFF = 1.5

PLACEHOLDERS = {"", "-", "0", "0.0", "0.00", "none", "null", "n/a", "na", "unknown", "undefined"}
UNKNOWN_TEXT = "Tidak diketahui"


def _get_db_conn() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "217.217.252.155"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "maritime-os"),
        user=os.getenv("DB_USER", "maritime-os"),
        password=os.getenv("DB_PASSWORD", "@Maritime210526"),
        connect_timeout=10,
    )


def _headers() -> dict[str, str]:
    token = os.getenv("KKP_DATAMART_TOKEN")
    if not token:
        raise RuntimeError("KKP_DATAMART_TOKEN is required")
    return {"Authorization": f"Bearer {token}"}


def _request_json(
    path: str,
    params: dict[str, Any],
    timeout: int = 45,
    max_retries: int = MAX_RETRIES,
) -> dict[str, Any]:
    url = f"{BASE_URL}{path}"
    delay = RETRY_BACKOFF
    for attempt in range(1, max_retries + 1):
        try:
            request_timeout = (min(5, timeout), timeout)
            resp = requests.get(url, params=params, headers=_headers(), timeout=request_timeout)
            resp.raise_for_status()
            payload = resp.json()
            if not isinstance(payload, dict):
                raise ValueError(f"Expected JSON object, got {type(payload).__name__}")
            return payload
        except (requests.RequestException, ValueError) as exc:
            logger.warning("KKP request %s attempt %d/%d failed: %s", path, attempt, max_retries, exc)
            if attempt < max_retries:
                time.sleep(delay)
                delay *= 2
    return {}


def _clean_text(value: Any, max_len: int | None = None) -> str | None:
    if value is None:
        return None
    text = re.sub(r"\s+", " ", str(value)).strip()
    if text.lower() in PLACEHOLDERS:
        return None
    return text[:max_len] if max_len else text


def _clean_decimal(value: Any, allow_zero: bool = False) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip().replace(",", ".")
        if value.lower() in PLACEHOLDERS:
            return None
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    if number == 0 and not allow_zero:
        return None
    return number


def _is_active(row: dict[str, Any]) -> bool:
    raw_end = _clean_text(row.get("tanggal_akhir_sipi"))
    if not raw_end:
        return True
    try:
        return datetime.fromisoformat(raw_end[:10]).date() >= date.today()
    except ValueError:
        return True


def _first_text(row: dict[str, Any], *keys: str, max_len: int | None = None) -> str | None:
    for key in keys:
        value = _clean_text(row.get(key), max_len=max_len)
        if value:
            return value
    return None


def _normalize_mmsi(value: Any) -> str | None:
    raw = _clean_text(value, max_len=32)
    if not raw:
        return None
    digits = re.sub(r"[^0-9]", "", raw)
    if len(digits) == 9:
        return digits
    return None


def fetch_vessel_index(page: int = 1, limit: int = 100000) -> list[dict[str, Any]]:
    payload = _request_json(
        "/kapal/search-kapal-bkp",
        {
            "page": page,
            "limit": limit,
            "sort_by": "nama_kapal",
            "sort_order": "asc",
        },
    )
    data = payload.get("data") or []
    if not isinstance(data, list):
        return []
    return [row for row in data if isinstance(row, dict)]


def fetch_vessel_detail(transmitter_no: str) -> dict[str, Any] | None:
    payload = _request_json("/kapal/data-kapal", {"transmitter_no": transmitter_no})
    data = payload.get("data")
    return data if isinstance(data, dict) and data else None


def fetch_vessel_license(nomor_bkp: str) -> dict[str, Any] | None:
    payload = _request_json("/kapal/data-kapal-izin", {"nomor_bkp": nomor_bkp})
    data = payload.get("data")
    if isinstance(data, list) and data:
        rows = [row for row in data if isinstance(row, dict)]
        if not rows:
            return None

        def _end_date(row: dict[str, Any]) -> str:
            return _clean_text(row.get("tanggal_akhir_sipi")) or ""

        return sorted(rows, key=_end_date, reverse=True)[0]
    return data if isinstance(data, dict) and data else None


def _map_vessel(index_row: dict[str, Any], detail: dict[str, Any] | None) -> dict[str, Any]:
    row = {**index_row, **(detail or {})}
    nomor_bkp = _first_text(row, "no_bkp", "nomor_buku_kapal", max_len=50)
    no_transmitter = _first_text(row, "nomor_transmitter", "transmitter_no", max_len=100)
    ukuran_kapal = _clean_decimal(row.get("ukuran_gt") or row.get("gt_kapal"), allow_zero=True) or Decimal("0")
    kekuatan_mesin = _clean_decimal(row.get("kekuatan_mesin"), allow_zero=True) or Decimal("0")

    return {
        "nama_kapal": _first_text(row, "nama_kapal", max_len=150),
        "nomor_bkp": nomor_bkp,
        "tanda_selar": _first_text(row, "tanda_selar", max_len=100) or UNKNOWN_TEXT,
        "ukuran_kapal": ukuran_kapal,
        "no_transmitter": no_transmitter,
        "pemilik": _first_text(row, "pemilik_kapal", "nama_perusahaan", "penanggung_jawab", max_len=150) or UNKNOWN_TEXT,
        "alat_tangkap": _first_text(row, "alat_tangkap", "jenis_alat_tangkap", max_len=100) or UNKNOWN_TEXT,
        "kekuatan_mesin": kekuatan_mesin,
        "merek_mesin": _first_text(row, "merk_mesin", "merek_mesin", max_len=100) or UNKNOWN_TEXT,
        "wilayah_tangkap": _first_text(row, "nama_wpp", "nama_dpi", max_len=100) or UNKNOWN_TEXT,
        "pelabuhan_pangkalan": _first_text(
            row,
            "nama_pelabuhan_pangkalan",
            "pelabuhan",
            "pelabuhan_pangkalan",
            max_len=150,
        ) or UNKNOWN_TEXT,
        "aktif": _is_active(row),
    }


UPSERT_SQL = """
INSERT INTO master_kapal (
    nama_kapal, nomor_bkp, tanda_selar, ukuran_kapal, no_transmitter,
    pemilik, alat_tangkap, kekuatan_mesin, merek_mesin, wilayah_tangkap,
    pelabuhan_pangkalan, aktif, created_at, updated_at
)
VALUES (
    %(nama_kapal)s, %(nomor_bkp)s, %(tanda_selar)s, %(ukuran_kapal)s, %(no_transmitter)s,
    %(pemilik)s, %(alat_tangkap)s, %(kekuatan_mesin)s, %(merek_mesin)s, %(wilayah_tangkap)s,
    %(pelabuhan_pangkalan)s, %(aktif)s, NOW(), NOW()
)
ON CONFLICT DO NOTHING
"""

MERGE_SQL = """
UPDATE master_kapal
SET
    nama_kapal          = COALESCE(%(nama_kapal)s, master_kapal.nama_kapal),
    nomor_bkp           = COALESCE(master_kapal.nomor_bkp, %(nomor_bkp)s),
    no_transmitter      = COALESCE(%(no_transmitter)s, master_kapal.no_transmitter),
    tanda_selar         = COALESCE(%(tanda_selar)s, NULLIF(NULLIF(master_kapal.tanda_selar, '-'), 'Tidak diketahui')),
    ukuran_kapal        = COALESCE(%(ukuran_kapal)s, NULLIF(master_kapal.ukuran_kapal, 0)),
    pemilik             = COALESCE(%(pemilik)s, NULLIF(NULLIF(master_kapal.pemilik, '-'), 'Tidak diketahui')),
    alat_tangkap        = COALESCE(%(alat_tangkap)s, NULLIF(NULLIF(master_kapal.alat_tangkap, '-'), 'Tidak diketahui')),
    kekuatan_mesin      = COALESCE(%(kekuatan_mesin)s, NULLIF(master_kapal.kekuatan_mesin, 0)),
    merek_mesin         = COALESCE(%(merek_mesin)s, NULLIF(NULLIF(master_kapal.merek_mesin, '-'), 'Tidak diketahui')),
    wilayah_tangkap     = COALESCE(%(wilayah_tangkap)s, NULLIF(NULLIF(master_kapal.wilayah_tangkap, '-'), 'Tidak diketahui')),
    pelabuhan_pangkalan = COALESCE(%(pelabuhan_pangkalan)s, NULLIF(NULLIF(master_kapal.pelabuhan_pangkalan, '-'), 'Tidak diketahui')),
    aktif               = COALESCE(%(aktif)s, master_kapal.aktif),
    updated_at          = NOW()
WHERE
    (%(nomor_bkp)s IS NOT NULL AND master_kapal.nomor_bkp = %(nomor_bkp)s)
    OR
    (%(no_transmitter)s IS NOT NULL AND master_kapal.no_transmitter = %(no_transmitter)s)
"""


MISSING_SQL = """
SELECT no_transmitter
FROM master_kapal
WHERE no_transmitter IS NOT NULL
  AND (
    tanda_selar IN ('', '-', 'Tidak diketahui') OR tanda_selar IS NULL OR
    ukuran_kapal IS NULL OR ukuran_kapal = 0 OR
    pemilik IN ('', '-', 'Tidak diketahui') OR pemilik IS NULL OR
    alat_tangkap IN ('', '-', 'Tidak diketahui') OR alat_tangkap IS NULL OR
    kekuatan_mesin IS NULL OR kekuatan_mesin = 0 OR
    merek_mesin IN ('', '-', 'Tidak diketahui') OR merek_mesin IS NULL OR
    wilayah_tangkap IN ('', '-', 'Tidak diketahui') OR wilayah_tangkap IS NULL OR
    pelabuhan_pangkalan IN ('', '-', 'Tidak diketahui') OR pelabuhan_pangkalan IS NULL
  )
ORDER BY nama_kapal
"""


TRACKING_EXISTS_SQL = """
SELECT 1 FROM master_vessel_tracking
WHERE nomor_bkp = %(nomor_bkp)s
  AND timestamp = %(timestamp)s
  AND latitude = %(latitude)s
  AND longitude = %(longitude)s
LIMIT 1
"""


TRACKING_INSERT_SQL = """
INSERT INTO master_vessel_tracking (
    nama_kapal, nomor_bkp, transmitter_no, mmsi, latitude, longitude,
    direction, speed, timestamp, status_kapal, wpp_id, source, created_at
)
VALUES (
    %(nama_kapal)s, %(nomor_bkp)s, %(transmitter_no)s, %(mmsi)s, %(latitude)s, %(longitude)s,
    %(direction)s, %(speed)s, %(timestamp)s, %(status_kapal)s, %(wpp_id)s, %(source)s, NOW()
)
"""


def _load_wpp_lookup(conn: psycopg2.extensions.connection) -> dict[str, int]:
    with conn.cursor() as cur:
        cur.execute("SELECT id, kode_wpp FROM master_wpp")
        return {code: wpp_id for wpp_id, code in cur.fetchall()}


def _assign_wpp_id(lat: Decimal | None, lon: Decimal | None, wpp_lookup: dict[str, int]) -> int | None:
    wpp_id = get_wpp_id_for_point(float(lat), float(lon)) if lat is not None and lon is not None else None
    if wpp_id:
        return wpp_id
    return None


def _status_from_speed(speed: Decimal | None) -> str | None:
    if speed is None:
        return None
    if speed == 0:
        return "berhenti"
    return "berlayar"


def _sanitize_speed(speed: Decimal | None) -> Decimal | None:
    if speed is None:
        return None
    if speed < 0:
        return None
    # DB column speed is DECIMAL(5,2): max 999.99
    if speed > Decimal("999.99"):
        return None
    return speed


def _sanitize_direction(direction: Decimal | None) -> Decimal | None:
    if direction is None:
        return None
    # Normalize to 0–360 range.
    return direction % Decimal("360")


def fetch_tracking(nomor_bkp: str, interval: int) -> list[dict[str, Any]]:
    timeout = int(os.getenv("KKP_TRACKING_TIMEOUT", "15"))
    retries = int(os.getenv("KKP_TRACKING_RETRIES", "1"))
    payload = _request_json(
        "/kapal/data-kapal-lokasi-interval",
        {"nomor_bkp": nomor_bkp, "interval": interval},
        timeout=timeout,
        max_retries=retries,
    )
    features = payload.get("features") or []
    return [feature for feature in features if isinstance(feature, dict)]


def _parse_tracking_intervals(raw: str | None, default_interval: int) -> list[int]:
    if not raw:
        return [default_interval]
    items: list[int] = []
    for part in raw.split(","):
        text = part.strip()
        if not text:
            continue
        try:
            value = int(text)
        except ValueError:
            continue
        if value > 0:
            items.append(value)
    if not items:
        return [default_interval]
    # Keep order, remove duplicates.
    seen: set[int] = set()
    ordered: list[int] = []
    for value in items:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


def _load_tracking_vessels_from_master(conn: psycopg2.extensions.connection) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH tracking_summary AS (
                SELECT nomor_bkp, MAX(timestamp) AS last_tracking, COUNT(*) AS tracking_count
                FROM master_vessel_tracking
                GROUP BY nomor_bkp
            )
            SELECT mk.nama_kapal, mk.nomor_bkp, mk.no_transmitter
            FROM master_kapal mk
            LEFT JOIN tracking_summary ts ON ts.nomor_bkp = mk.nomor_bkp
            WHERE mk.nomor_bkp IS NOT NULL
              AND NULLIF(TRIM(mk.nomor_bkp), '') IS NOT NULL
              AND mk.nomor_bkp <> '-'
              AND LOWER(TRIM(mk.nomor_bkp)) NOT IN ('0', 'none', 'null', 'unknown', 'tidak diketahui')
              AND mk.no_transmitter IS NOT NULL
              AND NULLIF(TRIM(mk.no_transmitter), '') IS NOT NULL
              AND mk.no_transmitter <> '-'
              AND LOWER(TRIM(mk.no_transmitter)) NOT IN ('0', 'none', 'null', 'unknown', 'tidak diketahui')
            ORDER BY
              (ts.tracking_count IS NOT NULL),
              CASE WHEN mk.nomor_bkp ~ '^[0-9]+$' THEN mk.nomor_bkp::bigint END NULLS LAST,
              ts.last_tracking NULLS FIRST,
              mk.nama_kapal
            """
        )
        rows = cur.fetchall()
    vessels: list[dict[str, Any]] = []
    for nama_kapal, nomor_bkp, no_transmitter in rows:
        vessels.append(
            {
                "nama_kapal": _clean_text(nama_kapal, max_len=150),
                "nomor_bkp": _clean_text(nomor_bkp, max_len=50),
                "no_transmitter": _clean_text(no_transmitter, max_len=100),
            }
        )
    return vessels


def _map_tracking_features(
    vessel: dict[str, Any],
    features: list[dict[str, Any]],
    wpp_lookup: dict[str, int],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    nama_kapal = vessel.get("nama_kapal")
    nomor_bkp = vessel.get("nomor_bkp")
    transmitter_no = vessel.get("no_transmitter")

    for feature in features:
        geometry = feature.get("geometry") or {}
        if geometry.get("type") != "Point":
            continue
        coords = geometry.get("coordinates") or []
        if len(coords) < 2:
            continue
        props = feature.get("properties") or {}
        lon = _clean_decimal(coords[0])
        lat = _clean_decimal(coords[1])
        ping_time = _first_text(props, "ping_time", "last_ping")
        if lat is None or lon is None or not ping_time:
            continue
        speed = _clean_decimal(props.get("speed"), allow_zero=True)
        if speed is None:
            speed = _clean_decimal(props.get("average_speed"), allow_zero=True)
        speed = _sanitize_speed(speed)
        direction = _clean_decimal(props.get("heading"), allow_zero=True)
        if direction is None:
            direction = _clean_decimal(props.get("last_heading"), allow_zero=True)
        direction = _sanitize_direction(direction)
        mmsi = _normalize_mmsi(props.get("mmsi")) or _normalize_mmsi(props.get("MMSI"))
        if mmsi is None:
            mmsi = _normalize_mmsi(transmitter_no)
        if mmsi is None:
            mmsi = _clean_text(transmitter_no, max_len=20)
        rows.append({
            "nama_kapal": nama_kapal,
            "nomor_bkp": _clean_text(props.get("nomor_bkp"), max_len=50) or nomor_bkp,
            "transmitter_no": transmitter_no,
            "mmsi": mmsi,
            "latitude": lat,
            "longitude": lon,
            "direction": direction,
            "speed": speed if speed is not None else Decimal("0"),
            "timestamp": ping_time,
            "status_kapal": _status_from_speed(speed),
            "wpp_id": _assign_wpp_id(lat, lon, wpp_lookup),
            "source": "VMS",
        })
    return rows


def _upsert_tracking_rows(conn: psycopg2.extensions.connection, rows: list[dict[str, Any]]) -> tuple[int, int]:
    if not rows:
        return 0, 0

    columns = [
        "nama_kapal",
        "nomor_bkp",
        "transmitter_no",
        "mmsi",
        "latitude",
        "longitude",
        "direction",
        "speed",
        "timestamp",
        "status_kapal",
        "wpp_id",
        "source",
    ]
    values = [tuple(row.get(column) for column in columns) for row in rows]
    template = "(" + ",".join(["%s"] * len(columns)) + ")"

    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TEMP TABLE tmp_tracking_upsert (
                nama_kapal text,
                nomor_bkp text,
                transmitter_no text,
                mmsi text,
                latitude numeric,
                longitude numeric,
                direction numeric,
                speed numeric,
                timestamp timestamp,
                status_kapal text,
                wpp_id integer,
                source text
            ) ON COMMIT DROP
            """
        )
        psycopg2.extras.execute_values(
            cur,
            f"INSERT INTO tmp_tracking_upsert ({', '.join(columns)}) VALUES %s",
            values,
            template=template,
            page_size=5000,
        )
        cur.execute(
            """
            CREATE TEMP TABLE tmp_tracking_dedup ON COMMIT DROP AS
            SELECT DISTINCT ON (nomor_bkp, timestamp, latitude, longitude) *
            FROM tmp_tracking_upsert
            WHERE nomor_bkp IS NOT NULL
              AND timestamp IS NOT NULL
              AND latitude IS NOT NULL
              AND longitude IS NOT NULL
            ORDER BY nomor_bkp, timestamp, latitude, longitude
            """
        )
        cur.execute("SELECT count(*) FROM tmp_tracking_dedup")
        dedup_count = cur.fetchone()[0]
        cur.execute(
            """
            INSERT INTO master_vessel_tracking (
                nama_kapal, nomor_bkp, transmitter_no, mmsi, latitude, longitude,
                direction, speed, timestamp, status_kapal, wpp_id, source, created_at
            )
            SELECT
                s.nama_kapal, s.nomor_bkp, s.transmitter_no, s.mmsi, s.latitude, s.longitude,
                s.direction, s.speed, s.timestamp, s.status_kapal, s.wpp_id, s.source, NOW()
            FROM tmp_tracking_dedup s
            WHERE NOT EXISTS (
                SELECT 1
                FROM master_vessel_tracking t
                WHERE t.nomor_bkp = s.nomor_bkp
                  AND t.timestamp = s.timestamp
                  AND t.latitude = s.latitude
                  AND t.longitude = s.longitude
            )
            """
        )
        inserted = max(cur.rowcount, 0)
    skipped = max(dedup_count - inserted, 0)
    return inserted, skipped


def _fetch_tracking_rows_for_vessel(
    vessel: dict[str, Any],
    tracking_interval: int,
    wpp_lookup: dict[str, int],
) -> list[dict[str, Any]]:
    features = fetch_tracking(vessel["nomor_bkp"], interval=tracking_interval)
    return _map_tracking_features(vessel, features, wpp_lookup)


def collect(
    limit: int = 100000,
    max_vessels: int | None = None,
    only_missing: bool = False,
    collect_tracking: bool = False,
    interval: int = 30,
    tracking_intervals: list[int] | None = None,
    tracking_only_from_master: bool = False,
    batch_size: int = 1000,
    enrich: bool = True,
    enrich_license: bool = True,
    sleep_seconds: float = 0.08,
    workers: int = 1,
    shard_index: int = 0,
    shard_count: int = 1,
) -> dict[str, int]:
    conn = _get_db_conn()
    fetched = enriched = upserted = skipped = tracking_inserted = tracking_skipped = 0
    try:
        wpp_lookup = _load_wpp_lookup(conn)
        if tracking_only_from_master:
            collect_tracking = True
            vessel_rows = _load_tracking_vessels_from_master(conn)
            if max_vessels:
                vessel_rows = vessel_rows[:max_vessels]
            if shard_count > 1:
                vessel_rows = [
                    vessel
                    for idx, vessel in enumerate(vessel_rows)
                    if idx % shard_count == shard_index
                ]
                logger.info(
                    "Tracking shard %d/%d selected %d vessels",
                    shard_index + 1,
                    shard_count,
                    len(vessel_rows),
                )
            intervals = tracking_intervals or [interval]
            for offset in range(0, len(vessel_rows), batch_size):
                chunk = vessel_rows[offset : offset + batch_size]
                tracking_rows: list[dict[str, Any]] = []
                jobs = [
                    (vessel, tracking_interval)
                    for vessel in chunk
                    if vessel.get("nomor_bkp")
                    for tracking_interval in intervals
                ]
                skipped += len(chunk) - len({id(vessel) for vessel, _ in jobs})
                logger.info(
                    "Tracking-only chunk %d/%d vessels=%d jobs=%d workers=%d",
                    min(offset + len(chunk), len(vessel_rows)),
                    len(vessel_rows),
                    len(chunk),
                    len(jobs),
                    workers,
                )

                if workers > 1:
                    request_timeout = int(os.getenv("KKP_TRACKING_TIMEOUT", "15"))
                    chunk_timeout = float(
                        os.getenv(
                            "KKP_TRACKING_CHUNK_TIMEOUT",
                            str(max(20, request_timeout * ((len(jobs) + workers - 1) // workers + 1))),
                        )
                    )
                    executor = ThreadPoolExecutor(max_workers=workers)
                    future_map = {
                        executor.submit(_fetch_tracking_rows_for_vessel, vessel, tracking_interval, wpp_lookup): (
                            vessel,
                            tracking_interval,
                        )
                        for vessel, tracking_interval in jobs
                    }
                    completed = 0
                    try:
                        for future in as_completed(future_map, timeout=chunk_timeout):
                            vessel, tracking_interval = future_map[future]
                            completed += 1
                            try:
                                tracking_rows.extend(future.result())
                            except Exception as exc:  # noqa: BLE001 - keep the batch moving on flaky KKP requests.
                                logger.warning(
                                    "Tracking request failed for nomor_bkp=%s interval=%s: %s",
                                    vessel.get("nomor_bkp"),
                                    tracking_interval,
                                    exc,
                                )
                    except FuturesTimeoutError:
                        pending = len(future_map) - completed
                        logger.warning(
                            "Tracking chunk timed out after %.1fs; committing partial rows and skipping %d pending jobs",
                            chunk_timeout,
                            pending,
                        )
                        for future in future_map:
                            if not future.done():
                                future.cancel()
                    finally:
                        executor.shutdown(wait=False, cancel_futures=True)
                else:
                    for vessel, tracking_interval in jobs:
                        tracking_rows.extend(_fetch_tracking_rows_for_vessel(vessel, tracking_interval, wpp_lookup))
                        if sleep_seconds > 0:
                            time.sleep(sleep_seconds)

                fetched += len(chunk)
                ins, skp = _upsert_tracking_rows(conn, tracking_rows)
                conn.commit()
                tracking_inserted += ins
                tracking_skipped += skp
                logger.info(
                    "Tracking-only progress %d/%d fetched=%d rows=%d tracking_ins=%d tracking_skip=%d",
                    min(offset + len(chunk), len(vessel_rows)),
                    len(vessel_rows),
                    fetched,
                    len(tracking_rows),
                    tracking_inserted,
                    tracking_skipped,
                )
            summary = {
                "fetched": fetched,
                "enriched": enriched,
                "upserted": upserted,
                "skipped": skipped,
                "tracking_inserted": tracking_inserted,
                "tracking_skipped": tracking_skipped,
            }
            logger.info("KKP Datamart tracking-only collection done: %s", summary)
            return summary

        if only_missing:
            with conn.cursor() as cur:
                cur.execute(MISSING_SQL)
                transmitters = [row[0] for row in cur.fetchall()]
            index_rows = [{"transmitter_no": tx} for tx in transmitters]
        else:
            index_rows = fetch_vessel_index(limit=limit)

        if max_vessels:
            index_rows = index_rows[:max_vessels]

        with conn.cursor() as cur:
            for idx, index_row in enumerate(index_rows, start=1):
                tx = _first_text(index_row, "transmitter_no", "nomor_transmitter", max_len=100)
                if not tx:
                    tx = None

                detail = fetch_vessel_detail(tx) if enrich else None
                raw_bkp = _first_text(detail or {}, "no_bkp", "nomor_buku_kapal", max_len=50) or _first_text(index_row, "nomor_buku_kapal", "no_bkp", max_len=50)
                license_row = fetch_vessel_license(raw_bkp) if enrich and enrich_license and raw_bkp else None
                fetched += 1
                if detail:
                    enriched += 1

                merged_detail = {**(detail or {}), **(license_row or {})}
                mapped = _map_vessel(index_row, merged_detail)
                if not mapped["no_transmitter"] and not mapped["nomor_bkp"]:
                    skipped += 1
                    continue

                cur.execute(UPSERT_SQL, mapped)
                cur.execute(MERGE_SQL, mapped)
                upserted += 1

                if collect_tracking and mapped.get("nomor_bkp"):
                    intervals = tracking_intervals or [interval]
                    for tracking_interval in intervals:
                        features = fetch_tracking(mapped["nomor_bkp"], interval=tracking_interval)
                        tracking_rows = _map_tracking_features(mapped, features, wpp_lookup)
                        ins, skp = _upsert_tracking_rows(conn, tracking_rows)
                        tracking_inserted += ins
                        tracking_skipped += skp

                if upserted % batch_size == 0:
                    conn.commit()
                    logger.info(
                        "Progress %d/%d fetched=%d enriched=%d upserted=%d tracking_ins=%d tracking_skip=%d",
                        idx, len(index_rows), fetched, enriched, upserted, tracking_inserted, tracking_skipped,
                    )
                if sleep_seconds > 0:
                    time.sleep(sleep_seconds)
        conn.commit()
    finally:
        conn.close()

    summary = {
        "fetched": fetched,
        "enriched": enriched,
        "upserted": upserted,
        "skipped": skipped,
        "tracking_inserted": tracking_inserted,
        "tracking_skipped": tracking_skipped,
    }
    logger.info("KKP Datamart vessel collection done: %s", summary)
    return summary


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect KKP Datamart vessel master data into master_kapal")
    parser.add_argument("--limit", type=int, default=100000, help="Search endpoint limit (default: 100000)")
    parser.add_argument("--max-vessels", type=int, help="Cap processed vessels for test runs")
    parser.add_argument("--only-missing", action="store_true", help="Only enrich rows with placeholder/missing fields")
    parser.add_argument("--interval", type=int, default=30, help="Tracking location interval in minutes")
    parser.add_argument(
        "--tracking-intervals",
        type=str,
        default="",
        help="Comma-separated tracking intervals in minutes (e.g. 30,720,10080,43200,525600)",
    )
    parser.add_argument(
        "--tracking-only-from-master",
        action="store_true",
        help="Skip vessel enrichment and collect tracking for existing master_kapal rows only",
    )
    parser.add_argument("--batch-size", type=int, default=1000, help="Commit every N vessels")
    parser.add_argument("--no-tracking", action="store_true", help="Skip vessel tracking collection")
    parser.add_argument("--no-enrich", action="store_true", help="Skip data-kapal detail enrichment")
    parser.add_argument("--no-license-enrich", action="store_true", help="Skip data-kapal-izin enrichment")
    parser.add_argument("--sleep", type=float, default=0.08, help="Delay between detail requests in seconds")
    parser.add_argument("--workers", type=int, default=1, help="Concurrent request workers for tracking-only collection")
    parser.add_argument("--shard-index", type=int, default=0, help="Zero-based shard index for tracking-only collection")
    parser.add_argument("--shard-count", type=int, default=1, help="Total shard count for tracking-only collection")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    print(collect(
        limit=args.limit,
        max_vessels=args.max_vessels,
        only_missing=args.only_missing,
        collect_tracking=not args.no_tracking,
        interval=args.interval,
        tracking_intervals=_parse_tracking_intervals(args.tracking_intervals, args.interval),
        tracking_only_from_master=args.tracking_only_from_master,
        batch_size=args.batch_size,
        enrich=not args.no_enrich,
        enrich_license=not args.no_license_enrich,
        sleep_seconds=args.sleep,
        workers=max(1, args.workers),
        shard_index=max(0, args.shard_index),
        shard_count=max(1, args.shard_count),
    ))
