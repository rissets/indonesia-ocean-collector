#!/usr/bin/env python3
"""Dump/import VMS tracking history into maritime-os.

Source table:
    public.api_pusdal_lastdatavms_det

The source connection is always opened with PostgreSQL read-only settings.
The script can:
  1. plan chunks by year/month/week,
  2. dump source chunks to gzip CSV files,
  3. load those dumps into master_vessel_tracking with idempotent inserts.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import logging
import os
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterator

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

LOGGER = logging.getLogger("import_vms_tracking")

SOURCE_TABLE = "public.api_pusdal_lastdatavms_det"
DEFAULT_DUMP_DIR = "data/vms_dumps"


@dataclass(frozen=True)
class Chunk:
    start: date
    end: date
    label: str
    rows: int | None = None
    bytes_estimate: int | None = None


def _dsn_from_env(prefix: str) -> str:
    explicit = os.getenv(f"{prefix}_DSN")
    if explicit:
        return explicit

    host = os.getenv(f"{prefix}_HOST")
    port = os.getenv(f"{prefix}_PORT", "5432")
    db = os.getenv(f"{prefix}_DB") or os.getenv(f"{prefix}_DATABASE") or os.getenv(f"{prefix}_NAME")
    user = os.getenv(f"{prefix}_USER")
    password = os.getenv(f"{prefix}_PASSWORD")
    missing = [
        name
        for name, value in {
            f"{prefix}_HOST": host,
            f"{prefix}_DB": db,
            f"{prefix}_USER": user,
            f"{prefix}_PASSWORD": password,
        }.items()
        if not value
    ]
    if missing:
        raise RuntimeError(f"Missing env vars: {', '.join(missing)}")

    return (
        f"host={host} port={port} dbname={db} user={user} "
        f"password={password} connect_timeout={os.getenv(f'{prefix}_CONNECT_TIMEOUT', '15')}"
    )


@contextmanager
def source_conn():
    dsn = _dsn_from_env("VMS_SOURCE")
    conn = psycopg2.connect(
        dsn,
        options="-c default_transaction_read_only=on -c statement_timeout=0",
    )
    conn.set_session(readonly=True, autocommit=False)
    try:
        with conn.cursor() as cur:
            cur.execute("SET default_transaction_read_only = on")
            cur.execute("SET TRANSACTION READ ONLY")
        yield conn
    finally:
        conn.close()


@contextmanager
def target_conn():
    dsn = os.getenv("MARITIME_TARGET_DSN")
    if not dsn:
        dsn = _dsn_from_env("DB")
    conn = psycopg2.connect(dsn)
    try:
        yield conn
    finally:
        conn.close()


def check_source_privileges() -> dict[str, bool]:
    sql = """
    SELECT
      has_table_privilege(current_user, %s, 'SELECT') AS can_select,
      has_table_privilege(current_user, %s, 'INSERT') AS can_insert,
      has_table_privilege(current_user, %s, 'UPDATE') AS can_update,
      has_table_privilege(current_user, %s, 'DELETE') AS can_delete
    """
    with source_conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, (SOURCE_TABLE, SOURCE_TABLE, SOURCE_TABLE, SOURCE_TABLE))
        return dict(cur.fetchone())


def source_range() -> tuple[date, date]:
    with source_conn() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT min(ping_time)::date, max(ping_time)::date FROM {SOURCE_TABLE}")
        min_date, max_date = cur.fetchone()
    if not min_date or not max_date:
        raise RuntimeError("Source VMS table is empty or ping_time is unavailable")
    return min_date, max_date


def _month_start(d: date) -> date:
    return date(d.year, d.month, 1)


def _add_month(d: date) -> date:
    if d.month == 12:
        return date(d.year + 1, 1, 1)
    return date(d.year, d.month + 1, 1)


def _iter_years(start: date, end: date) -> Iterator[Chunk]:
    year = start.year
    while year <= end.year:
        chunk_start = max(start, date(year, 1, 1))
        chunk_end = min(end, date(year, 12, 31))
        yield Chunk(chunk_start, chunk_end, str(year))
        year += 1


def _iter_months(start: date, end: date) -> Iterator[Chunk]:
    current = _month_start(start)
    while current <= end:
        next_month = _add_month(current)
        chunk_start = max(start, current)
        chunk_end = min(end, next_month - timedelta(days=1))
        yield Chunk(chunk_start, chunk_end, current.strftime("%Y%m"))
        current = next_month


def _iter_weeks(start: date, end: date) -> Iterator[Chunk]:
    current = start
    index = 1
    while current <= end:
        chunk_end = min(end, current + timedelta(days=6))
        yield Chunk(current, chunk_end, f"{current:%Y%m%d}_w{index:02d}")
        current = chunk_end + timedelta(days=1)
        index += 1


def iter_chunks(start: date, end: date, mode: str) -> Iterator[Chunk]:
    if mode == "year":
        return _iter_years(start, end)
    if mode == "month":
        return _iter_months(start, end)
    if mode == "week":
        return _iter_weeks(start, end)
    raise ValueError(f"Unsupported chunk mode: {mode}")


def estimate_chunk(chunk: Chunk) -> Chunk:
    sql = f"""
    SELECT count(*)::bigint AS rows,
           coalesce(sum(pg_column_size(t)), 0)::bigint AS bytes_estimate
    FROM {SOURCE_TABLE} t
    WHERE ping_time >= %s
      AND ping_time < %s
    """
    with source_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (chunk.start, chunk.end + timedelta(days=1)))
        rows, bytes_estimate = cur.fetchone()
    return Chunk(chunk.start, chunk.end, chunk.label, int(rows), int(bytes_estimate))


def plan_chunks(start: date, end: date, mode: str, max_bytes: int) -> list[Chunk]:
    base = list(iter_chunks(start, end, mode))
    planned: list[Chunk] = []
    for chunk in base:
        estimated = estimate_chunk(chunk)
        if mode == "year" and estimated.bytes_estimate and estimated.bytes_estimate > max_bytes:
            LOGGER.info(
                "Year %s is %s bytes; splitting into months",
                chunk.label,
                estimated.bytes_estimate,
            )
            planned.extend(estimate_chunk(month) for month in _iter_months(chunk.start, chunk.end))
        else:
            planned.append(estimated)
    return planned


def dump_path(dump_dir: Path, chunk: Chunk) -> Path:
    return dump_dir / f"vms_tracking_{chunk.label}_{chunk.start}_{chunk.end}.csv.gz"


def dump_chunk(chunk: Chunk, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    sql = f"""
    COPY (
      SELECT
        nomor_buku_kapal,
        transmitter_no,
        ping_time,
        last_latitude,
        last_longitude,
        speed,
        heading
      FROM {SOURCE_TABLE}
      WHERE ping_time >= DATE %s
        AND ping_time < DATE %s
        AND ping_time IS NOT NULL
        AND last_latitude IS NOT NULL
        AND last_longitude IS NOT NULL
      ORDER BY ping_time, nomor_buku_kapal, transmitter_no
    ) TO STDOUT WITH CSV HEADER
    """
    LOGGER.info("Dumping %s to %s", chunk.label, output_path)
    with source_conn() as conn, conn.cursor() as cur:
        with gzip.open(output_path, "wt", newline="") as gz:
            cur.copy_expert(cur.mogrify(sql, (chunk.start, chunk.end + timedelta(days=1))).decode(), gz)


def target_index_exists(conn) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM pg_indexes
                WHERE schemaname = 'public'
                  AND tablename = 'master_vessel_tracking'
                  AND indexname = 'idx_master_vessel_tracking_bkp_tx_ts_lat_lon'
            )
            """
        )
        return bool(cur.fetchone()[0])


def ensure_target_indexes(conn) -> None:
    if target_index_exists(conn):
        LOGGER.info("Target duplicate-check index already exists")
        return

    LOGGER.info("Creating target duplicate-check index. This can take a while once on a large table.")
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_master_vessel_tracking_bkp_tx_ts_lat_lon
            ON master_vessel_tracking (nomor_bkp, transmitter_no, timestamp, latitude, longitude)
            """
        )
    conn.commit()
    LOGGER.info("Target index check done")


def create_target_index_concurrently() -> None:
    with target_conn() as conn:
        conn.set_session(autocommit=True)
        if target_index_exists(conn):
            LOGGER.info("Target duplicate-check index already exists")
            return
        LOGGER.info("Creating target duplicate-check index CONCURRENTLY")
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE INDEX CONCURRENTLY idx_master_vessel_tracking_bkp_tx_ts_lat_lon
                ON master_vessel_tracking (nomor_bkp, transmitter_no, timestamp, latitude, longitude)
                """
            )
        LOGGER.info("Concurrent target index creation done")


def load_dump(path: Path, batch_label: str, skip_target_index_check: bool = False) -> tuple[int, int]:
    LOGGER.info("Loading %s", path)
    with target_conn() as conn:
        if skip_target_index_check:
            LOGGER.info("Skipping target duplicate-check index check")
        else:
            ensure_target_indexes(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TEMP TABLE tmp_vms_tracking_import (
                    nomor_buku_kapal text,
                    transmitter_no text,
                    ping_time timestamp,
                    last_latitude double precision,
                    last_longitude double precision,
                    speed double precision,
                    heading double precision
                ) ON COMMIT DROP
                """
            )
            with gzip.open(path, "rt", newline="") as gz:
                cur.copy_expert(
                    """
                    COPY tmp_vms_tracking_import (
                        nomor_buku_kapal, transmitter_no, ping_time,
                        last_latitude, last_longitude, speed, heading
                    ) FROM STDIN WITH CSV HEADER
                    """,
                    gz,
                )
            cur.execute("SELECT count(*) FROM tmp_vms_tracking_import")
            staged = int(cur.fetchone()[0])
            LOGGER.info("Staged %d raw rows for %s", staged, batch_label)

            LOGGER.info("Normalizing and deduplicating staged rows")
            cur.execute(
                """
                CREATE TEMP TABLE tmp_vms_tracking_clean AS
                SELECT DISTINCT ON (
                    nomor_bkp, transmitter_no, timestamp, latitude, longitude
                )
                    nullif(trim(nomor_buku_kapal), '') AS nomor_bkp,
                    nullif(trim(transmitter_no), '') AS transmitter_no,
                    ping_time AS timestamp,
                    last_latitude::numeric(9,6) AS latitude,
                    last_longitude::numeric(9,6) AS longitude,
                    CASE
                        WHEN heading BETWEEN 0 AND 360 THEN heading::numeric(5,2)
                        ELSE NULL
                    END AS direction,
                    CASE
                        WHEN speed BETWEEN 0 AND 999.99 THEN speed::numeric(5,2)
                        ELSE NULL
                    END AS speed,
                    CASE
                        WHEN speed IS NULL OR speed < 0 OR speed >= 999.99 THEN 'tidak diketahui'
                        WHEN speed <= 0.5 THEN 'berhenti'
                        ELSE 'berlayar'
                    END AS status_kapal
                FROM tmp_vms_tracking_import
                WHERE ping_time IS NOT NULL
                  AND last_latitude BETWEEN -90 AND 90
                  AND last_longitude BETWEEN -180 AND 180
                ORDER BY nomor_bkp, transmitter_no, timestamp, latitude, longitude
                """
            )
            cur.execute(
                """
                CREATE INDEX tmp_vms_tracking_clean_key_idx
                ON tmp_vms_tracking_clean (nomor_bkp, transmitter_no, timestamp, latitude, longitude)
                """
            )
            cur.execute("ANALYZE tmp_vms_tracking_clean")
            cur.execute("SELECT count(*) FROM tmp_vms_tracking_clean")
            cleaned = int(cur.fetchone()[0])
            LOGGER.info("Cleaned %d rows for %s", cleaned, batch_label)

            LOGGER.info("Preparing existing-key cache for duplicate skip")
            cur.execute(
                """
                CREATE TEMP TABLE tmp_vms_existing AS
                SELECT
                    t.nomor_bkp,
                    t.transmitter_no,
                    t.timestamp,
                    t.latitude,
                    t.longitude
                FROM master_vessel_tracking t
                WHERE t.timestamp >= (SELECT min(timestamp) FROM tmp_vms_tracking_clean)
                  AND t.timestamp <= (SELECT max(timestamp) FROM tmp_vms_tracking_clean)
                """
            )
            cur.execute(
                """
                CREATE INDEX tmp_vms_existing_key_idx
                ON tmp_vms_existing (nomor_bkp, transmitter_no, timestamp, latitude, longitude)
                """
            )
            cur.execute("ANALYZE tmp_vms_existing")

            LOGGER.info("Preparing vessel-name lookup")
            cur.execute(
                """
                CREATE TEMP TABLE tmp_vms_kapal_lookup AS
                SELECT DISTINCT ON (key_type, key_value)
                    key_type,
                    key_value,
                    nama_kapal
                FROM (
                    SELECT 'bkp' AS key_type, nullif(trim(nomor_bkp), '') AS key_value, nama_kapal, 0 AS priority, updated_at
                    FROM master_kapal
                    WHERE nullif(trim(nomor_bkp), '') IS NOT NULL
                    UNION ALL
                    SELECT 'tx' AS key_type, nullif(trim(no_transmitter), '') AS key_value, nama_kapal, 1 AS priority, updated_at
                    FROM master_kapal
                    WHERE nullif(trim(no_transmitter), '') IS NOT NULL
                ) s
                WHERE key_value IS NOT NULL
                ORDER BY key_type, key_value, priority, updated_at DESC NULLS LAST
                """
            )
            cur.execute("CREATE INDEX tmp_vms_kapal_lookup_idx ON tmp_vms_kapal_lookup (key_type, key_value)")
            cur.execute("ANALYZE tmp_vms_kapal_lookup")

            LOGGER.info("Inserting new tracking rows into target")
            cur.execute(
                """
                INSERT INTO master_vessel_tracking (
                    nama_kapal,
                    nomor_bkp,
                    transmitter_no,
                    mmsi,
                    latitude,
                    longitude,
                    direction,
                    speed,
                    timestamp,
                    status_kapal,
                    wpp_id,
                    source,
                    created_at
                )
                SELECT
                    COALESCE(k_bkp.nama_kapal, k_tx.nama_kapal, 'Tidak diketahui') AS nama_kapal,
                    s.nomor_bkp,
                    s.transmitter_no,
                    NULL AS mmsi,
                    s.latitude,
                    s.longitude,
                    s.direction,
                    s.speed,
                    s.timestamp,
                    s.status_kapal,
                    NULL AS wpp_id,
                    'VMS_DB' AS source,
                    now()
                FROM tmp_vms_tracking_clean s
                LEFT JOIN tmp_vms_kapal_lookup k_bkp
                  ON k_bkp.key_type = 'bkp'
                 AND k_bkp.key_value = s.nomor_bkp
                LEFT JOIN tmp_vms_kapal_lookup k_tx
                  ON k_tx.key_type = 'tx'
                 AND k_tx.key_value = s.transmitter_no
                LEFT JOIN tmp_vms_existing e
                  ON e.nomor_bkp IS NOT DISTINCT FROM s.nomor_bkp
                 AND e.transmitter_no IS NOT DISTINCT FROM s.transmitter_no
                 AND e.timestamp = s.timestamp
                 AND e.latitude = s.latitude
                 AND e.longitude = s.longitude
                WHERE e.timestamp IS NULL
                """
            )
            inserted = max(cur.rowcount, 0)
        conn.commit()
    LOGGER.info("Loaded %s: staged=%d inserted=%d", batch_label, staged, inserted)
    return staged, inserted


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def main() -> int:
    load_dotenv()

    parser = argparse.ArgumentParser(description="Dump/import VMS tracking into maritime-os")
    parser.add_argument("--start", type=parse_date, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", type=parse_date, help="End date YYYY-MM-DD")
    parser.add_argument("--chunk", choices=["year", "month", "week"], default="year")
    parser.add_argument("--max-bytes", type=int, default=1_000_000_000)
    parser.add_argument("--dump-dir", default=DEFAULT_DUMP_DIR)
    parser.add_argument("--plan-only", action="store_true")
    parser.add_argument("--dump-only", action="store_true")
    parser.add_argument("--load-only", action="store_true")
    parser.add_argument("--skip-target-index-check", action="store_true")
    parser.add_argument("--create-target-index-concurrently", action="store_true")
    parser.add_argument("--require-source-readonly-privileges", action="store_true")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level.upper()), format="%(asctime)s %(levelname)s %(message)s")

    if args.create_target_index_concurrently:
        create_target_index_concurrently()
        return 0

    if args.load_only:
        if not args.start or not args.end:
            raise RuntimeError("--load-only requires --start and --end so dump filenames can be resolved without querying VMS source")
        start = args.start
        end = args.end
        chunks = list(iter_chunks(start, end, args.chunk))
        LOGGER.info("Load-only mode: skipping source privilege/range/plan queries")
    else:
        privileges = check_source_privileges()
        LOGGER.info("Source privileges: %s", privileges)
        if not privileges.get("can_select"):
            raise RuntimeError("Source user cannot SELECT from VMS table")
        writable = any(privileges.get(key) for key in ("can_insert", "can_update", "can_delete"))
        if writable and args.require_source_readonly_privileges:
            raise RuntimeError(f"Source user has write privileges: {privileges}")
        if writable:
            LOGGER.warning("Source user has write privileges, but this script forces all source transactions READ ONLY.")

        min_date, max_date = source_range()
        start = args.start or min_date
        end = args.end or max_date
        if start < min_date:
            start = min_date
        if end > max_date:
            end = max_date

        chunks = plan_chunks(start, end, args.chunk, args.max_bytes)

    dump_dir = Path(args.dump_dir)
    for chunk in chunks:
        LOGGER.info(
            "PLAN label=%s start=%s end=%s rows=%s bytes=%s",
            chunk.label,
            chunk.start,
            chunk.end,
            chunk.rows,
            chunk.bytes_estimate,
        )

    if args.plan_only:
        return 0

    for chunk in chunks:
        path = dump_path(dump_dir, chunk)
        if not args.load_only:
            dump_chunk(chunk, path)
        if not args.dump_only:
            load_dump(path, chunk.label, skip_target_index_check=args.skip_target_index_check)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
