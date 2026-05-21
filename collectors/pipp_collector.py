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
import re
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
RETRY_BACKOFF = 2.0

# FAO 3-alpha codes mapped from latin names (most common Indonesian species)
_LATIN_TO_FAO: dict[str, str] = {
    "katsuwonus pelamis": "SKJ",
    "thunnus albacares": "YFT",
    "thunnus obesus": "BET",
    "thunnus alalunga": "ALB",
    "thunnus tonggol": "LOT",
    "auxis thazard": "FRI",
    "auxis rochei": "BLT",
    "euthynnus affinis": "KAW",
    "rastrelliger kanagurta": "RAK",
    "rastrelliger brachysoma": "RAS",
    "decapterus russelli": "RUS",
    "decapterus macrosoma": "LAY",
    "decapterus maruadsi": "MRS",
    "selar crumenophthalmus": "BIG",
    "selaroides leptolepis": "YTS",
    "sardinella lemuru": "SLM",
    "sardinella gibbosa": "SAG",
    "amblygaster sirm": "SIR",
    "stolephorus spp": "STO",
    "engraulis spp": "ENG",
    "scomberomorus commerson": "COM",
    "scomberomorus guttatus": "GUT",
    "acanthocybium solandri": "WAH",
    "istiophorus platypterus": "SAI",
    "makaira mazara": "BUM",
    "xiphias gladius": "SWO",
    "coryphaena hippurus": "DOL",
    "loligo spp": "SQC",
    "sthenoteuthis oualaniensis": "OUM",
    "octopus spp": "OCT",
    "penaeus monodon": "GIT",
    "penaeus merguiensis": "BAP",
    "metapenaeus spp": "MET",
    "portunus pelagicus": "SWB",
    "portunus spp": "CRA",
    "scylla serrata": "MUD",
    "lutjanus malabaricus": "LJM",
    "lutjanus sebae": "EMB",
    "lutjanus russelli": "RUS",
    "epinephelus spp": "GPX",
    "siganus javus": "SIJ",
    "siganus guttatus": "SIG",
    "mugil cephalus": "MUL",
    "pampus argenteus": "SIL",
    "parastromateus niger": "BLB",
    "sphyraena jello": "BAR",
    "sphyraena obtusata": "OBT",
    "rachycentron canadum": "CBA",
    "ruvettus pretiosus": "OIL",
    "trichiurus lepturus": "LHT",
    "nemipterus spp": "NEM",
    "upeneus spp": "GOX",
    "johnius spp": "CRO",
    "pennahia argentata": "SIL",
    "saurida tumbil": "GRE",
    "tylosurus crocodilus": "NED",
    "chirocentrus dorab": "DOB",
    "ilisha elongata": "ILI",
    "carcharhinus spp": "SHX",
    "isurus spp": "MAK",
    "scoliodon laticaudus": "PCS",
}


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


def _extract_wpp(dpi_operasi: str | None, wpp_operasi: str | None, wpp_pelabuhan: str | None) -> str | None:
    """
    Extract canonical WPP-7xx code. Priority:
    1. dpi_operasi contains explicit WPP code like "WPP-NRI 712"
    2. wpp_operasi / wpp_pelabuhan free-text mapping
    """
    # Try dpi_operasi first — it has the most reliable WPP number
    for src in [dpi_operasi, wpp_operasi, wpp_pelabuhan]:
        if not src:
            continue
        # Match patterns like "WPP-NRI 712", "WPP 712", "WPP-RI 712", "WPP-712"
        m = re.search(r'WPP[-\s](?:NRI|RI)?\s*[-]?\s*(\d{3})', src, re.IGNORECASE)
        if m:
            return f"WPP-{m.group(1)}"

    # Fallback: free-text keyword mapping
    mapping = {
        "laut jawa": "WPP-712",
        "utara jawa": "WPP-712",
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
        "laut andaman": "WPP-571",
        "laut arafuru": "WPP-718",
        "laut aru": "WPP-718",
        "laut timor": "WPP-718",
        "teluk cendrawasih": "WPP-717",
        "samudera pasifik": "WPP-717",
        "laut china selatan": "WPP-711",
    }
    for src in [wpp_operasi, wpp_pelabuhan]:
        if not src:
            continue
        lower = src.lower()
        for key, code in mapping.items():
            if key in lower:
                return code

    return None


def _fao_code_from_latin(latin: str | None) -> str | None:
    """Look up FAO 3-alpha code from latin name."""
    if not latin:
        return None
    return _LATIN_TO_FAO.get(latin.strip().lower())


def _compute_lama_trip(tgl_berangkat: str | None, tgl_bongkar: str | None) -> int | None:
    """Compute trip duration in days from departure and landing dates."""
    if not tgl_berangkat or not tgl_bongkar:
        return None
    try:
        fmt = "%Y-%m-%d"
        d_berangkat = datetime.strptime(tgl_berangkat[:10], fmt).date()
        d_bongkar = datetime.strptime(tgl_bongkar[:10], fmt).date()
        days = (d_bongkar - d_berangkat).days
        return days if days >= 0 else None
    except (ValueError, TypeError):
        return None


def _flatten_records(activities: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Flatten nested API response into one row per (vessel activity × fish species).
    Each activity has a `hasil` list; we emit one row per hasil entry.
    """
    rows = []
    for act in activities:
        hasil_list = act.get("hasil") or []
        if not hasil_list:
            hasil_list = [{}]

        total_kg = sum(
            float(h.get("jml_ikan") or 0) for h in act.get("hasil") or []
        )

        # nomor_bkp: use nomor_buku_kapal when it's a real value (> 0)
        raw_bkp = act.get("nomor_buku_kapal")
        nomor_bkp = str(raw_bkp).strip() if raw_bkp and str(raw_bkp).strip() not in ("0", "", "None") else None

        lama_trip = _compute_lama_trip(act.get("tgl_berangkat"), act.get("tgl_bongkar") or act.get("tgl_aktivitas"))

        wpp = _extract_wpp(
            act.get("dpi_operasi"),
            act.get("wpp_operasi"),
            act.get("wpp_pelabuhan"),
        )

        base = {
            "nama_kapal": (act.get("nama_kapal") or "").strip() or None,
            "nomor_bkp": nomor_bkp,
            "tanggal_bongkar": act.get("tgl_bongkar") or act.get("tgl_aktivitas"),
            "pelabuhan": (act.get("pelabuhan_kedatangan") or "").strip() or None,
            "pelabuhan_kode": str(act.get("id_pelabuhan_dss_kedatangan") or "").strip() or None,
            "total_tangkapan": total_kg if total_kg > 0 else None,
            "wpp_tangkap": wpp,
            "lama_trip": lama_trip,
            # trip_ke and jumlah_abk are not available from PIPP API
            "trip_ke": None,
            "jumlah_abk": None,
            "sumber_data": SOURCE_NAME,
        }

        for h in hasil_list:
            jml = float(h.get("jml_ikan") or 0)
            harga = float(h.get("harga_produsen") or 0)
            latin = (h.get("nama_latin") or "").strip() or None
            row = {
                **base,
                "jenis_ikan": (h.get("nama_jenis_ikan") or "").strip() or None,
                "kode_ikan": _fao_code_from_latin(latin),
                "berat_per_jenis": jml if jml > 0 else None,
                "nilai_tangkapan": round(jml * harga, 2) if jml and harga else None,
                "harga_per_kg": harga if harga > 0 else None,
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

EXISTS_SQL = """
SELECT 1 FROM master_tangkapan_pipp
WHERE nama_kapal = %(nama_kapal)s
  AND tanggal_bongkar = %(tanggal_bongkar)s
  AND jenis_ikan IS NOT DISTINCT FROM %(jenis_ikan)s
LIMIT 1
"""

UPDATE_SQL = """
UPDATE master_tangkapan_pipp SET
    nomor_bkp       = COALESCE(%(nomor_bkp)s, nomor_bkp),
    pelabuhan       = %(pelabuhan)s,
    pelabuhan_kode  = %(pelabuhan_kode)s,
    total_tangkapan = %(total_tangkapan)s,
    berat_per_jenis = %(berat_per_jenis)s,
    nilai_tangkapan = %(nilai_tangkapan)s,
    harga_per_kg    = %(harga_per_kg)s,
    wpp_tangkap     = COALESCE(%(wpp_tangkap)s, wpp_tangkap),
    kode_ikan       = COALESCE(%(kode_ikan)s, kode_ikan),
    lama_trip       = COALESCE(%(lama_trip)s, lama_trip),
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


def collect(start_date: str, end_date: str, batch_size: int = 30) -> dict[str, int]:
    """
    Main entry point. Fetches PIPP data for [start_date, end_date] inclusive
    and upserts into master_tangkapan_pipp.

    batch_size: commit every N days (reduces memory for large date ranges).
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
        batch_rows: list[dict[str, Any]] = []
        batch_day_count = 0

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
            batch_rows.extend(rows)
            batch_day_count += 1

            logger.info("  %s: %d activities → %d rows", day_str, len(activities), len(rows))

            # Flush batch
            if batch_day_count >= batch_size:
                ins, upd = _upsert_rows(conn, batch_rows)
                total_inserted += ins
                total_updated += upd
                logger.info("  Batch flushed: ins=%d upd=%d", ins, upd)
                batch_rows = []
                batch_day_count = 0

            current += timedelta(days=1)
            time.sleep(0.5)

        # Flush remaining
        if batch_rows:
            ins, upd = _upsert_rows(conn, batch_rows)
            total_inserted += ins
            total_updated += upd
            logger.info("  Final batch flushed: ins=%d upd=%d", ins, upd)

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
    parser.add_argument("--batch-size", type=int, default=30, help="Commit every N days (default: 30)")
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
    summary = collect(start_date, end_date, batch_size=args.batch_size)
    print(f"Completed: {summary}")
