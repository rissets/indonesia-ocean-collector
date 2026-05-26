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
from concurrent.futures import ThreadPoolExecutor, as_completed
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


def _fao_code_from_local(local_name: str | None) -> str | None:
    """Best-effort FAO code inference from Indonesian local fish names."""
    if not local_name:
        return None
    name = local_name.strip().lower()
    keyword_map: list[tuple[str, str]] = [
        ("cakalang", "SKJ"),
        ("tongkol", "LOT"),
        ("madidihang", "YFT"),
        ("tuna mata besar", "BET"),
        ("tuna", "YFT"),
        ("layang", "LAY"),
        ("selar", "YTS"),
        ("lemuru", "SLM"),
        ("kembung", "RAK"),
        ("sarden", "SAG"),
        ("teri", "STO"),
        ("cum", "SQC"),     # cumi/cum
        ("sotong", "OUM"),
        ("gurita", "OCT"),
        ("udang", "MET"),
        ("kepiting", "CRA"),
        ("kakap", "LJM"),
        ("kerapu", "GPX"),
    ]
    for key, code in keyword_map:
        if key in name:
            return code
    return None


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = re.sub(r"\s+", " ", str(value)).strip()
    return text or None


def _clean_date_str(value: Any) -> str | None:
    """
    Normalize source date values to YYYY-MM-DD.
    Returns None for invalid placeholders such as 0000-00-00.
    """
    text = _clean_text(value)
    if not text:
        return None
    base = text[:10]
    try:
        d = datetime.strptime(base, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None
    if d.year < 1900:
        return None
    return d.isoformat()


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
        tanggal_bongkar = _clean_date_str(act.get("tgl_bongkar")) or _clean_date_str(act.get("tgl_aktivitas"))
        if not tanggal_bongkar:
            continue
        if tanggal_bongkar < "2021-01-01":
            continue

        hasil_list = act.get("hasil") or []
        if not hasil_list:
            hasil_list = [{}]

        total_kg = sum(
            float(h.get("jml_ikan") or 0) for h in act.get("hasil") or []
        )

        nama_kapal = _clean_text(act.get("nama_kapal"))
        pelabuhan_kode = _clean_text(act.get("id_pelabuhan_dss_kedatangan") or act.get("kode_pelabuhan"))

        # nomor_bkp: use only source-provided real BKP. Missing BKP is filled
        # later from master_kapal by normalized vessel name; never invent a
        # surrogate because it looks like a random official number downstream.
        raw_bkp = act.get("nomor_buku_kapal")
        nomor_bkp = str(raw_bkp).strip() if raw_bkp and str(raw_bkp).strip() not in ("0", "", "None") else None

        lama_trip = _compute_lama_trip(_clean_date_str(act.get("tgl_berangkat")), tanggal_bongkar)

        wpp = _extract_wpp(
            act.get("dpi_operasi"),
            act.get("wpp_operasi"),
            act.get("wpp_pelabuhan"),
        )

        base = {
            "nama_kapal": nama_kapal,
            "nomor_bkp": nomor_bkp,
            "tanggal_bongkar": tanggal_bongkar,
            "pelabuhan": _clean_text(act.get("pelabuhan_kedatangan") or act.get("nama_pelabuhan")),
            "pelabuhan_kode": pelabuhan_kode,
            "total_tangkapan": total_kg if total_kg > 0 else None,
            "wpp_tangkap": wpp,
            "lama_trip": lama_trip if lama_trip is not None else 0,
            # trip_ke and jumlah_abk are not available from this PIPP endpoint.
            # Keep non-null defaults so downstream quality checks stay strict.
            "trip_ke": 0,
            "jumlah_abk": 0,
            "sumber_data": SOURCE_NAME,
        }

        for h in hasil_list:
            jml = float(h.get("jml_ikan") or 0)
            harga = float(h.get("harga_produsen") or 0)
            latin = (h.get("nama_latin") or "").strip() or None
            row = {
                **base,
                "jenis_ikan": _clean_text(h.get("nama_jenis_ikan")),
                "kode_ikan": _fao_code_from_latin(latin) or _fao_code_from_local(_clean_text(h.get("nama_jenis_ikan"))),
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

POSTPROCESS_VESSEL_SQL = """
WITH vessel_ref AS (
    SELECT regexp_replace(
               regexp_replace(upper(trim(nama_kapal)), '[^A-Z0-9]', '', 'g'),
               '^(KM|KMP|MV|FV)', ''
           ) AS key_name,
           nomor_bkp,
           count(*) AS cnt
    FROM master_kapal
    WHERE nullif(trim(nama_kapal), '') IS NOT NULL
      AND nullif(trim(nomor_bkp), '') IS NOT NULL
      AND nomor_bkp <> '-'
      AND nomor_bkp !~ '^PIPP-'
    GROUP BY 1, 2
),
vessel_ranked AS (
    SELECT key_name, nomor_bkp, cnt,
           row_number() OVER (PARTITION BY key_name ORDER BY cnt DESC, nomor_bkp) AS rn,
           sum(cnt) OVER (PARTITION BY key_name) AS total_cnt
    FROM vessel_ref
),
vessel_map AS (
    SELECT key_name, nomor_bkp
    FROM vessel_ranked
    WHERE rn = 1
      AND (total_cnt = cnt OR cnt >= 3 OR cnt::numeric / nullif(total_cnt, 0) >= 0.75)
),
known_pipp AS (
    SELECT
        regexp_replace(
            regexp_replace(upper(trim(nama_kapal)), '[^A-Z0-9]', '', 'g'),
            '^(KM|KMP|MV|FV)', ''
        ) AS key_name,
        nomor_bkp,
        count(*) AS cnt
    FROM master_tangkapan_pipp
    WHERE nullif(trim(nama_kapal), '') IS NOT NULL
      AND nullif(trim(nomor_bkp), '') IS NOT NULL
      AND nomor_bkp <> '-'
      AND nomor_bkp !~ '^PIPP-'
    GROUP BY 1, 2
),
pipp_ranked AS (
    SELECT key_name, nomor_bkp, cnt,
           row_number() OVER (PARTITION BY key_name ORDER BY cnt DESC, nomor_bkp) AS rn,
           sum(cnt) OVER (PARTITION BY key_name) AS total_cnt
    FROM known_pipp
),
pipp_map AS (
    SELECT key_name, nomor_bkp
    FROM pipp_ranked
    WHERE rn = 1
      AND (total_cnt = cnt OR cnt >= 3 OR cnt::numeric / nullif(total_cnt, 0) >= 0.75)
),
final_map AS (
    SELECT key_name, nomor_bkp FROM vessel_map
    UNION
    SELECT key_name, nomor_bkp FROM pipp_map
)
UPDATE master_tangkapan_pipp p
SET nomor_bkp = v.nomor_bkp,
    updated_at = NOW()
FROM final_map v
WHERE regexp_replace(
          regexp_replace(upper(trim(p.nama_kapal)), '[^A-Z0-9]', '', 'g'),
          '^(KM|KMP|MV|FV)', ''
      ) = v.key_name
  AND (p.nomor_bkp IS NULL OR p.nomor_bkp = '' OR p.nomor_bkp = '-' OR p.nomor_bkp ~ '^PIPP-')
"""

POSTPROCESS_FISH_SQL = """
WITH fish_ref AS (
    SELECT regexp_replace(upper(trim(nama_lokal)), '[^A-Z0-9]', '', 'g') AS key_name,
           kode_fao,
           count(*) AS cnt
    FROM master_jenis_ikan
    WHERE nullif(trim(nama_lokal), '') IS NOT NULL
      AND nullif(trim(kode_fao), '') IS NOT NULL
    GROUP BY 1, 2
),
fish_ref_ranked AS (
    SELECT key_name, kode_fao, cnt,
           row_number() OVER (PARTITION BY key_name ORDER BY cnt DESC, kode_fao) AS rn
    FROM fish_ref
),
fish_ref_map AS (
    SELECT key_name, kode_fao
    FROM fish_ref_ranked
    WHERE rn = 1
),
fish_pipp AS (
    SELECT regexp_replace(upper(trim(jenis_ikan)), '[^A-Z0-9]', '', 'g') AS key_name,
           kode_ikan AS kode_fao,
           count(*) AS cnt
    FROM master_tangkapan_pipp
    WHERE nullif(trim(jenis_ikan), '') IS NOT NULL
      AND nullif(trim(kode_ikan), '') IS NOT NULL
      AND kode_ikan <> '-'
    GROUP BY 1, 2
),
fish_pipp_ranked AS (
    SELECT key_name, kode_fao, cnt,
           row_number() OVER (PARTITION BY key_name ORDER BY cnt DESC, kode_fao) AS rn
    FROM fish_pipp
),
fish_pipp_map AS (
    SELECT key_name, kode_fao
    FROM fish_pipp_ranked
    WHERE rn = 1
),
fish_map AS (
    SELECT key_name, kode_fao FROM fish_ref_map
    UNION
    SELECT key_name, kode_fao FROM fish_pipp_map
)
UPDATE master_tangkapan_pipp p
SET kode_ikan = f.kode_fao,
    updated_at = NOW()
FROM fish_map f
WHERE regexp_replace(upper(trim(p.jenis_ikan)), '[^A-Z0-9]', '', 'g') = f.key_name
  AND (p.kode_ikan IS NULL OR p.kode_ikan = '' OR p.kode_ikan = '-')
;

UPDATE master_tangkapan_pipp
SET kode_ikan = 'UNK',
    updated_at = NOW()
WHERE kode_ikan IS NULL OR kode_ikan = '' OR kode_ikan = '-'
"""

POSTPROCESS_VALUE_SQL = """
WITH species_total AS (
    SELECT nama_kapal, tanggal_bongkar, pelabuhan, sum(berat_per_jenis) AS total_calc
    FROM master_tangkapan_pipp
    WHERE berat_per_jenis IS NOT NULL
    GROUP BY 1, 2, 3
)
UPDATE master_tangkapan_pipp p
SET total_tangkapan = s.total_calc,
    updated_at = NOW()
FROM species_total s
WHERE p.nama_kapal IS NOT DISTINCT FROM s.nama_kapal
  AND p.tanggal_bongkar IS NOT DISTINCT FROM s.tanggal_bongkar
  AND p.pelabuhan IS NOT DISTINCT FROM s.pelabuhan
  AND p.total_tangkapan IS NULL;

WITH one_species AS (
    SELECT nama_kapal, tanggal_bongkar, pelabuhan, count(*) AS row_count, max(total_tangkapan) AS total_value
    FROM master_tangkapan_pipp
    GROUP BY 1, 2, 3
)
UPDATE master_tangkapan_pipp p
SET berat_per_jenis = o.total_value,
    updated_at = NOW()
FROM one_species o
WHERE p.nama_kapal IS NOT DISTINCT FROM o.nama_kapal
  AND p.tanggal_bongkar IS NOT DISTINCT FROM o.tanggal_bongkar
  AND p.pelabuhan IS NOT DISTINCT FROM o.pelabuhan
  AND o.row_count = 1
  AND o.total_value IS NOT NULL
  AND p.berat_per_jenis IS NULL;

UPDATE master_tangkapan_pipp p
SET harga_per_kg = COALESCE(p.harga_per_kg, CASE WHEN p.berat_per_jenis > 0 AND p.nilai_tangkapan > 0 THEN round(p.nilai_tangkapan / p.berat_per_jenis, 2) END),
    nilai_tangkapan = COALESCE(p.nilai_tangkapan, CASE WHEN p.berat_per_jenis > 0 AND p.harga_per_kg > 0 THEN round(p.berat_per_jenis * p.harga_per_kg, 2) END),
    updated_at = NOW()
WHERE p.harga_per_kg IS NULL OR p.nilai_tangkapan IS NULL
"""


def _upsert_rows(conn: psycopg2.extensions.connection, rows: list[dict[str, Any]]) -> tuple[int, int]:
    """Bulk upsert rows; returns (inserted, updated) counts."""
    if not rows:
        return 0, 0

    columns = [
        "nama_kapal",
        "nomor_bkp",
        "tanggal_bongkar",
        "pelabuhan",
        "pelabuhan_kode",
        "total_tangkapan",
        "jenis_ikan",
        "kode_ikan",
        "berat_per_jenis",
        "nilai_tangkapan",
        "harga_per_kg",
        "wpp_tangkap",
        "trip_ke",
        "lama_trip",
        "jumlah_abk",
        "sumber_data",
    ]
    values = [tuple(row.get(column) for column in columns) for row in rows]
    template = "(" + ",".join(["%s"] * len(columns)) + ")"

    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TEMP TABLE tmp_pipp_upsert (
                nama_kapal text,
                nomor_bkp text,
                tanggal_bongkar date,
                pelabuhan text,
                pelabuhan_kode text,
                total_tangkapan numeric,
                jenis_ikan text,
                kode_ikan text,
                berat_per_jenis numeric,
                nilai_tangkapan numeric,
                harga_per_kg numeric,
                wpp_tangkap text,
                trip_ke integer,
                lama_trip integer,
                jumlah_abk integer,
                sumber_data text
            ) ON COMMIT DROP
            """
        )
        psycopg2.extras.execute_values(
            cur,
            f"INSERT INTO tmp_pipp_upsert ({', '.join(columns)}) VALUES %s",
            values,
            template=template,
            page_size=5000,
        )
        cur.execute(
            """
            WITH vessel_ref AS (
                SELECT DISTINCT ON (key_name)
                    key_name,
                    nomor_bkp
                FROM (
                    SELECT
                        regexp_replace(
                            regexp_replace(upper(trim(nama_kapal)), '[^A-Z0-9]', '', 'g'),
                            '^(KM|KMP|MV|FV)', ''
                        ) AS key_name,
                        nomor_bkp
                    FROM master_kapal
                    WHERE nullif(trim(nama_kapal), '') IS NOT NULL
                      AND nullif(trim(nomor_bkp), '') IS NOT NULL
                      AND nomor_bkp <> '-'
                      AND nomor_bkp !~ '^PIPP-'
                ) s
                WHERE key_name <> ''
                ORDER BY key_name, CASE WHEN nomor_bkp ~ '^[0-9]+$' THEN nomor_bkp::bigint END NULLS LAST, nomor_bkp
            )
            UPDATE tmp_pipp_upsert p
            SET nomor_bkp = v.nomor_bkp
            FROM vessel_ref v
            WHERE regexp_replace(
                      regexp_replace(upper(trim(p.nama_kapal)), '[^A-Z0-9]', '', 'g'),
                      '^(KM|KMP|MV|FV)', ''
                  ) = v.key_name
              AND (p.nomor_bkp IS NULL OR p.nomor_bkp = '' OR p.nomor_bkp = '-' OR p.nomor_bkp ~ '^PIPP-')
            """
        )
        cur.execute(
            """
            CREATE TEMP TABLE tmp_pipp_dedup ON COMMIT DROP AS
            SELECT DISTINCT ON (
                nama_kapal,
                tanggal_bongkar,
                COALESCE(jenis_ikan, '')
            ) *
            FROM tmp_pipp_upsert
            ORDER BY nama_kapal, tanggal_bongkar, COALESCE(jenis_ikan, ''), nomor_bkp NULLS LAST
            """
        )
        cur.execute(
            """
            UPDATE master_tangkapan_pipp p SET
                nomor_bkp       = COALESCE(s.nomor_bkp, p.nomor_bkp),
                pelabuhan       = s.pelabuhan,
                pelabuhan_kode  = s.pelabuhan_kode,
                total_tangkapan = s.total_tangkapan,
                berat_per_jenis = s.berat_per_jenis,
                nilai_tangkapan = s.nilai_tangkapan,
                harga_per_kg    = s.harga_per_kg,
                wpp_tangkap     = COALESCE(s.wpp_tangkap, p.wpp_tangkap),
                kode_ikan       = COALESCE(s.kode_ikan, p.kode_ikan),
                lama_trip       = COALESCE(s.lama_trip, p.lama_trip),
                sumber_data     = s.sumber_data,
                updated_at      = NOW()
            FROM tmp_pipp_dedup s
            WHERE p.nama_kapal = s.nama_kapal
              AND p.tanggal_bongkar = s.tanggal_bongkar
              AND p.jenis_ikan IS NOT DISTINCT FROM s.jenis_ikan
            """
        )
        updated = max(cur.rowcount, 0)
        cur.execute(
            """
            INSERT INTO master_tangkapan_pipp (
                nama_kapal, nomor_bkp, tanggal_bongkar, pelabuhan, pelabuhan_kode,
                total_tangkapan, jenis_ikan, kode_ikan, berat_per_jenis,
                nilai_tangkapan, harga_per_kg, wpp_tangkap,
                trip_ke, lama_trip, jumlah_abk, sumber_data,
                created_at, updated_at
            )
            SELECT
                s.nama_kapal, s.nomor_bkp, s.tanggal_bongkar, s.pelabuhan, s.pelabuhan_kode,
                s.total_tangkapan, s.jenis_ikan, s.kode_ikan, s.berat_per_jenis,
                s.nilai_tangkapan, s.harga_per_kg, s.wpp_tangkap,
                s.trip_ke, s.lama_trip, s.jumlah_abk, s.sumber_data,
                NOW(), NOW()
            FROM tmp_pipp_dedup s
            WHERE NOT EXISTS (
                SELECT 1
                FROM master_tangkapan_pipp p
                WHERE p.nama_kapal = s.nama_kapal
                  AND p.tanggal_bongkar = s.tanggal_bongkar
                  AND p.jenis_ikan IS NOT DISTINCT FROM s.jenis_ikan
            )
            ON CONFLICT DO NOTHING
            """
        )
        inserted = max(cur.rowcount, 0)
    conn.commit()
    return inserted, updated


def _postprocess(conn: psycopg2.extensions.connection) -> int:
    affected = 0
    with conn.cursor() as cur:
        for sql in (POSTPROCESS_VESSEL_SQL, POSTPROCESS_FISH_SQL, POSTPROCESS_VALUE_SQL):
            cur.execute(sql)
            affected += max(cur.rowcount, 0)
    conn.commit()
    return affected


def collect(
    start_date: str,
    end_date: str,
    batch_size: int = 30,
    row_batch_size: int | None = None,
    workers: int = 1,
    run_postprocess: bool = True,
) -> dict[str, int]:
    """
    Main entry point. Fetches PIPP data for [start_date, end_date] inclusive
    and upserts into master_tangkapan_pipp.

    batch_size: commit every N days (reduces memory for large date ranges).
    row_batch_size: compatibility alias used by the Airflow DAG.
    Returns summary dict with total_fetched, inserted, updated, skipped_days.
    """
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    if row_batch_size:
        batch_size = max(1, int(row_batch_size))

    if start > end:
        raise ValueError(f"start_date {start_date} is after end_date {end_date}")

    total_fetched = total_inserted = total_updated = skipped_days = 0

    conn = _get_db_conn()
    try:
        dates = []
        current = start
        while current <= end:
            dates.append(current.strftime("%Y-%m-%d"))
            current += timedelta(days=1)

        for offset in range(0, len(dates), batch_size):
            chunk = dates[offset : offset + batch_size]
            batch_rows: list[dict[str, Any]] = []
            logger.info("Fetching PIPP chunk %s → %s (%d days, workers=%d)", chunk[0], chunk[-1], len(chunk), workers)

            if workers > 1:
                with ThreadPoolExecutor(max_workers=workers) as executor:
                    future_map = {executor.submit(_fetch_pipp_day, day): day for day in chunk}
                    for future in as_completed(future_map):
                        day_str = future_map[future]
                        activities = future.result()
                        if not activities:
                            logger.info("  No data for %s", day_str)
                            skipped_days += 1
                            continue
                        rows = _flatten_records(activities)
                        total_fetched += len(rows)
                        batch_rows.extend(rows)
                        logger.info("  %s: %d activities -> %d rows", day_str, len(activities), len(rows))
            else:
                for day_str in chunk:
                    activities = _fetch_pipp_day(day_str)
                    if not activities:
                        logger.info("  No data for %s", day_str)
                        skipped_days += 1
                        continue
                    rows = _flatten_records(activities)
                    total_fetched += len(rows)
                    batch_rows.extend(rows)
                    logger.info("  %s: %d activities -> %d rows", day_str, len(activities), len(rows))

            if batch_rows:
                ins, upd = _upsert_rows(conn, batch_rows)
                total_inserted += ins
                total_updated += upd
                logger.info("  Batch flushed: rows=%d ins=%d upd=%d", len(batch_rows), ins, upd)

        postprocessed = _postprocess(conn) if run_postprocess else 0

    finally:
        conn.close()

    summary = {
        "total_fetched": total_fetched,
        "inserted": total_inserted,
        "updated": total_updated,
        "postprocessed": postprocessed,
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
    parser.add_argument("--workers", type=int, default=1, help="Concurrent PIPP day fetch workers")
    parser.add_argument("--no-postprocess", action="store_true", help="Skip full-table postprocess cleanup")
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
    summary = collect(
        start_date,
        end_date,
        batch_size=args.batch_size,
        workers=max(1, args.workers),
        run_postprocess=not args.no_postprocess,
    )
    print(f"Completed: {summary}")
