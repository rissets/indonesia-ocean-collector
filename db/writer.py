"""
PostgreSQL writer for master_oceanography.

Upserts rows keyed on (tanggal, latitude, longitude, sumber_data).
Requires env vars: DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD.
"""

from __future__ import annotations

import logging
import math
import os
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

_INSERT_SQL = """
INSERT INTO master_oceanography (
    tanggal, latitude, longitude,
    suhu_permukaan, sst, ssh, klorofil,
    arus_laut_u, arus_laut_v, kecepatan_arus,
    tinggi_gelombang,
    kecepatan_angin, arah_angin, radiasi_matahari,
    cuaca, sumber_data
) VALUES (
    %(tanggal)s, %(latitude)s, %(longitude)s,
    %(suhu_permukaan)s, %(sst)s, %(ssh)s, %(klorofil)s,
    %(arus_laut_u)s, %(arus_laut_v)s, %(kecepatan_arus)s,
    %(tinggi_gelombang)s,
    %(kecepatan_angin)s, %(arah_angin)s, %(radiasi_matahari)s,
    %(cuaca)s, %(sumber_data)s
)
ON CONFLICT (tanggal, latitude, longitude, sumber_data)
DO UPDATE SET
    suhu_permukaan   = COALESCE(EXCLUDED.suhu_permukaan,   master_oceanography.suhu_permukaan),
    sst              = COALESCE(EXCLUDED.sst,              master_oceanography.sst),
    ssh              = COALESCE(EXCLUDED.ssh,              master_oceanography.ssh),
    klorofil         = COALESCE(EXCLUDED.klorofil,         master_oceanography.klorofil),
    arus_laut_u      = COALESCE(EXCLUDED.arus_laut_u,      master_oceanography.arus_laut_u),
    arus_laut_v      = COALESCE(EXCLUDED.arus_laut_v,      master_oceanography.arus_laut_v),
    kecepatan_arus   = COALESCE(EXCLUDED.kecepatan_arus,   master_oceanography.kecepatan_arus),
    tinggi_gelombang = COALESCE(EXCLUDED.tinggi_gelombang, master_oceanography.tinggi_gelombang),
    kecepatan_angin  = COALESCE(EXCLUDED.kecepatan_angin,  master_oceanography.kecepatan_angin),
    arah_angin       = COALESCE(EXCLUDED.arah_angin,       master_oceanography.arah_angin),
    radiasi_matahari = COALESCE(EXCLUDED.radiasi_matahari, master_oceanography.radiasi_matahari),
    cuaca            = COALESCE(EXCLUDED.cuaca,            master_oceanography.cuaca)
"""

_BATCH_SIZE = 500


def _get_conn():
    import psycopg2  # noqa: PLC0415
    return psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=int(os.environ.get("DB_PORT", 5432)),
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
    )


def _nan_to_none(v: Any) -> Any:
    if v is None:
        return None
    try:
        if math.isnan(float(v)):
            return None
    except (TypeError, ValueError):
        pass
    return v


def _row_to_params(row: dict) -> dict:
    def g(key):
        return _nan_to_none(row.get(key))

    u = g("arus_laut_u")
    v = g("arus_laut_v")
    speed = None
    if u is not None and v is not None:
        speed = round(math.sqrt(float(u) ** 2 + float(v) ** 2), 4)

    return {
        "tanggal":          g("tanggal"),
        "latitude":         g("latitude"),
        "longitude":        g("longitude"),
        "suhu_permukaan":   g("suhu_permukaan") or g("sst"),
        "sst":              g("sst") or g("suhu_permukaan"),
        "ssh":              g("ssh"),
        "klorofil":         g("klorofil"),
        "arus_laut_u":      u,
        "arus_laut_v":      v,
        "kecepatan_arus":   g("kecepatan_arus") or speed,
        "tinggi_gelombang": g("tinggi_gelombang"),
        "kecepatan_angin":  g("kecepatan_angin"),
        "arah_angin":       g("arah_angin"),
        "radiasi_matahari": g("radiasi_matahari"),
        "cuaca":            g("cuaca"),
        "sumber_data":      g("sumber_data"),
    }


def upsert_dataframe(df: pd.DataFrame, source_name: str) -> int:
    """
    Upsert a normalised DataFrame into master_oceanography.

    Expected columns (any subset — missing ones become NULL):
      tanggal, latitude, longitude,
      sst / suhu_permukaan, ssh, klorofil,
      arus_laut_u, arus_laut_v,
      tinggi_gelombang, kecepatan_angin, arah_angin, radiasi_matahari, cuaca

    Returns the number of rows upserted.
    """
    if df.empty:
        logger.warning("upsert_dataframe called with empty DataFrame for source=%s", source_name)
        return 0

    if "sumber_data" not in df.columns:
        df = df.copy()
        df["sumber_data"] = source_name

    rows = df.to_dict(orient="records")
    params_list = [_row_to_params(r) for r in rows]

    try:
        conn = _get_conn()
    except Exception as exc:
        logger.error("DB connection failed: %s", exc)
        raise

    total = 0
    try:
        with conn:
            with conn.cursor() as cur:
                for i in range(0, len(params_list), _BATCH_SIZE):
                    batch = params_list[i : i + _BATCH_SIZE]
                    cur.executemany(_INSERT_SQL, batch)
                    total += len(batch)
                    logger.debug("Upserted batch %d–%d", i, i + len(batch))
    finally:
        conn.close()

    logger.info("Upserted %d rows into master_oceanography (source=%s)", total, source_name)
    return total
