"""
DB writer — inserts/upserts rows into master_oceanography.

Upsert key: (tanggal, latitude, longitude, sumber_data)
"""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from typing import Generator

import pandas as pd
import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)

_DSN_KEYS = ("DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD")


def _dsn() -> dict:
    return {
        "host": os.environ["DB_HOST"],
        "port": int(os.environ.get("DB_PORT", 5432)),
        "dbname": os.environ["DB_NAME"],
        "user": os.environ["DB_USER"],
        "password": os.environ["DB_PASSWORD"],
        "connect_timeout": 10,
    }


@contextmanager
def _conn() -> Generator:
    conn = psycopg2.connect(**_dsn())
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


_UPSERT_SQL = """
INSERT INTO master_oceanography (
    tanggal, latitude, longitude,
    suhu_permukaan, sst,
    ssh, klorofil,
    arus_laut_u, arus_laut_v, kecepatan_arus,
    tinggi_gelombang, periode_gelombang,
    kecepatan_angin, arah_angin,
    radiasi_matahari, cuaca,
    sumber_data
)
VALUES %s
ON CONFLICT (tanggal, latitude, longitude, sumber_data)
DO UPDATE SET
    suhu_permukaan   = EXCLUDED.suhu_permukaan,
    sst              = EXCLUDED.sst,
    ssh              = EXCLUDED.ssh,
    klorofil         = EXCLUDED.klorofil,
    arus_laut_u      = EXCLUDED.arus_laut_u,
    arus_laut_v      = EXCLUDED.arus_laut_v,
    kecepatan_arus   = EXCLUDED.kecepatan_arus,
    tinggi_gelombang = EXCLUDED.tinggi_gelombang,
    periode_gelombang= EXCLUDED.periode_gelombang,
    kecepatan_angin  = EXCLUDED.kecepatan_angin,
    arah_angin       = EXCLUDED.arah_angin,
    radiasi_matahari = EXCLUDED.radiasi_matahari,
    cuaca            = EXCLUDED.cuaca
"""


def _row(r: pd.Series) -> tuple:
    def _f(col: str):
        v = r.get(col)
        return None if (v is None or (isinstance(v, float) and pd.isna(v))) else v

    u = _f("u_current_ms") or _f("arus_laut_u")
    v = _f("v_current_ms") or _f("arus_laut_v")
    speed = None
    if u is not None and v is not None:
        speed = round((u**2 + v**2) ** 0.5, 4)

    return (
        _f("tanggal") or _f("time"),
        _f("latitude"),
        _f("longitude"),
        _f("sst_celsius") or _f("suhu_permukaan"),
        _f("sst_celsius") or _f("sst"),
        _f("ssh_m") or _f("ssh"),
        _f("chlorophyll_mgm3") or _f("klorofil"),
        u,
        v,
        speed,
        _f("wave_height_m") or _f("tinggi_gelombang"),
        _f("wave_period_s") or _f("periode_gelombang"),
        _f("wind_speed_ms") or _f("kecepatan_angin"),
        _f("wind_direction_deg") or _f("arah_angin"),
        _f("solar_radiation_wm2") or _f("radiasi_matahari"),
        _f("cuaca"),
        _f("source") or _f("sumber_data"),
    )


def upsert_dataframe(df: pd.DataFrame, batch_size: int = 1000) -> int:
    """Upsert df rows into master_oceanography. Returns total rows upserted."""
    if df.empty:
        return 0

    rows = [_row(r) for _, r in df.iterrows()]
    # Filter rows missing required key fields
    rows = [r for r in rows if r[0] is not None and r[1] is not None and r[2] is not None and r[16] is not None]

    if not rows:
        logger.warning("No valid rows to upsert after filtering.")
        return 0

    total = 0
    with _conn() as conn:
        cur = conn.cursor()
        for i in range(0, len(rows), batch_size):
            chunk = rows[i : i + batch_size]
            psycopg2.extras.execute_values(cur, _UPSERT_SQL, chunk)
            total += len(chunk)
            logger.info("Upserted %d/%d rows", total, len(rows))

    logger.info("Done: %d rows upserted into master_oceanography.", total)
    return total
