"""PostgreSQL writer for master_oceanography."""

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
    tinggi_gelombang, periode_gelombang,
    kecepatan_angin, arah_angin, radiasi_matahari,
    kedalaman_laut, jarak_padang, pasang_surut,
    cuaca, sumber_data, wpp
) VALUES (
    %(tanggal)s, %(latitude)s, %(longitude)s,
    %(suhu_permukaan)s, %(sst)s, %(ssh)s, %(klorofil)s,
    %(arus_laut_u)s, %(arus_laut_v)s, %(kecepatan_arus)s,
    %(tinggi_gelombang)s, %(periode_gelombang)s,
    %(kecepatan_angin)s, %(arah_angin)s, %(radiasi_matahari)s,
    %(kedalaman_laut)s, %(jarak_padang)s, %(pasang_surut)s,
    %(cuaca)s, %(sumber_data)s, %(wpp)s
)
ON CONFLICT (tanggal, latitude, longitude, sumber_data)
DO UPDATE SET
    suhu_permukaan    = COALESCE(EXCLUDED.suhu_permukaan, master_oceanography.suhu_permukaan),
    sst               = COALESCE(EXCLUDED.sst, master_oceanography.sst),
    ssh               = COALESCE(EXCLUDED.ssh, master_oceanography.ssh),
    klorofil          = COALESCE(EXCLUDED.klorofil, master_oceanography.klorofil),
    arus_laut_u       = COALESCE(EXCLUDED.arus_laut_u, master_oceanography.arus_laut_u),
    arus_laut_v       = COALESCE(EXCLUDED.arus_laut_v, master_oceanography.arus_laut_v),
    kecepatan_arus    = COALESCE(EXCLUDED.kecepatan_arus, master_oceanography.kecepatan_arus),
    tinggi_gelombang  = COALESCE(EXCLUDED.tinggi_gelombang, master_oceanography.tinggi_gelombang),
    periode_gelombang = COALESCE(EXCLUDED.periode_gelombang, master_oceanography.periode_gelombang),
    kecepatan_angin   = COALESCE(EXCLUDED.kecepatan_angin, master_oceanography.kecepatan_angin),
    arah_angin        = COALESCE(EXCLUDED.arah_angin, master_oceanography.arah_angin),
    radiasi_matahari  = COALESCE(EXCLUDED.radiasi_matahari, master_oceanography.radiasi_matahari),
    kedalaman_laut    = COALESCE(EXCLUDED.kedalaman_laut, master_oceanography.kedalaman_laut),
    jarak_padang      = COALESCE(EXCLUDED.jarak_padang, master_oceanography.jarak_padang),
    pasang_surut      = COALESCE(EXCLUDED.pasang_surut, master_oceanography.pasang_surut),
    cuaca             = COALESCE(EXCLUDED.cuaca, master_oceanography.cuaca),
    wpp               = COALESCE(EXCLUDED.wpp, master_oceanography.wpp),
    updated_at        = NOW()
"""

_BATCH_SIZE = 1000


def _get_conn():
    import psycopg2

    return psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=int(os.environ.get("DB_PORT", "5432")),
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        connect_timeout=10,
    )


def _nan_to_none(value: Any) -> Any:
    if value is None:
        return None
    try:
        if math.isnan(float(value)):
            return None
    except (TypeError, ValueError):
        pass
    return value


def _first(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = _nan_to_none(row.get(key))
        if value is not None:
            return value
    return None


def _row_to_params(row: dict[str, Any]) -> dict[str, Any]:
    tanggal = _first(row, "tanggal", "time", "date")
    if hasattr(tanggal, "date"):
        tanggal = tanggal.date()

    u = _first(row, "arus_laut_u", "u_current_ms", "uo", "ugos")
    v = _first(row, "arus_laut_v", "v_current_ms", "vo", "vgos")
    speed = _first(row, "kecepatan_arus", "ocean_current_velocity_max")
    if speed is None and u is not None and v is not None:
        speed = round(math.sqrt(float(u) ** 2 + float(v) ** 2), 4)

    return {
        "tanggal": tanggal,
        "latitude": _first(row, "latitude", "lat"),
        "longitude": _first(row, "longitude", "lon"),
        "suhu_permukaan": _first(row, "suhu_permukaan", "sst", "sst_celsius", "thetao"),
        "sst": _first(row, "sst", "suhu_permukaan", "sst_celsius", "thetao"),
        "ssh": _first(row, "ssh", "ssh_m", "zos", "sla"),
        "klorofil": _first(row, "klorofil", "chlorophyll_mgm3", "CHL", "chlorophyll"),
        "arus_laut_u": u,
        "arus_laut_v": v,
        "kecepatan_arus": speed,
        "tinggi_gelombang": _first(row, "tinggi_gelombang", "wave_height_m", "wave_height_max"),
        "periode_gelombang": _first(row, "periode_gelombang", "wave_period_s", "wave_period_max"),
        "kecepatan_angin": _first(row, "kecepatan_angin", "wind_speed_ms", "wind_speed_10m_max"),
        "arah_angin": _first(row, "arah_angin", "wind_direction_deg", "wind_direction_10m_dominant"),
        "radiasi_matahari": _first(row, "radiasi_matahari", "solar_radiation_wm2", "shortwave_radiation_sum"),
        "kedalaman_laut": _first(row, "kedalaman_laut", "depth_m", "elevation"),
        "jarak_padang": _first(row, "jarak_padang"),
        "pasang_surut": _first(row, "pasang_surut", "sea_level_height_msl"),
        "cuaca": _first(row, "cuaca", "weather_code"),
        "sumber_data": _first(row, "sumber_data", "source"),
        "wpp": _first(row, "wpp", "wpp_hint"),
    }


def upsert_dataframe(df: pd.DataFrame, source_name: str | None = None) -> int:
    if df.empty:
        logger.warning("upsert_dataframe called with empty DataFrame")
        return 0

    if source_name and "sumber_data" not in df.columns and "source" not in df.columns:
        df = df.copy()
        df["sumber_data"] = source_name

    rows = [_row_to_params(row) for row in df.to_dict(orient="records")]
    rows = [row for row in rows if row["tanggal"] and row["latitude"] is not None and row["longitude"] is not None and row["sumber_data"]]
    if not rows:
        return 0

    total = 0
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                for i in range(0, len(rows), _BATCH_SIZE):
                    batch = rows[i : i + _BATCH_SIZE]
                    cur.executemany(_INSERT_SQL, batch)
                    total += len(batch)
                    logger.info("Upserted %d/%d rows", total, len(rows))
    finally:
        conn.close()

    logger.info("Done: %d rows upserted into master_oceanography.", total)
    return total
