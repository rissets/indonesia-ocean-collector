"""
WPP (Wilayah Pengelolaan Perikanan) lookup from master_wpp table.

Loads WPP polygon/bbox data from the DB at startup and provides
assign_wpp() to tag any DataFrame with the correct WPP region.

Falls back to hardcoded WPP_REGIONS from config.py if DB is unavailable.
"""

from __future__ import annotations

import logging
import os
from functools import lru_cache
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def _load_wpp_from_db() -> Optional[pd.DataFrame]:
    """
    Load master_wpp rows from DB.
    Returns DataFrame with columns: kode_wpp, nama_wpp, min_lat, max_lat, min_lon, max_lon
    Returns None if DB is unavailable or table doesn't have bbox columns.
    """
    try:
        import psycopg2  # noqa: PLC0415

        conn = psycopg2.connect(
            host=os.environ["DB_HOST"],
            port=int(os.environ.get("DB_PORT", 5432)),
            dbname=os.environ["DB_NAME"],
            user=os.environ["DB_USER"],
            password=os.environ["DB_PASSWORD"],
            connect_timeout=10,
        )
        cur = conn.cursor()

        # Inspect available columns
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'master_wpp'
            ORDER BY ordinal_position
        """)
        cols = [r[0] for r in cur.fetchall()]
        logger.info("master_wpp columns: %s", cols)

        # Try to load bbox-style columns
        bbox_cols = {"min_lat", "max_lat", "min_lon", "max_lon"}
        if bbox_cols.issubset(set(cols)):
            cur.execute("""
                SELECT kode_wpp, nama_wpp, min_lat, max_lat, min_lon, max_lon
                FROM master_wpp
                WHERE min_lat IS NOT NULL AND max_lat IS NOT NULL
                  AND min_lon IS NOT NULL AND max_lon IS NOT NULL
            """)
            rows = cur.fetchall()
            conn.close()
            if rows:
                df = pd.DataFrame(rows, columns=["kode_wpp", "nama_wpp", "min_lat", "max_lat", "min_lon", "max_lon"])
                logger.info("Loaded %d WPP regions from master_wpp (bbox mode)", len(df))
                return df

        # Try geometry-based: extract bbox from PostGIS geometry if available
        if "geom" in cols or "geometry" in cols:
            geom_col = "geom" if "geom" in cols else "geometry"
            name_col = "nama_wpp" if "nama_wpp" in cols else (cols[1] if len(cols) > 1 else cols[0])
            code_col = "kode_wpp" if "kode_wpp" in cols else cols[0]
            cur.execute(f"""
                SELECT
                    {code_col},
                    {name_col},
                    ST_YMin(ST_Envelope({geom_col})) AS min_lat,
                    ST_YMax(ST_Envelope({geom_col})) AS max_lat,
                    ST_XMin(ST_Envelope({geom_col})) AS min_lon,
                    ST_XMax(ST_Envelope({geom_col})) AS max_lon
                FROM master_wpp
                WHERE {geom_col} IS NOT NULL
            """)
            rows = cur.fetchall()
            conn.close()
            if rows:
                df = pd.DataFrame(rows, columns=["kode_wpp", "nama_wpp", "min_lat", "max_lat", "min_lon", "max_lon"])
                logger.info("Loaded %d WPP regions from master_wpp (geometry mode)", len(df))
                return df

        # Fallback: dump all columns and try to infer
        cur.execute("SELECT * FROM master_wpp LIMIT 5")
        sample = cur.fetchall()
        conn.close()
        logger.warning("master_wpp has no bbox/geometry columns. Sample: %s | Cols: %s", sample[:2], cols)
        return None

    except Exception as exc:
        logger.warning("Could not load master_wpp from DB: %s — using hardcoded WPP_REGIONS", exc)
        return None


def _load_wpp_fallback() -> pd.DataFrame:
    """Return hardcoded WPP regions as a DataFrame."""
    from config import WPP_REGIONS  # noqa: PLC0415

    rows = []
    for name, bbox in WPP_REGIONS.items():
        rows.append({
            "kode_wpp": name,
            "nama_wpp": name,
            "min_lat": bbox["min_lat"],
            "max_lat": bbox["max_lat"],
            "min_lon": bbox["min_lon"],
            "max_lon": bbox["max_lon"],
        })
    return pd.DataFrame(rows)


def get_wpp_regions() -> pd.DataFrame:
    """
    Return WPP regions DataFrame with bbox columns.
    Tries DB first, falls back to hardcoded config.
    """
    db_df = _load_wpp_from_db()
    if db_df is not None and not db_df.empty:
        return db_df
    logger.info("Using hardcoded WPP_REGIONS fallback")
    return _load_wpp_fallback()


def assign_wpp(df: pd.DataFrame, lat_col: str = "latitude", lon_col: str = "longitude") -> pd.DataFrame:
    """
    Assign wpp_region to each row based on lat/lon, matching against master_wpp.

    Uses the DB-sourced WPP regions (falls back to hardcoded if DB unavailable).
    Rows outside all WPP bboxes get 'OUTSIDE_WPP'.
    """
    wpp_df = get_wpp_regions()
    df = df.copy()

    def _find(lat: float, lon: float) -> str:
        for _, wpp in wpp_df.iterrows():
            if (wpp["min_lat"] <= lat <= wpp["max_lat"] and
                    wpp["min_lon"] <= lon <= wpp["max_lon"]):
                # Prefer kode_wpp if it looks like a WPP code, else use nama_wpp
                code = str(wpp.get("kode_wpp", ""))
                name = str(wpp.get("nama_wpp", ""))
                return code if code.startswith("WPP") else (name if name else code)
        return "OUTSIDE_WPP"

    df["wpp_region"] = df.apply(lambda r: _find(r[lat_col], r[lon_col]), axis=1)
    return df


def invalidate_cache() -> None:
    """Clear the cached WPP data (useful for testing or after DB updates)."""
    _load_wpp_from_db.cache_clear()
