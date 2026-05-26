"""
WPP (Wilayah Pengelolaan Perikanan) filter utilities.

Primary path uses `master_wpp.geojson_polygon` via `db.wpp` for real polygon
membership (not just coarse bbox). If DB polygons are unavailable, it falls
back to config bboxes.
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from config import WPP_REGIONS
from db.wpp import get_wpp_code_for_point, load_wpp_polygons

logger = logging.getLogger(__name__)


def is_in_wpp(lat: float, lon: float) -> bool:
    """Return True if (lat, lon) falls within any WPP region."""
    try:
        return get_wpp_code_for_point(lat, lon) is not None
    except Exception:
        # Conservative fallback to bbox if polygon lookup is unavailable.
        for bbox in WPP_REGIONS.values():
            if bbox["min_lat"] <= lat <= bbox["max_lat"] and bbox["min_lon"] <= lon <= bbox["max_lon"]:
                return True
        return False


def get_wpp_name(lat: float, lon: float) -> str | None:
    """
    Return canonical WPP code (e.g., WPP-712) for (lat, lon),
    or None if outside all WPP regions.
    """
    try:
        return get_wpp_code_for_point(lat, lon)
    except Exception:
        # Fallback: derive code from bbox key naming convention.
        for name, bbox in WPP_REGIONS.items():
            if bbox["min_lat"] <= lat <= bbox["max_lat"] and bbox["min_lon"] <= lon <= bbox["max_lon"]:
                parts = name.split("_")
                if len(parts) >= 2 and parts[1].isdigit():
                    return f"WPP-{parts[1]}"
                return name
        return None


def filter_wpp(df: pd.DataFrame, lat_col: str = "latitude", lon_col: str = "longitude") -> pd.DataFrame:
    """
    Filter DataFrame to rows within WPP regions only.

    Returns filtered DataFrame with an added 'wpp' column (canonical code).
    Rows outside all WPP regions are dropped.
    """
    if df.empty:
        return df

    filtered = df.copy()
    filtered["wpp"] = filtered.apply(lambda r: get_wpp_name(r[lat_col], r[lon_col]), axis=1)
    filtered = filtered[filtered["wpp"].notna()].copy()
    dropped = len(df) - len(filtered)
    if dropped > 0:
        logger.info("WPP filter: dropped %d/%d rows outside WPP regions.", dropped, len(df))
    return filtered.reset_index(drop=True)


def delete_outside_wpp_sql() -> str:
    """
    Return SQL to delete rows from master_oceanography outside WPP envelopes.
    Uses bbox envelopes computed from active `master_wpp` polygons when available,
    with config bbox fallback.
    """
    bboxes: list[dict[str, Any]] = []
    try:
        for item in load_wpp_polygons():
            min_lon, min_lat, max_lon, max_lat = item["bbox"]
            bboxes.append(
                {
                    "min_lat": min_lat,
                    "max_lat": max_lat,
                    "min_lon": min_lon,
                    "max_lon": max_lon,
                }
            )
    except Exception as exc:
        logger.warning("Failed to load WPP polygon bbox for delete SQL, fallback to config: %s", exc)

    if not bboxes:
        bboxes = list(WPP_REGIONS.values())

    conditions = []
    for bbox in bboxes:
        conditions.append(
            f"(latitude BETWEEN {bbox['min_lat']} AND {bbox['max_lat']} "
            f"AND longitude BETWEEN {bbox['min_lon']} AND {bbox['max_lon']})"
        )
    wpp_condition = " OR ".join(conditions)
    return f"DELETE FROM master_oceanography WHERE NOT ({wpp_condition})"
