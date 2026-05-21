"""
WPP (Wilayah Pengelolaan Perikanan) filter utilities.

Provides functions to check whether a lat/lon point falls within any
Indonesian WPP region, and to filter DataFrames to WPP-only rows.
"""

from __future__ import annotations

import pandas as pd

from config import WPP_REGIONS


def is_in_wpp(lat: float, lon: float) -> bool:
    """Return True if (lat, lon) falls within any WPP region."""
    for bbox in WPP_REGIONS.values():
        if (bbox["min_lat"] <= lat <= bbox["max_lat"] and
                bbox["min_lon"] <= lon <= bbox["max_lon"]):
            return True
    return False


def get_wpp_name(lat: float, lon: float) -> str | None:
    """Return the WPP region name for (lat, lon), or None if outside all WPP."""
    for name, bbox in WPP_REGIONS.items():
        if (bbox["min_lat"] <= lat <= bbox["max_lat"] and
                bbox["min_lon"] <= lon <= bbox["max_lon"]):
            return name
    return None


def filter_wpp(df: pd.DataFrame, lat_col: str = "latitude", lon_col: str = "longitude") -> pd.DataFrame:
    """
    Filter DataFrame to rows within WPP regions only.

    Returns filtered DataFrame with an added 'wpp_region' column.
    Rows outside all WPP regions are dropped.
    """
    if df.empty:
        return df

    mask = df.apply(lambda r: is_in_wpp(r[lat_col], r[lon_col]), axis=1)
    filtered = df[mask].copy()
    filtered["wpp_region"] = filtered.apply(
        lambda r: get_wpp_name(r[lat_col], r[lon_col]), axis=1
    )
    dropped = len(df) - len(filtered)
    if dropped > 0:
        import logging
        logging.getLogger(__name__).info(
            "WPP filter: dropped %d/%d rows outside WPP regions.", dropped, len(df)
        )
    return filtered.reset_index(drop=True)


def delete_outside_wpp_sql() -> str:
    """
    Return SQL to delete all rows from master_oceanography that fall outside WPP regions.
    Uses a NOT EXISTS approach against the WPP bounding boxes.
    """
    conditions = []
    for bbox in WPP_REGIONS.values():
        conditions.append(
            f"(latitude BETWEEN {bbox['min_lat']} AND {bbox['max_lat']} "
            f"AND longitude BETWEEN {bbox['min_lon']} AND {bbox['max_lon']})"
        )
    wpp_condition = " OR ".join(conditions)
    return f"DELETE FROM master_oceanography WHERE NOT ({wpp_condition})"
