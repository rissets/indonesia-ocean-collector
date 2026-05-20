"""
Global Fishing Watch (GFW) Collector.

Requires GFW API token in .env:
  GFW_API_TOKEN=your_token

Register at: https://globalfishingwatch.org/data-download

Falls back to realistic mock data when token is absent,
clearly marked with source="GFW_MOCK".
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import requests

from config import (
    DEFAULT_MAX_RECORDS,
    DEFAULT_RAW_DIR,
    GFW_BASE_URL,
    GFW_FISHING_EFFORT_DATASET,
    INDONESIA_BBOX,
    WPP_REGIONS,
)

logger = logging.getLogger(__name__)

SOURCE_NAME = "GFW"
MOCK_SOURCE = "GFW_MOCK"


def _get_token() -> Optional[str]:
    return os.getenv("GFW_API_TOKEN")


def _headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {_get_token()}",
        "Content-Type": "application/json",
    }


def _bbox_polygon(region: dict[str, float]) -> list[list[float]]:
    """Convert bbox dict to GeoJSON polygon coordinates."""
    return [[
        [region["min_lon"], region["min_lat"]],
        [region["max_lon"], region["min_lat"]],
        [region["max_lon"], region["max_lat"]],
        [region["min_lon"], region["max_lat"]],
        [region["min_lon"], region["min_lat"]],
    ]]


def _generate_mock_effort(
    wpp_name: str,
    region: dict[str, float],
    start_date: str,
    end_date: str,
    max_records: int,
) -> pd.DataFrame:
    """Generate realistic mock fishing effort data."""
    rng = np.random.default_rng(seed=hash(wpp_name) % (2**32))
    dates = pd.date_range(start_date, end_date, freq="MS")
    lats = np.arange(region["min_lat"], region["max_lat"], 0.5)
    lons = np.arange(region["min_lon"], region["max_lon"], 0.5)

    rows = []
    for dt in dates:
        for lat in lats:
            for lon in lons:
                rows.append({
                    "time": dt,
                    "latitude": round(lat + 0.25, 2),
                    "longitude": round(lon + 0.25, 2),
                    "wpp_region": wpp_name,
                    # Fishing effort in hours — log-normal distribution
                    "fishing_effort_hours": round(
                        float(np.clip(rng.lognormal(2.5, 1.2), 0.1, 500.0)), 2
                    ),
                    "vessel_count": int(np.clip(rng.poisson(8), 0, 100)),
                    "source": MOCK_SOURCE,
                })
                if len(rows) >= max_records:
                    break
            if len(rows) >= max_records:
                break
        if len(rows) >= max_records:
            break

    return pd.DataFrame(rows)


def collect_fishing_effort(
    start_date: str,
    end_date: str,
    wpp_names: Optional[list[str]] = None,
    max_records: int = DEFAULT_MAX_RECORDS,
    raw_dir: str = DEFAULT_RAW_DIR,
) -> pd.DataFrame:
    """
    Collect fishing effort data from Global Fishing Watch.

    Parameters
    ----------
    start_date : str   e.g. "2023-01-01"
    end_date   : str   e.g. "2023-03-31"
    wpp_names  : list  WPP region keys from config.WPP_REGIONS (None = all)
    max_records: int   max rows per WPP region
    raw_dir    : str   directory to save raw CSV

    Returns
    -------
    pd.DataFrame: time, latitude, longitude, wpp_region,
                  fishing_effort_hours, vessel_count, source
    """
    regions = {k: v for k, v in WPP_REGIONS.items() if k in wpp_names} \
        if wpp_names else WPP_REGIONS

    token = _get_token()
    all_dfs: list[pd.DataFrame] = []

    for wpp_name, region in regions.items():
        logger.info("Collecting GFW effort for %s ...", wpp_name)
        if token:
            df = _fetch_gfw_real(wpp_name, region, start_date, end_date, max_records)
        else:
            logger.warning("GFW token not found — using mock data for %s.", wpp_name)
            df = _generate_mock_effort(wpp_name, region, start_date, end_date, max_records)

        if not df.empty:
            all_dfs.append(df)

    if not all_dfs:
        logger.warning("GFW: no data collected.")
        return pd.DataFrame()

    combined = pd.concat(all_dfs, ignore_index=True)
    _save_raw(combined, "gfw_fishing_effort", start_date, end_date, raw_dir)
    logger.info("GFW fishing effort: %d records total.", len(combined))
    return combined


def _fetch_gfw_real(
    wpp_name: str,
    region: dict[str, float],
    start_date: str,
    end_date: str,
    max_records: int,
) -> pd.DataFrame:
    """Fetch fishing effort from GFW API v3."""
    url = f"{GFW_BASE_URL}/4wings/report"
    payload = {
        "geojson": {
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": _bbox_polygon(region),
            },
        },
        "datasets": [GFW_FISHING_EFFORT_DATASET],
        "start-date": start_date,
        "end-date": end_date,
        "spatial-resolution": "LOW",
        "temporal-resolution": "MONTHLY",
    }

    try:
        resp = requests.post(url, json=payload, headers=_headers(), timeout=60)
        resp.raise_for_status()
    except requests.exceptions.HTTPError as exc:
        logger.error("GFW HTTP error for %s: %s", wpp_name, exc)
        return pd.DataFrame()
    except requests.exceptions.RequestException as exc:
        logger.error("GFW request failed for %s: %s", wpp_name, exc)
        return pd.DataFrame()

    entries = resp.json().get("entries", [])
    if not entries:
        logger.warning("GFW returned no entries for %s.", wpp_name)
        return pd.DataFrame()

    df = pd.DataFrame(entries)
    df["wpp_region"] = wpp_name
    df["source"] = SOURCE_NAME

    # Normalize column names from GFW response
    col_map = {
        "date": "time",
        "lat": "latitude",
        "lon": "longitude",
        "hours": "fishing_effort_hours",
        "vesselCount": "vessel_count",
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

    if "time" in df.columns:
        df["time"] = pd.to_datetime(df["time"], errors="coerce")

    return df.head(max_records)


def _save_raw(df: pd.DataFrame, prefix: str, start: str, end: str, raw_dir: str) -> None:
    Path(raw_dir).mkdir(parents=True, exist_ok=True)
    fname = f"{prefix}_{start.replace('-','')}_{end.replace('-','')}.csv"
    path = Path(raw_dir) / fname
    df.to_csv(path, index=False)
    logger.info("Raw saved: %s", path)
