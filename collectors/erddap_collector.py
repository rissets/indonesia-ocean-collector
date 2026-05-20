"""
NOAA ERDDAP Collector — no authentication required.

Collects SST and chlorophyll-a data for Indonesian waters
from NOAA CoastWatch ERDDAP server.
"""

from __future__ import annotations

import io
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

from config import (
    DEFAULT_MAX_RECORDS,
    DEFAULT_RAW_DIR,
    ERDDAP_BASE_URL,
    ERDDAP_CHL_DATASET,
    ERDDAP_SST_DATASET,
    INDONESIA_BBOX,
)

logger = logging.getLogger(__name__)

SOURCE_NAME = "NOAA_ERDDAP"


def _build_erddap_url(
    dataset: str,
    variable: str,
    start_date: str,
    end_date: str,
    bbox: dict[str, float],
    stride: int = 1,
) -> str:
    """Build ERDDAP griddap CSV URL with time/lat/lon constraints."""
    return (
        f"{ERDDAP_BASE_URL}/{dataset}.csv"
        f"?{variable}"
        f"[({start_date}):1:({end_date})]"
        f"[({bbox['min_lat']}):{stride}:({bbox['max_lat']})]"
        f"[({bbox['min_lon']}):{stride}:({bbox['max_lon']})]"
    )


def _fetch_erddap(url: str, variable: str, max_records: int) -> pd.DataFrame:
    """Fetch CSV from ERDDAP and return as DataFrame, capped at max_records."""
    logger.info("Fetching ERDDAP: %s", url)
    try:
        resp = requests.get(url, timeout=180)
        resp.raise_for_status()
    except requests.exceptions.HTTPError as exc:
        logger.error("ERDDAP HTTP error: %s", exc)
        return pd.DataFrame()
    except requests.exceptions.RequestException as exc:
        logger.error("ERDDAP request failed: %s", exc)
        return pd.DataFrame()

    # First row is units, skip it
    df = pd.read_csv(io.StringIO(resp.text), skiprows=[1])
    df.columns = [c.strip() for c in df.columns]

    # Rename to standard column names
    rename = {"time": "time", "latitude": "latitude", "longitude": "longitude"}
    rename[variable] = variable
    df = df.rename(columns=rename)

    df["time"] = pd.to_datetime(df["time"], utc=True, errors="coerce")
    df = df.dropna(subset=["time", "latitude", "longitude", variable])
    df = df.head(max_records)
    df["source"] = SOURCE_NAME
    return df


def collect_sst(
    start_date: str,
    end_date: str,
    max_records: int = DEFAULT_MAX_RECORDS,
    bbox: Optional[dict[str, float]] = None,
    raw_dir: str = DEFAULT_RAW_DIR,
    stride: int = 2,
) -> pd.DataFrame:
    """
    Collect Sea Surface Temperature from NOAA ERDDAP.

    Parameters
    ----------
    start_date : str  e.g. "2023-01-01"
    end_date   : str  e.g. "2023-03-31"
    max_records: int  maximum rows to return
    bbox       : dict override bounding box
    raw_dir    : str  directory to save raw CSV
    stride     : int  spatial stride (higher = coarser, faster)

    Returns
    -------
    pd.DataFrame with columns: time, latitude, longitude, sst, source
    """
    bbox = bbox or INDONESIA_BBOX
    url = _build_erddap_url(ERDDAP_SST_DATASET, "sst", start_date, end_date, bbox, stride)
    df = _fetch_erddap(url, "sst", max_records)

    if df.empty:
        logger.warning("ERDDAP SST returned no data.")
        return df

    df = df.rename(columns={"sst": "sst_celsius"})
    df["variable"] = "sst"

    _save_raw(df, "erddap_sst", start_date, end_date, raw_dir)
    logger.info("ERDDAP SST: %d records collected.", len(df))
    return df


def collect_chlorophyll(
    start_date: str,
    end_date: str,
    max_records: int = DEFAULT_MAX_RECORDS,
    bbox: Optional[dict[str, float]] = None,
    raw_dir: str = DEFAULT_RAW_DIR,
    stride: int = 2,
) -> pd.DataFrame:
    """
    Collect chlorophyll-a concentration from NOAA ERDDAP.

    Returns
    -------
    pd.DataFrame with columns: time, latitude, longitude, chlorophyll_mgm3, source
    """
    bbox = bbox or INDONESIA_BBOX
    url = _build_erddap_url(ERDDAP_CHL_DATASET, "chlorophyll", start_date, end_date, bbox, stride)
    df = _fetch_erddap(url, "chlorophyll", max_records)

    if df.empty:
        logger.warning("ERDDAP chlorophyll returned no data.")
        return df

    df = df.rename(columns={"chlorophyll": "chlorophyll_mgm3"})
    df["variable"] = "chlorophyll"

    _save_raw(df, "erddap_chlorophyll", start_date, end_date, raw_dir)
    logger.info("ERDDAP chlorophyll: %d records collected.", len(df))
    return df


def _save_raw(df: pd.DataFrame, prefix: str, start: str, end: str, raw_dir: str) -> None:
    Path(raw_dir).mkdir(parents=True, exist_ok=True)
    fname = f"{prefix}_{start.replace('-','')}_{end.replace('-','')}.csv"
    path = Path(raw_dir) / fname
    df.to_csv(path, index=False)
    logger.info("Raw data saved: %s", path)
