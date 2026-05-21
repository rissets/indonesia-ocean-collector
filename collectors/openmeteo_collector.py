"""
Open-Meteo collector — wave and weather data for Indonesian waters.

Marine API  : https://marine-api.open-meteo.com/v1/marine
  Variables : wave_height, wave_period
Forecast API: https://api.open-meteo.com/v1/forecast
  Variables : wind_speed_10m, wind_direction_10m, shortwave_radiation

Grid is sampled at DEFAULT_GRID_RESOLUTION (0.5°) over INDONESIA_BBOX.
No API key required.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import requests

from config import (
    DEFAULT_GRID_RESOLUTION,
    DEFAULT_MAX_RECORDS,
    DEFAULT_RAW_DIR,
    INDONESIA_BBOX,
)

logger = logging.getLogger(__name__)

SOURCE_NAME = "Open-Meteo"

_MARINE_URL   = "https://marine-api.open-meteo.com/v1/marine"
_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"

_REQUEST_TIMEOUT = 30  # seconds per point — Open-Meteo is fast


def _build_grid(bbox: dict[str, float], resolution: float) -> list[tuple[float, float]]:
    lats = np.arange(bbox["min_lat"], bbox["max_lat"], resolution)
    lons = np.arange(bbox["min_lon"], bbox["max_lon"], resolution)
    return [(round(float(lat), 4), round(float(lon), 4)) for lat in lats for lon in lons]


def _date_range_daily(start_date: str, end_date: str) -> tuple[str, str]:
    """Return (start, end) clamped to a 92-day window (Open-Meteo free tier limit)."""
    s = pd.Timestamp(start_date)
    e = pd.Timestamp(end_date)
    if (e - s).days > 92:
        e = s + pd.Timedelta(days=92)
        logger.warning("Open-Meteo date range clamped to 92 days: %s → %s", s.date(), e.date())
    return s.strftime("%Y-%m-%d"), e.strftime("%Y-%m-%d")


def _fetch_marine(lat: float, lon: float, start: str, end: str) -> pd.DataFrame:
    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": "wave_height_max,wave_period_max",
        "start_date": start,
        "end_date": end,
        "timezone": "UTC",
    }
    try:
        resp = requests.get(_MARINE_URL, params=params, timeout=_REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        logger.debug("Marine API failed lat=%.2f lon=%.2f: %s", lat, lon, exc)
        return pd.DataFrame()

    daily = data.get("daily", {})
    dates = daily.get("time", [])
    if not dates:
        return pd.DataFrame()

    df = pd.DataFrame({
        "tanggal":          pd.to_datetime(dates),
        "latitude":         lat,
        "longitude":        lon,
        "tinggi_gelombang": daily.get("wave_height_max"),
        "periode_gelombang":daily.get("wave_period_max"),
    })
    return df


def _fetch_forecast(lat: float, lon: float, start: str, end: str) -> pd.DataFrame:
    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": "wind_speed_10m_max,wind_direction_10m_dominant,shortwave_radiation_sum",
        "start_date": start,
        "end_date": end,
        "timezone": "UTC",
        "wind_speed_unit": "ms",
    }
    try:
        resp = requests.get(_FORECAST_URL, params=params, timeout=_REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        logger.debug("Forecast API failed lat=%.2f lon=%.2f: %s", lat, lon, exc)
        return pd.DataFrame()

    daily = data.get("daily", {})
    dates = daily.get("time", [])
    if not dates:
        return pd.DataFrame()

    df = pd.DataFrame({
        "tanggal":          pd.to_datetime(dates),
        "latitude":         lat,
        "longitude":        lon,
        "kecepatan_angin":  daily.get("wind_speed_10m_max"),
        "arah_angin":       daily.get("wind_direction_10m_dominant"),
        "radiasi_matahari": daily.get("shortwave_radiation_sum"),
    })
    return df


def collect(
    start_date: str,
    end_date: str,
    max_records: int = DEFAULT_MAX_RECORDS,
    bbox: Optional[dict[str, float]] = None,
    grid_resolution: float = DEFAULT_GRID_RESOLUTION,
    raw_dir: str = DEFAULT_RAW_DIR,
) -> pd.DataFrame:
    """
    Collect wave + weather data from Open-Meteo for a grid over Indonesia.

    Returns
    -------
    pd.DataFrame with columns:
      tanggal, latitude, longitude,
      tinggi_gelombang, periode_gelombang,
      kecepatan_angin, arah_angin, radiasi_matahari,
      sumber_data
    """
    bbox = bbox or INDONESIA_BBOX
    start, end = _date_range_daily(start_date, end_date)
    grid = _build_grid(bbox, grid_resolution)

    marine_frames: list[pd.DataFrame] = []
    forecast_frames: list[pd.DataFrame] = []
    collected = 0

    for lat, lon in grid:
        if collected >= max_records:
            break

        m = _fetch_marine(lat, lon, start, end)
        f = _fetch_forecast(lat, lon, start, end)

        if not m.empty:
            marine_frames.append(m)
        if not f.empty:
            forecast_frames.append(f)

        collected += max(len(m), len(f), 1)

    if not marine_frames and not forecast_frames:
        logger.warning("Open-Meteo returned no data.")
        return pd.DataFrame()

    key = ["tanggal", "latitude", "longitude"]

    if marine_frames and forecast_frames:
        marine_all   = pd.concat(marine_frames,   ignore_index=True)
        forecast_all = pd.concat(forecast_frames, ignore_index=True)
        df = marine_all.merge(forecast_all, on=key, how="outer")
    elif marine_frames:
        df = pd.concat(marine_frames, ignore_index=True)
    else:
        df = pd.concat(forecast_frames, ignore_index=True)

    df["sumber_data"] = SOURCE_NAME
    df = df.head(max_records)

    _save_raw(df, "openmeteo", start_date, end_date, raw_dir)
    logger.info("Open-Meteo: %d records (%d grid points).", len(df), len(grid))
    return df


def _save_raw(df: pd.DataFrame, prefix: str, start: str, end: str, raw_dir: str) -> None:
    Path(raw_dir).mkdir(parents=True, exist_ok=True)
    fname = f"{prefix}_{start.replace('-','')}_{end.replace('-','')}.csv"
    path = Path(raw_dir) / fname
    df.to_csv(path, index=False)
    logger.info("Raw saved: %s (%d rows)", path, len(df))
