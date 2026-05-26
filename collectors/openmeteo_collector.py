"""
Open-Meteo collector — wave, weather, tidal, and depth data for Indonesian waters.

Marine API  : https://marine-api.open-meteo.com/v1/marine
  Variables : wave_height_max, wave_period_max, ocean_current_velocity_max,
              sea_level_height_msl (pasang_surut)
Forecast API: https://api.open-meteo.com/v1/forecast
  Variables : wind_speed_10m_max, wind_direction_10m_dominant,
              shortwave_radiation_sum, weather_code (cuaca)
Bathymetry API: https://api.opentopodata.org/v1/etopo1
  Used once per grid point to get kedalaman_laut from ETOPO1 bathymetry.

Grid sampled at DEFAULT_GRID_RESOLUTION (0.5°) over WPP regions only.
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
    WPP_REGIONS,
)

logger = logging.getLogger(__name__)

SOURCE_NAME = "Open-Meteo"

_MARINE_URL    = "https://marine-api.open-meteo.com/v1/marine"
_FORECAST_URL  = "https://api.open-meteo.com/v1/forecast"
_ARCHIVE_URL   = "https://archive-api.open-meteo.com/v1/archive"
_BATHY_URL     = "https://api.opentopodata.org/v1/etopo1"
_REQUEST_TIMEOUT = 30

# Forecast API only covers ~7 days into the past; use archive for older dates
_FORECAST_LOOKBACK_DAYS = 7

# WMO weather code → human-readable label (subset covering marine conditions)
_WMO_CODES: dict[int, str] = {
    0: "Cerah", 1: "Cerah Sebagian", 2: "Berawan Sebagian", 3: "Mendung",
    45: "Berkabut", 48: "Berkabut Beku",
    51: "Gerimis Ringan", 53: "Gerimis Sedang", 55: "Gerimis Lebat",
    61: "Hujan Ringan", 63: "Hujan Sedang", 65: "Hujan Lebat",
    71: "Salju Ringan", 73: "Salju Sedang", 75: "Salju Lebat",
    80: "Hujan Lokal Ringan", 81: "Hujan Lokal Sedang", 82: "Hujan Lokal Lebat",
    95: "Badai Petir", 96: "Badai Petir + Hujan Es Ringan",
    99: "Badai Petir + Hujan Es Lebat",
}


def _wmo_to_label(code) -> Optional[str]:
    if code is None:
        return None
    try:
        return _WMO_CODES.get(int(code), f"Kode-{int(code)}")
    except (TypeError, ValueError):
        return None


def _build_grid(bbox: dict[str, float], resolution: float) -> list[tuple[float, float]]:
    lats = np.arange(bbox["min_lat"], bbox["max_lat"], resolution)
    lons = np.arange(bbox["min_lon"], bbox["max_lon"], resolution)
    return [(round(float(lat), 4), round(float(lon), 4)) for lat in lats for lon in lons]


def _build_wpp_grid(resolution: float) -> list[tuple[float, float]]:
    """Build grid covering only WPP regions (deduped)."""
    points: set[tuple[float, float]] = set()
    for bbox in WPP_REGIONS.values():
        points.update(_build_grid(bbox, resolution))
    return sorted(points)


def _clamp_date_range(start_date: str, end_date: str) -> tuple[str, str]:
    """Clamp to 92-day window (Open-Meteo free tier limit for forecast API)."""
    s = pd.Timestamp(start_date)
    e = pd.Timestamp(end_date)
    if (e - s).days > 92:
        e = s + pd.Timedelta(days=92)
        logger.warning("Open-Meteo date range clamped to 92 days: %s → %s", s.date(), e.date())
    return s.strftime("%Y-%m-%d"), e.strftime("%Y-%m-%d")


def _fetch_bathymetry_batch(points: list[tuple[float, float]]) -> dict[tuple[float, float], float]:
    """
    Fetch ETOPO1 elevation/depth for up to 100 points in one call.
    Negative elevation is stored as kedalaman_laut (negative meters), matching
    the target schema example and avoiding dummy depth values.
    Returns dict of (lat, lon) -> elevation_m (negative below sea level).
    """
    if not points:
        return {}
    lats = [p[0] for p in points]
    lons = [p[1] for p in points]
    try:
        resp = requests.get(
            _BATHY_URL,
            params={"locations": "|".join(f"{lat},{lon}" for lat, lon in points)},
            timeout=_REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        results = resp.json().get("results", [])
        result = {}
        for pt, item in zip(points, results):
            elev = item.get("elevation") if isinstance(item, dict) else None
            result[pt] = round(float(elev), 2) if elev is not None and float(elev) < 0 else None
        return result
    except Exception as exc:
        logger.debug("Bathymetry API failed: %s", exc)
        return {}


def _fetch_marine(lat: float, lon: float, start: str, end: str) -> pd.DataFrame:
    # Fetch wave variables as daily
    wave_params = {
        "latitude": lat,
        "longitude": lon,
        "daily": "wave_height_max,wave_period_max",
        "start_date": start,
        "end_date": end,
        "timezone": "UTC",
    }
    # Fetch sea level as hourly (no daily aggregate available), then take daily max
    tidal_params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "sea_level_height_msl",
        "start_date": start,
        "end_date": end,
        "timezone": "UTC",
    }
    try:
        wave_resp = requests.get(_MARINE_URL, params=wave_params, timeout=_REQUEST_TIMEOUT)
        wave_resp.raise_for_status()
        wave_data = wave_resp.json()
    except Exception as exc:
        logger.debug("Marine wave API failed lat=%.2f lon=%.2f: %s", lat, lon, exc)
        return pd.DataFrame()

    daily = wave_data.get("daily", {})
    dates = daily.get("time", [])
    if not dates:
        return pd.DataFrame()

    df = pd.DataFrame({
        "tanggal":           pd.to_datetime(dates),
        "latitude":          lat,
        "longitude":         lon,
        "tinggi_gelombang":  daily.get("wave_height_max"),
        "periode_gelombang": daily.get("wave_period_max"),
    })

    # Fetch tidal data separately and aggregate hourly → daily max
    try:
        tidal_resp = requests.get(_MARINE_URL, params=tidal_params, timeout=_REQUEST_TIMEOUT)
        tidal_resp.raise_for_status()
        tidal_data = tidal_resp.json()
        hourly = tidal_data.get("hourly", {})
        htimes = hourly.get("time", [])
        hvals  = hourly.get("sea_level_height_msl", [])
        if htimes and hvals:
            tidal_df = pd.DataFrame({
                "hour":  pd.to_datetime(htimes),
                "sea_level": hvals,
            })
            tidal_df["tanggal"] = tidal_df["hour"].dt.normalize()
            daily_tidal = tidal_df.groupby("tanggal")["sea_level"].max().reset_index()
            daily_tidal = daily_tidal.rename(columns={"sea_level": "pasang_surut"})
            df = df.merge(daily_tidal, on="tanggal", how="left")
        else:
            df["pasang_surut"] = None
    except Exception as exc:
        logger.debug("Marine tidal API failed lat=%.2f lon=%.2f: %s", lat, lon, exc)
        df["pasang_surut"] = None

    return df


def _fetch_forecast(lat: float, lon: float, start: str, end: str) -> pd.DataFrame:
    # Use archive API for historical dates (older than FORECAST_LOOKBACK_DAYS)
    cutoff = (pd.Timestamp.now("UTC").normalize() - pd.Timedelta(days=_FORECAST_LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    url = _ARCHIVE_URL if start < cutoff else _FORECAST_URL

    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": "wind_speed_10m_max,wind_direction_10m_dominant,shortwave_radiation_sum,weather_code",
        "start_date": start,
        "end_date": end,
        "timezone": "UTC",
        "wind_speed_unit": "ms",
    }
    try:
        resp = requests.get(url, params=params, timeout=_REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        logger.debug("Forecast API failed lat=%.2f lon=%.2f: %s", lat, lon, exc)
        return pd.DataFrame()

    daily = data.get("daily", {})
    dates = daily.get("time", [])
    if not dates:
        return pd.DataFrame()

    weather_codes = daily.get("weather_code", [None] * len(dates))
    cuaca_labels = [_wmo_to_label(c) for c in weather_codes]

    return pd.DataFrame({
        "tanggal":          pd.to_datetime(dates),
        "latitude":         lat,
        "longitude":        lon,
        "kecepatan_angin":  daily.get("wind_speed_10m_max"),
        "arah_angin":       daily.get("wind_direction_10m_dominant"),
        "radiasi_matahari": daily.get("shortwave_radiation_sum"),
        "cuaca":            cuaca_labels,
    })


def collect(
    start_date: str,
    end_date: str,
    max_records: int = DEFAULT_MAX_RECORDS,
    bbox: Optional[dict[str, float]] = None,
    grid_resolution: float = DEFAULT_GRID_RESOLUTION,
    raw_dir: str = DEFAULT_RAW_DIR,
    wpp_only: bool = True,
) -> pd.DataFrame:
    """
    Collect wave, weather, tidal, and depth data from Open-Meteo for Indonesian waters.

    Returns DataFrame with columns:
      tanggal, latitude, longitude,
      tinggi_gelombang, periode_gelombang, pasang_surut,
      kecepatan_angin, arah_angin, radiasi_matahari, cuaca,
      kedalaman_laut, sumber_data

    Parameters
    ----------
    wpp_only : bool
        If True (default), only collect grid points within WPP regions.
        If False, collect over the full Indonesia bbox.
    """
    start, end = _clamp_date_range(start_date, end_date)

    if bbox:
        grid = _build_grid(bbox, grid_resolution)
    elif wpp_only:
        grid = _build_wpp_grid(grid_resolution)
    else:
        grid = _build_grid(INDONESIA_BBOX, grid_resolution)

    # Keep row volume bounded while preserving spatial coverage:
    # sample grid points evenly across all WPP points instead of truncating
    # early to the first few coordinates.
    day_count = max(1, (pd.Timestamp(end) - pd.Timestamp(start)).days + 1)
    max_points = max(1, min(len(grid), max_records // day_count))
    if max_points < len(grid):
        step = len(grid) / float(max_points)
        sample_idx = sorted({int(i * step) for i in range(max_points)})
        grid = [grid[i] for i in sample_idx]

    depth_map: dict[tuple[float, float], float] = {}
    batch_size = 100
    for i in range(0, len(grid), batch_size):
        batch = grid[i:i + batch_size]
        depth_map.update(_fetch_bathymetry_batch(batch))

    point_frames: list[pd.DataFrame] = []

    for lat, lon in grid:
        m = _fetch_marine(lat, lon, start, end)
        f = _fetch_forecast(lat, lon, start, end)

        if m.empty and f.empty:
            continue

        key = ["tanggal", "latitude", "longitude"]
        if not m.empty and not f.empty:
            depth = depth_map.get((lat, lon))
            m["kedalaman_laut"] = depth
            merged = m.merge(f, on=key, how="outer")
        elif not m.empty:
            depth = depth_map.get((lat, lon))
            m["kedalaman_laut"] = depth
            merged = m
        else:
            merged = f
            merged["kedalaman_laut"] = None

        point_frames.append(merged)

    if not point_frames:
        logger.warning("Open-Meteo returned no data.")
        return pd.DataFrame()

    df = pd.concat(point_frames, ignore_index=True)

    # Ensure kedalaman_laut column exists
    if "kedalaman_laut" not in df.columns:
        df["kedalaman_laut"] = None

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
