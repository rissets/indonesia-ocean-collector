"""
Open-Meteo Marine + Forecast Collector.

Marine API  : https://marine-api.open-meteo.com/v1/marine
  - wave_height, wave_period

Forecast API: https://api.open-meteo.com/v1/forecast
  - wind_speed_10m, wind_direction_10m, shortwave_radiation

Grid: Indonesia bbox at configurable resolution (default 0.5°).
No API key required.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import requests

from config import DEFAULT_MAX_RECORDS, DEFAULT_RAW_DIR, INDONESIA_BBOX

logger = logging.getLogger(__name__)
SOURCE_NAME = "Open-Meteo"

_MARINE_URL = "https://marine-api.open-meteo.com/v1/marine"
_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
_SESSION = requests.Session()
_SESSION.headers.update({"Accept-Encoding": "gzip"})


def _grid_points(bbox: dict, resolution: float) -> list[tuple[float, float]]:
    lats = np.arange(bbox["min_lat"], bbox["max_lat"] + resolution / 2, resolution)
    lons = np.arange(bbox["min_lon"], bbox["max_lon"] + resolution / 2, resolution)
    return [(round(float(la), 4), round(float(lo), 4)) for la in lats for lo in lons]


def _fetch_marine(lat: float, lon: float, start: str, end: str) -> dict | None:
    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": "wave_height_max,wave_period_max",
        "start_date": start,
        "end_date": end,
        "timezone": "UTC",
    }
    try:
        r = _SESSION.get(_MARINE_URL, params=params, timeout=30)
        r.raise_for_status()
        return r.json()
    except Exception as exc:
        logger.debug("Marine API error at (%.2f, %.2f): %s", lat, lon, exc)
        return None


def _fetch_forecast(lat: float, lon: float, start: str, end: str) -> dict | None:
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
        r = _SESSION.get(_FORECAST_URL, params=params, timeout=30)
        r.raise_for_status()
        return r.json()
    except Exception as exc:
        logger.debug("Forecast API error at (%.2f, %.2f): %s", lat, lon, exc)
        return None


def _parse_daily(data: dict, lat: float, lon: float, col_map: dict[str, str]) -> pd.DataFrame:
    daily = data.get("daily", {})
    dates = daily.get("time", [])
    if not dates:
        return pd.DataFrame()
    df = pd.DataFrame({"time": pd.to_datetime(dates, utc=True)})
    df["latitude"] = lat
    df["longitude"] = lon
    for api_col, our_col in col_map.items():
        df[our_col] = daily.get(api_col)
    return df


def collect_weather(
    start_date: str,
    end_date: str,
    max_records: int = DEFAULT_MAX_RECORDS,
    bbox: Optional[dict] = None,
    raw_dir: str = DEFAULT_RAW_DIR,
    grid_resolution: float = 0.5,
) -> pd.DataFrame:
    """
    Collect wave + wind + solar radiation from Open-Meteo for Indonesia grid.

    Returns
    -------
    pd.DataFrame: time, latitude, longitude,
                  wave_height_m, wave_period_s,
                  wind_speed_ms, wind_direction_deg,
                  solar_radiation_wm2, source
    """
    bbox = bbox or INDONESIA_BBOX
    points = _grid_points(bbox, grid_resolution)

    # Open-Meteo historical API only goes back to 1940 for forecast vars
    # and ~1984 for marine. Clamp end_date to today.
    today = pd.Timestamp.utcnow().strftime("%Y-%m-%d")
    end_date = min(end_date, today)

    all_dfs: list[pd.DataFrame] = []
    collected = 0

    for i, (lat, lon) in enumerate(points):
        if collected >= max_records:
            break

        marine_data = _fetch_marine(lat, lon, start_date, end_date)
        forecast_data = _fetch_forecast(lat, lon, start_date, end_date)

        marine_df = pd.DataFrame()
        forecast_df = pd.DataFrame()

        if marine_data:
            marine_df = _parse_daily(marine_data, lat, lon, {
                "wave_height_max": "wave_height_m",
                "wave_period_max": "wave_period_s",
            })

        if forecast_data:
            forecast_df = _parse_daily(forecast_data, lat, lon, {
                "wind_speed_10m_max": "wind_speed_ms",
                "wind_direction_10m_dominant": "wind_direction_deg",
                "shortwave_radiation_sum": "solar_radiation_wm2",
            })

        if not marine_df.empty and not forecast_df.empty:
            merged = marine_df.merge(
                forecast_df.drop(columns=["latitude", "longitude"]),
                on="time",
                how="outer",
            )
        elif not marine_df.empty:
            merged = marine_df
        elif not forecast_df.empty:
            merged = forecast_df
        else:
            continue

        merged["source"] = SOURCE_NAME
        all_dfs.append(merged)
        collected += len(merged)

        # Polite rate limiting — Open-Meteo allows ~10k req/day free
        if i % 20 == 19:
            time.sleep(0.5)

        if (i + 1) % 50 == 0:
            logger.info("Open-Meteo: processed %d/%d grid points, %d rows so far",
                        i + 1, len(points), collected)

    if not all_dfs:
        logger.warning("Open-Meteo returned no data.")
        return pd.DataFrame()

    result = pd.concat(all_dfs, ignore_index=True).head(max_records)
    _save_raw(result, "openmeteo_weather", start_date, end_date, raw_dir)
    logger.info("Open-Meteo: %d records collected.", len(result))
    return result


def _save_raw(df: pd.DataFrame, prefix: str, start: str, end: str, raw_dir: str) -> None:
    Path(raw_dir).mkdir(parents=True, exist_ok=True)
    fname = f"{prefix}_{start.replace('-','')}_{end.replace('-','')}.csv"
    path = Path(raw_dir) / fname
    df.to_csv(path, index=False)
    logger.info("Raw saved: %s (%d rows)", path, len(df))
