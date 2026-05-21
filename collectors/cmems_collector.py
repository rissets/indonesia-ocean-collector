"""
Copernicus Marine Service (CMEMS) Collector.

Requires credentials in .env:
  COPERNICUSMARINE_SERVICE_USERNAME=...
  COPERNICUSMARINE_SERVICE_PASSWORD=...

Falls back to realistic mock data when credentials are absent,
clearly marked with source="CMEMS_MOCK".
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from config import (
    DEFAULT_MAX_RECORDS,
    DEFAULT_RAW_DIR,
    INDONESIA_BBOX,
    WPP_REGIONS,
)

logger = logging.getLogger(__name__)

SOURCE_NAME = "CMEMS"
MOCK_SOURCE = "CMEMS_MOCK"


def _has_credentials() -> bool:
    # Support both legacy username/password and new client_id/secret flow
    has_legacy = bool(
        os.getenv("COPERNICUSMARINE_SERVICE_USERNAME")
        and os.getenv("COPERNICUSMARINE_SERVICE_PASSWORD")
    )
    has_client = bool(
        os.getenv("CMEMS_CLIENT_ID")
        and os.getenv("CMEMS_CLIENT_SECRET")
    )
    return has_legacy or has_client


def _configure_credentials() -> None:
    """Set env vars expected by copernicusmarine SDK from client credentials if needed."""
    if os.getenv("COPERNICUSMARINE_SERVICE_USERNAME"):
        return  # legacy creds already set
    client_id = os.getenv("CMEMS_CLIENT_ID")
    secret = os.getenv("CMEMS_CLIENT_SECRET")
    if client_id and secret:
        os.environ["COPERNICUSMARINE_SERVICE_USERNAME"] = client_id
        os.environ["COPERNICUSMARINE_SERVICE_PASSWORD"] = secret


def _generate_mock_grid(
    start_date: str,
    end_date: str,
    bbox: dict[str, float],
    max_records: int,
) -> pd.DataFrame:
    """Generate realistic mock grid data for demo/CI when credentials absent."""
    rng = np.random.default_rng(seed=42)
    dates = pd.date_range(start_date, end_date, freq="MS")
    lats = np.arange(bbox["min_lat"], bbox["max_lat"], 1.0)
    lons = np.arange(bbox["min_lon"], bbox["max_lon"], 1.0)

    rows = []
    for dt in dates:
        for lat in lats:
            for lon in lons:
                rows.append({
                    "time": dt,
                    "latitude": round(lat, 2),
                    "longitude": round(lon, 2),
                })
                if len(rows) >= max_records:
                    break
            if len(rows) >= max_records:
                break
        if len(rows) >= max_records:
            break

    df = pd.DataFrame(rows)
    n = len(df)

    # Realistic SST: 26–30°C tropical range with seasonal variation
    month = df["time"].dt.month
    df["sst_celsius"] = (
        27.5
        + rng.normal(0, 0.8, n)
        + np.sin((month - 1) * np.pi / 6) * 1.2
    ).round(3)

    # Realistic chlorophyll: 0.05–1.5 mg/m³, log-normal
    df["chlorophyll_mgm3"] = np.clip(
        np.exp(rng.normal(-1.2, 0.8, n)), 0.01, 5.0
    ).round(4)

    # Currents: small values typical of Indonesian seas (m/s)
    df["u_current_ms"] = rng.normal(0, 0.15, n).round(4)
    df["v_current_ms"] = rng.normal(0, 0.15, n).round(4)

    # SSH: sea surface height anomaly (m)
    df["ssh_m"] = rng.normal(0, 0.08, n).round(4)

    # Salinity: 32–35 PSU
    df["salinity_psu"] = np.clip(rng.normal(33.5, 0.5, n), 30.0, 36.0).round(3)

    df["source"] = MOCK_SOURCE
    return df


def collect_sst(
    start_date: str,
    end_date: str,
    max_records: int = DEFAULT_MAX_RECORDS,
    bbox: Optional[dict[str, float]] = None,
    raw_dir: str = DEFAULT_RAW_DIR,
) -> pd.DataFrame:
    """
    Collect SST from CMEMS (or mock if no credentials).

    Returns
    -------
    pd.DataFrame: time, latitude, longitude, sst_celsius, source
    """
    bbox = bbox or INDONESIA_BBOX

    if _has_credentials():
        df = _collect_cmems_real(
            dataset_id="cmems_mod_glo_phy_my_0.083deg_P1D-m",
            variables=["thetao"],
            start_date=start_date,
            end_date=end_date,
            bbox=bbox,
            max_records=max_records,
        )
        df = df.rename(columns={"thetao": "sst_celsius"})
        df["source"] = SOURCE_NAME
    else:
        logger.warning("CMEMS credentials not found — using mock SST data.")
        df = _generate_mock_grid(start_date, end_date, bbox, max_records)[
            ["time", "latitude", "longitude", "sst_celsius", "source"]
        ]

    _save_raw(df, "cmems_sst", start_date, end_date, raw_dir)
    logger.info("CMEMS SST: %d records (source=%s).", len(df), df["source"].iloc[0] if len(df) else "none")
    return df


def collect_chlorophyll(
    start_date: str,
    end_date: str,
    max_records: int = DEFAULT_MAX_RECORDS,
    bbox: Optional[dict[str, float]] = None,
    raw_dir: str = DEFAULT_RAW_DIR,
) -> pd.DataFrame:
    """
    Collect chlorophyll-a from CMEMS (or mock).

    Returns
    -------
    pd.DataFrame: time, latitude, longitude, chlorophyll_mgm3, source
    """
    bbox = bbox or INDONESIA_BBOX

    if _has_credentials():
        df = _collect_cmems_real(
            dataset_id="cmems_obs-oc_glo_bgc-plankton_my_l4-gapfree-multi-4km_P1M",
            variables=["CHL"],
            start_date=start_date,
            end_date=end_date,
            bbox=bbox,
            max_records=max_records,
        )
        df = df.rename(columns={"CHL": "chlorophyll_mgm3"})
        df["source"] = SOURCE_NAME
    else:
        logger.warning("CMEMS credentials not found — using mock chlorophyll data.")
        df = _generate_mock_grid(start_date, end_date, bbox, max_records)[
            ["time", "latitude", "longitude", "chlorophyll_mgm3", "source"]
        ]

    _save_raw(df, "cmems_chlorophyll", start_date, end_date, raw_dir)
    return df


def collect_currents(
    start_date: str,
    end_date: str,
    max_records: int = DEFAULT_MAX_RECORDS,
    bbox: Optional[dict[str, float]] = None,
    raw_dir: str = DEFAULT_RAW_DIR,
) -> pd.DataFrame:
    """
    Collect ocean currents (u, v) and SSH from CMEMS (or mock).

    Returns
    -------
    pd.DataFrame: time, latitude, longitude, u_current_ms, v_current_ms, ssh_m, source
    """
    bbox = bbox or INDONESIA_BBOX

    if _has_credentials():
        df = _collect_cmems_real(
            dataset_id="cmems_mod_glo_phy_my_0.083deg_P1D-m",
            variables=["uo", "vo", "zos"],
            start_date=start_date,
            end_date=end_date,
            bbox=bbox,
            max_records=max_records,
        )
        df = df.rename(columns={"uo": "u_current_ms", "vo": "v_current_ms", "zos": "ssh_m"})
        df["source"] = SOURCE_NAME
    else:
        logger.warning("CMEMS credentials not found — using mock currents data.")
        df = _generate_mock_grid(start_date, end_date, bbox, max_records)[
            ["time", "latitude", "longitude", "u_current_ms", "v_current_ms", "ssh_m", "source"]
        ]

    _save_raw(df, "cmems_currents", start_date, end_date, raw_dir)
    logger.info("CMEMS currents: %d records.", len(df))
    return df


def collect_all(
    start_date: str,
    end_date: str,
    max_records: int = DEFAULT_MAX_RECORDS,
    bbox: Optional[dict[str, float]] = None,
    raw_dir: str = DEFAULT_RAW_DIR,
) -> pd.DataFrame:
    """
    Collect SST + chlorophyll + currents and merge into one DataFrame.

    Returns
    -------
    pd.DataFrame with all CMEMS variables merged on time/lat/lon
    """
    bbox = bbox or INDONESIA_BBOX

    if _has_credentials():
        _configure_credentials()
        df = _collect_cmems_real(
            dataset_id="cmems_mod_glo_phy_my_0.083deg_P1D-m",
            variables=["thetao", "uo", "vo", "zos"],
            start_date=start_date,
            end_date=end_date,
            bbox=bbox,
            max_records=max_records,
        )
        if df.empty:
            logger.warning("CMEMS real download returned empty — falling back to mock data.")
            df = _generate_mock_grid(start_date, end_date, bbox, max_records)
        else:
            df = df.rename(columns={
                "thetao": "sst_celsius",
                "uo": "u_current_ms",
                "vo": "v_current_ms",
                "zos": "ssh_m",
            })
            # Merge chlorophyll separately (different dataset)
            chl = collect_chlorophyll(start_date, end_date, max_records, bbox, raw_dir)
            if not chl.empty and "chlorophyll_mgm3" in chl.columns:
                df = df.merge(
                    chl[["time", "latitude", "longitude", "chlorophyll_mgm3"]],
                    on=["time", "latitude", "longitude"],
                    how="left",
                )
            else:
                df["chlorophyll_mgm3"] = None
            df["source"] = SOURCE_NAME
    else:
        df = _generate_mock_grid(start_date, end_date, bbox, max_records)

    _save_raw(df, "cmems_all", start_date, end_date, raw_dir)
    logger.info("CMEMS all: %d records (source=%s).", len(df), df["source"].iloc[0] if len(df) else "none")
    return df


def _collect_cmems_real(
    dataset_id: str,
    variables: list[str],
    start_date: str,
    end_date: str,
    bbox: dict[str, float],
    max_records: int,
) -> pd.DataFrame:
    """Download from CMEMS using copernicusmarine SDK and return DataFrame."""
    try:
        import copernicusmarine  # noqa: PLC0415
        import xarray as xr      # noqa: PLC0415
        import tempfile, os      # noqa: PLC0415, E401
    except ImportError as exc:
        logger.error("Missing dependency: %s. Run: pip install copernicusmarine xarray", exc)
        return pd.DataFrame()

    username = os.getenv("COPERNICUSMARINE_SERVICE_USERNAME") or os.getenv("CMEMS_CLIENT_ID")
    password = os.getenv("COPERNICUSMARINE_SERVICE_PASSWORD") or os.getenv("CMEMS_CLIENT_SECRET")

    with tempfile.NamedTemporaryFile(suffix=".nc", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        copernicusmarine.subset(
            dataset_id=dataset_id,
            variables=variables,
            minimum_longitude=bbox["min_lon"],
            maximum_longitude=bbox["max_lon"],
            minimum_latitude=bbox["min_lat"],
            maximum_latitude=bbox["max_lat"],
            start_datetime=f"{start_date}T00:00:00",
            end_datetime=f"{end_date}T23:59:59",
            minimum_depth=0,
            maximum_depth=1,
            output_filename=tmp_path,
            username=username,
            password=password,
        )
        ds = xr.open_dataset(tmp_path)
        df = ds.to_dataframe().reset_index()
        df = df.dropna(subset=variables)
        df = df.head(max_records)
        df = df.rename(columns={"lon": "longitude", "lat": "latitude"})
        return df
    except Exception as exc:
        logger.error("CMEMS download failed: %s", exc)
        return pd.DataFrame()
    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)


def _save_raw(df: pd.DataFrame, prefix: str, start: str, end: str, raw_dir: str) -> None:
    Path(raw_dir).mkdir(parents=True, exist_ok=True)
    fname = f"{prefix}_{start.replace('-','')}_{end.replace('-','')}.csv"
    path = Path(raw_dir) / fname
    df.to_csv(path, index=False)
    logger.info("Raw saved: %s", path)
