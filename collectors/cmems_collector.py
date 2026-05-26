"""
Copernicus Marine Service (CMEMS) Collector.

Requires credentials in .env:
  COPERNICUSMARINE_SERVICE_USERNAME=...
  COPERNICUSMARINE_SERVICE_PASSWORD=...
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Optional

import pandas as pd

from config import (
    DEFAULT_MAX_RECORDS,
    DEFAULT_RAW_DIR,
    INDONESIA_BBOX,
    WPP_REGIONS,
)

logger = logging.getLogger(__name__)

SOURCE_NAME = "CMEMS"

def _has_credentials() -> bool:
    return bool(
        os.getenv("COPERNICUSMARINE_SERVICE_USERNAME")
        and os.getenv("COPERNICUSMARINE_SERVICE_PASSWORD")
    )


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

    if not _has_credentials():
        raise RuntimeError("CMEMS credentials are required; mock fallback is disabled for production.")
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

    if not _has_credentials():
        raise RuntimeError("CMEMS credentials are required; mock fallback is disabled for production.")
    df = _collect_cmems_real(
        dataset_id="cmems_obs-oc_glo_bgc-plankton_my_l4-gapfree-multi-4km_P1D",
        variables=["CHL"],
        start_date=start_date,
        end_date=end_date,
        bbox=bbox,
        max_records=max_records,
    )
    df = df.rename(columns={"CHL": "chlorophyll_mgm3"})
    df["source"] = SOURCE_NAME

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

    if not _has_credentials():
        raise RuntimeError("CMEMS credentials are required; mock fallback is disabled for production.")
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

    if not _has_credentials():
        raise RuntimeError("CMEMS credentials are required; mock fallback is disabled for production.")
    df = _collect_cmems_real(
        dataset_id="cmems_mod_glo_phy_my_0.083deg_P1D-m",
        variables=["thetao", "uo", "vo", "zos"],
        start_date=start_date,
        end_date=end_date,
        bbox=bbox,
        max_records=max_records,
    )
    df = df.rename(columns={
        "thetao": "sst_celsius",
        "uo": "u_current_ms",
        "vo": "v_current_ms",
        "zos": "ssh_m",
    })
    chl = collect_chlorophyll(start_date, end_date, max_records, bbox, raw_dir)
    if not chl.empty:
        # Physical and biogeochemical products often use different grids.
        # Merge chlorophyll by day and rounded coordinates for practical alignment.
        phys = df.copy()
        phys["date_key"] = pd.to_datetime(phys["time"], utc=True, errors="coerce").dt.date
        phys["lat_key"] = pd.to_numeric(phys["latitude"], errors="coerce").round(1)
        phys["lon_key"] = pd.to_numeric(phys["longitude"], errors="coerce").round(1)

        bio = chl.copy()
        bio["date_key"] = pd.to_datetime(bio["time"], utc=True, errors="coerce").dt.date
        bio["lat_key"] = pd.to_numeric(bio["latitude"], errors="coerce").round(1)
        bio["lon_key"] = pd.to_numeric(bio["longitude"], errors="coerce").round(1)
        bio = (
            bio[["date_key", "lat_key", "lon_key", "chlorophyll_mgm3"]]
            .dropna(subset=["date_key", "lat_key", "lon_key", "chlorophyll_mgm3"])
            .groupby(["date_key", "lat_key", "lon_key"], as_index=False)["chlorophyll_mgm3"]
            .mean()
        )

        phys = phys.merge(
            bio,
            on=["date_key", "lat_key", "lon_key"],
            how="left",
        )
        phys = phys.drop(columns=["date_key", "lat_key", "lon_key"])
        df = phys
    df["source"] = SOURCE_NAME

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

    try:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = os.path.join(tmp_dir, "subset.nc")
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
                output_directory=tmp_dir,
                output_filename="subset.nc",
                overwrite=True,
            )
            ds = xr.open_dataset(tmp_path, engine="netcdf4")
            df = ds.to_dataframe().reset_index()
            df = df.dropna(subset=variables)
            # Avoid biased top-rows sampling (which often sticks to one latitude
            # band) by taking evenly spaced rows across the full subset.
            if len(df) > max_records:
                step = len(df) / float(max_records)
                indices = [int(i * step) for i in range(max_records)]
                df = df.iloc[indices]
            df = df.rename(columns={"lon": "longitude", "lat": "latitude"})
            return df
    except Exception as exc:
        logger.error("CMEMS download failed: %s", exc)
        return pd.DataFrame()


def _save_raw(df: pd.DataFrame, prefix: str, start: str, end: str, raw_dir: str) -> None:
    Path(raw_dir).mkdir(parents=True, exist_ok=True)
    fname = f"{prefix}_{start.replace('-','')}_{end.replace('-','')}.csv"
    path = Path(raw_dir) / fname
    df.to_csv(path, index=False)
    logger.info("Raw saved: %s", path)
