"""
NOAA ERDDAP Collector — no authentication required.

Collects SST, chlorophyll-a, SSH, and geostrophic currents for Indonesian waters
from NOAA CoastWatch ERDDAP server. All datasets are gap-free (no NaN from cloud masking).

Strategy: collect per WPP region per month to ensure even spatial coverage
across all of Indonesia, not just one corner of the bbox.

Datasets used:
  SST        : erdMH1sstdmday_R2022SQNotMasked  (MODIS Aqua monthly, ~4km, gap-free)
  Chlorophyll: erdMH1chlamday                    (MODIS Aqua monthly, ~4km)
  SSH + Currents: nesdisSSH1day                  (NESDIS altimetry daily, 0.25°, gap-free)
"""

from __future__ import annotations

import io
import logging
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
import urllib3.util.connection as urllib3_connection

from config import (
    DEFAULT_MAX_RECORDS,
    DEFAULT_RAW_DIR,
    ERDDAP_BASE_URL,
    ERDDAP_CHL_DATASET,
    ERDDAP_SSH_DATASET,
    ERDDAP_SST_DATASET,
    INDONESIA_BBOX,
    WPP_REGIONS,
)

logger = logging.getLogger(__name__)
SOURCE_NAME = "NOAA_ERDDAP"

# Force IPv4 for ERDDAP requests. The VM repeatedly stalls on IPv6 SYN-SENT
# to coastwatch.pfeg.noaa.gov, while IPv4 is reachable and stable.
urllib3_connection.HAS_IPV6 = False

# Records per WPP per month — keeps data evenly distributed
_RECORDS_PER_CHUNK = 50


def _fetch_erddap_csv(url: str, max_rows: int) -> pd.DataFrame:
    """Fetch CSV from ERDDAP, skip units row, return DataFrame capped at max_rows."""
    logger.debug("Fetching: %s", url)
    try:
        # Keep connect timeout short so unreachable IPv6 routes do not stall
        # batch backfill tasks for minutes before retrying on IPv4.
        resp = requests.get(url, timeout=(20, 300))
        resp.raise_for_status()
    except requests.exceptions.HTTPError as exc:
        logger.warning("ERDDAP HTTP error: %s", exc)
        return pd.DataFrame()
    except requests.exceptions.RequestException as exc:
        logger.warning("ERDDAP request failed: %s", exc)
        return pd.DataFrame()

    df = pd.read_csv(io.StringIO(resp.text), skiprows=[1])
    df.columns = [c.strip() for c in df.columns]
    df["time"] = pd.to_datetime(df["time"], utc=True, errors="coerce")
    df = df.dropna(subset=["time", "latitude", "longitude"])
    df["source"] = SOURCE_NAME
    return df.head(max_rows)


def _build_url(dataset: str, variable: str, date: str, bbox: dict, stride: int = 4) -> str:
    """Build ERDDAP griddap CSV URL for a single time step."""
    return (
        f"{ERDDAP_BASE_URL}/{dataset}.csv"
        f"?{variable}"
        f"[({date}):1:({date})]"
        f"[({bbox['min_lat']}):{stride}:({bbox['max_lat']})]"
        f"[({bbox['min_lon']}):{stride}:({bbox['max_lon']})]"
    )


def _monthly_dates(start_date: str, end_date: str, dataset_type: str = "monthly") -> list[str]:
    """Return representative dates per month (16th for monthly MODIS, every 5 days for SSH)."""
    months = pd.date_range(start_date, end_date, freq="MS")
    if dataset_type == "monthly":
        return [m.replace(day=16).strftime("%Y-%m-%d") for m in months]
    else:
        # For daily SSH: sample every 10 days per month
        dates = []
        for m in months:
            for day in [5, 15, 25]:
                try:
                    dates.append(m.replace(day=day).strftime("%Y-%m-%d"))
                except ValueError:
                    pass
        return dates


def collect_sst(
    start_date: str,
    end_date: str,
    max_records: int = DEFAULT_MAX_RECORDS,
    bbox: Optional[dict] = None,
    raw_dir: str = DEFAULT_RAW_DIR,
    stride: int = 4,
) -> pd.DataFrame:
    """
    Collect monthly SST from MODIS Aqua (gap-free, no cloud masking).
    Collects per WPP region per month for even spatial coverage.

    Parameters
    ----------
    start_date  : str  e.g. "2023-01-01"
    end_date    : str  e.g. "2023-03-31"
    max_records : int  total max rows across all regions/months
    bbox        : dict override bounding box (None = collect per WPP)
    raw_dir     : str  directory to save raw CSV
    stride      : int  spatial stride (4 = ~8km, good balance speed/coverage)

    Returns
    -------
    pd.DataFrame: time, latitude, longitude, sst_celsius, source
    """
    dates = _monthly_dates(start_date, end_date, "monthly")
    regions = {None: bbox} if bbox else WPP_REGIONS
    records_per_chunk = max(10, max_records // (len(dates) * len(regions)))

    all_dfs = []
    for date in dates:
        for region_name, region_bbox in regions.items():
            url = _build_url(ERDDAP_SST_DATASET, "sst", date, region_bbox, stride)
            df = _fetch_erddap_csv(url, records_per_chunk)
            if not df.empty:
                df = df.rename(columns={"sst": "sst_celsius"})
                df = df.dropna(subset=["sst_celsius"])
                if region_name:
                    df["wpp_hint"] = region_name
                all_dfs.append(df)

    if not all_dfs:
        logger.warning("SST returned no data.")
        return pd.DataFrame()

    result = pd.concat(all_dfs, ignore_index=True).head(max_records)
    _save_raw(result, "erddap_sst", start_date, end_date, raw_dir)
    logger.info("ERDDAP SST: %d records across %d regions.", len(result), len(regions))
    return result


def collect_chlorophyll(
    start_date: str,
    end_date: str,
    max_records: int = DEFAULT_MAX_RECORDS,
    bbox: Optional[dict] = None,
    raw_dir: str = DEFAULT_RAW_DIR,
    stride: int = 4,
) -> pd.DataFrame:
    """
    Collect monthly chlorophyll-a from MODIS Aqua.
    Collects per WPP region per month for even spatial coverage.

    Returns
    -------
    pd.DataFrame: time, latitude, longitude, chlorophyll_mgm3, source
    """
    dates = _monthly_dates(start_date, end_date, "monthly")
    regions = {None: bbox} if bbox else WPP_REGIONS
    records_per_chunk = max(10, max_records // (len(dates) * len(regions)))

    all_dfs = []
    for date in dates:
        for region_name, region_bbox in regions.items():
            url = _build_url(ERDDAP_CHL_DATASET, "chlorophyll", date, region_bbox, stride)
            df = _fetch_erddap_csv(url, records_per_chunk)
            if not df.empty:
                df = df.rename(columns={"chlorophyll": "chlorophyll_mgm3"})
                df = df.dropna(subset=["chlorophyll_mgm3"])
                if region_name:
                    df["wpp_hint"] = region_name
                all_dfs.append(df)

    if not all_dfs:
        logger.warning("Chlorophyll returned no data.")
        return pd.DataFrame()

    result = pd.concat(all_dfs, ignore_index=True).head(max_records)
    _save_raw(result, "erddap_chlorophyll", start_date, end_date, raw_dir)
    logger.info("ERDDAP chlorophyll: %d records.", len(result))
    return result


def collect_ssh_currents(
    start_date: str,
    end_date: str,
    max_records: int = DEFAULT_MAX_RECORDS,
    bbox: Optional[dict] = None,
    raw_dir: str = DEFAULT_RAW_DIR,
    stride: int = 2,
) -> pd.DataFrame:
    """
    Collect SSH (sea level anomaly) and geostrophic currents from NESDIS altimetry.
    Data is gap-free (derived from satellite altimetry, not optical).
    Collects per WPP region, sampling 3 dates per month.

    Returns
    -------
    pd.DataFrame: time, latitude, longitude, ssh_m, u_current_ms, v_current_ms, source
    """
    dates = _monthly_dates(start_date, end_date, "daily")
    regions = {None: bbox} if bbox else WPP_REGIONS
    records_per_chunk = max(5, max_records // (len(dates) * len(regions)))

    all_dfs = []
    for date in dates:
        for region_name, region_bbox in regions.items():
            chunk_dfs = []
            for var, col in [("sla", "ssh_m"), ("ugos", "u_current_ms"), ("vgos", "v_current_ms")]:
                url = _build_url(ERDDAP_SSH_DATASET, var, date, region_bbox, stride)
                df_var = _fetch_erddap_csv(url, records_per_chunk)
                if not df_var.empty:
                    df_var = df_var.rename(columns={var: col})
                    df_var = df_var.dropna(subset=[col])
                    chunk_dfs.append(df_var[["time", "latitude", "longitude", col, "source"]])

            if chunk_dfs:
                merged = chunk_dfs[0]
                for d in chunk_dfs[1:]:
                    merged = merged.merge(
                        d.drop(columns=["source"]),
                        on=["time", "latitude", "longitude"],
                        how="outer"
                    )
                if region_name:
                    merged["wpp_hint"] = region_name
                all_dfs.append(merged)

    if not all_dfs:
        logger.warning("SSH/currents returned no data.")
        return pd.DataFrame()

    result = pd.concat(all_dfs, ignore_index=True).head(max_records)
    result["source"] = SOURCE_NAME
    _save_raw(result, "erddap_ssh_currents", start_date, end_date, raw_dir)
    logger.info("ERDDAP SSH+currents: %d records.", len(result))
    return result


def _save_raw(df: pd.DataFrame, prefix: str, start: str, end: str, raw_dir: str) -> None:
    Path(raw_dir).mkdir(parents=True, exist_ok=True)
    fname = f"{prefix}_{start.replace('-','')}_{end.replace('-','')}.csv"
    path = Path(raw_dir) / fname
    df.to_csv(path, index=False)
    logger.info("Raw saved: %s (%d rows)", path, len(df))
