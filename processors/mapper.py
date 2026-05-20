"""
Data mapper — merges all source DataFrames into a single unified CSV.

Output schema:
  month, lat_grid, lon_grid,
  sst_mean, chlorophyll_mean, u_current_mean, v_current_mean, ssh_mean,
  fishing_effort_hours, vessel_count,
  fishing_ground_index, wpp_region, data_sources
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from config import (
    DEFAULT_GRID_RESOLUTION,
    DEFAULT_OUTPUT_DIR,
    FGI_CHLOROPHYLL_WEIGHT,
    FGI_OPTIMAL_SST,
    FGI_SST_TOLERANCE,
    FGI_SST_WEIGHT,
    WPP_REGIONS,
)

logger = logging.getLogger(__name__)


def _snap_to_grid(df: pd.DataFrame, res: float) -> pd.DataFrame:
    """Snap lat/lon to nearest grid cell centre."""
    df = df.copy()
    df["lat_grid"] = (np.floor(df["latitude"] / res) * res + res / 2).round(4)
    df["lon_grid"] = (np.floor(df["longitude"] / res) * res + res / 2).round(4)
    return df


def _to_month(df: pd.DataFrame) -> pd.DataFrame:
    """Add 'month' column (YYYY-MM string) from 'time'."""
    df = df.copy()
    df["time"] = pd.to_datetime(df["time"], utc=True, errors="coerce")
    df["month"] = df["time"].dt.to_period("M").astype(str)
    return df


def _assign_wpp(df: pd.DataFrame) -> pd.DataFrame:
    """Assign WPP region name based on lat/lon."""
    df = df.copy()
    if "wpp_region" in df.columns:
        return df

    def _find_wpp(lat: float, lon: float) -> str:
        for name, bbox in WPP_REGIONS.items():
            if bbox["min_lat"] <= lat <= bbox["max_lat"] and \
               bbox["min_lon"] <= lon <= bbox["max_lon"]:
                return name
        return "OUTSIDE_WPP"

    df["wpp_region"] = df.apply(lambda r: _find_wpp(r["lat_grid"], r["lon_grid"]), axis=1)
    return df


def _compute_fgi(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute Fishing Ground Index (0–1).

    FGI = 0.6 × chl_norm  +  0.4 × sst_score
    where:
      chl_norm  = min-max normalised chlorophyll
      sst_score = 1 - |SST - optimal| / tolerance  (clipped 0–1)
    """
    df = df.copy()

    # Chlorophyll score
    chl = df["chlorophyll_mean"].fillna(0)
    chl_min, chl_max = chl.min(), chl.max()
    if chl_max > chl_min:
        df["chl_norm"] = (chl - chl_min) / (chl_max - chl_min)
    else:
        df["chl_norm"] = 0.0

    # SST score
    sst = df["sst_mean"].fillna(FGI_OPTIMAL_SST)
    df["sst_score"] = (1 - (sst - FGI_OPTIMAL_SST).abs() / FGI_TOLERANCE).clip(0, 1)

    df["fishing_ground_index"] = (
        FGI_CHLOROPHYLL_WEIGHT * df["chl_norm"]
        + FGI_SST_WEIGHT * df["sst_score"]
    ).round(4)

    df = df.drop(columns=["chl_norm", "sst_score"])
    return df


# expose tolerance constant for use in _compute_fgi
FGI_TOLERANCE = FGI_SST_TOLERANCE


def merge_all_sources(
    sst_df: Optional[pd.DataFrame] = None,
    chlorophyll_df: Optional[pd.DataFrame] = None,
    currents_df: Optional[pd.DataFrame] = None,
    effort_df: Optional[pd.DataFrame] = None,
    grid_resolution: float = DEFAULT_GRID_RESOLUTION,
) -> pd.DataFrame:
    """
    Merge all source DataFrames into a unified fishing ground dataset.

    Parameters
    ----------
    sst_df         : DataFrame with columns time, latitude, longitude, sst_celsius
    chlorophyll_df : DataFrame with columns time, latitude, longitude, chlorophyll_mgm3
    currents_df    : DataFrame with columns time, latitude, longitude, u_current_ms, v_current_ms, ssh_m
    effort_df      : DataFrame with columns time, latitude, longitude, wpp_region, fishing_effort_hours, vessel_count
    grid_resolution: float  spatial grid resolution in degrees

    Returns
    -------
    pd.DataFrame with unified schema
    """
    res = grid_resolution
    sources_used: list[str] = []

    # ---- SST ----
    if sst_df is not None and not sst_df.empty:
        sst = _snap_to_grid(_to_month(sst_df), res)
        sst_agg = (
            sst.groupby(["month", "lat_grid", "lon_grid"])["sst_celsius"]
            .mean()
            .reset_index()
            .rename(columns={"sst_celsius": "sst_mean"})
        )
        sst_agg["sst_mean"] = sst_agg["sst_mean"].round(3)
        sources_used.append(sst_df["source"].iloc[0] if "source" in sst_df.columns else "unknown")
    else:
        sst_agg = pd.DataFrame(columns=["month", "lat_grid", "lon_grid", "sst_mean"])

    # ---- Chlorophyll ----
    if chlorophyll_df is not None and not chlorophyll_df.empty:
        chl = _snap_to_grid(_to_month(chlorophyll_df), res)
        chl_agg = (
            chl.groupby(["month", "lat_grid", "lon_grid"])["chlorophyll_mgm3"]
            .mean()
            .reset_index()
            .rename(columns={"chlorophyll_mgm3": "chlorophyll_mean"})
        )
        chl_agg["chlorophyll_mean"] = chl_agg["chlorophyll_mean"].round(4)
        src = chlorophyll_df["source"].iloc[0] if "source" in chlorophyll_df.columns else "unknown"
        if src not in sources_used:
            sources_used.append(src)
    else:
        chl_agg = pd.DataFrame(columns=["month", "lat_grid", "lon_grid", "chlorophyll_mean"])

    # ---- Currents ----
    if currents_df is not None and not currents_df.empty:
        cur = _snap_to_grid(_to_month(currents_df), res)
        cur_agg = (
            cur.groupby(["month", "lat_grid", "lon_grid"])[
                ["u_current_ms", "v_current_ms", "ssh_m"]
            ]
            .mean()
            .reset_index()
            .rename(columns={
                "u_current_ms": "u_current_mean",
                "v_current_ms": "v_current_mean",
                "ssh_m": "ssh_mean",
            })
        )
        for col in ["u_current_mean", "v_current_mean", "ssh_mean"]:
            cur_agg[col] = cur_agg[col].round(4)
        src = currents_df["source"].iloc[0] if "source" in currents_df.columns else "unknown"
        if src not in sources_used:
            sources_used.append(src)
    else:
        cur_agg = pd.DataFrame(columns=["month", "lat_grid", "lon_grid",
                                         "u_current_mean", "v_current_mean", "ssh_mean"])

    # ---- Fishing Effort ----
    if effort_df is not None and not effort_df.empty:
        eff = _snap_to_grid(_to_month(effort_df), res)
        eff_agg = (
            eff.groupby(["month", "lat_grid", "lon_grid"])
            .agg(
                fishing_effort_hours=("fishing_effort_hours", "sum"),
                vessel_count=("vessel_count", "sum"),
                wpp_region=("wpp_region", "first"),
            )
            .reset_index()
        )
        eff_agg["fishing_effort_hours"] = eff_agg["fishing_effort_hours"].round(2)
        src = effort_df["source"].iloc[0] if "source" in effort_df.columns else "unknown"
        if src not in sources_used:
            sources_used.append(src)
    else:
        eff_agg = pd.DataFrame(columns=["month", "lat_grid", "lon_grid",
                                          "fishing_effort_hours", "vessel_count", "wpp_region"])

    # ---- Merge all on month + grid ----
    key = ["month", "lat_grid", "lon_grid"]

    # Start from whichever has data
    frames = [f for f in [sst_agg, chl_agg, cur_agg, eff_agg] if not f.empty]
    if not frames:
        logger.warning("No data to merge.")
        return pd.DataFrame()

    merged = frames[0]
    for frame in frames[1:]:
        merged = merged.merge(frame, on=key, how="outer")

    # Fill missing numeric columns
    for col in ["sst_mean", "chlorophyll_mean", "u_current_mean",
                "v_current_mean", "ssh_mean", "fishing_effort_hours", "vessel_count"]:
        if col not in merged.columns:
            merged[col] = np.nan

    # Assign WPP if not already present
    if "wpp_region" not in merged.columns or merged["wpp_region"].isna().all():
        merged = _assign_wpp(merged)

    # Compute Fishing Ground Index
    merged = _compute_fgi(merged)

    # Add metadata
    merged["data_sources"] = "|".join(sources_used) if sources_used else "unknown"

    # Final column order
    final_cols = [
        "month", "lat_grid", "lon_grid",
        "sst_mean", "chlorophyll_mean",
        "u_current_mean", "v_current_mean", "ssh_mean",
        "fishing_effort_hours", "vessel_count",
        "fishing_ground_index", "wpp_region", "data_sources",
    ]
    for col in final_cols:
        if col not in merged.columns:
            merged[col] = np.nan

    merged = merged[final_cols].sort_values(["month", "lat_grid", "lon_grid"]).reset_index(drop=True)
    logger.info("Merged dataset: %d rows, %d columns.", len(merged), len(merged.columns))
    return merged


def save_processed(
    df: pd.DataFrame,
    start_date: str,
    end_date: str,
    output_dir: str = DEFAULT_OUTPUT_DIR,
) -> Path:
    """Save merged DataFrame to CSV and return the file path."""
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    fname = f"fishing_ground_{start_date.replace('-','')}_{end_date.replace('-','')}.csv"
    path = Path(output_dir) / fname
    df.to_csv(path, index=False)
    logger.info("Processed dataset saved: %s (%d rows)", path, len(df))
    return path
