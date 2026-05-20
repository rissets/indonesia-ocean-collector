"""
Data mapper — merges all source DataFrames into a single unified CSV.

Strategy: snap all sources to the same 0.5° grid, then merge on month+grid.
For sources with different native resolutions, we aggregate (mean/sum) per grid cell.

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
FGI_TOLERANCE = FGI_SST_TOLERANCE


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
    """Assign WPP region name based on lat_grid/lon_grid."""
    df = df.copy()

    def _find_wpp(lat: float, lon: float) -> str:
        for name, bbox in WPP_REGIONS.items():
            if bbox["min_lat"] <= lat <= bbox["max_lat"] and \
               bbox["min_lon"] <= lon <= bbox["max_lon"]:
                return name
        return "OUTSIDE_WPP"

    df["wpp_region"] = df.apply(lambda r: _find_wpp(r["lat_grid"], r["lon_grid"]), axis=1)
    return df


def _agg_to_grid(df: pd.DataFrame, value_cols: list[str],
                 agg: str, res: float) -> pd.DataFrame:
    """Snap to grid, convert to monthly, aggregate value_cols."""
    df = _snap_to_grid(_to_month(df), res)
    key = ["month", "lat_grid", "lon_grid"]
    return df.groupby(key)[value_cols].agg(agg).reset_index()


def _compute_fgi(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute Fishing Ground Index (0–1).
    FGI = 0.6 × chl_norm  +  0.4 × sst_score
    """
    df = df.copy()

    chl = df["chlorophyll_mean"].fillna(0)
    chl_min, chl_max = chl.min(), chl.max()
    df["chl_norm"] = (chl - chl_min) / (chl_max - chl_min) if chl_max > chl_min else 0.0

    sst = df["sst_mean"].fillna(FGI_OPTIMAL_SST)
    df["sst_score"] = (1 - (sst - FGI_OPTIMAL_SST).abs() / FGI_TOLERANCE).clip(0, 1)

    df["fishing_ground_index"] = (
        FGI_CHLOROPHYLL_WEIGHT * df["chl_norm"]
        + FGI_SST_WEIGHT * df["sst_score"]
    ).round(4)

    return df.drop(columns=["chl_norm", "sst_score"])


def merge_all_sources(
    sst_df: Optional[pd.DataFrame] = None,
    chlorophyll_df: Optional[pd.DataFrame] = None,
    currents_df: Optional[pd.DataFrame] = None,
    effort_df: Optional[pd.DataFrame] = None,
    grid_resolution: float = DEFAULT_GRID_RESOLUTION,
) -> pd.DataFrame:
    """
    Merge all source DataFrames into a unified fishing ground dataset.

    Each source is independently aggregated to the same grid resolution,
    then outer-joined on (month, lat_grid, lon_grid).
    """
    res = grid_resolution
    sources_used: list[str] = []
    key = ["month", "lat_grid", "lon_grid"]

    # ---- SST ----
    if sst_df is not None and not sst_df.empty and "sst_celsius" in sst_df.columns:
        sst_agg = _agg_to_grid(sst_df, ["sst_celsius"], "mean", res)
        sst_agg = sst_agg.rename(columns={"sst_celsius": "sst_mean"})
        sst_agg["sst_mean"] = sst_agg["sst_mean"].round(3)
        src = sst_df["source"].iloc[0] if "source" in sst_df.columns else "unknown"
        if src not in sources_used:
            sources_used.append(src)
    else:
        sst_agg = pd.DataFrame(columns=key + ["sst_mean"])

    # ---- Chlorophyll ----
    if chlorophyll_df is not None and not chlorophyll_df.empty and "chlorophyll_mgm3" in chlorophyll_df.columns:
        chl_agg = _agg_to_grid(chlorophyll_df, ["chlorophyll_mgm3"], "mean", res)
        chl_agg = chl_agg.rename(columns={"chlorophyll_mgm3": "chlorophyll_mean"})
        chl_agg["chlorophyll_mean"] = chl_agg["chlorophyll_mean"].round(4)
        src = chlorophyll_df["source"].iloc[0] if "source" in chlorophyll_df.columns else "unknown"
        if src not in sources_used:
            sources_used.append(src)
    else:
        chl_agg = pd.DataFrame(columns=key + ["chlorophyll_mean"])

    # ---- Currents + SSH ----
    cur_cols = [c for c in ["u_current_ms", "v_current_ms", "ssh_m"]
                if currents_df is not None and c in currents_df.columns]
    if currents_df is not None and not currents_df.empty and cur_cols:
        cur_agg = _agg_to_grid(currents_df, cur_cols, "mean", res)
        rename_map = {"u_current_ms": "u_current_mean",
                      "v_current_ms": "v_current_mean",
                      "ssh_m": "ssh_mean"}
        cur_agg = cur_agg.rename(columns={k: v for k, v in rename_map.items() if k in cur_agg.columns})
        for col in ["u_current_mean", "v_current_mean", "ssh_mean"]:
            if col in cur_agg.columns:
                cur_agg[col] = cur_agg[col].round(4)
        src = currents_df["source"].iloc[0] if "source" in currents_df.columns else "unknown"
        if src not in sources_used:
            sources_used.append(src)
    else:
        cur_agg = pd.DataFrame(columns=key + ["u_current_mean", "v_current_mean", "ssh_mean"])

    # ---- Fishing Effort ----
    if effort_df is not None and not effort_df.empty and "fishing_effort_hours" in effort_df.columns:
        eff = _snap_to_grid(_to_month(effort_df), res)
        eff_agg = (
            eff.groupby(key)
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
        eff_agg = pd.DataFrame(columns=key + ["fishing_effort_hours", "vessel_count", "wpp_region"])

    # ---- Build master grid from GFW effort (most complete spatial coverage) ----
    # Use effort grid as base, then left-join oceanographic data
    frames_with_data = [(f, len(f)) for f in [eff_agg, sst_agg, chl_agg, cur_agg] if not f.empty]
    if not frames_with_data:
        logger.warning("No data to merge.")
        return pd.DataFrame()

    # Sort by coverage — use largest as base
    frames_with_data.sort(key=lambda x: x[1], reverse=True)
    merged = frames_with_data[0][0]

    for frame, _ in frames_with_data[1:]:
        if frame.empty:
            continue
        # Get columns to merge (exclude key cols already in merged)
        new_cols = [c for c in frame.columns if c not in key or c in key]
        merged = merged.merge(frame, on=key, how="left")

    # Fill missing columns
    for col in ["sst_mean", "chlorophyll_mean", "u_current_mean",
                "v_current_mean", "ssh_mean", "fishing_effort_hours",
                "vessel_count", "wpp_region"]:
        if col not in merged.columns:
            merged[col] = np.nan

    # Assign WPP where missing
    missing_wpp = merged["wpp_region"].isna() | (merged["wpp_region"] == "")
    if missing_wpp.any():
        sub = merged[missing_wpp].copy()
        sub = _assign_wpp(sub)
        merged.loc[missing_wpp, "wpp_region"] = sub["wpp_region"].values

    # Compute FGI
    merged = _compute_fgi(merged)

    # Metadata
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

    # Report fill rates
    fill_pct = (merged.notnull().sum() / len(merged) * 100).round(1)
    logger.info("Fill rates: SST=%.1f%% CHL=%.1f%% SSH=%.1f%% Effort=%.1f%%",
                fill_pct.get("sst_mean", 0), fill_pct.get("chlorophyll_mean", 0),
                fill_pct.get("ssh_mean", 0), fill_pct.get("fishing_effort_hours", 0))
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
