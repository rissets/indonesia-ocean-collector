"""
CLI entrypoint for Indonesia Ocean Fishing Ground Collector.

Usage examples:

  # Collect from all sources, Jan–Mar 2023, max 500 records per source
  python main.py --start 2023-01-01 --end 2023-03-31 --max-records 500

  # Only ERDDAP (no credentials needed), write to DB
  python main.py --start 2023-01-01 --end 2023-01-31 --sources erddap --write-db

  # Copernicus + Open-Meteo only, write to DB
  python main.py --start 2023-01-01 --end 2023-03-31 --sources cmems openmeteo --write-db

  # Specific WPP regions only
  python main.py --start 2023-01-01 --end 2023-03-31 \\
    --wpp WPP_712_Laut_Jawa WPP_713_Selat_Makassar

  # Custom output directory
  python main.py --start 2023-01-01 --end 2023-06-30 --output my_data/
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

from config import (
    DEFAULT_END_DATE,
    DEFAULT_GRID_RESOLUTION,
    DEFAULT_MAX_RECORDS,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_RAW_DIR,
    DEFAULT_START_DATE,
    WPP_REGIONS,
)
from collectors import erddap_collector, cmems_collector, gfw_collector
from collectors import openmeteo_collector
from processors.mapper import merge_all_sources, save_processed

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("main")

ALL_SOURCES = ["erddap", "cmems", "openmeteo", "gfw"]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Collect oceanographic + fishing data for Indonesian waters.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--start", default=DEFAULT_START_DATE, metavar="YYYY-MM-DD",
                        help=f"Start date (default: {DEFAULT_START_DATE})")
    parser.add_argument("--end", default=DEFAULT_END_DATE, metavar="YYYY-MM-DD",
                        help=f"End date (default: {DEFAULT_END_DATE})")
    parser.add_argument("--max-records", type=int, default=DEFAULT_MAX_RECORDS, metavar="N",
                        help=f"Max records per source (default: {DEFAULT_MAX_RECORDS})")
    parser.add_argument("--sources", nargs="+", choices=ALL_SOURCES, default=ALL_SOURCES,
                        metavar="SOURCE", help=f"Data sources: {ALL_SOURCES} (default: all)")
    parser.add_argument("--wpp", nargs="+", choices=list(WPP_REGIONS.keys()), default=None,
                        metavar="WPP", help="WPP regions for GFW (default: all)")
    parser.add_argument("--output", default=DEFAULT_OUTPUT_DIR, metavar="DIR",
                        help=f"Output directory for processed CSV (default: {DEFAULT_OUTPUT_DIR})")
    parser.add_argument("--raw-dir", default=DEFAULT_RAW_DIR, metavar="DIR",
                        help=f"Directory for raw downloads (default: {DEFAULT_RAW_DIR})")
    parser.add_argument("--grid-resolution", type=float, default=DEFAULT_GRID_RESOLUTION,
                        metavar="DEG", help="Grid resolution in degrees (default: 0.5)")
    parser.add_argument("--write-db", action="store_true",
                        help="Write collected data to master_oceanography in PostgreSQL")
    return parser.parse_args(argv)


def _to_oceanography_df(df, source_name: str, field_map: dict):
    """Rename source columns to master_oceanography column names."""
    import pandas as pd
    if df is None or df.empty:
        return None
    out = df.rename(columns=field_map).copy()
    # Normalise time column to 'tanggal' (date only)
    for col in ("tanggal", "time"):
        if col in out.columns:
            out["tanggal"] = pd.to_datetime(out[col], utc=True, errors="coerce").dt.date
            if col != "tanggal":
                out = out.drop(columns=[col], errors="ignore")
            break
    out["sumber_data"] = source_name
    return out


def _write_to_db(df, source_name: str) -> int:
    from db.writer import upsert_dataframe
    return upsert_dataframe(df, source_name)


def run(args: argparse.Namespace) -> Path:
    logger.info("=" * 60)
    logger.info("Indonesia Ocean Fishing Ground Collector")
    logger.info("  Date range  : %s → %s", args.start, args.end)
    logger.info("  Max records : %d per source", args.max_records)
    logger.info("  Sources     : %s", args.sources)
    logger.info("  WPP filter  : %s", args.wpp or "all")
    logger.info("  Output dir  : %s", args.output)
    logger.info("  Write DB    : %s", args.write_db)
    logger.info("=" * 60)

    sst_df = chl_df = cur_df = effort_df = None

    # ------------------------------------------------------------------ ERDDAP
    if "erddap" in args.sources:
        logger.info("[1/4] Collecting from NOAA ERDDAP ...")
        sst_erddap = erddap_collector.collect_sst(
            args.start, args.end, args.max_records, raw_dir=args.raw_dir, stride=2
        )
        chl_erddap = erddap_collector.collect_chlorophyll(
            args.start, args.end, args.max_records, raw_dir=args.raw_dir, stride=2
        )
        ssh_erddap = erddap_collector.collect_ssh_currents(
            args.start, args.end, args.max_records, raw_dir=args.raw_dir, stride=1
        )
        sst_df = sst_erddap if not sst_erddap.empty else sst_df
        chl_df = chl_erddap if not chl_erddap.empty else chl_df
        cur_df = ssh_erddap if not ssh_erddap.empty else cur_df

        if args.write_db:
            _write_erddap_to_db(sst_erddap, chl_erddap, ssh_erddap, args.grid_resolution)

    # ------------------------------------------------------------------ CMEMS
    if "cmems" in args.sources:
        logger.info("[2/4] Collecting from Copernicus Marine (CMEMS) ...")
        cmems_all = cmems_collector.collect_all(
            args.start, args.end, args.max_records, raw_dir=args.raw_dir
        )
        if not cmems_all.empty:
            if "sst_celsius" in cmems_all.columns:
                sst_df = cmems_all[["time", "latitude", "longitude", "sst_celsius", "source"]].copy()
            if "chlorophyll_mgm3" in cmems_all.columns:
                chl_df = cmems_all[["time", "latitude", "longitude", "chlorophyll_mgm3", "source"]].copy()
            if all(c in cmems_all.columns for c in ["u_current_ms", "v_current_ms", "ssh_m"]):
                cur_df = cmems_all[["time", "latitude", "longitude",
                                    "u_current_ms", "v_current_ms", "ssh_m", "source"]].copy()

            if args.write_db:
                _write_cmems_to_db(cmems_all)

    # ------------------------------------------------------------------ Open-Meteo
    if "openmeteo" in args.sources:
        logger.info("[3/4] Collecting from Open-Meteo ...")
        om_df = openmeteo_collector.collect(
            args.start, args.end,
            max_records=args.max_records,
            grid_resolution=args.grid_resolution,
            raw_dir=args.raw_dir,
        )
        if not om_df.empty and args.write_db:
            _write_to_db(om_df, "Open-Meteo")

    # ------------------------------------------------------------------ GFW
    if "gfw" in args.sources:
        logger.info("[4/4] Collecting from Global Fishing Watch ...")
        effort_df = gfw_collector.collect_fishing_effort(
            args.start, args.end,
            wpp_names=args.wpp,
            max_records=args.max_records,
            raw_dir=args.raw_dir,
        )

    # ------------------------------------------------------------------ Merge → CSV
    logger.info("Merging all sources ...")
    merged = merge_all_sources(
        sst_df=sst_df,
        chlorophyll_df=chl_df,
        currents_df=cur_df,
        effort_df=effort_df,
        grid_resolution=args.grid_resolution,
    )

    if merged.empty:
        logger.error("No data collected. Check credentials and network.")
        sys.exit(1)

    output_path = save_processed(merged, args.start, args.end, args.output)

    logger.info("=" * 60)
    logger.info("Done! Output: %s", output_path)
    logger.info("  Rows    : %d", len(merged))
    logger.info("  Columns : %s", list(merged.columns))
    logger.info("  Sources : %s", merged["data_sources"].iloc[0])
    logger.info("=" * 60)

    return output_path


def _write_erddap_to_db(sst_df, chl_df, cur_df, grid_resolution: float) -> None:
    """Merge ERDDAP frames and write to master_oceanography."""
    import pandas as pd
    frames = []

    if sst_df is not None and not sst_df.empty:
        f = sst_df.rename(columns={"sst_celsius": "sst", "time": "tanggal"}).copy()
        f["suhu_permukaan"] = f["sst"]
        frames.append(f)

    if chl_df is not None and not chl_df.empty:
        f = chl_df.rename(columns={"chlorophyll_mgm3": "klorofil", "time": "tanggal"}).copy()
        frames.append(f)

    if cur_df is not None and not cur_df.empty:
        f = cur_df.rename(columns={
            "u_current_ms": "arus_laut_u",
            "v_current_ms": "arus_laut_v",
            "ssh_m": "ssh",
            "time": "tanggal",
        }).copy()
        frames.append(f)

    if not frames:
        return

    key = ["tanggal", "latitude", "longitude"]
    merged = frames[0]
    for f in frames[1:]:
        cols = [c for c in f.columns if c not in merged.columns or c in key]
        merged = merged.merge(f[cols], on=key, how="outer")

    merged["tanggal"] = pd.to_datetime(merged["tanggal"], utc=True, errors="coerce").dt.date
    merged["sumber_data"] = "NOAA_ERDDAP"
    _write_to_db(merged, "NOAA_ERDDAP")


def _write_cmems_to_db(df) -> None:
    """Map CMEMS DataFrame columns and write to master_oceanography."""
    import pandas as pd
    out = df.rename(columns={
        "sst_celsius":      "sst",
        "chlorophyll_mgm3": "klorofil",
        "u_current_ms":     "arus_laut_u",
        "v_current_ms":     "arus_laut_v",
        "ssh_m":            "ssh",
        "salinity_psu":     "salinitas",
        "time":             "tanggal",
    }).copy()
    if "sst" in out.columns:
        out["suhu_permukaan"] = out["sst"]
    out["tanggal"] = pd.to_datetime(out["tanggal"], utc=True, errors="coerce").dt.date
    src = df["source"].iloc[0] if "source" in df.columns else "CMEMS"
    out["sumber_data"] = src
    _write_to_db(out, src)


def main() -> None:
    args = parse_args()
    run(args)


if __name__ == "__main__":
    main()
