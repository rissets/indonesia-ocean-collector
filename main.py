"""
CLI entrypoint for Indonesia Ocean Fishing Ground Collector.

Usage examples:

  # Collect from all sources, Jan–Mar 2023, max 500 records per source
  python main.py --start 2023-01-01 --end 2023-03-31 --max-records 500

  # Only ERDDAP (no credentials needed)
  python main.py --start 2023-01-01 --end 2023-01-31 --sources erddap

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
    DEFAULT_MAX_RECORDS,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_RAW_DIR,
    DEFAULT_START_DATE,
    WPP_REGIONS,
)
from collectors import erddap_collector, cmems_collector, gfw_collector
from processors.mapper import merge_all_sources, save_processed

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("main")

ALL_SOURCES = ["erddap", "cmems", "gfw"]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Collect oceanographic + fishing data for Indonesian waters.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--start",
        default=DEFAULT_START_DATE,
        metavar="YYYY-MM-DD",
        help=f"Start date (default: {DEFAULT_START_DATE})",
    )
    parser.add_argument(
        "--end",
        default=DEFAULT_END_DATE,
        metavar="YYYY-MM-DD",
        help=f"End date (default: {DEFAULT_END_DATE})",
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=DEFAULT_MAX_RECORDS,
        metavar="N",
        help=f"Max records per source (default: {DEFAULT_MAX_RECORDS})",
    )
    parser.add_argument(
        "--sources",
        nargs="+",
        choices=ALL_SOURCES,
        default=ALL_SOURCES,
        metavar="SOURCE",
        help=f"Data sources to collect from: {ALL_SOURCES} (default: all)",
    )
    parser.add_argument(
        "--wpp",
        nargs="+",
        choices=list(WPP_REGIONS.keys()),
        default=None,
        metavar="WPP",
        help="WPP regions to include for GFW (default: all). "
             f"Available: {list(WPP_REGIONS.keys())}",
    )
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT_DIR,
        metavar="DIR",
        help=f"Output directory for processed CSV (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--raw-dir",
        default=DEFAULT_RAW_DIR,
        metavar="DIR",
        help=f"Directory for raw downloads (default: {DEFAULT_RAW_DIR})",
    )
    parser.add_argument(
        "--grid-resolution",
        type=float,
        default=0.5,
        metavar="DEG",
        help="Grid resolution in degrees for merging (default: 0.5)",
    )
    return parser.parse_args(argv)


def run(args: argparse.Namespace) -> Path:
    logger.info("=" * 60)
    logger.info("Indonesia Ocean Fishing Ground Collector")
    logger.info("  Date range  : %s → %s", args.start, args.end)
    logger.info("  Max records : %d per source", args.max_records)
    logger.info("  Sources     : %s", args.sources)
    logger.info("  WPP filter  : %s", args.wpp or "all")
    logger.info("  Output dir  : %s", args.output)
    logger.info("=" * 60)

    sst_df = chl_df = cur_df = effort_df = None

    # ------------------------------------------------------------------ ERDDAP
    if "erddap" in args.sources:
        logger.info("[1/3] Collecting from NOAA ERDDAP ...")
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

    # ------------------------------------------------------------------ CMEMS
    if "cmems" in args.sources:
        logger.info("[2/3] Collecting from Copernicus Marine (CMEMS) ...")
        cmems_all = cmems_collector.collect_all(
            args.start, args.end, args.max_records, raw_dir=args.raw_dir
        )
        if not cmems_all.empty:
            # Prefer CMEMS SST/chl if ERDDAP also ran (CMEMS is higher quality)
            if "sst_celsius" in cmems_all.columns:
                sst_df = cmems_all[["time", "latitude", "longitude", "sst_celsius", "source"]].copy()
            if "chlorophyll_mgm3" in cmems_all.columns:
                chl_df = cmems_all[["time", "latitude", "longitude", "chlorophyll_mgm3", "source"]].copy()
            if all(c in cmems_all.columns for c in ["u_current_ms", "v_current_ms", "ssh_m"]):
                cur_df = cmems_all[["time", "latitude", "longitude",
                                    "u_current_ms", "v_current_ms", "ssh_m", "source"]].copy()

    # ------------------------------------------------------------------ GFW
    if "gfw" in args.sources:
        logger.info("[3/3] Collecting from Global Fishing Watch ...")
        effort_df = gfw_collector.collect_fishing_effort(
            args.start, args.end,
            wpp_names=args.wpp,
            max_records=args.max_records,
            raw_dir=args.raw_dir,
        )

    # ------------------------------------------------------------------ Merge
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


def main() -> None:
    args = parse_args()
    run(args)


if __name__ == "__main__":
    main()
