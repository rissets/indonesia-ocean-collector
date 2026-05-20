"""
Google Sheets export for Indonesia Ocean Fishing Ground data.

Uses Composio's Google Sheets integration when COMPOSIO_API_KEY is set and a
Google Sheets connection exists.  Falls back to gspread (service-account JSON)
when GOOGLE_SERVICE_ACCOUNT_JSON is set.  When neither credential is available
the script writes sheet-ready CSVs to --sheets-dir and exits cleanly.

Tabs created
------------
  Summary          — one row per WPP with aggregated stats
  WPP_Coverage     — which WPP regions appear in the data and row counts
  Parameter_Coverage — which parameters have non-null values and fill rates
  Sources          — data provenance per source
  Sample_Data      — first 500 rows of the combined dataset

Usage
-----
  # Dry-run (no credentials needed) — writes sheet-ready CSVs
  python sheets/export_to_sheets.py --csv data/processed/fishing_ground_*.csv

  # Upload via Composio (COMPOSIO_API_KEY must be set)
  COMPOSIO_API_KEY=<key> python sheets/export_to_sheets.py \\
      --csv data/processed/fishing_ground_20230101_20230331.csv

  # Upload via gspread service account
  GOOGLE_SERVICE_ACCOUNT_JSON=credentials.json \\
      python sheets/export_to_sheets.py \\
      --csv data/processed/fishing_ground_20230101_20230331.csv

  # Write to an existing sheet instead of creating a new one
  python sheets/export_to_sheets.py --csv ... --sheet-id <spreadsheet_id>

Required env vars (at least one for upload)
-------------------------------------------
  COMPOSIO_API_KEY              — Composio API key
  GOOGLE_SERVICE_ACCOUNT_JSON   — path to Google service-account JSON file

Optional env vars
-----------------
  GOOGLE_SHEET_TITLE            — title for new spreadsheet (default: auto)
  GOOGLE_SHARE_EMAIL            — email to share the sheet with after creation
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Optional

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("sheets_export")

# ---------------------------------------------------------------------------
# Tab definitions
# ---------------------------------------------------------------------------

TAB_SAMPLE_ROWS = 500

WPP_DISPLAY_NAMES = {
    "WPP_571_Selat_Malaka":           "571 – Selat Malaka",
    "WPP_572_Samudera_Hindia_Barat":  "572 – Samudera Hindia Barat",
    "WPP_573_Samudera_Hindia_Selatan":"573 – Samudera Hindia Selatan",
    "WPP_711_Laut_Natuna":            "711 – Laut Natuna",
    "WPP_712_Laut_Jawa":              "712 – Laut Jawa",
    "WPP_713_Selat_Makassar":         "713 – Selat Makassar",
    "WPP_714_Laut_Banda":             "714 – Laut Banda",
    "WPP_715_Laut_Maluku":            "715 – Laut Maluku",
    "WPP_716_Laut_Sulawesi":          "716 – Laut Sulawesi",
    "WPP_717_Teluk_Cendrawasih":      "717 – Teluk Cendrawasih",
    "WPP_718_Laut_Arafuru":           "718 – Laut Arafuru",
}

NUMERIC_PARAMS = [
    "sst_mean",
    "chlorophyll_mean",
    "u_current_mean",
    "v_current_mean",
    "ssh_mean",
    "fishing_effort_hours",
    "vessel_count",
    "fishing_ground_index",
]

PARAM_LABELS = {
    "sst_mean":              "Sea Surface Temperature (°C)",
    "chlorophyll_mean":      "Chlorophyll-a (mg/m³)",
    "u_current_mean":        "U Current (m/s)",
    "v_current_mean":        "V Current (m/s)",
    "ssh_mean":              "Sea Surface Height (m)",
    "fishing_effort_hours":  "Fishing Effort (hours)",
    "vessel_count":          "Vessel Count",
    "fishing_ground_index":  "Fishing Ground Index (0–1)",
}


# ---------------------------------------------------------------------------
# Tab builders
# ---------------------------------------------------------------------------

def build_summary_tab(df: pd.DataFrame) -> pd.DataFrame:
    """One row per WPP with aggregated stats."""
    rows = []
    for wpp, grp in df.groupby("wpp_region"):
        row: dict = {
            "WPP Region": WPP_DISPLAY_NAMES.get(wpp, wpp),
            "Row Count": len(grp),
            "Months Covered": grp["month"].nunique() if "month" in grp.columns else "",
            "Date Range": f"{grp['month'].min()} – {grp['month'].max()}" if "month" in grp.columns else "",
        }
        for param in NUMERIC_PARAMS:
            if param in grp.columns:
                col = grp[param].dropna()
                row[f"{PARAM_LABELS[param]} — Mean"] = round(col.mean(), 4) if len(col) else ""
                row[f"{PARAM_LABELS[param]} — Min"]  = round(col.min(),  4) if len(col) else ""
                row[f"{PARAM_LABELS[param]} — Max"]  = round(col.max(),  4) if len(col) else ""
        rows.append(row)
    return pd.DataFrame(rows)


def build_wpp_coverage_tab(df: pd.DataFrame) -> pd.DataFrame:
    """WPP coverage — row counts and month ranges."""
    rows = []
    all_wpp = set(WPP_DISPLAY_NAMES.keys())
    present_wpp = set(df["wpp_region"].unique()) if "wpp_region" in df.columns else set()

    for wpp_key, label in WPP_DISPLAY_NAMES.items():
        grp = df[df["wpp_region"] == wpp_key] if "wpp_region" in df.columns else pd.DataFrame()
        rows.append({
            "WPP Code": wpp_key.split("_")[1] if "_" in wpp_key else wpp_key,
            "WPP Name": label,
            "Present in Data": "Yes" if wpp_key in present_wpp else "No",
            "Row Count": len(grp),
            "Months": grp["month"].nunique() if not grp.empty and "month" in grp.columns else 0,
            "Date Range": (
                f"{grp['month'].min()} – {grp['month'].max()}"
                if not grp.empty and "month" in grp.columns else "—"
            ),
        })
    return pd.DataFrame(rows)


def build_parameter_coverage_tab(df: pd.DataFrame) -> pd.DataFrame:
    """Parameter fill rates across the whole dataset."""
    rows = []
    total = len(df)
    for param, label in PARAM_LABELS.items():
        if param in df.columns:
            non_null = df[param].notna().sum()
            fill_pct = round(100 * non_null / total, 1) if total else 0
        else:
            non_null = 0
            fill_pct = 0
        rows.append({
            "Parameter": label,
            "Column Name": param,
            "Non-Null Rows": non_null,
            "Total Rows": total,
            "Fill Rate (%)": fill_pct,
            "Status": "OK" if fill_pct >= 50 else ("Partial" if fill_pct > 0 else "Missing"),
        })
    return pd.DataFrame(rows)


def build_sources_tab(df: pd.DataFrame) -> pd.DataFrame:
    """Data provenance — which sources contributed."""
    if "data_sources" not in df.columns:
        return pd.DataFrame({"Note": ["data_sources column not found"]})

    source_counts: dict[str, int] = {}
    for val in df["data_sources"].dropna():
        for src in str(val).split("|"):
            src = src.strip()
            if src:
                source_counts[src] = source_counts.get(src, 0) + 1

    rows = [
        {"Source": src, "Rows Contributed": count}
        for src, count in sorted(source_counts.items(), key=lambda x: -x[1])
    ]
    return pd.DataFrame(rows) if rows else pd.DataFrame({"Note": ["No source data"]})


def build_sample_tab(df: pd.DataFrame, n: int = TAB_SAMPLE_ROWS) -> pd.DataFrame:
    return df.head(n).copy()


# ---------------------------------------------------------------------------
# Sheet-ready CSV export (dry-run / fallback)
# ---------------------------------------------------------------------------

def export_sheet_ready_csvs(tabs: dict[str, pd.DataFrame], out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    for tab_name, tab_df in tabs.items():
        path = out_dir / f"{tab_name}.csv"
        tab_df.to_csv(path, index=False)
        logger.info("  Wrote %s (%d rows)", path, len(tab_df))


# ---------------------------------------------------------------------------
# Composio upload
# ---------------------------------------------------------------------------

def _composio_upload(
    tabs: dict[str, pd.DataFrame],
    sheet_id: Optional[str],
    title: str,
    share_email: Optional[str],
) -> str:
    """Upload tabs via Composio Google Sheets integration."""
    try:
        from composio_core import ComposioToolSet  # type: ignore
    except ImportError:
        try:
            from composio import ComposioToolSet  # type: ignore
        except ImportError:
            raise ImportError(
                "composio-core is not installed. Run: pip install composio-core"
            )

    api_key = os.environ["COMPOSIO_API_KEY"]
    toolset = ComposioToolSet(api_key=api_key)

    # Create spreadsheet if no sheet_id provided
    if not sheet_id:
        logger.info("Creating new Google Spreadsheet via Composio: %s", title)
        result = toolset.execute_action(
            action="GOOGLESHEETS_CREATE_SPREADSHEET",
            params={"title": title},
        )
        sheet_id = (
            result.get("data", {}).get("spreadsheetId")
            or result.get("spreadsheetId")
        )
        if not sheet_id:
            raise RuntimeError(f"Could not get spreadsheetId from Composio response: {result}")
        logger.info("Created spreadsheet: %s", sheet_id)

    # Write each tab
    for tab_name, tab_df in tabs.items():
        if tab_df.empty:
            continue
        # Convert to list-of-lists (header + rows)
        values = [tab_df.columns.tolist()] + tab_df.fillna("").astype(str).values.tolist()

        logger.info("Writing tab '%s' (%d rows) via Composio ...", tab_name, len(tab_df))
        toolset.execute_action(
            action="GOOGLESHEETS_BATCH_UPDATE_VALUES",
            params={
                "spreadsheetId": sheet_id,
                "valueInputOption": "USER_ENTERED",
                "data": [{"range": f"{tab_name}!A1", "values": values}],
            },
        )

    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}"
    if share_email:
        try:
            toolset.execute_action(
                action="GOOGLEDRIVE_SHARE_FILE",
                params={
                    "fileId": sheet_id,
                    "role": "writer",
                    "type": "user",
                    "emailAddress": share_email,
                },
            )
            logger.info("Shared with %s", share_email)
        except Exception as exc:
            logger.warning("Could not share sheet: %s", exc)

    return url


# ---------------------------------------------------------------------------
# gspread upload (fallback)
# ---------------------------------------------------------------------------

def _gspread_upload(
    tabs: dict[str, pd.DataFrame],
    sheet_id: Optional[str],
    title: str,
    share_email: Optional[str],
    creds_path: str,
) -> str:
    try:
        import gspread  # type: ignore
        from google.oauth2.service_account import Credentials  # type: ignore
    except ImportError:
        raise ImportError(
            "gspread and google-auth are not installed.\n"
            "Run: pip install gspread google-auth"
        )

    scopes = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
    gc = gspread.authorize(creds)

    if sheet_id:
        sh = gc.open_by_key(sheet_id)
        logger.info("Opened existing spreadsheet: %s", sheet_id)
    else:
        sh = gc.create(title)
        logger.info("Created spreadsheet: %s", sh.id)
        if share_email:
            sh.share(share_email, perm_type="user", role="writer")
            logger.info("Shared with %s", share_email)

    existing_titles = {ws.title for ws in sh.worksheets()}

    for tab_name, tab_df in tabs.items():
        if tab_df.empty:
            continue
        if tab_name in existing_titles:
            ws = sh.worksheet(tab_name)
            ws.clear()
        else:
            ws = sh.add_worksheet(title=tab_name, rows=len(tab_df) + 10, cols=len(tab_df.columns) + 2)

        values = [tab_df.columns.tolist()] + tab_df.fillna("").astype(str).values.tolist()
        ws.update(values)
        logger.info("  Wrote tab '%s' (%d rows)", tab_name, len(tab_df))

    # Remove default empty Sheet1 if we created a new spreadsheet
    if not sheet_id:
        try:
            default_ws = sh.worksheet("Sheet1")
            sh.del_worksheet(default_ws)
        except Exception:
            pass

    return f"https://docs.google.com/spreadsheets/d/{sh.id}"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export oceanography CSVs to Google Sheets.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--csv",
        nargs="+",
        required=True,
        metavar="FILE",
        help="One or more processed CSV files to export.",
    )
    parser.add_argument(
        "--sheet-id",
        default=os.environ.get("GOOGLE_SHEET_ID"),
        metavar="ID",
        help="Existing spreadsheet ID to update (default: create new).",
    )
    parser.add_argument(
        "--title",
        default=os.environ.get("GOOGLE_SHEET_TITLE", "Indonesia Ocean Fishing Ground Data"),
        metavar="TITLE",
        help="Title for new spreadsheet.",
    )
    parser.add_argument(
        "--share-email",
        default=os.environ.get("GOOGLE_SHARE_EMAIL"),
        metavar="EMAIL",
        help="Email to share the sheet with after creation.",
    )
    parser.add_argument(
        "--sheets-dir",
        default="data/sheets",
        metavar="DIR",
        help="Directory for sheet-ready CSV output (dry-run / fallback).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Always write CSVs only, never upload.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    # ---- Load and concatenate input CSVs ----
    dfs = []
    for pattern in args.csv:
        from glob import glob
        matched = sorted(glob(pattern))
        if not matched:
            logger.warning("No files matched: %s", pattern)
            continue
        for path in matched:
            logger.info("Loading %s ...", path)
            dfs.append(pd.read_csv(path))

    if not dfs:
        logger.error("No CSV files loaded. Exiting.")
        sys.exit(1)

    df = pd.concat(dfs, ignore_index=True)
    logger.info("Loaded %d rows total from %d file(s).", len(df), len(dfs))

    # ---- Build tabs ----
    tabs: dict[str, pd.DataFrame] = {
        "Summary":            build_summary_tab(df),
        "WPP_Coverage":       build_wpp_coverage_tab(df),
        "Parameter_Coverage": build_parameter_coverage_tab(df),
        "Sources":            build_sources_tab(df),
        "Sample_Data":        build_sample_tab(df),
    }

    for name, tab in tabs.items():
        logger.info("Tab %-22s — %d rows, %d cols", name, len(tab), len(tab.columns))

    # ---- Detect credentials ----
    composio_key   = os.environ.get("COMPOSIO_API_KEY")
    gspread_creds  = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")

    if args.dry_run or (not composio_key and not gspread_creds):
        if not args.dry_run:
            logger.warning(
                "No upload credentials found.\n"
                "  Set COMPOSIO_API_KEY  — to upload via Composio\n"
                "  Set GOOGLE_SERVICE_ACCOUNT_JSON  — to upload via gspread service account\n"
                "Falling back to sheet-ready CSV export."
            )
        sheets_dir = Path(args.sheets_dir)
        export_sheet_ready_csvs(tabs, sheets_dir)
        logger.info(
            "\nSheet-ready CSVs written to: %s\n"
            "To import manually:\n"
            "  1. Open https://sheets.google.com and create a new spreadsheet.\n"
            "  2. For each CSV: File → Import → Upload → select file → Insert new sheet.\n"
            "  3. Rename each sheet tab to match the filename (Summary, WPP_Coverage, etc.).",
            sheets_dir,
        )
        return

    # ---- Upload ----
    url: Optional[str] = None

    if composio_key:
        logger.info("Uploading via Composio ...")
        try:
            url = _composio_upload(tabs, args.sheet_id, args.title, args.share_email)
        except Exception as exc:
            logger.error("Composio upload failed: %s", exc)
            logger.info("Falling back to sheet-ready CSV export ...")
            export_sheet_ready_csvs(tabs, Path(args.sheets_dir))
            sys.exit(1)

    elif gspread_creds:
        logger.info("Uploading via gspread ...")
        try:
            url = _gspread_upload(tabs, args.sheet_id, args.title, args.share_email, gspread_creds)
        except Exception as exc:
            logger.error("gspread upload failed: %s", exc)
            export_sheet_ready_csvs(tabs, Path(args.sheets_dir))
            sys.exit(1)

    if url:
        logger.info("\nGoogle Sheet ready: %s", url)
        print(url)


if __name__ == "__main__":
    main()
