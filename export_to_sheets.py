"""
Export collected oceanography CSV data to Google Sheets.

Setup:
  1. Create a Google Cloud project and enable the Google Sheets + Drive APIs.
  2. Create a Service Account, download the JSON key, save as credentials.json
     (or set GOOGLE_CREDENTIALS_FILE env var to the path).
  3. Share the target Google Sheet with the service account email.

Usage:
  # Create a new sheet with auto-generated name
  python export_to_sheets.py --csv data/processed/fishing_ground_20230101_20230331.csv

  # Write to an existing sheet by ID
  python export_to_sheets.py --csv data/processed/fishing_ground_20230101_20230331.csv \
      --sheet-id 1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms

  # Custom worksheet tab name
  python export_to_sheets.py --csv data/processed/fishing_ground_20230101_20230331.csv \
      --worksheet "Jan-Mar 2023"
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pandas as pd

try:
    import gspread
    from google.oauth2.service_account import Credentials
except ImportError:
    print("Missing dependencies. Run: pip3 install gspread google-auth")
    sys.exit(1)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

COLUMN_DESCRIPTIONS = {
    "month": "Bulan (YYYY-MM)",
    "lat_grid": "Latitude grid center (°)",
    "lon_grid": "Longitude grid center (°)",
    "sst_mean": "Rata-rata SST (°C)",
    "chlorophyll_mean": "Rata-rata Klorofil-a (mg/m³)",
    "u_current_mean": "Arus U rata-rata (m/s)",
    "v_current_mean": "Arus V rata-rata (m/s)",
    "ssh_mean": "Sea Surface Height rata-rata (m)",
    "fishing_effort_hours": "Total jam penangkapan ikan",
    "vessel_count": "Jumlah kapal",
    "fishing_ground_index": "Indeks Fishing Ground (0–1)",
    "wpp_region": "Wilayah Pengelolaan Perikanan (WPP)",
    "data_sources": "Sumber data",
}


def get_credentials(credentials_file: str) -> Credentials:
    if not Path(credentials_file).exists():
        print(f"Credentials file not found: {credentials_file}")
        print("Please provide a Google Service Account JSON key file.")
        print("Set GOOGLE_CREDENTIALS_FILE env var or use --credentials flag.")
        sys.exit(1)
    return Credentials.from_service_account_file(credentials_file, scopes=SCOPES)


def export_to_sheets(
    csv_path: str,
    sheet_id: str | None,
    worksheet_name: str,
    credentials_file: str,
) -> str:
    df = pd.read_csv(csv_path)
    print(f"Loaded {len(df)} rows from {csv_path}")

    creds = get_credentials(credentials_file)
    client = gspread.authorize(creds)

    if sheet_id:
        spreadsheet = client.open_by_key(sheet_id)
        print(f"Opened existing spreadsheet: {spreadsheet.title}")
    else:
        title = f"Indonesia Oceanography Data - {Path(csv_path).stem}"
        spreadsheet = client.create(title)
        # Make it accessible to anyone with the link (optional)
        spreadsheet.share(None, perm_type="anyone", role="reader")
        print(f"Created new spreadsheet: {title}")

    # Get or create the worksheet tab
    try:
        ws = spreadsheet.worksheet(worksheet_name)
        ws.clear()
        print(f"Cleared existing worksheet: {worksheet_name}")
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=worksheet_name, rows=len(df) + 10, cols=len(df.columns) + 2)
        print(f"Created new worksheet: {worksheet_name}")

    # Write header row with descriptions as second row
    headers = list(df.columns)
    descriptions = [COLUMN_DESCRIPTIONS.get(col, "") for col in headers]

    # Replace NaN with empty string for Sheets compatibility
    df = df.fillna("")

    rows = [headers, descriptions] + df.values.tolist()
    ws.update(rows, value_input_option="USER_ENTERED")

    # Format header row bold
    ws.format("1:1", {"textFormat": {"bold": True}})
    ws.format("2:2", {"textFormat": {"italic": True}, "backgroundColor": {"red": 0.95, "green": 0.95, "blue": 0.95}})

    url = f"https://docs.google.com/spreadsheets/d/{spreadsheet.id}"
    print(f"\nDone! Google Sheet URL: {url}")
    print(f"Worksheet: {worksheet_name}")
    print(f"Rows written: {len(df)}")
    return url


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export oceanography CSV to Google Sheets")
    parser.add_argument(
        "--csv",
        default="data/processed/fishing_ground_20230101_20230331.csv",
        help="Path to the processed CSV file",
    )
    parser.add_argument(
        "--sheet-id",
        default=None,
        help="Existing Google Sheet ID (leave blank to create new)",
    )
    parser.add_argument(
        "--worksheet",
        default="Fishing Ground Data",
        help="Worksheet tab name (default: 'Fishing Ground Data')",
    )
    parser.add_argument(
        "--credentials",
        default=os.environ.get("GOOGLE_CREDENTIALS_FILE", "credentials.json"),
        help="Path to Google Service Account JSON key (default: credentials.json)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    export_to_sheets(
        csv_path=args.csv,
        sheet_id=args.sheet_id,
        worksheet_name=args.worksheet,
        credentials_file=args.credentials,
    )
