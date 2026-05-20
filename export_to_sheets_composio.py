"""
Export oceanography CSV data to Google Sheets using Composio CLI.

Usage:
    python export_to_sheets_composio.py --csv data/processed/fishing_ground_20230101_20230331.csv
    python export_to_sheets_composio.py --csv data/processed/fishing_ground_20230101_20230331.csv --sheet-id <existing_id>
    python export_to_sheets_composio.py --csv data/processed/fishing_ground_20230101_20230331.csv --chunk-size 500
"""

import argparse
import csv
import json
import math
import subprocess
import sys
from pathlib import Path


SHEET_TITLE = "Indonesia Oceanography - Fishing Ground Data"
CHUNK_SIZE = 500  # rows per append call to stay within API limits


def run_composio(tool_slug: str, data: dict) -> dict:
    result = subprocess.run(
        ["composio", "execute", tool_slug, "-d", json.dumps(data)],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"[ERROR] {tool_slug} failed:\n{result.stderr}", file=sys.stderr)
        sys.exit(1)
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        return {"raw": result.stdout}


def load_csv(path: str) -> tuple[list[str], list[list]]:
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        headers = next(reader)
        rows = [row for row in reader]
    return headers, rows


def create_spreadsheet(title: str) -> str:
    print(f"Creating spreadsheet: {title}")
    resp = run_composio("GOOGLESHEETS_CREATE_GOOGLE_SHEET1", {"title": title})
    spreadsheet_id = (
        resp.get("data", {}).get("spreadsheetId")
        or resp.get("spreadsheetId")
        or resp.get("data", {}).get("id")
    )
    if not spreadsheet_id:
        print(f"[ERROR] Could not extract spreadsheetId from response: {resp}", file=sys.stderr)
        sys.exit(1)
    spreadsheet_url = (
        resp.get("data", {}).get("spreadsheetUrl")
        or resp.get("spreadsheetUrl")
        or f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"
    )
    print(f"Created: {spreadsheet_url}")
    return spreadsheet_id, spreadsheet_url


def get_sheet_name(spreadsheet_id: str) -> str:
    resp = run_composio("GOOGLESHEETS_GET_SHEET_NAMES", {"spreadsheet_id": spreadsheet_id})
    names = resp.get("data", {}).get("sheet_names") or resp.get("sheet_names") or ["Sheet1"]
    return names[0]


def write_headers(spreadsheet_id: str, sheet_name: str, headers: list[str]):
    print(f"Writing {len(headers)} column headers...")
    run_composio("GOOGLESHEETS_VALUES_UPDATE", {
        "spreadsheetId": spreadsheet_id,
        "range": f"{sheet_name}!A1",
        "valueInputOption": "RAW",
        "values": [headers],
    })


def append_rows(spreadsheet_id: str, sheet_name: str, rows: list[list], chunk_size: int):
    total = len(rows)
    chunks = math.ceil(total / chunk_size)
    print(f"Appending {total} rows in {chunks} chunk(s) of up to {chunk_size}...")
    for i in range(chunks):
        batch = rows[i * chunk_size : (i + 1) * chunk_size]
        # Replace empty strings with None-safe values; Sheets API rejects NaN/Inf
        safe_batch = [
            [("" if (v == "" or v != v) else v) for v in row]
            for row in batch
        ]
        run_composio("GOOGLESHEETS_SPREADSHEETS_VALUES_APPEND", {
            "spreadsheetId": spreadsheet_id,
            "range": f"{sheet_name}!A1",
            "valueInputOption": "RAW",
            "insertDataOption": "INSERT_ROWS",
            "values": safe_batch,
        })
        print(f"  Chunk {i+1}/{chunks} done ({len(batch)} rows)")


def main():
    parser = argparse.ArgumentParser(description="Export oceanography CSV to Google Sheets via Composio")
    parser.add_argument("--csv", required=True, help="Path to CSV file")
    parser.add_argument("--sheet-id", help="Existing spreadsheet ID (skip creation)")
    parser.add_argument("--title", default=SHEET_TITLE, help="Spreadsheet title if creating new")
    parser.add_argument("--chunk-size", type=int, default=CHUNK_SIZE, help="Rows per API call")
    args = parser.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        print(f"[ERROR] CSV not found: {csv_path}", file=sys.stderr)
        sys.exit(1)

    headers, rows = load_csv(str(csv_path))
    print(f"Loaded {len(rows)} rows, {len(headers)} columns from {csv_path.name}")

    if args.sheet_id:
        spreadsheet_id = args.sheet_id
        spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"
        print(f"Using existing spreadsheet: {spreadsheet_url}")
    else:
        spreadsheet_id, spreadsheet_url = create_spreadsheet(args.title)

    sheet_name = get_sheet_name(spreadsheet_id)
    print(f"Target sheet tab: {sheet_name}")

    write_headers(spreadsheet_id, sheet_name, headers)
    append_rows(spreadsheet_id, sheet_name, rows, args.chunk_size)

    print(f"\nDone. Spreadsheet URL: {spreadsheet_url}")
    return spreadsheet_url


if __name__ == "__main__":
    main()
