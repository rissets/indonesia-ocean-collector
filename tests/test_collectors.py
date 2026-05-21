"""
Test suite: verify collector output completeness and DB insertion.

Run:
    cd indonesia-ocean-collector
    python -m pytest tests/test_collectors.py -v

Requirements:
    - .env with DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
    - pip install pytest pandas requests psycopg2-binary python-dotenv
"""

from __future__ import annotations

import math
import os
import sys
from pathlib import Path

import pandas as pd
import pytest

# Ensure repo root is on path
_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))

from dotenv import load_dotenv
load_dotenv(_REPO_ROOT / ".env")

# Short date range for fast tests
TEST_START = "2024-01-01"
TEST_END   = "2024-01-31"
TEST_BATCH = 50  # small batch — just enough to verify columns


# ---------------------------------------------------------------------------
# Required columns per source (must not be all-null after collection)
# ---------------------------------------------------------------------------

ERDDAP_REQUIRED = ["tanggal", "latitude", "longitude", "sst", "suhu_permukaan", "sumber_data"]
CMEMS_REQUIRED  = ["tanggal", "latitude", "longitude", "sst", "suhu_permukaan", "sumber_data"]
OPENMETEO_REQUIRED = [
    "tanggal", "latitude", "longitude",
    "tinggi_gelombang", "kecepatan_angin", "arah_angin", "radiasi_matahari",
    "sumber_data",
]

# Columns that MUST have zero nulls (identity columns)
IDENTITY_COLS = ["tanggal", "latitude", "longitude", "sumber_data"]


def _assert_no_null_identity(df: pd.DataFrame, source: str) -> None:
    for col in IDENTITY_COLS:
        if col in df.columns:
            nulls = df[col].isna().sum()
            assert nulls == 0, f"[{source}] Column '{col}' has {nulls} null values — identity columns must be complete"


def _assert_required_cols_present(df: pd.DataFrame, required: list[str], source: str) -> None:
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"[{source}] Missing columns: {missing}"


def _assert_not_empty(df: pd.DataFrame, source: str) -> None:
    assert not df.empty, f"[{source}] Collector returned empty DataFrame"


# ---------------------------------------------------------------------------
# ERDDAP
# ---------------------------------------------------------------------------

def test_erddap_sst_columns():
    from collectors import erddap_collector
    df = erddap_collector.collect_sst(TEST_START, TEST_END, max_records=TEST_BATCH, stride=4)
    _assert_not_empty(df, "ERDDAP SST")
    assert "sst_celsius" in df.columns or "sst" in df.columns, "ERDDAP SST: missing sst column"
    assert "latitude" in df.columns
    assert "longitude" in df.columns
    assert "time" in df.columns or "tanggal" in df.columns


def test_erddap_chlorophyll_columns():
    from collectors import erddap_collector
    df = erddap_collector.collect_chlorophyll(TEST_START, TEST_END, max_records=TEST_BATCH, stride=4)
    _assert_not_empty(df, "ERDDAP chlorophyll")
    assert "chlorophyll_mgm3" in df.columns, "ERDDAP chlorophyll: missing chlorophyll_mgm3"


def test_erddap_ssh_columns():
    from collectors import erddap_collector
    df = erddap_collector.collect_ssh_currents(TEST_START, TEST_END, max_records=TEST_BATCH, stride=2)
    _assert_not_empty(df, "ERDDAP SSH")
    for col in ["ssh_m", "u_current_ms", "v_current_ms"]:
        assert col in df.columns, f"ERDDAP SSH: missing {col}"


def test_erddap_merged_no_null_identity():
    """After merging SST+CHL+SSH, identity columns must have zero nulls."""
    from collectors import erddap_collector
    sst = erddap_collector.collect_sst(TEST_START, TEST_END, max_records=TEST_BATCH, stride=4)
    if sst.empty:
        pytest.skip("ERDDAP SST returned no data")

    df = sst.rename(columns={"time": "tanggal", "sst_celsius": "sst"})
    df["suhu_permukaan"] = df["sst"]
    df["sumber_data"] = "NOAA_ERDDAP"

    _assert_required_cols_present(df, ERDDAP_REQUIRED, "ERDDAP merged")
    _assert_no_null_identity(df, "ERDDAP merged")


# ---------------------------------------------------------------------------
# CMEMS
# ---------------------------------------------------------------------------

def test_cmems_collect_all_columns():
    from collectors import cmems_collector
    df = cmems_collector.collect_all(TEST_START, TEST_END, max_records=TEST_BATCH)
    _assert_not_empty(df, "CMEMS")

    expected = ["time", "latitude", "longitude", "sst_celsius", "source"]
    for col in expected:
        assert col in df.columns, f"CMEMS: missing column '{col}'"


def test_cmems_merged_no_null_identity():
    from collectors import cmems_collector
    df = cmems_collector.collect_all(TEST_START, TEST_END, max_records=TEST_BATCH)
    if df.empty:
        pytest.skip("CMEMS returned no data")

    df = df.rename(columns={
        "time": "tanggal",
        "sst_celsius": "sst",
        "chlorophyll_mgm3": "klorofil",
        "ssh_m": "ssh",
        "u_current_ms": "arus_laut_u",
        "v_current_ms": "arus_laut_v",
    })
    df["suhu_permukaan"] = df.get("sst", None)
    df["sumber_data"] = "CMEMS"

    _assert_required_cols_present(df, CMEMS_REQUIRED, "CMEMS merged")
    _assert_no_null_identity(df, "CMEMS merged")


# ---------------------------------------------------------------------------
# Open-Meteo
# ---------------------------------------------------------------------------

def test_openmeteo_columns():
    from collectors.openmeteo_collector import collect
    df = collect(TEST_START, TEST_END, max_records=TEST_BATCH)
    _assert_not_empty(df, "Open-Meteo")
    _assert_required_cols_present(df, OPENMETEO_REQUIRED, "Open-Meteo")
    _assert_no_null_identity(df, "Open-Meteo")


def test_openmeteo_no_null_wave_or_wind():
    """At least one of wave/wind columns must be non-null per row."""
    from collectors.openmeteo_collector import collect
    df = collect(TEST_START, TEST_END, max_records=TEST_BATCH)
    if df.empty:
        pytest.skip("Open-Meteo returned no data")

    wave_wind_cols = ["tinggi_gelombang", "kecepatan_angin", "arah_angin", "radiasi_matahari"]
    present = [c for c in wave_wind_cols if c in df.columns]
    if present:
        all_null = df[present].isna().all(axis=1)
        rows_all_null = all_null.sum()
        assert rows_all_null == 0, (
            f"Open-Meteo: {rows_all_null} rows have all wave/wind columns null"
        )


# ---------------------------------------------------------------------------
# DB insertion test
# ---------------------------------------------------------------------------

def _db_available() -> bool:
    return all(os.getenv(k) for k in ["DB_HOST", "DB_NAME", "DB_USER", "DB_PASSWORD"])


@pytest.mark.skipif(not _db_available(), reason="DB env vars not set")
def test_db_upsert_and_verify():
    """Upsert a small batch and verify rows exist in master_oceanography."""
    from collectors.openmeteo_collector import collect
    from db.writer import upsert_dataframe
    import psycopg2

    df = collect(TEST_START, TEST_END, max_records=20)
    if df.empty:
        pytest.skip("Open-Meteo returned no data for DB test")

    n = upsert_dataframe(df, "Open-Meteo")
    assert n > 0, "upsert_dataframe returned 0 rows"

    conn = psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=int(os.environ.get("DB_PORT", 5432)),
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM master_oceanography WHERE sumber_data = %s",
                ("Open-Meteo",),
            )
            count = cur.fetchone()[0]
        assert count > 0, f"DB has 0 rows for sumber_data='Open-Meteo' after upsert"
        print(f"\nDB verification: {count} rows found for sumber_data='Open-Meteo'")
    finally:
        conn.close()


@pytest.mark.skipif(not _db_available(), reason="DB env vars not set")
def test_db_no_null_required_columns():
    """Verify that key columns in master_oceanography are not all null for a source."""
    import psycopg2

    conn = psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=int(os.environ.get("DB_PORT", 5432)),
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
    )
    try:
        with conn.cursor() as cur:
            # Check that tanggal, latitude, longitude are never null
            cur.execute("""
                SELECT COUNT(*) FROM master_oceanography
                WHERE tanggal IS NULL OR latitude IS NULL OR longitude IS NULL OR sumber_data IS NULL
            """)
            bad_rows = cur.fetchone()[0]
            assert bad_rows == 0, (
                f"master_oceanography has {bad_rows} rows with null identity columns"
            )

            # Check overall row count
            cur.execute("SELECT sumber_data, COUNT(*) FROM master_oceanography GROUP BY sumber_data")
            rows = cur.fetchall()
            print("\nDB row counts by source:")
            for source, cnt in rows:
                print(f"  {source}: {cnt}")
    finally:
        conn.close()
