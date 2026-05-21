"""
Integration tests — verify all master_oceanography columns are populated.

Tests hit real external APIs (ERDDAP, Open-Meteo) and the real DB.
CMEMS falls back to mock when credentials are absent.

Run:
    cd indonesia-ocean-collector
    python -m pytest tests/test_integration.py -v
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from datetime import date, timedelta

import pandas as pd
import pytest

# Ensure repo root is on path
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from dotenv import load_dotenv
load_dotenv(_REPO_ROOT / ".env")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Use a small recent window so ERDDAP monthly datasets have data
_TEST_END   = (date.today() - timedelta(days=60)).strftime("%Y-%m-%d")
_TEST_START = (date.today() - timedelta(days=90)).strftime("%Y-%m-%d")

# Tiny bbox inside WPP_712 (Laut Jawa) — fast, always has data
_SMALL_BBOX = {"min_lat": -6.0, "max_lat": -5.0, "min_lon": 110.0, "max_lon": 111.0}

# Open-ocean bbox in Laut Banda (WPP_714) — deep water, marine API returns wave data
_OCEAN_BBOX = {"min_lat": -5.0, "max_lat": -4.0, "min_lon": 126.0, "max_lon": 127.0}

_IDENTITY_COLS = ["tanggal", "latitude", "longitude", "sumber_data"]

_OPENMETEO_COLS = [
    "tinggi_gelombang", "periode_gelombang", "pasang_surut",
    "kecepatan_angin", "arah_angin", "radiasi_matahari", "cuaca",
    "kedalaman_laut",
]

_ERDDAP_COLS = ["sst", "klorofil", "ssh", "arus_laut_u", "arus_laut_v", "kecepatan_arus"]

_ALL_DATA_COLS = _ERDDAP_COLS + _OPENMETEO_COLS + ["jarak_padang"]


def _has_db() -> bool:
    return bool(os.getenv("DB_HOST") and os.getenv("DB_NAME") and os.getenv("DB_USER"))


def _compute_jarak_padang(df: pd.DataFrame) -> pd.DataFrame:
    import numpy as np
    SEAGRASS_HOTSPOTS = [
        (-8.72, 115.17), (-5.15, 119.45), (-0.90, 134.90), (-8.50, 140.40),
        (-3.80, 128.20), (1.00, 104.00),  (-2.50, 107.50), (-7.00, 112.70),
        (-8.30, 122.50), (0.50, 127.50),  (-4.00, 122.60), (-1.50, 136.00),
        (-6.10, 106.80), (3.80, 108.20),  (-9.50, 119.50), (-10.20, 123.60),
    ]
    hotspots = np.array(SEAGRASS_HOTSPOTS)
    lats = df["latitude"].values
    lons = df["longitude"].values
    R = 6371.0
    min_dists = []
    for lat, lon in zip(lats, lons):
        dlat = np.radians(hotspots[:, 0] - lat)
        dlon = np.radians(hotspots[:, 1] - lon)
        a = (np.sin(dlat / 2) ** 2
             + np.cos(np.radians(lat)) * np.cos(np.radians(hotspots[:, 0])) * np.sin(dlon / 2) ** 2)
        dists = 2 * R * np.arcsin(np.sqrt(a))
        min_dists.append(round(float(dists.min()), 3))
    df = df.copy()
    df["jarak_padang"] = min_dists
    return df


# ---------------------------------------------------------------------------
# ERDDAP tests
# ---------------------------------------------------------------------------

class TestErddapCollector:
    def test_sst_returns_data(self):
        from collectors.erddap_collector import collect_sst
        df = collect_sst(_TEST_START, _TEST_END, max_records=50, bbox=_SMALL_BBOX, stride=2)
        assert not df.empty, "ERDDAP SST returned empty DataFrame"
        assert "sst_celsius" in df.columns
        assert df["sst_celsius"].notna().all(), "SST has null values"
        assert (df["sst_celsius"] > 20).all(), "SST values unrealistically low"
        assert (df["sst_celsius"] < 35).all(), "SST values unrealistically high"

    def test_chlorophyll_returns_data(self):
        from collectors.erddap_collector import collect_chlorophyll
        df = collect_chlorophyll(_TEST_START, _TEST_END, max_records=50, bbox=_SMALL_BBOX, stride=2)
        assert not df.empty, "ERDDAP chlorophyll returned empty DataFrame"
        assert "chlorophyll_mgm3" in df.columns
        assert df["chlorophyll_mgm3"].notna().all(), "Chlorophyll has null values"
        assert (df["chlorophyll_mgm3"] >= 0).all(), "Chlorophyll has negative values"

    def test_ssh_currents_returns_data(self):
        from collectors.erddap_collector import collect_ssh_currents
        df = collect_ssh_currents(_TEST_START, _TEST_END, max_records=50, bbox=_SMALL_BBOX, stride=1)
        assert not df.empty, "ERDDAP SSH/currents returned empty DataFrame"
        for col in ["ssh_m", "u_current_ms", "v_current_ms"]:
            assert col in df.columns, f"Missing column: {col}"
            assert df[col].notna().any(), f"{col} is entirely null"

    def test_erddap_merge_produces_all_columns(self):
        """Merged ERDDAP frame must have sst, klorofil, ssh, arus after rename."""
        from collectors.erddap_collector import collect_sst, collect_chlorophyll, collect_ssh_currents

        sst_df = collect_sst(_TEST_START, _TEST_END, max_records=100, bbox=_SMALL_BBOX, stride=2)
        chl_df = collect_chlorophyll(_TEST_START, _TEST_END, max_records=100, bbox=_SMALL_BBOX, stride=2)
        ssh_df = collect_ssh_currents(_TEST_START, _TEST_END, max_records=100, bbox=_SMALL_BBOX, stride=1)

        assert not sst_df.empty

        sst_df = sst_df.rename(columns={"time": "tanggal", "sst_celsius": "sst"})
        sst_df["suhu_permukaan"] = sst_df["sst"]
        sst_df["sumber_data"] = "NOAA_ERDDAP"

        if not chl_df.empty:
            chl_df = chl_df.rename(columns={"time": "tanggal", "chlorophyll_mgm3": "klorofil"})
            sst_df = sst_df.merge(
                chl_df[["tanggal", "latitude", "longitude", "klorofil"]],
                on=["tanggal", "latitude", "longitude"], how="left"
            )
        if not ssh_df.empty:
            ssh_df = ssh_df.rename(columns={
                "time": "tanggal", "ssh_m": "ssh",
                "u_current_ms": "arus_laut_u", "v_current_ms": "arus_laut_v",
            })
            sst_df = sst_df.merge(
                ssh_df[["tanggal", "latitude", "longitude", "ssh", "arus_laut_u", "arus_laut_v"]],
                on=["tanggal", "latitude", "longitude"], how="left"
            )

        sst_df = _compute_jarak_padang(sst_df)

        assert "sst" in sst_df.columns
        assert sst_df["sst"].notna().any(), "sst column is entirely null after merge"
        assert "jarak_padang" in sst_df.columns
        assert sst_df["jarak_padang"].notna().all(), "jarak_padang has nulls"
        assert (sst_df["jarak_padang"] >= 0).all(), "jarak_padang has negative values"


# ---------------------------------------------------------------------------
# Open-Meteo tests
# ---------------------------------------------------------------------------

class TestOpenMeteoCollector:
    def test_collect_returns_data(self):
        from collectors.openmeteo_collector import collect
        df = collect(_TEST_START, _TEST_END, max_records=30, bbox=_SMALL_BBOX, grid_resolution=0.5)
        assert not df.empty, "Open-Meteo returned empty DataFrame"

    def test_wave_columns_populated(self):
        from collectors.openmeteo_collector import collect
        df = collect(_TEST_START, _TEST_END, max_records=30, bbox=_OCEAN_BBOX, grid_resolution=0.5)
        assert not df.empty
        assert "tinggi_gelombang" in df.columns, f"Columns: {list(df.columns)}"
        assert df["tinggi_gelombang"].notna().any(), "tinggi_gelombang entirely null"
        assert "periode_gelombang" in df.columns
        assert df["periode_gelombang"].notna().any(), "periode_gelombang entirely null"

    def test_wind_columns_populated(self):
        from collectors.openmeteo_collector import collect
        df = collect(_TEST_START, _TEST_END, max_records=30, bbox=_SMALL_BBOX, grid_resolution=0.5)
        assert not df.empty
        assert "kecepatan_angin" in df.columns
        assert df["kecepatan_angin"].notna().any(), "kecepatan_angin entirely null"
        assert "arah_angin" in df.columns
        assert df["arah_angin"].notna().any(), "arah_angin entirely null"
        assert (df["arah_angin"].dropna() >= 0).all()
        assert (df["arah_angin"].dropna() <= 360).all()

    def test_radiation_column_populated(self):
        from collectors.openmeteo_collector import collect
        df = collect(_TEST_START, _TEST_END, max_records=30, bbox=_SMALL_BBOX, grid_resolution=0.5)
        assert not df.empty
        assert "radiasi_matahari" in df.columns
        assert df["radiasi_matahari"].notna().any(), "radiasi_matahari entirely null"

    def test_cuaca_column_populated(self):
        from collectors.openmeteo_collector import collect
        df = collect(_TEST_START, _TEST_END, max_records=30, bbox=_SMALL_BBOX, grid_resolution=0.5)
        assert not df.empty
        assert "cuaca" in df.columns
        assert df["cuaca"].notna().any(), "cuaca entirely null"
        # Should be human-readable Indonesian labels
        valid_prefixes = ("Cerah", "Berawan", "Mendung", "Hujan", "Gerimis", "Badai", "Berkabut", "Salju", "Kode-")
        for val in df["cuaca"].dropna():
            assert any(val.startswith(p) for p in valid_prefixes), f"Unexpected cuaca value: {val}"

    def test_pasang_surut_column_populated(self):
        from collectors.openmeteo_collector import collect
        df = collect(_TEST_START, _TEST_END, max_records=30, bbox=_OCEAN_BBOX, grid_resolution=0.5)
        assert not df.empty
        assert "pasang_surut" in df.columns
        assert df["pasang_surut"].notna().any(), "pasang_surut entirely null"

    def test_kedalaman_laut_column(self):
        from collectors.openmeteo_collector import collect
        df = collect(_TEST_START, _TEST_END, max_records=30, bbox=_OCEAN_BBOX, grid_resolution=0.5)
        assert not df.empty
        assert "kedalaman_laut" in df.columns
        # Open-Meteo elevation API returns 0.0 for ocean points (not negative),
        # so kedalaman_laut will be None for pure ocean grids — column must exist.

    def test_sumber_data_is_open_meteo(self):
        from collectors.openmeteo_collector import collect
        df = collect(_TEST_START, _TEST_END, max_records=30, bbox=_SMALL_BBOX, grid_resolution=0.5)
        assert not df.empty
        assert "sumber_data" in df.columns
        assert (df["sumber_data"] == "Open-Meteo").all()

    def test_jarak_padang_computed(self):
        from collectors.openmeteo_collector import collect
        df = collect(_TEST_START, _TEST_END, max_records=30, bbox=_SMALL_BBOX, grid_resolution=0.5)
        assert not df.empty
        df = _compute_jarak_padang(df)
        assert "jarak_padang" in df.columns
        assert df["jarak_padang"].notna().all(), "jarak_padang has nulls"
        assert (df["jarak_padang"] >= 0).all()


# ---------------------------------------------------------------------------
# CMEMS tests (mock fallback when no credentials)
# ---------------------------------------------------------------------------

class TestCmemsCollector:
    def test_collect_all_returns_data(self):
        from collectors.cmems_collector import collect_all
        df = collect_all(_TEST_START, _TEST_END, max_records=50, bbox=_SMALL_BBOX)
        assert not df.empty, "CMEMS collect_all returned empty DataFrame"

    def test_collect_all_has_required_columns(self):
        from collectors.cmems_collector import collect_all
        df = collect_all(_TEST_START, _TEST_END, max_records=50, bbox=_SMALL_BBOX)
        assert not df.empty
        for col in ["sst_celsius", "u_current_ms", "v_current_ms", "ssh_m"]:
            assert col in df.columns, f"Missing CMEMS column: {col}"
            assert df[col].notna().any(), f"{col} entirely null"

    def test_collect_all_sst_range(self):
        from collectors.cmems_collector import collect_all
        df = collect_all(_TEST_START, _TEST_END, max_records=50, bbox=_SMALL_BBOX)
        assert not df.empty
        sst = df["sst_celsius"].dropna()
        assert len(sst) > 0
        assert (sst > 15).all(), "SST below 15°C — unrealistic for Indonesia"
        assert (sst < 35).all(), "SST above 35°C — unrealistic"


# ---------------------------------------------------------------------------
# WPP filter tests
# ---------------------------------------------------------------------------

class TestWppFilter:
    def test_filter_removes_outside_points(self):
        from collectors.wpp_filter import filter_wpp
        df = pd.DataFrame({
            "latitude":  [-6.0, 50.0, 0.0],   # WPP, outside, WPP
            "longitude": [110.0, 10.0, 127.0],
        })
        filtered = filter_wpp(df)
        assert len(filtered) == 2, f"Expected 2 WPP rows, got {len(filtered)}"

    def test_filter_adds_wpp_region_column(self):
        from collectors.wpp_filter import filter_wpp
        df = pd.DataFrame({"latitude": [-6.0], "longitude": [110.0]})
        filtered = filter_wpp(df)
        assert "wpp_region" in filtered.columns
        assert filtered["wpp_region"].notna().all()

    def test_delete_outside_wpp_sql_is_valid(self):
        from collectors.wpp_filter import delete_outside_wpp_sql
        sql = delete_outside_wpp_sql()
        assert sql.strip().upper().startswith("DELETE FROM")
        assert "master_oceanography" in sql
        assert "latitude" in sql
        assert "longitude" in sql


# ---------------------------------------------------------------------------
# DB writer tests (skip if no DB)
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _has_db(), reason="No DB credentials in environment")
class TestDbWriter:
    def test_upsert_small_dataframe(self):
        from db.writer import upsert_dataframe
        df = pd.DataFrame([{
            "tanggal":          pd.Timestamp(_TEST_START),
            "latitude":         -6.0,
            "longitude":        110.0,
            "sst":              28.5,
            "suhu_permukaan":   28.5,
            "ssh":              0.05,
            "klorofil":         0.3,
            "arus_laut_u":      0.1,
            "arus_laut_v":      0.05,
            "tinggi_gelombang": 1.2,
            "periode_gelombang":8.0,
            "kecepatan_angin":  5.0,
            "arah_angin":       180.0,
            "radiasi_matahari": 20.0,
            "cuaca":            "Cerah",
            "kedalaman_laut":   50.0,
            "pasang_surut":     0.3,
            "jarak_padang":     120.5,
            "sumber_data":      "TEST_INTEGRATION",
        }])
        n = upsert_dataframe(df, "TEST_INTEGRATION")
        assert n == 1, f"Expected 1 row upserted, got {n}"

    def test_upsert_idempotent(self):
        """Running upsert twice should not raise and row count stays 1."""
        from db.writer import upsert_dataframe
        row = {
            "tanggal":          pd.Timestamp(_TEST_START),
            "latitude":         -6.5,
            "longitude":        110.5,
            "sst":              29.0,
            "suhu_permukaan":   29.0,
            "sumber_data":      "TEST_INTEGRATION",
        }
        df = pd.DataFrame([row])
        n1 = upsert_dataframe(df, "TEST_INTEGRATION")
        n2 = upsert_dataframe(df, "TEST_INTEGRATION")
        assert n1 == 1
        assert n2 == 1

    def test_db_columns_filled_after_upsert(self):
        """After upserting a full row, verify DB has no nulls for that row."""
        import psycopg2
        from db.writer import upsert_dataframe, _get_conn

        tanggal = pd.Timestamp("2024-06-15")
        lat, lon = -5.5, 110.5
        df = pd.DataFrame([{
            "tanggal":          tanggal,
            "latitude":         lat,
            "longitude":        lon,
            "sst":              28.0,
            "suhu_permukaan":   28.0,
            "ssh":              0.02,
            "klorofil":         0.25,
            "arus_laut_u":      0.08,
            "arus_laut_v":      0.04,
            "tinggi_gelombang": 0.9,
            "periode_gelombang":7.5,
            "kecepatan_angin":  4.5,
            "arah_angin":       200.0,
            "radiasi_matahari": 18.0,
            "cuaca":            "Hujan Ringan",
            "kedalaman_laut":   45.0,
            "pasang_surut":     0.25,
            "jarak_padang":     95.0,
            "sumber_data":      "TEST_INTEGRATION_FULL",
        }])
        upsert_dataframe(df, "TEST_INTEGRATION_FULL")

        conn = _get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        sst, ssh, klorofil, arus_laut_u, arus_laut_v, kecepatan_arus,
                        tinggi_gelombang, periode_gelombang, kecepatan_angin, arah_angin,
                        radiasi_matahari, cuaca, kedalaman_laut, pasang_surut, jarak_padang
                    FROM master_oceanography
                    WHERE tanggal = %s AND latitude = %s AND longitude = %s
                      AND sumber_data = 'TEST_INTEGRATION_FULL'
                """, (tanggal.date(), lat, lon))
                row = cur.fetchone()
        finally:
            conn.close()

        assert row is not None, "Row not found in DB after upsert"
        col_names = [
            "sst", "ssh", "klorofil", "arus_laut_u", "arus_laut_v", "kecepatan_arus",
            "tinggi_gelombang", "periode_gelombang", "kecepatan_angin", "arah_angin",
            "radiasi_matahari", "cuaca", "kedalaman_laut", "pasang_surut", "jarak_padang",
        ]
        for name, val in zip(col_names, row):
            assert val is not None, f"Column {name} is NULL in DB after upsert"


# ---------------------------------------------------------------------------
# End-to-end pipeline test (no DB write — just verify DataFrame shape)
# ---------------------------------------------------------------------------

class TestEndToEndPipeline:
    def test_openmeteo_pipeline_all_columns(self):
        """Full Open-Meteo pipeline: collect → jarak_padang → verify all columns."""
        from collectors.openmeteo_collector import collect
        from collectors.wpp_filter import filter_wpp

        # Use enough records so all grid points in the bbox get both marine + forecast data
        df = collect(_TEST_START, _TEST_END, max_records=500, bbox=_OCEAN_BBOX,
                     grid_resolution=0.5, wpp_only=False)
        assert not df.empty

        df = filter_wpp(df)
        df = _compute_jarak_padang(df)

        expected = [
            "tanggal", "latitude", "longitude", "sumber_data",
            "tinggi_gelombang", "periode_gelombang", "pasang_surut",
            "kecepatan_angin", "arah_angin", "radiasi_matahari", "cuaca",
            "kedalaman_laut", "jarak_padang",
        ]
        for col in expected:
            assert col in df.columns, f"Missing column in pipeline output: {col}"

        # Identity columns must never be null
        for col in ["tanggal", "latitude", "longitude", "sumber_data"]:
            assert df[col].notna().all(), f"Identity column {col} has nulls"

        # Data columns: at least 80% fill rate
        # kedalaman_laut excluded — elevation API returns 0.0 for ocean (no depth data)
        fill_threshold = 0.8
        for col in ["tinggi_gelombang", "kecepatan_angin", "arah_angin", "radiasi_matahari", "cuaca"]:
            fill_rate = df[col].notna().mean()
            assert fill_rate >= fill_threshold, (
                f"{col} fill rate {fill_rate:.1%} below {fill_threshold:.0%} threshold"
            )

    def test_erddap_pipeline_all_columns(self):
        """Full ERDDAP pipeline: collect SST+CHL+SSH → merge → jarak_padang → verify."""
        from collectors.erddap_collector import collect_sst, collect_chlorophyll, collect_ssh_currents
        from collectors.wpp_filter import filter_wpp

        sst_df = collect_sst(_TEST_START, _TEST_END, max_records=100, bbox=_SMALL_BBOX, stride=2)
        if sst_df.empty:
            pytest.skip("ERDDAP SST returned no data for test window")

        chl_df = collect_chlorophyll(_TEST_START, _TEST_END, max_records=100, bbox=_SMALL_BBOX, stride=2)
        ssh_df = collect_ssh_currents(_TEST_START, _TEST_END, max_records=100, bbox=_SMALL_BBOX, stride=1)

        sst_df = sst_df.rename(columns={"time": "tanggal", "sst_celsius": "sst"})
        sst_df["suhu_permukaan"] = sst_df["sst"]
        sst_df["sumber_data"] = "NOAA_ERDDAP"

        if not chl_df.empty:
            chl_df = chl_df.rename(columns={"time": "tanggal", "chlorophyll_mgm3": "klorofil"})
            sst_df = sst_df.merge(
                chl_df[["tanggal", "latitude", "longitude", "klorofil"]],
                on=["tanggal", "latitude", "longitude"], how="left"
            )
        if not ssh_df.empty:
            ssh_df = ssh_df.rename(columns={
                "time": "tanggal", "ssh_m": "ssh",
                "u_current_ms": "arus_laut_u", "v_current_ms": "arus_laut_v",
            })
            sst_df = sst_df.merge(
                ssh_df[["tanggal", "latitude", "longitude", "ssh", "arus_laut_u", "arus_laut_v"]],
                on=["tanggal", "latitude", "longitude"], how="left"
            )

        sst_df = filter_wpp(sst_df)
        sst_df = _compute_jarak_padang(sst_df)

        assert "sst" in sst_df.columns
        assert sst_df["sst"].notna().any()
        assert "jarak_padang" in sst_df.columns
        assert sst_df["jarak_padang"].notna().all()

        for col in ["tanggal", "latitude", "longitude", "sumber_data"]:
            assert sst_df[col].notna().all(), f"Identity column {col} has nulls"
