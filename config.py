"""
Central configuration for Indonesia Ocean Fishing Ground Collector.
All values can be overridden via CLI args or environment variables.
"""

from __future__ import annotations
from datetime import date

# ---------------------------------------------------------------------------
# Bounding box — seluruh perairan Indonesia
# ---------------------------------------------------------------------------
INDONESIA_BBOX: dict[str, float] = {
    "min_lon": 95.0,
    "max_lon": 141.0,
    "min_lat": -11.0,
    "max_lat":  6.0,
}

# ---------------------------------------------------------------------------
# Wilayah Pengelolaan Perikanan (WPP) Indonesia
# Sumber: Permen KP No. 18/2014
# ---------------------------------------------------------------------------
WPP_REGIONS: dict[str, dict[str, float]] = {
    "WPP_571_Selat_Malaka":        {"min_lat":  1.0, "max_lat":  6.0, "min_lon":  95.0, "max_lon": 104.0},
    "WPP_572_Samudera_Hindia_Barat":{"min_lat":-6.0, "max_lat":  2.0, "min_lon":  95.0, "max_lon": 105.0},
    "WPP_573_Samudera_Hindia_Selatan":{"min_lat":-11.0,"max_lat":-6.0,"min_lon": 102.0, "max_lon": 115.0},
    "WPP_711_Laut_Natuna":         {"min_lat":  0.0, "max_lat":  6.0, "min_lon": 104.0, "max_lon": 110.0},
    "WPP_712_Laut_Jawa":           {"min_lat": -8.0, "max_lat": -2.0, "min_lon": 106.0, "max_lon": 116.0},
    "WPP_713_Selat_Makassar":      {"min_lat": -8.0, "max_lat":  2.0, "min_lon": 116.0, "max_lon": 122.0},
    "WPP_714_Laut_Banda":          {"min_lat": -8.0, "max_lat": -2.0, "min_lon": 122.0, "max_lon": 132.0},
    "WPP_715_Laut_Maluku":         {"min_lat": -4.0, "max_lat":  4.0, "min_lon": 124.0, "max_lon": 132.0},
    "WPP_716_Laut_Sulawesi":       {"min_lat":  0.0, "max_lat":  6.0, "min_lon": 118.0, "max_lon": 128.0},
    "WPP_717_Teluk_Cendrawasih":   {"min_lat": -4.0, "max_lat":  2.0, "min_lon": 132.0, "max_lon": 138.0},
    "WPP_718_Laut_Arafuru":        {"min_lat": -9.0, "max_lat": -4.0, "min_lon": 132.0, "max_lon": 141.0},
}

# ---------------------------------------------------------------------------
# Defaults — overridable via CLI
# ---------------------------------------------------------------------------
DEFAULT_START_DATE: str = "2023-01-01"
DEFAULT_END_DATE:   str = "2023-03-31"
DEFAULT_MAX_RECORDS: int = 1000
DEFAULT_GRID_RESOLUTION: float = 0.5   # degrees
DEFAULT_OUTPUT_DIR: str = "data/processed"
DEFAULT_RAW_DIR:    str = "data/raw"

# ---------------------------------------------------------------------------
# ERDDAP endpoints
# ---------------------------------------------------------------------------
ERDDAP_BASE_URL = "https://coastwatch.pfeg.noaa.gov/erddap/griddap"
ERDDAP_SST_DATASET        = "erdMH1sstdmday_R2022SQNotMasked"  # MODIS monthly SST, gap-free
ERDDAP_SST_HADISST        = "erdHadISST"                        # HadISST monthly, 1°, gap-free fallback
ERDDAP_CHL_DATASET        = "erdMH1chlamday_R2022NRT"           # MODIS monthly chlorophyll
ERDDAP_SSH_DATASET        = "nesdisSSH1day"                     # NESDIS SSH + geostrophic currents

# ---------------------------------------------------------------------------
# Global Fishing Watch
# ---------------------------------------------------------------------------
GFW_BASE_URL = "https://gateway.api.globalfishingwatch.org/v3"
GFW_FISHING_EFFORT_DATASET = "public-global-fishing-effort:latest"

# ---------------------------------------------------------------------------
# Fishing Ground Index weights
# ---------------------------------------------------------------------------
FGI_CHLOROPHYLL_WEIGHT: float = 0.6
FGI_SST_WEIGHT:         float = 0.4
FGI_OPTIMAL_SST:        float = 27.5   # °C — optimal SST for tropical fish
FGI_SST_TOLERANCE:      float = 10.0   # °C — tolerance window
