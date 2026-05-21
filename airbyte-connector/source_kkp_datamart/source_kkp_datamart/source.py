"""
Airbyte Source: KKP Datamart

Streams:
  - master_kapal          : full vessel registry (search-kapal-bkp + data-kapal detail)
                            Use OVERWRITE sync mode — emits all records fresh every run.
  - master_vessel_tracking: GPS pings per vessel (data-kapal-lokasi-interval)
                            Use OVERWRITE sync mode — emits all current pings fresh every run.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Iterable, List, Mapping, Optional, Tuple

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.models import SyncMode

logger = logging.getLogger("airbyte")

KKP_BASE = "https://insight.kkp.go.id/datamart/api"
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
RETRY_BACKOFF = 2.0
RATE_LIMIT_DELAY = 0.3  # seconds between per-vessel calls


# ---------------------------------------------------------------------------
# HTTP helper
# ---------------------------------------------------------------------------

def _get(url: str, token: str, params: Optional[dict] = None) -> Optional[Any]:
    headers = {"Authorization": f"Bearer {token}"}
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 429:
                wait = RETRY_BACKOFF ** attempt
                logger.warning("Rate limited on %s — waiting %.1fs", url, wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.RequestException as exc:
            logger.error("Request failed [attempt %d/%d] %s: %s", attempt, MAX_RETRIES, url, exc)
            if attempt == MAX_RETRIES:
                return None
            time.sleep(RETRY_BACKOFF)
    return None


def _fetch_kapal_list(token: str) -> list:
    """Fetch full vessel list from search-kapal-bkp endpoint."""
    url = f"{KKP_BASE}/kapal/search-kapal-bkp"
    params = {"page": 1, "limit": 100000, "sort_by": "nama_kapal", "sort_order": "asc"}
    data = _get(url, token, params)
    if data is None:
        return []
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and "data" in data:
        return data["data"]
    return []


# ---------------------------------------------------------------------------
# Value helpers
# ---------------------------------------------------------------------------

def _str(val: Any, maxlen: int = 150) -> Optional[str]:
    if val is None:
        return None
    s = str(val).strip()
    return s[:maxlen] if s else None


def _float(val: Any) -> Optional[float]:
    if val is None or val == "":
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Stream: master_kapal
# ---------------------------------------------------------------------------

class MasterKapal(Stream):
    """
    Fetches ALL vessels from KKP search-kapal-bkp, then enriches each with
    data-kapal detail. Emits one record per vessel.

    Use OVERWRITE sync mode in Airbyte — destination table is truncated and
    fully reloaded on every sync run. No upsert / incremental logic.
    """

    primary_key = "nomor_bkp"

    def __init__(self, token: str, enrich_detail: bool = True):
        self._token = token
        self._enrich_detail = enrich_detail

    @property
    def name(self) -> str:
        return "master_kapal"

    def get_json_schema(self) -> Mapping[str, Any]:
        return {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "nama_kapal":          {"type": ["null", "string"]},
                "nomor_bkp":           {"type": ["null", "string"]},
                "no_transmitter":      {"type": ["null", "string"]},
                "tanda_selar":         {"type": ["null", "string"]},
                "ukuran_kapal":        {"type": ["null", "number"]},
                "pemilik":             {"type": ["null", "string"]},
                "alat_tangkap":        {"type": ["null", "string"]},
                "kekuatan_mesin":      {"type": ["null", "number"]},
                "merek_mesin":         {"type": ["null", "string"]},
                "wilayah_tangkap":     {"type": ["null", "string"]},
                "pelabuhan_pangkalan": {"type": ["null", "string"]},
                "aktif":               {"type": ["null", "boolean"]},
            },
        }

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Mapping[str, Any]]:
        raw_list = _fetch_kapal_list(self._token)
        if not raw_list:
            logger.error("KKP master_kapal: no records returned from search-kapal-bkp")
            return

        logger.info("KKP master_kapal: fetched %d vessels", len(raw_list))

        for i, raw in enumerate(raw_list, 1):
            nomor_bkp = _str(
                raw.get("nomor_buku_kapal") or raw.get("nomor_bkp") or raw.get("no_bkp"), 50
            )
            if not nomor_bkp:
                continue

            transmitter_no = _str(
                raw.get("transmitter_no") or raw.get("no_transmitter") or raw.get("nomor_transmitter"), 100
            )

            record: dict = {
                "nama_kapal":          _str(raw.get("nama_kapal"), 150),
                "nomor_bkp":           nomor_bkp,
                "no_transmitter":      transmitter_no,
                "tanda_selar":         None,
                "ukuran_kapal":        None,
                "pemilik":             None,
                "alat_tangkap":        None,
                "kekuatan_mesin":      None,
                "merek_mesin":         None,
                "wilayah_tangkap":     None,
                "pelabuhan_pangkalan": None,
                "aktif":               True,
            }

            if self._enrich_detail and transmitter_no:
                detail_data = _get(
                    f"{KKP_BASE}/kapal/data-kapal",
                    self._token,
                    {"transmitter_no": transmitter_no},
                )
                detail: Optional[dict] = None
                if isinstance(detail_data, dict):
                    if "data" in detail_data and isinstance(detail_data["data"], dict):
                        detail = detail_data["data"]
                    else:
                        detail = detail_data
                elif isinstance(detail_data, list) and detail_data:
                    detail = detail_data[0]

                if detail:
                    alat = _str(detail.get("jenis_alat_tangkap") or detail.get("alat_tangkap"), 100)
                    record.update({
                        "nomor_bkp":           _str(detail.get("no_bkp") or detail.get("nomor_bkp") or nomor_bkp, 50),
                        "no_transmitter":      _str(detail.get("nomor_transmitter") or transmitter_no, 100),
                        "tanda_selar":         _str(detail.get("tanda_selar"), 100),
                        "ukuran_kapal":        _float(detail.get("ukuran_gt")),
                        "pemilik":             _str(detail.get("pemilik_kapal"), 150),
                        "alat_tangkap":        alat,
                        "kekuatan_mesin":      _float(detail.get("kekuatan_mesin")),
                        "merek_mesin":         _str(detail.get("merk_mesin"), 100),
                        "wilayah_tangkap":     _str(detail.get("nama_wpp"), 100),
                        "pelabuhan_pangkalan": _str(detail.get("nama_pelabuhan_pangkalan"), 150),
                    })

                time.sleep(RATE_LIMIT_DELAY)

            if i % 100 == 0:
                logger.info("  master_kapal: processed %d/%d vessels", i, len(raw_list))

            yield record


# ---------------------------------------------------------------------------
# Stream: master_vessel_tracking
# ---------------------------------------------------------------------------

class MasterVesselTracking(Stream):
    """
    Fetches GPS tracking pings for every vessel from data-kapal-lokasi-interval.
    Emits one record per ping point.

    Use OVERWRITE sync mode in Airbyte — destination table is truncated and
    fully reloaded on every sync run.
    """

    primary_key = None

    def __init__(self, token: str, interval: int = 30):
        self._token = token
        self._interval = interval

    @property
    def name(self) -> str:
        return "master_vessel_tracking"

    def get_json_schema(self) -> Mapping[str, Any]:
        return {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "nama_kapal":     {"type": ["null", "string"]},
                "nomor_bkp":      {"type": ["null", "string"]},
                "transmitter_no": {"type": ["null", "string"]},
                "mmsi":           {"type": ["null", "string"]},
                "latitude":       {"type": ["null", "number"]},
                "longitude":      {"type": ["null", "number"]},
                "direction":      {"type": ["null", "number"]},
                "speed":          {"type": ["null", "number"]},
                "timestamp":      {"type": ["null", "string"]},
                "status_kapal":   {"type": ["null", "string"]},
                "source":         {"type": ["null", "string"]},
            },
        }

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Mapping[str, Any]]:
        kapal_list = _fetch_kapal_list(self._token)
        if not kapal_list:
            logger.error("KKP tracking: no vessels found — cannot collect tracking data")
            return

        logger.info("KKP tracking: iterating %d vessels (interval=%d min)", len(kapal_list), self._interval)

        for i, kapal in enumerate(kapal_list, 1):
            nomor_bkp = _str(
                kapal.get("nomor_buku_kapal") or kapal.get("nomor_bkp") or kapal.get("no_bkp"), 50
            )
            if not nomor_bkp:
                continue

            nama_kapal = _str(kapal.get("nama_kapal"), 100)
            transmitter_no = _str(
                kapal.get("transmitter_no") or kapal.get("no_transmitter") or kapal.get("nomor_transmitter"), 100
            )

            data = _get(
                f"{KKP_BASE}/kapal/data-kapal-lokasi-interval",
                self._token,
                {"nomor_bkp": nomor_bkp, "interval": self._interval},
            )

            if data is None:
                time.sleep(RATE_LIMIT_DELAY)
                continue

            points: list = []
            if isinstance(data, dict) and data.get("type") == "FeatureCollection":
                for feature in data.get("features", []):
                    geom  = feature.get("geometry", {})
                    props = feature.get("properties", {})
                    if geom.get("type") == "Point":
                        coords = geom.get("coordinates", [])
                        points.append({
                            "longitude": coords[0] if len(coords) > 0 else None,
                            "latitude":  coords[1] if len(coords) > 1 else None,
                            "speed":     props.get("speed"),
                            "direction": props.get("heading"),
                            "ping_time": props.get("ping_time"),
                            "nomor_bkp": props.get("nomor_bkp", nomor_bkp),
                        })
            elif isinstance(data, list):
                points = data

            for pt in points:
                lat = _float(pt.get("latitude"))
                lon = _float(pt.get("longitude"))
                if lat is None or lon is None:
                    continue
                yield {
                    "nama_kapal":     nama_kapal,
                    "nomor_bkp":      _str(pt.get("nomor_bkp") or nomor_bkp, 50),
                    "transmitter_no": transmitter_no,
                    "mmsi":           None,
                    "latitude":       lat,
                    "longitude":      lon,
                    "direction":      _float(pt.get("direction") or pt.get("heading")),
                    "speed":          _float(pt.get("speed")),
                    "timestamp":      pt.get("ping_time") or pt.get("timestamp") or pt.get("waktu"),
                    "status_kapal":   None,
                    "source":         "KKP_VMS",
                }

            if i % 50 == 0:
                logger.info("  tracking: processed %d/%d vessels", i, len(kapal_list))

            time.sleep(RATE_LIMIT_DELAY)


# ---------------------------------------------------------------------------
# Source
# ---------------------------------------------------------------------------

class SourceKkpDatamart(AbstractSource):

    def check_connection(self, logger: logging.Logger, config: Mapping[str, Any]) -> Tuple[bool, Optional[Any]]:
        token = config.get("kkp_datamart_token", "")
        if not token:
            return False, "kkp_datamart_token is required"
        data = _get(
            f"{KKP_BASE}/kapal/search-kapal-bkp",
            token,
            {"page": 1, "limit": 1, "sort_by": "nama_kapal", "sort_order": "asc"},
        )
        if data is None:
            return False, "Failed to connect to KKP Datamart API — check token and network"
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        token         = config["kkp_datamart_token"]
        enrich_detail = config.get("enrich_detail", True)
        interval      = int(config.get("tracking_interval", 30))
        return [
            MasterKapal(token=token, enrich_detail=enrich_detail),
            MasterVesselTracking(token=token, interval=interval),
        ]
