"""WPP spatial helpers backed by master_wpp GeoJSON polygons."""

from __future__ import annotations

import json
import logging
import os
import re
from functools import lru_cache
from typing import Any

from config import WPP_REGIONS

logger = logging.getLogger(__name__)


def _connect():
    import psycopg2

    return psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=int(os.environ.get("DB_PORT", "5432")),
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        connect_timeout=10,
    )


def _polygon_bbox(geometry: dict[str, Any]) -> tuple[float, float, float, float] | None:
    points: list[tuple[float, float]] = []

    def walk(coords: Any) -> None:
        if isinstance(coords, list) and len(coords) >= 2 and all(isinstance(v, (int, float)) for v in coords[:2]):
            points.append((float(coords[0]), float(coords[1])))
        elif isinstance(coords, list):
            for item in coords:
                walk(item)

    walk(geometry.get("coordinates"))
    if not points:
        return None
    xs = [p[0] for p in points]
    ys = [p[1] for p in points]
    return min(xs), min(ys), max(xs), max(ys)


def _point_in_ring(lon: float, lat: float, ring: list[list[float]]) -> bool:
    inside = False
    j = len(ring) - 1
    for i, point in enumerate(ring):
        xi, yi = float(point[0]), float(point[1])
        xj, yj = float(ring[j][0]), float(ring[j][1])
        crosses = (yi > lat) != (yj > lat)
        if crosses:
            x_intersect = (xj - xi) * (lat - yi) / ((yj - yi) or 1e-12) + xi
            if lon < x_intersect:
                inside = not inside
        j = i
    return inside


def _point_in_polygon(lon: float, lat: float, rings: list[list[list[float]]]) -> bool:
    if not rings or not _point_in_ring(lon, lat, rings[0]):
        return False
    return not any(_point_in_ring(lon, lat, hole) for hole in rings[1:])


def _point_in_geometry(lon: float, lat: float, geometry: dict[str, Any]) -> bool:
    gtype = geometry.get("type")
    coords = geometry.get("coordinates") or []
    if gtype == "Polygon":
        return _point_in_polygon(lon, lat, coords)
    if gtype == "MultiPolygon":
        return any(_point_in_polygon(lon, lat, polygon) for polygon in coords)
    return False


@lru_cache(maxsize=1)
def load_wpp_polygons() -> list[dict[str, Any]]:
    """Load active WPP polygons from DB; fall back to config bboxes if DB is unavailable."""
    try:
        conn = _connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, kode_wpp, nama_wpp, geojson_polygon
                    FROM master_wpp
                    WHERE status = 'aktif' AND geojson_polygon IS NOT NULL
                    ORDER BY kode_wpp
                    """
                )
                rows = cur.fetchall()
        finally:
            conn.close()
    except Exception as exc:
        logger.warning("Could not load WPP polygons from DB, using bbox fallback: %s", exc)
        rows = []

    polygons: list[dict[str, Any]] = []
    for wpp_id, code, name, geometry in rows:
        if isinstance(geometry, str):
            geometry = json.loads(geometry)
        bbox = _polygon_bbox(geometry)
        if bbox:
            polygons.append({"id": wpp_id, "code": code, "name": name, "geometry": geometry, "bbox": bbox})

    if polygons:
        return polygons

    fallback = []
    for name, bbox in WPP_REGIONS.items():
        match = re.search(r"WPP_(\d{3})", name)
        if not match:
            continue
        code = f"WPP-{match.group(1)}"
        fallback.append({
            "id": None,
            "code": code,
            "name": name,
            "geometry": None,
            "bbox": (bbox["min_lon"], bbox["min_lat"], bbox["max_lon"], bbox["max_lat"]),
        })
    return fallback


def get_wpp_id_for_point(lat: float | int | None, lon: float | int | None) -> int | None:
    code = get_wpp_code_for_point(lat, lon)
    if not code:
        return None
    for item in load_wpp_polygons():
        if item["code"] == code:
            return item["id"]
    return None


def get_wpp_code_for_point(lat: float | int | None, lon: float | int | None) -> str | None:
    if lat is None or lon is None:
        return None
    flat = float(lat)
    flon = float(lon)
    bbox_match = None
    for item in load_wpp_polygons():
        min_lon, min_lat, max_lon, max_lat = item["bbox"]
        if not (min_lat <= flat <= max_lat and min_lon <= flon <= max_lon):
            continue
        if item["geometry"] and _point_in_geometry(flon, flat, item["geometry"]):
            return item["code"]
        bbox_match = item["code"]
    return bbox_match


def assign_wpp(df):
    """Add a WPP code column to a DataFrame with latitude/longitude columns."""
    if df.empty:
        return df
    result = df.copy()
    result["wpp"] = [
        get_wpp_code_for_point(lat, lon)
        for lat, lon in zip(result["latitude"], result["longitude"])
    ]
    return result[result["wpp"].notna()]
