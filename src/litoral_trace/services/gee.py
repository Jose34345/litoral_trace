"""Servicio de integracion con Google Earth Engine (GEE)."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
import hashlib
import json
import time
from typing import Any

from litoral_trace.config import get_settings


ALGORITHM_VERSION = "2.4.0-gee-sentinel2-scl-v2"


class GeeFailureCategory(StrEnum):
    TIMEOUT = "timeout"
    TEMPORARY_NETWORK = "temporary_network"
    TEMPORARY_SERVICE = "temporary_service"
    TEMPORARY_RATE_LIMIT = "temporary_rate_limit"
    AUTHENTICATION = "authentication"
    PERMISSION = "permission"
    CONFIGURATION = "configuration"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class GeeFailureInfo:
    category: GeeFailureCategory
    error_code: str
    detail_message: str


@dataclass(frozen=True)
class GeeInitializationResult:
    success: bool
    detail_message: str
    initialization_time_ms: int
    failure: GeeFailureInfo | None = None


def generate_geometry_hash(polygon_wkt_or_geojson: str | dict) -> str:
    """Genera un hash SHA-256 deterministico de la geometria normalizada."""
    try:
        from shapely import geometry, wkt
        from shapely.geometry import mapping

        if isinstance(polygon_wkt_or_geojson, dict):
            geom = geometry.shape(polygon_wkt_or_geojson)
        else:
            geom = wkt.loads(str(polygon_wkt_or_geojson).strip())

        rounded_geojson = mapping(geom)
        canonical_str = json.dumps(rounded_geojson, sort_keys=True)
    except Exception:
        canonical_str = str(polygon_wkt_or_geojson).strip().upper()

    return hashlib.sha256(canonical_str.encode("utf-8")).hexdigest()


def _exception_status_code(exc: Exception) -> int | None:
    for attribute_name in ("status_code", "code"):
        raw_value = getattr(exc, attribute_name, None)
        if isinstance(raw_value, int):
            return raw_value

    response = getattr(exc, "response", None)
    status_code = getattr(response, "status_code", None)
    if isinstance(status_code, int):
        return status_code

    return None


def _classify_gee_exception(exc: Exception) -> GeeFailureInfo:
    status_code = _exception_status_code(exc)

    if isinstance(exc, TimeoutError):
        return GeeFailureInfo(
            category=GeeFailureCategory.TIMEOUT,
            error_code="gee_timeout",
            detail_message=f"{type(exc).__name__} - {exc}",
        )

    if isinstance(exc, ConnectionError):
        return GeeFailureInfo(
            category=GeeFailureCategory.TEMPORARY_NETWORK,
            error_code="gee_temporary_network_failure",
            detail_message=f"{type(exc).__name__} - {exc}",
        )

    if status_code == 429:
        return GeeFailureInfo(
            category=GeeFailureCategory.TEMPORARY_RATE_LIMIT,
            error_code="gee_temporary_rate_limit",
            detail_message=f"{type(exc).__name__} - {exc}",
        )

    if status_code in {500, 502, 503, 504}:
        return GeeFailureInfo(
            category=GeeFailureCategory.TEMPORARY_SERVICE,
            error_code="gee_temporary_service_failure",
            detail_message=f"{type(exc).__name__} - {exc}",
        )

    if status_code == 401:
        return GeeFailureInfo(
            category=GeeFailureCategory.AUTHENTICATION,
            error_code="gee_authentication_failed",
            detail_message=f"{type(exc).__name__} - {exc}",
        )

    if status_code == 403 or isinstance(exc, PermissionError):
        return GeeFailureInfo(
            category=GeeFailureCategory.PERMISSION,
            error_code="gee_permission_denied",
            detail_message=f"{type(exc).__name__} - {exc}",
        )

    if isinstance(exc, (ValueError, TypeError, KeyError, json.JSONDecodeError)):
        return GeeFailureInfo(
            category=GeeFailureCategory.CONFIGURATION,
            error_code="gee_configuration_error",
            detail_message=f"{type(exc).__name__} - {exc}",
        )

    return GeeFailureInfo(
        category=GeeFailureCategory.UNKNOWN,
        error_code="gee_execution_failed",
        detail_message=f"{type(exc).__name__} - {exc}",
    )


def _build_gee_error_result(
    *,
    gee_connected: bool,
    geometry_hash: str,
    gee_initialization_ms: int,
    gee_query_ms: int,
    failure: GeeFailureInfo,
) -> dict[str, Any]:
    return {
        "status": "error",
        "gee_connected": gee_connected,
        "error_detail": failure.detail_message,
        "error_code": failure.error_code,
        "error_category": failure.category.value,
        "geometry_hash": geometry_hash,
        "total_observations": 0,
        "gee_initialization_ms": gee_initialization_ms,
        "gee_query_ms": gee_query_ms,
        "observations": [],
    }


def _initialize_earth_engine_detailed() -> GeeInitializationResult:
    """Inicializa GEE y conserva metadata machine-readable para D4."""
    t0 = time.time()
    settings = get_settings()
    gcp_project = settings.gee.project_id

    try:
        import ee

        sa_json = settings.gee.service_account_json
        if sa_json:
            try:
                sa_data = json.loads(sa_json)
                creds = ee.ServiceAccountCredentials(
                    sa_data["client_email"],
                    key_data=sa_data["private_key"],
                )
                if gcp_project:
                    ee.Initialize(creds, project=gcp_project)
                else:
                    ee.Initialize(creds)
                init_ms = int((time.time() - t0) * 1000)
                return GeeInitializationResult(
                    success=True,
                    detail_message=(
                        "GEE inicializado via Service Account JSON "
                        f"(proyecto: {gcp_project})"
                    ),
                    initialization_time_ms=init_ms,
                )
            except Exception as exc:
                init_ms = int((time.time() - t0) * 1000)
                failure = _classify_gee_exception(exc)
                return GeeInitializationResult(
                    success=False,
                    detail_message=(
                        "Error de autenticacion GEE: "
                        f"{failure.detail_message}"
                    ),
                    initialization_time_ms=init_ms,
                    failure=failure,
                )

        if gcp_project:
            ee.Initialize(project=gcp_project)
        else:
            ee.Initialize()

        init_ms = int((time.time() - t0) * 1000)
        return GeeInitializationResult(
            success=True,
            detail_message=(
                "GEE inicializado via ADC / OAuth persistente "
                f"(proyecto: {gcp_project})"
            ),
            initialization_time_ms=init_ms,
        )

    except Exception as exc:
        init_ms = int((time.time() - t0) * 1000)
        failure = _classify_gee_exception(exc)
        return GeeInitializationResult(
            success=False,
            detail_message=(
                "Error de autenticacion GEE: "
                f"{failure.detail_message}"
            ),
            initialization_time_ms=init_ms,
            failure=failure,
        )


def initialize_earth_engine() -> tuple[bool, str, int]:
    """Inicializa GEE manteniendo el contrato legacy de tres valores."""
    result = _initialize_earth_engine_detailed()
    return (
        result.success,
        result.detail_message,
        result.initialization_time_ms,
    )


def consultar_serie_temporal_ndvi_gee(
    polygon_wkt: str,
    start_date: str = "2020-12-31",
    end_date: str | None = None,
    max_cloud_pct: float = 20.0,
) -> dict[str, Any]:
    """Consulta la serie historica de NDVI sobre el poligono real del lote."""
    t_start = time.time()
    end_date_str = end_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    geom_hash = generate_geometry_hash(polygon_wkt)
    init_result = _initialize_earth_engine_detailed()
    gee_ready = init_result.success
    gee_msg = init_result.detail_message
    init_ms = init_result.initialization_time_ms

    if not gee_ready:
        settings = get_settings()
        is_test = settings.is_test or settings.gee.test_mode
        if is_test:
            from litoral_trace.services.ndvi import calcular_ndvi_simulado

            observations = [
                {
                    "observation_date": point["fecha"],
                    "ndvi_mean": round(float(point["ndvi_mean"]), 4),
                    "ndvi_min": round(float(point["ndvi_mean"]), 4),
                    "ndvi_max": round(float(point["ndvi_mean"]), 4),
                    "ndvi_std": 0.0,
                    "scene_cloud_percentage": 5.0,
                    "aoi_cloud_percentage": 1.0,
                    "valid_pixel_percentage": 98.0,
                    "satellite": "Sentinel-2_TestMock",
                    "collection": "COPERNICUS/S2_SR_HARMONIZED",
                    "processing_date": datetime.now(timezone.utc).isoformat(),
                    "geometry_hash": geom_hash,
                    "algorithm_version": ALGORITHM_VERSION,
                }
                for point in calcular_ndvi_simulado(0.0, 0.0)
            ]
            return {
                "status": "success",
                "gee_connected": False,
                "geometry_hash": geom_hash,
                "total_observations": len(observations),
                "gee_initialization_ms": init_ms,
                "gee_query_ms": 0,
                "observations": observations,
            }

        failure = init_result.failure or GeeFailureInfo(
            category=GeeFailureCategory.UNKNOWN,
            error_code="gee_execution_failed",
            detail_message=f"Servicio satelital GEE no disponible: {gee_msg}",
        )
        return _build_gee_error_result(
            gee_connected=False,
            geometry_hash=geom_hash,
            gee_initialization_ms=init_ms,
            gee_query_ms=0,
            failure=GeeFailureInfo(
                category=failure.category,
                error_code=failure.error_code,
                detail_message=f"Servicio satelital GEE no disponible: {gee_msg}",
            ),
        )

    try:
        t_query_start = time.time()
        import ee
        from shapely import wkt as shapely_wkt
        from shapely.geometry import mapping

        geom_obj = shapely_wkt.loads(polygon_wkt)
        geojson_geom = mapping(geom_obj)
        ee_polygon = ee.Geometry(geojson_geom)

        s2_collection = (
            ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
            .filterDate(start_date, end_date_str)
            .filterBounds(ee_polygon)
            .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", max_cloud_pct))
        )

        def mask_clouds_scl_and_add_ndvi(img):
            scl = img.select("SCL")
            valid_mask = scl.eq(4).Or(scl.eq(5)).Or(scl.eq(6))
            cloud_aoi_mask = scl.eq(3).Or(scl.eq(8)).Or(scl.eq(9)).Or(scl.eq(10))
            ndvi = img.normalizedDifference(["B8", "B4"]).rename("NDVI")
            ndvi_masked = ndvi.updateMask(valid_mask)

            return (
                img.addBands(ndvi_masked)
                .addBands(valid_mask.rename("VALID_MASK"))
                .addBands(cloud_aoi_mask.rename("CLOUD_AOI_MASK"))
            )

        processed_coll = s2_collection.map(mask_clouds_scl_and_add_ndvi)

        reducer = (
            ee.Reducer.mean()
            .combine(ee.Reducer.minMax(), "", True)
            .combine(ee.Reducer.stdDev(), "", True)
        )

        def extract_parcel_stats(img):
            stats = img.select(["NDVI", "VALID_MASK", "CLOUD_AOI_MASK"]).reduceRegion(
                reducer=reducer,
                geometry=ee_polygon,
                scale=10,
                maxPixels=1e8,
            )
            return ee.Feature(
                None,
                {
                    "date": img.date().format("YYYY-MM-dd"),
                    "ndvi_mean": stats.get("NDVI_mean"),
                    "ndvi_min": stats.get("NDVI_min"),
                    "ndvi_max": stats.get("NDVI_max"),
                    "ndvi_std": stats.get("NDVI_stdDev"),
                    "valid_pixel_pct": stats.get("VALID_MASK_mean"),
                    "cloud_aoi_pct": stats.get("CLOUD_AOI_MASK_mean"),
                    "scene_cloud_pct": img.get("CLOUDY_PIXEL_PERCENTAGE"),
                },
            )

        feat_list = (
            processed_coll.map(extract_parcel_stats)
            .filter(ee.Filter.notNull(["ndvi_mean"]))
            .getInfo()
        )

        observations = []
        for feat in feat_list.get("features", []):
            props = feat.get("properties", {})
            val_mean = props.get("ndvi_mean")
            if val_mean is not None:
                valid_pct = float(props.get("valid_pixel_pct", 1.0) or 0.0) * 100.0
                aoi_cloud_pct = float(props.get("cloud_aoi_pct", 0.0) or 0.0) * 100.0
                observations.append(
                    {
                        "observation_date": props.get("date"),
                        "ndvi_mean": round(float(val_mean), 4),
                        "ndvi_min": round(float(props.get("ndvi_min", val_mean)), 4),
                        "ndvi_max": round(float(props.get("ndvi_max", val_mean)), 4),
                        "ndvi_std": round(float(props.get("ndvi_std", 0.0) or 0.0), 4),
                        "scene_cloud_percentage": round(
                            float(props.get("scene_cloud_pct", 0.0)),
                            2,
                        ),
                        "aoi_cloud_percentage": round(aoi_cloud_pct, 2),
                        "valid_pixel_percentage": round(valid_pct, 2),
                        "satellite": "Sentinel-2",
                        "collection": "COPERNICUS/S2_SR_HARMONIZED",
                        "processing_date": datetime.now(timezone.utc).isoformat(),
                        "geometry_hash": geom_hash,
                        "algorithm_version": ALGORITHM_VERSION,
                    }
                )

        query_ms = int((time.time() - t_query_start) * 1000)
        return {
            "status": "success",
            "gee_connected": True,
            "geometry_hash": geom_hash,
            "total_observations": len(observations),
            "gee_initialization_ms": init_ms,
            "gee_query_ms": query_ms,
            "observations": observations,
        }
    except Exception as exc:
        query_ms = int((time.time() - t_start) * 1000)
        failure = _classify_gee_exception(exc)
        return _build_gee_error_result(
            gee_connected=True,
            geometry_hash=geom_hash,
            gee_initialization_ms=init_ms,
            gee_query_ms=query_ms,
            failure=GeeFailureInfo(
                category=failure.category,
                error_code=failure.error_code,
                detail_message=(
                    "Error procesando satelite en GEE: "
                    f"{failure.detail_message}"
                ),
            ),
        )
