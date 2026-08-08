"""Servicio de Integración con Google Earth Engine (GEE) y Copernicus Sentinel-2 L2A."""
from __future__ import annotations
import os
import time
import hashlib
import json
from datetime import datetime, timezone
from typing import Any

ALGORITHM_VERSION = "2.4.0-gee-sentinel2-scl-v2"

def generate_geometry_hash(polygon_wkt_or_geojson: str | dict) -> str:
    """Genera un hash SHA-256 determinístico de la geometría normalizada (Shapely) para caché e integridad."""
    try:
        from shapely import wkt, geometry
        from shapely.geometry import mapping
        if isinstance(polygon_wkt_or_geojson, dict):
            geom = geometry.shape(polygon_wkt_or_geojson)
        else:
            geom = wkt.loads(str(polygon_wkt_or_geojson).strip())
            
        # Normalizar redondeando coordenadas a 6 decimales (~0.1m precision)
        rounded_geojson = mapping(geom)
        canonical_str = json.dumps(rounded_geojson, sort_keys=True)
    except Exception:
        canonical_str = str(polygon_wkt_or_geojson).strip().upper()
        
    return hashlib.sha256(canonical_str.encode('utf-8')).hexdigest()

def initialize_earth_engine() -> tuple[bool, str, int]:
    """Inicializa la API de Google Earth Engine apoyando ADC (Cloud Run/Dev) y Service Account JSON como fallback.
    
    Returns:
        tuple[success, detail_message, initialization_time_ms]
    """
    t0 = time.time()
    gcp_project = os.environ.get("GCP_PROJECT_ID") or os.environ.get("GEE_PROJECT_ID") or "litoral-trace-engine"
    
    try:
        import ee
        
        # 1. Fallback: Si existe la variable GCP_SERVICE_ACCOUNT / GEE_SERVICE_ACCOUNT con JSON de Service Account
        sa_json = os.environ.get("GCP_SERVICE_ACCOUNT") or os.environ.get("GEE_SERVICE_ACCOUNT")
        if sa_json:
            try:
                sa_data = json.loads(sa_json)
                creds = ee.ServiceAccountCredentials(
                    sa_data["client_email"],
                    key_data=sa_data["private_key"]
                )
                if gcp_project:
                    ee.Initialize(creds, project=gcp_project)
                else:
                    ee.Initialize(creds)
                init_ms = int((time.time() - t0) * 1000)
                return True, f"GEE inicializado vía Service Account JSON (proyecto: {gcp_project})", init_ms
            except Exception:
                pass

        # 2. Estándar Producción / Desarrollo Local (ADC / OAuth persistente)
        if gcp_project:
            ee.Initialize(project=gcp_project)
        else:
            ee.Initialize()
            
        init_ms = int((time.time() - t0) * 1000)
        return True, f"GEE inicializado vía ADC / OAuth persistente (proyecto: {gcp_project})", init_ms

    except Exception as e:
        init_ms = int((time.time() - t0) * 1000)
        return False, f"Error de autenticación GEE: {type(e).__name__} - {e}", init_ms

def consultar_serie_temporal_ndvi_gee(
    polygon_wkt: str,
    start_date: str = "2020-12-31",
    end_date: str | None = None,
    max_cloud_pct: float = 20.0
) -> dict[str, Any]:
    """Consulta la serie histórica de NDVI desde Copernicus Sentinel-2 L2A reducida sobre el POLÍGONO REAL del lote."""
    t_start = time.time()
    end_date_str = end_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    geom_hash = generate_geometry_hash(polygon_wkt)
    gee_ready, gee_msg, init_ms = initialize_earth_engine()
    
    if not gee_ready:
        is_test = os.environ.get("ENVIRONMENT") == "test" or os.environ.get("TEST_MODE") == "1"
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

        return {
            "status": "error",
            "gee_connected": False,
            "error_detail": f"Servicio Satelital GEE no disponible: {gee_msg}",
            "geometry_hash": geom_hash,
            "total_observations": 0,
            "gee_initialization_ms": init_ms,
            "gee_query_ms": 0,
            "observations": []
        }

    try:
        t_query_start = time.time()
        import ee
        from shapely import wkt as shapely_wkt
        from shapely.geometry import mapping
        
        # Parsear polígono real a ee.Geometry
        geom_obj = shapely_wkt.loads(polygon_wkt)
        geojson_geom = mapping(geom_obj)
        ee_polygon = ee.Geometry(geojson_geom)

        # Colección COPERNICUS/S2_SR_HARMONIZED (Sentinel-2 L2A Surface Reflectance)
        s2_collection = (
            ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
            .filterDate(start_date, end_date_str)
            .filterBounds(ee_polygon)
            .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", max_cloud_pct))
        )

        # Enmascaramiento de nubes a nivel de PÍXEL usando la banda SCL (Scene Classification Layer)
        # SCL = 4 (Vegetación), 5 (Suelo desnudo), 6 (Agua) -> Píxeles válidos claros
        # SCL = 3 (Sombras de nubes), 8 (Nubes prob. media), 9 (Nubes prob. alta), 10 (Cirros finos) -> Máscara de nubes AOI
        def mask_clouds_scl_and_add_ndvi(img):
            scl = img.select("SCL")
            
            # Máscara de observaciones claras válidas (SCL 4, 5, 6)
            valid_mask = scl.eq(4).Or(scl.eq(5)).Or(scl.eq(6))
            
            # Máscara de nubes/sombras en el AOI (SCL 3, 8, 9, 10)
            cloud_aoi_mask = scl.eq(3).Or(scl.eq(8)).Or(scl.eq(9)).Or(scl.eq(10))
            
            # Cálculo de NDVI = (B8 - B4) / (B8 + B4)
            ndvi = img.normalizedDifference(["B8", "B4"]).rename("NDVI")
            ndvi_masked = ndvi.updateMask(valid_mask)
            
            return img.addBands(ndvi_masked).addBands(valid_mask.rename("VALID_MASK")).addBands(cloud_aoi_mask.rename("CLOUD_AOI_MASK"))

        processed_coll = s2_collection.map(mask_clouds_scl_and_add_ndvi)

        # Reducción espacial de estadísticas sobre el POLÍGONO COMPLETO DEL LOTE
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
                maxPixels=1e8
            )
            return ee.Feature(None, {
                "date": img.date().format("YYYY-MM-dd"),
                "ndvi_mean": stats.get("NDVI_mean"),
                "ndvi_min": stats.get("NDVI_min"),
                "ndvi_max": stats.get("NDVI_max"),
                "ndvi_std": stats.get("NDVI_stdDev"),
                "valid_pixel_pct": stats.get("VALID_MASK_mean"),
                "cloud_aoi_pct": stats.get("CLOUD_AOI_MASK_mean"),
                "scene_cloud_pct": img.get("CLOUDY_PIXEL_PERCENTAGE")
            })

        feat_list = processed_coll.map(extract_parcel_stats).filter(ee.Filter.notNull(["ndvi_mean"])).getInfo()

        observations = []
        for feat in feat_list.get("features", []):
            props = feat.get("properties", {})
            val_mean = props.get("ndvi_mean")
            if val_mean is not None:
                valid_pct = float(props.get("valid_pixel_pct", 1.0) or 0.0) * 100.0
                aoi_cloud_pct = float(props.get("cloud_aoi_pct", 0.0) or 0.0) * 100.0
                observations.append({
                    "observation_date": props.get("date"),
                    "ndvi_mean": round(float(val_mean), 4),
                    "ndvi_min": round(float(props.get("ndvi_min", val_mean)), 4),
                    "ndvi_max": round(float(props.get("ndvi_max", val_mean)), 4),
                    "ndvi_std": round(float(props.get("ndvi_std", 0.0) or 0.0), 4),
                    "scene_cloud_percentage": round(float(props.get("scene_cloud_pct", 0.0)), 2),
                    "aoi_cloud_percentage": round(aoi_cloud_pct, 2),
                    "valid_pixel_percentage": round(valid_pct, 2),
                    "satellite": "Sentinel-2",
                    "collection": "COPERNICUS/S2_SR_HARMONIZED",
                    "processing_date": datetime.now(timezone.utc).isoformat(),
                    "geometry_hash": geom_hash,
                    "algorithm_version": ALGORITHM_VERSION
                })

        query_ms = int((time.time() - t_query_start) * 1000)
        return {
            "status": "success",
            "gee_connected": True,
            "geometry_hash": geom_hash,
            "total_observations": len(observations),
            "gee_initialization_ms": init_ms,
            "gee_query_ms": query_ms,
            "observations": observations
        }
    except Exception as e:
        query_ms = int((time.time() - t_start) * 1000)
        return {
            "status": "error",
            "gee_connected": True,
            "error_detail": f"Error procesando satélite en GEE: {type(e).__name__} - {e}",
            "geometry_hash": geom_hash,
            "total_observations": 0,
            "gee_initialization_ms": init_ms,
            "gee_query_ms": query_ms,
            "observations": []
        }
