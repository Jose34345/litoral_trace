"""Router REST B2B de Telemetria Satelital, Procesamiento Incremental y Cache Redis."""
from __future__ import annotations

import time
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from sqlalchemy import select

from litoral_trace.api.auth import UserTenantContext
from litoral_trace.auth.rbac import Permission, require_permission
from litoral_trace.config import get_settings
from litoral_trace.db.tenant import get_tenant_scoped_db_session
from litoral_trace.services.audit import (
    AuditAction,
    AuditOutcome,
    build_audit_actor_from_user,
    build_request_audit_context,
    record_audit_event_now,
)
from litoral_trace.services.cache import (
    build_ndvi_cache_key,
    get_cached_satellite_data,
    set_cached_satellite_data,
)
from litoral_trace.services.gee import (
    ALGORITHM_VERSION,
    consultar_serie_temporal_ndvi_gee,
    generate_geometry_hash,
)
from litoral_trace.services.satellite_ndvi_processing import (
    normalize_ndvi_execution_result,
    persist_ndvi_execution_result,
)
from litoral_trace.services.ndvi import (
    calcular_ndvi_simulado,
    evaluar_indicador_variacion_biomasa,
)

router = APIRouter(prefix="/api/v1/satellite", tags=["Telemetria Satelital GEE"])


class SatelliteQueryByLoteRequest(BaseModel):
    lote_id: int = Field(
        ...,
        description="ID del lote en PostgreSQL",
        json_schema_extra={"example": 101},
    )
    start_date: str = Field(
        default="2020-12-31",
        json_schema_extra={"example": "2020-12-31"},
    )
    end_date: str | None = Field(
        default=None,
        json_schema_extra={"example": "2026-08-01"},
    )
    max_cloud_pct: float = Field(default=20.0, ge=0.0, le=100.0)
    force_refresh: bool = Field(default=False)


def _get_tenant_lote_geometry(
    *,
    lote_id: int,
    user: UserTenantContext,
) -> tuple[str, float, float]:
    from litoral_trace.db.models import Lote

    session = get_tenant_scoped_db_session(user.organization_id)
    if session is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Servicio de base de datos no disponible.",
        )

    try:
        lote = session.execute(
            select(Lote).where(
                Lote.id == lote_id,
                Lote.organization_id == user.organization_id,
            )
        ).scalar_one_or_none()

        if lote is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Lote no encontrado.",
            )

        polygon_wkt = lote.polygon_wkt or (
            f"POLYGON(("
            f"{lote.longitud-0.01} {lote.latitud-0.01}, "
            f"{lote.longitud+0.01} {lote.latitud-0.01}, "
            f"{lote.longitud+0.01} {lote.latitud+0.01}, "
            f"{lote.longitud-0.01} {lote.latitud+0.01}, "
            f"{lote.longitud-0.01} {lote.latitud-0.01}"
            f"))"
        )
        return polygon_wkt, lote.latitud, lote.longitud
    finally:
        session.close()


@router.post("/ndvi", tags=["Telemetria Satelital GEE"])
async def consultar_ndvi_satelital_lote_endpoint(
    payload: SatelliteQueryByLoteRequest,
    request: Request = None,
    user: UserTenantContext = Depends(require_permission(Permission.SATELLITE_RUN)),
) -> JSONResponse:
    """Consulta la serie temporal NDVI para un lote tenant-scoped."""
    t_start = time.time()
    end_date_str = payload.end_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    request_context = build_request_audit_context(request)

    try:
        polygon_wkt, lat, lon = _get_tenant_lote_geometry(
            lote_id=payload.lote_id,
            user=user,
        )

        geom_hash = generate_geometry_hash(polygon_wkt)
        cache_key = build_ndvi_cache_key(
            org_id=user.organization_id,
            lote_id=payload.lote_id,
            geometry_hash=geom_hash,
            start_date=payload.start_date,
            end_date=end_date_str,
            cloud_threshold=payload.max_cloud_pct,
            algorithm_version=ALGORITHM_VERSION,
        )

        if not payload.force_refresh:
            cached_res, redis_read_ms = get_cached_satellite_data(cache_key)
            if cached_res:
                total_ms = int((time.time() - t_start) * 1000)
                cached_res["metrics"]["redis_read_ms"] = redis_read_ms
                cached_res["metrics"]["total_processing_ms"] = total_ms
                cached_res["cache_hit"] = True
                cached_res["source"] = "redis_cache"
                record_audit_event_now(
                    actor=build_audit_actor_from_user(user),
                    action=AuditAction.SATELLITE_NDVI_RUN,
                    entity_type="satellite_ndvi_run",
                    entity_id=payload.lote_id,
                    outcome=AuditOutcome.SUCCESS,
                    request_context=request_context,
                    metadata={
                        "cache_hit": True,
                        "start_date": payload.start_date,
                        "end_date": end_date_str,
                        "force_refresh": payload.force_refresh,
                    },
                    best_effort=True,
                )
                return JSONResponse(status_code=status.HTTP_200_OK, content=cached_res)
        else:
            redis_read_ms = 0

        result_gee = consultar_serie_temporal_ndvi_gee(
            polygon_wkt=polygon_wkt,
            start_date=payload.start_date,
            end_date=end_date_str,
            max_cloud_pct=payload.max_cloud_pct,
        )

        obs_list = result_gee.get("observations", [])

        if result_gee.get("status") == "error" and not obs_list:
            settings = get_settings()
            is_test = settings.is_test or settings.gee.test_mode
            if not is_test and not result_gee.get("gee_connected"):
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail=result_gee.get(
                        "error_detail",
                        (
                            "Servicio satelital GEE no disponible en este entorno. "
                            "Se requieren credenciales GCP Service Account."
                        ),
                    ),
                )

            sim_puntos = calcular_ndvi_simulado(lat, lon)
            obs_list = [
                {
                    "observation_date": p["fecha"],
                    "ndvi_mean": float(p["ndvi"]),
                    "scene_cloud_percentage": 5.0,
                    "aoi_cloud_percentage": 1.0,
                    "valid_pixel_percentage": 98.0,
                    "satellite": "Sentinel-2_TestMock",
                    "collection": "COPERNICUS/S2_SR_HARMONIZED",
                    "processing_date": datetime.now(timezone.utc).isoformat(),
                    "geometry_hash": geom_hash,
                    "algorithm_version": ALGORITHM_VERSION,
                }
                for p in sim_puntos
            ]

        t_db_start = time.time()
        db_write_ms = 0
        try:
            session = get_tenant_scoped_db_session(user.organization_id)
            if session:
                try:
                    persist_ndvi_execution_result(
                        session,
                        organization_id=user.organization_id,
                        lote_id=payload.lote_id,
                        satellite_job_id=None,
                        result=normalize_ndvi_execution_result(
                            {
                                "geometry_hash": geom_hash,
                                "algorithm_version": ALGORITHM_VERSION,
                                "observations": obs_list,
                            }
                        ),
                    )
                    session.commit()
                    db_write_ms = int((time.time() - t_db_start) * 1000)
                except Exception:
                    session.rollback()
                finally:
                    session.close()
        except Exception:
            pass

        eudr_eval = evaluar_indicador_variacion_biomasa(obs_list)
        _, redis_write_ms = set_cached_satellite_data(cache_key, {}, ttl_seconds=86400)

        total_ms = int((time.time() - t_start) * 1000)
        response_payload = {
            "status": "success",
            "lote_id": payload.lote_id,
            "organization_id": user.organization_id,
            "geometry_hash": geom_hash,
            "algorithm_version": ALGORITHM_VERSION,
            "gee_connected": result_gee.get("gee_connected", False),
            "source": (
                "earth_engine"
                if result_gee.get("gee_connected")
                else "unauthenticated_test_mode"
            ),
            "cache_hit": False,
            "metrics": {
                "gee_initialization_ms": result_gee.get("gee_initialization_ms", 0),
                "gee_query_ms": result_gee.get("gee_query_ms", 0),
                "database_write_ms": db_write_ms,
                "redis_read_ms": redis_read_ms,
                "redis_write_ms": redis_write_ms,
                "total_processing_ms": total_ms,
            },
            "eudr_vegetation_analysis": eudr_eval,
            "total_observations": len(obs_list),
            "last_observation_date": obs_list[-1]["observation_date"] if obs_list else None,
            "observations": obs_list,
        }

        set_cached_satellite_data(cache_key, response_payload)
        record_audit_event_now(
            actor=build_audit_actor_from_user(user),
            action=AuditAction.SATELLITE_NDVI_RUN,
            entity_type="satellite_ndvi_run",
            entity_id=payload.lote_id,
            outcome=AuditOutcome.SUCCESS,
            request_context=request_context,
            metadata={
                "cache_hit": False,
                "gee_connected": result_gee.get("gee_connected", False),
                "total_observations": len(obs_list),
                "start_date": payload.start_date,
                "end_date": end_date_str,
                "force_refresh": payload.force_refresh,
            },
            best_effort=True,
        )

        return JSONResponse(status_code=status.HTTP_200_OK, content=response_payload)
    except HTTPException as exc:
        record_audit_event_now(
            actor=build_audit_actor_from_user(user),
            action=AuditAction.SATELLITE_NDVI_RUN,
            entity_type="satellite_ndvi_run",
            entity_id=payload.lote_id,
            outcome=AuditOutcome.FAILURE,
            request_context=request_context,
            metadata={
                "status_code": exc.status_code,
                "start_date": payload.start_date,
                "end_date": end_date_str,
                "force_refresh": payload.force_refresh,
            },
            best_effort=True,
        )
        raise
    except Exception:
        record_audit_event_now(
            actor=build_audit_actor_from_user(user),
            action=AuditAction.SATELLITE_NDVI_RUN,
            entity_type="satellite_ndvi_run",
            entity_id=payload.lote_id,
            outcome=AuditOutcome.FAILURE,
            request_context=request_context,
            metadata={
                "status_code": status.HTTP_500_INTERNAL_SERVER_ERROR,
                "start_date": payload.start_date,
                "end_date": end_date_str,
                "force_refresh": payload.force_refresh,
            },
            best_effort=True,
        )
        raise
