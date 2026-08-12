"""Router REST B2B de Telemetria Satelital, Procesamiento Incremental y Cache Redis."""
from __future__ import annotations

import time
from datetime import date, datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Path, Request, status
from fastapi.responses import JSONResponse
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)
from sqlalchemy import select

from litoral_trace.api.auth import UserTenantContext
from litoral_trace.auth.rbac import Permission, require_permission
from litoral_trace.config import get_settings
from litoral_trace.db.tenant import (
    get_tenant_scoped_db_session,
    set_tenant_db_context,
)
from litoral_trace.services.audit import (
    AuditAction,
    AuditOutcome,
    build_audit_actor_from_user,
    build_request_audit_context,
    record_audit_event,
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
from litoral_trace.services.satellite_jobs import (
    SatelliteJobIdempotencyConflictError,
    SatelliteJobLoteNotFoundError,
    build_satellite_job_status_view,
    enqueue_satellite_ndvi_job_in_session,
    get_satellite_job,
)
from litoral_trace.services.satellite_job_results import get_satellite_job_result

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


class SatelliteJobSubmitRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    lote_id: int = Field(gt=0)
    start_date: date
    end_date: date
    max_cloud_pct: float = Field(default=20.0, ge=0.0, le=100.0)
    idempotency_key: str | None = Field(default=None, max_length=255)

    @field_validator("idempotency_key", mode="before")
    @classmethod
    def _normalize_idempotency_key(cls, value: object) -> object:
        if value is None:
            return None
        normalized_value = str(value).strip()
        if not normalized_value:
            raise ValueError(
                "idempotency_key no puede ser vacio si se proporciona."
            )
        return normalized_value

    @model_validator(mode="after")
    def _validate_date_range(self) -> "SatelliteJobSubmitRequest":
        if self.start_date > self.end_date:
            raise ValueError(
                "start_date no puede ser posterior a end_date."
            )
        return self


class SatelliteJobSubmitResponse(BaseModel):
    job_id: int
    job_type: str
    status: str
    created_at: datetime
    next_attempt_at: datetime


class SatelliteJobStatusResponse(BaseModel):
    job_id: int
    lote_id: int
    job_type: str
    status: str
    attempt_count: int
    max_attempts: int
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    next_attempt_at: datetime
    error_code: str | None


class SatelliteJobResultObservationResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    observation_date: date
    ndvi_mean: float
    ndvi_min: float | None
    ndvi_max: float | None
    ndvi_std: float | None
    scene_cloud_percentage: float
    aoi_cloud_percentage: float | None
    valid_pixel_count: int | None
    valid_pixel_percentage: float | None
    satellite: str
    collection: str
    processing_date: datetime | None


class SatelliteJobResultResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: str
    job_id: int
    lote_id: int
    geometry_hash: str
    algorithm_version: str
    total_observations: int = Field(ge=0)
    observations: list[SatelliteJobResultObservationResponse]
    created_at: datetime

    @model_validator(mode="after")
    def _validate_observation_count(self) -> "SatelliteJobResultResponse":
        if self.total_observations != len(self.observations):
            raise ValueError("total_observations no coincide con observations.")
        return self


class SatelliteJobResultPendingResponse(BaseModel):
    job_id: int
    status: str
    detail: str
    next_attempt_at: datetime


class SatelliteJobResultFailedResponse(BaseModel):
    job_id: int
    status: str
    error_code: str
    detail: str


def _build_satellite_job_submit_response(
    *,
    job_id: int,
    job_type: str,
    status_value: str,
    created_at: datetime,
    next_attempt_at: datetime,
) -> SatelliteJobSubmitResponse:
    return SatelliteJobSubmitResponse(
        job_id=job_id,
        job_type=job_type,
        status=status_value,
        created_at=created_at,
        next_attempt_at=next_attempt_at,
    )


def _build_satellite_job_status_response(
    job,
) -> SatelliteJobStatusResponse:
    view = build_satellite_job_status_view(job)
    return SatelliteJobStatusResponse(
        job_id=view.job_id,
        lote_id=view.lote_id,
        job_type=view.job_type,
        status=view.status,
        attempt_count=view.attempt_count,
        max_attempts=view.max_attempts,
        created_at=view.created_at,
        updated_at=view.updated_at,
        started_at=view.started_at,
        finished_at=view.finished_at,
        next_attempt_at=view.next_attempt_at,
        error_code=view.error_code,
    )


def _build_satellite_job_result_response(
    *,
    job,
    result_row,
) -> SatelliteJobResultResponse:
    payload = SatelliteJobResultResponse.model_validate(
        {
            **result_row.result_payload,
            "created_at": result_row.created_at,
        }
    )
    if (
        payload.job_id != int(job.id)
        or payload.lote_id != int(job.lote_id)
        or payload.schema_version != result_row.result_schema_version
        or payload.geometry_hash != result_row.geometry_hash
        or payload.algorithm_version != result_row.algorithm_version
    ):
        raise ValueError("Snapshot satelital inconsistente.")
    return payload


def _record_satellite_job_submit_failure_audit(
    *,
    user: UserTenantContext,
    request_context,
    lote_id: int,
    start_date: date,
    end_date: date,
    max_cloud_pct: float,
    outcome: AuditOutcome,
    detail: str,
    entity_id: int | None = None,
) -> None:
    record_audit_event_now(
        actor=build_audit_actor_from_user(user),
        action=AuditAction.SATELLITE_JOB_SUBMIT,
        entity_type="satellite_job",
        entity_id=entity_id,
        outcome=outcome,
        request_context=request_context,
        metadata={
            "lote_id": lote_id,
            "job_type": "ndvi_timeseries",
            "created": False,
            "replayed": False,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "max_cloud_pct": max_cloud_pct,
        },
        detail=detail,
        best_effort=True,
    )


@router.post(
    "/jobs",
    response_model=SatelliteJobSubmitResponse,
    responses={
        status.HTTP_200_OK: {
            "description": "Idempotent replay of an existing durable job.",
            "headers": {
                "Location": {
                    "description": "Canonical resource path for the durable satellite job.",
                    "schema": {"type": "string"},
                }
            },
        },
        status.HTTP_202_ACCEPTED: {
            "description": "New durable satellite job accepted.",
            "model": SatelliteJobSubmitResponse,
            "headers": {
                "Location": {
                    "description": "Canonical resource path for the durable satellite job.",
                    "schema": {"type": "string"},
                }
            },
        },
        status.HTTP_401_UNAUTHORIZED: {
            "description": "Authentication required or invalid session."
        },
        status.HTTP_403_FORBIDDEN: {
            "description": "Authenticated user lacks satellite run capability."
        },
        status.HTTP_404_NOT_FOUND: {
            "description": "Lote no encontrado."
        },
        status.HTTP_409_CONFLICT: {
            "description": "The idempotency key was already used for a different effective payload."
        },
    },
)
async def submit_satellite_job_endpoint(
    payload: SatelliteJobSubmitRequest,
    request: Request = None,
    user: UserTenantContext = Depends(require_permission(Permission.SATELLITE_RUN)),
) -> JSONResponse:
    request_context = build_request_audit_context(request)
    session = get_tenant_scoped_db_session(user.organization_id)

    if session is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Servicio de base de datos no disponible.",
        )

    try:
        job, created = enqueue_satellite_ndvi_job_in_session(
            session,
            organization_id=user.organization_id,
            lote_id=payload.lote_id,
            start_date=payload.start_date,
            end_date=payload.end_date,
            max_cloud_pct=payload.max_cloud_pct,
            idempotency_key=payload.idempotency_key,
        )
        record_audit_event(
            session,
            actor=build_audit_actor_from_user(user),
            action=AuditAction.SATELLITE_JOB_SUBMIT,
            entity_type="satellite_job",
            entity_id=job.id,
            outcome=AuditOutcome.SUCCESS,
            request_context=request_context,
            metadata={
                "lote_id": payload.lote_id,
                "job_type": job.job_type,
                "created": created,
                "replayed": not created,
                "start_date": payload.start_date.isoformat(),
                "end_date": payload.end_date.isoformat(),
                "max_cloud_pct": payload.max_cloud_pct,
            },
        )
        session.commit()
        set_tenant_db_context(session, user.organization_id)
        session.refresh(job)

        response_payload = _build_satellite_job_submit_response(
            job_id=int(job.id),
            job_type=str(job.job_type),
            status_value=str(job.status),
            created_at=job.created_at,
            next_attempt_at=job.next_attempt_at,
        )
        response = JSONResponse(
            status_code=(
                status.HTTP_202_ACCEPTED
                if created
                else status.HTTP_200_OK
            ),
            content=response_payload.model_dump(mode="json"),
        )
        response.headers["Location"] = (
            f"/api/v1/satellite/jobs/{job.id}"
        )
        return response
    except SatelliteJobLoteNotFoundError as exc:
        session.rollback()
        _record_satellite_job_submit_failure_audit(
            user=user,
            request_context=request_context,
            lote_id=payload.lote_id,
            start_date=payload.start_date,
            end_date=payload.end_date,
            max_cloud_pct=payload.max_cloud_pct,
            outcome=AuditOutcome.DENIED,
            detail="Lote no encontrado.",
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Lote no encontrado.",
        ) from exc
    except SatelliteJobIdempotencyConflictError as exc:
        session.rollback()
        _record_satellite_job_submit_failure_audit(
            user=user,
            request_context=request_context,
            lote_id=payload.lote_id,
            start_date=payload.start_date,
            end_date=payload.end_date,
            max_cloud_pct=payload.max_cloud_pct,
            outcome=AuditOutcome.FAILURE,
            detail="Conflicto de idempotencia satelital.",
            entity_id=exc.existing_job_id,
        )
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(exc),
        ) from exc
    except HTTPException:
        session.rollback()
        raise
    except ValueError as exc:
        session.rollback()
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc
    except Exception:
        session.rollback()
        _record_satellite_job_submit_failure_audit(
            user=user,
            request_context=request_context,
            lote_id=payload.lote_id,
            start_date=payload.start_date,
            end_date=payload.end_date,
            max_cloud_pct=payload.max_cloud_pct,
            outcome=AuditOutcome.FAILURE,
            detail="No fue posible registrar el satellite job.",
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="No fue posible registrar el satellite job.",
        )
    finally:
        session.close()


@router.get(
    "/jobs/{job_id}",
    response_model=SatelliteJobStatusResponse,
    responses={
        status.HTTP_401_UNAUTHORIZED: {
            "description": "Authentication required or invalid session."
        },
        status.HTTP_403_FORBIDDEN: {
            "description": "Authenticated user lacks satellite run capability."
        },
        status.HTTP_404_NOT_FOUND: {
            "description": "Satellite job no encontrado."
        },
        status.HTTP_422_UNPROCESSABLE_ENTITY: {
            "description": "Invalid satellite job identifier."
        },
    },
)
async def get_satellite_job_status_endpoint(
    job_id: int = Path(gt=0),
    user: UserTenantContext = Depends(require_permission(Permission.SATELLITE_RUN)),
) -> SatelliteJobStatusResponse:
    """Return the authenticated tenant's public-safe durable job status."""
    try:
        job = get_satellite_job(
            organization_id=user.organization_id,
            job_id=job_id,
        )
    except RuntimeError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Servicio de base de datos no disponible.",
        ) from exc

    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Satellite job no encontrado.",
        )

    return _build_satellite_job_status_response(job)


@router.get(
    "/jobs/{job_id}/result",
    response_model=SatelliteJobResultResponse,
    responses={
        status.HTTP_401_UNAUTHORIZED: {
            "description": "Authentication required or invalid session."
        },
        status.HTTP_403_FORBIDDEN: {
            "description": "Authenticated user lacks satellite run capability."
        },
        status.HTTP_404_NOT_FOUND: {
            "description": "Satellite job no encontrado."
        },
        status.HTTP_409_CONFLICT: {
            "description": (
                "Satellite job result is not yet available because the job is "
                "queued or running, or the job failed with a safe error code."
            ),
            "model": (
                SatelliteJobResultPendingResponse
                | SatelliteJobResultFailedResponse
            ),
        },
        status.HTTP_422_UNPROCESSABLE_ENTITY: {
            "description": "Invalid satellite job identifier."
        },
        status.HTTP_500_INTERNAL_SERVER_ERROR: {
            "description": "Persisted satellite result is missing or inconsistent."
        },
    },
)
async def get_satellite_job_result_endpoint(
    job_id: int = Path(gt=0),
    user: UserTenantContext = Depends(require_permission(Permission.SATELLITE_RUN)),
):
    """Return a validated immutable result for the authenticated tenant."""
    try:
        job = get_satellite_job(
            organization_id=user.organization_id,
            job_id=job_id,
        )
        if job is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Satellite job no encontrado.",
            )

        if job.status in {"queued", "running"}:
            pending = SatelliteJobResultPendingResponse(
                job_id=int(job.id),
                status=str(job.status),
                detail="El resultado del satellite job aun no esta disponible.",
                next_attempt_at=job.next_attempt_at,
            )
            return JSONResponse(
                status_code=status.HTTP_409_CONFLICT,
                content=pending.model_dump(mode="json"),
            )

        if job.status == "failed":
            failed = SatelliteJobResultFailedResponse(
                job_id=int(job.id),
                status="failed",
                error_code=(job.error_code or "satellite_job_failed"),
                detail="El satellite job finalizo sin un resultado disponible.",
            )
            return JSONResponse(
                status_code=status.HTTP_409_CONFLICT,
                content=failed.model_dump(mode="json"),
            )

        if job.status != "succeeded":
            raise ValueError("Estado persistido de satellite job no soportado.")

        result_row = get_satellite_job_result(
            organization_id=user.organization_id,
            job_id=job_id,
        )
        if result_row is None:
            raise ValueError("Satellite job succeeded sin snapshot persistido.")

        return _build_satellite_job_result_response(
            job=job,
            result_row=result_row,
        )
    except HTTPException:
        raise
    except RuntimeError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Servicio de base de datos no disponible.",
        ) from exc
    except (ValidationError, ValueError, TypeError) as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="El resultado persistido del satellite job no esta disponible.",
        ) from exc


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
