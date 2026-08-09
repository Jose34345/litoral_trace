"""Minimal durable satellite job domain services."""
from __future__ import annotations

from datetime import date, datetime, timezone
from enum import StrEnum
from typing import Any

from sqlalchemy import select

from litoral_trace.db.models import Lote, SatelliteJob
from litoral_trace.db.tenant import get_tenant_scoped_db_session
from litoral_trace.services.gee import ALGORITHM_VERSION, generate_geometry_hash


class SatelliteJobType(StrEnum):
    NDVI_TIMESERIES = "ndvi_timeseries"


class SatelliteJobStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_organization_id(organization_id: int | str) -> int:
    normalized_organization_id = int(organization_id)
    if normalized_organization_id <= 0:
        raise ValueError("organization_id debe ser mayor que cero.")
    return normalized_organization_id


def _normalize_max_attempts(max_attempts: int) -> int:
    normalized_max_attempts = int(max_attempts)
    if normalized_max_attempts <= 0:
        raise ValueError("max_attempts debe ser mayor que cero.")
    return normalized_max_attempts


def _normalize_max_cloud_pct(max_cloud_pct: float) -> float:
    normalized_max_cloud_pct = float(max_cloud_pct)
    if normalized_max_cloud_pct < 0.0 or normalized_max_cloud_pct > 100.0:
        raise ValueError("max_cloud_pct debe estar entre 0 y 100.")
    return normalized_max_cloud_pct


def _normalize_status(status: SatelliteJobStatus | str) -> SatelliteJobStatus:
    try:
        return (
            status
            if isinstance(status, SatelliteJobStatus)
            else SatelliteJobStatus(str(status).strip())
        )
    except ValueError as exc:
        raise ValueError(f"Estado de satellite job no soportado: {status}") from exc


def _parse_iso_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value).strip())


def _build_lote_polygon_snapshot(lote: Lote) -> str:
    if lote.polygon_wkt:
        return lote.polygon_wkt
    return (
        "POLYGON(("
        f"{lote.longitud-0.01} {lote.latitud-0.01}, "
        f"{lote.longitud+0.01} {lote.latitud-0.01}, "
        f"{lote.longitud+0.01} {lote.latitud+0.01}, "
        f"{lote.longitud-0.01} {lote.latitud+0.01}, "
        f"{lote.longitud-0.01} {lote.latitud-0.01}"
        "))"
    )


def _serialize_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def _serialize_date(value: date | None) -> str | None:
    return value.isoformat() if value is not None else None


def serialize_satellite_job(job: SatelliteJob) -> dict[str, Any]:
    """Render a tenant-safe job representation without internal geometry payload."""
    return {
        "id": job.id,
        "organization_id": job.organization_id,
        "lote_id": job.lote_id,
        "job_type": job.job_type,
        "status": job.status,
        "attempt_count": job.attempt_count,
        "max_attempts": job.max_attempts,
        "next_attempt_at": _serialize_datetime(job.next_attempt_at),
        "locked_at": _serialize_datetime(job.locked_at),
        "locked_by": job.locked_by,
        "heartbeat_at": _serialize_datetime(job.heartbeat_at),
        "started_at": _serialize_datetime(job.started_at),
        "finished_at": _serialize_datetime(job.finished_at),
        "error_code": job.error_code,
        "error_message": job.error_message,
        "idempotency_key": job.idempotency_key,
        "request_start_date": _serialize_date(job.request_start_date),
        "request_end_date": _serialize_date(job.request_end_date),
        "max_cloud_pct": job.max_cloud_pct,
        "geometry_hash": job.geometry_hash,
        "algorithm_version": job.algorithm_version,
        "created_at": _serialize_datetime(job.created_at),
        "updated_at": _serialize_datetime(job.updated_at),
    }


def _assert_same_idempotent_payload(
    job: SatelliteJob,
    *,
    lote_id: int,
    request_start_date: date,
    request_end_date: date,
    max_cloud_pct: float,
    polygon_wkt_snapshot: str,
) -> None:
    expected_payload = (
        job.job_type == SatelliteJobType.NDVI_TIMESERIES.value
        and job.lote_id == lote_id
        and job.request_start_date == request_start_date
        and job.request_end_date == request_end_date
        and job.max_cloud_pct == max_cloud_pct
        and job.polygon_wkt_snapshot == polygon_wkt_snapshot
    )
    if not expected_payload:
        raise ValueError(
            "El idempotency_key ya fue utilizado para un payload satelital diferente."
        )


def enqueue_satellite_ndvi_job(
    *,
    organization_id: int | str,
    lote_id: int,
    start_date: str | date,
    end_date: str | date,
    max_cloud_pct: float = 20.0,
    idempotency_key: str | None = None,
    max_attempts: int = 3,
) -> tuple[SatelliteJob, bool]:
    """Create or reuse a durable NDVI job for a tenant-scoped lote."""
    normalized_organization_id = _normalize_organization_id(organization_id)
    normalized_lote_id = int(lote_id)
    normalized_start_date = _parse_iso_date(start_date)
    normalized_end_date = _parse_iso_date(end_date)
    normalized_max_cloud_pct = _normalize_max_cloud_pct(max_cloud_pct)
    normalized_max_attempts = _normalize_max_attempts(max_attempts)
    normalized_idempotency_key = (idempotency_key or "").strip() or None

    if normalized_start_date > normalized_end_date:
        raise ValueError("start_date no puede ser posterior a end_date.")

    session = get_tenant_scoped_db_session(normalized_organization_id)
    if session is None:
        raise RuntimeError("Servicio de base de datos no disponible.")

    try:
        lote = session.execute(
            select(Lote).where(
                Lote.id == normalized_lote_id,
                Lote.organization_id == normalized_organization_id,
            )
        ).scalar_one_or_none()
        if lote is None:
            raise ValueError("El lote indicado no existe para esa organizacion.")

        polygon_wkt_snapshot = _build_lote_polygon_snapshot(lote)
        geometry_hash = generate_geometry_hash(polygon_wkt_snapshot)

        if normalized_idempotency_key is not None:
            existing_job = session.execute(
                select(SatelliteJob).where(
                    SatelliteJob.organization_id == normalized_organization_id,
                    SatelliteJob.idempotency_key == normalized_idempotency_key,
                )
            ).scalar_one_or_none()
            if existing_job is not None:
                _assert_same_idempotent_payload(
                    existing_job,
                    lote_id=normalized_lote_id,
                    request_start_date=normalized_start_date,
                    request_end_date=normalized_end_date,
                    max_cloud_pct=normalized_max_cloud_pct,
                    polygon_wkt_snapshot=polygon_wkt_snapshot,
                )
                session.expunge(existing_job)
                return existing_job, False

        job = SatelliteJob(
            organization_id=normalized_organization_id,
            lote_id=normalized_lote_id,
            job_type=SatelliteJobType.NDVI_TIMESERIES.value,
            status=SatelliteJobStatus.QUEUED.value,
            attempt_count=0,
            max_attempts=normalized_max_attempts,
            next_attempt_at=_utc_now(),
            idempotency_key=normalized_idempotency_key,
            request_start_date=normalized_start_date,
            request_end_date=normalized_end_date,
            max_cloud_pct=normalized_max_cloud_pct,
            geometry_hash=geometry_hash,
            algorithm_version=ALGORITHM_VERSION,
            polygon_wkt_snapshot=polygon_wkt_snapshot,
        )
        session.add(job)
        session.commit()
        session.refresh(job)
        session.expunge(job)
        return job, True
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def get_satellite_job(
    *,
    organization_id: int | str,
    job_id: int,
) -> SatelliteJob | None:
    normalized_organization_id = _normalize_organization_id(organization_id)
    session = get_tenant_scoped_db_session(normalized_organization_id)
    if session is None:
        raise RuntimeError("Servicio de base de datos no disponible.")

    try:
        job = session.execute(
            select(SatelliteJob).where(
                SatelliteJob.id == int(job_id),
                SatelliteJob.organization_id == normalized_organization_id,
            )
        ).scalar_one_or_none()
        if job is not None:
            session.expunge(job)
        return job
    finally:
        session.close()


def update_satellite_job_status(
    *,
    organization_id: int | str,
    job_id: int,
    status: SatelliteJobStatus | str,
    locked_by: str | None = None,
    error_code: str | None = None,
    error_message: str | None = None,
    next_attempt_at: datetime | None = None,
) -> SatelliteJob:
    """Update durable lifecycle state without implementing queue claiming yet."""
    normalized_organization_id = _normalize_organization_id(organization_id)
    normalized_status = _normalize_status(status)
    session = get_tenant_scoped_db_session(normalized_organization_id)
    if session is None:
        raise RuntimeError("Servicio de base de datos no disponible.")

    try:
        job = session.execute(
            select(SatelliteJob).where(
                SatelliteJob.id == int(job_id),
                SatelliteJob.organization_id == normalized_organization_id,
            )
        ).scalar_one_or_none()
        if job is None:
            raise ValueError("Satellite job no encontrado para esa organizacion.")

        now = _utc_now()
        job.status = normalized_status.value

        if normalized_status is SatelliteJobStatus.QUEUED:
            job.locked_at = None
            job.locked_by = None
            job.heartbeat_at = None
            job.error_code = None
            job.error_message = None
            job.next_attempt_at = next_attempt_at or now
        elif normalized_status is SatelliteJobStatus.RUNNING:
            job.attempt_count += 1
            if job.attempt_count > job.max_attempts:
                raise ValueError(
                    "attempt_count no puede superar max_attempts durante RUNNING."
                )
            job.started_at = job.started_at or now
            job.locked_at = now
            job.locked_by = (locked_by or "").strip() or None
            job.heartbeat_at = now
            job.error_code = None
            job.error_message = None
        elif normalized_status is SatelliteJobStatus.SUCCEEDED:
            job.finished_at = now
            job.locked_at = None
            job.locked_by = None
            job.heartbeat_at = None
            job.error_code = None
            job.error_message = None
        elif normalized_status is SatelliteJobStatus.FAILED:
            job.finished_at = now
            job.locked_at = None
            job.locked_by = None
            job.heartbeat_at = None
            job.error_code = (error_code or "").strip() or None
            job.error_message = (error_message or "").strip()[:1024] or None
            if next_attempt_at is not None:
                job.next_attempt_at = next_attempt_at

        session.commit()
        session.refresh(job)
        session.expunge(job)
        return job
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
