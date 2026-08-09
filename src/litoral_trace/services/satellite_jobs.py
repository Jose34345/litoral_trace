"""Minimal durable satellite job domain services."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from enum import StrEnum
from typing import Any
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from litoral_trace.db.models import Lote, SatelliteJob
from litoral_trace.db.engine import get_db_session
from litoral_trace.db.tenant import get_tenant_scoped_db_session
from litoral_trace.services.gee import ALGORITHM_VERSION, generate_geometry_hash


class SatelliteJobType(StrEnum):
    NDVI_TIMESERIES = "ndvi_timeseries"


class SatelliteJobStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


WORKER_CLAIM_NEXT_FUNCTION = "public.worker_claim_next_satellite_job"


@dataclass(frozen=True)
class ClaimedSatelliteJob:
    id: int
    organization_id: int
    lote_id: int | None
    job_type: str
    status: str
    attempt_count: int
    max_attempts: int
    next_attempt_at: datetime
    locked_by: str
    locked_at: datetime
    heartbeat_at: datetime
    lease_token: UUID
    started_at: datetime | None
    request_start_date: date | None
    request_end_date: date | None
    max_cloud_pct: float | None
    geometry_hash: str | None
    algorithm_version: str | None
    polygon_wkt_snapshot: str | None


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


def _normalize_worker_id(worker_id: str) -> str:
    normalized_worker_id = (worker_id or "").strip()
    if not normalized_worker_id:
        raise ValueError("worker_id no puede ser nulo, vacio o whitespace.")
    if len(normalized_worker_id) > 255:
        raise ValueError("worker_id no puede superar 255 caracteres.")
    return normalized_worker_id


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


def _supports_postgresql_worker_claim_function(db_session: Session) -> bool:
    bind = db_session.get_bind()
    return bind is not None and bind.dialect.name == "postgresql"


def _map_claimed_job_row(row: Any) -> ClaimedSatelliteJob:
    lease_token = row["lease_token"]
    if not isinstance(lease_token, UUID):
        lease_token = UUID(str(lease_token))

    return ClaimedSatelliteJob(
        id=int(row["id"]),
        organization_id=int(row["organization_id"]),
        lote_id=int(row["lote_id"]) if row["lote_id"] is not None else None,
        job_type=str(row["job_type"]),
        status=str(row["status"]),
        attempt_count=int(row["attempt_count"]),
        max_attempts=int(row["max_attempts"]),
        next_attempt_at=row["next_attempt_at"],
        locked_by=str(row["locked_by"]),
        locked_at=row["locked_at"],
        heartbeat_at=row["heartbeat_at"],
        lease_token=lease_token,
        started_at=row["started_at"],
        request_start_date=row["request_start_date"],
        request_end_date=row["request_end_date"],
        max_cloud_pct=(
            float(row["max_cloud_pct"]) if row["max_cloud_pct"] is not None else None
        ),
        geometry_hash=(
            str(row["geometry_hash"]) if row["geometry_hash"] is not None else None
        ),
        algorithm_version=(
            str(row["algorithm_version"])
            if row["algorithm_version"] is not None
            else None
        ),
        polygon_wkt_snapshot=(
            str(row["polygon_wkt_snapshot"])
            if row["polygon_wkt_snapshot"] is not None
            else None
        ),
    )


def claim_next_satellite_job(
    *,
    worker_id: str,
    db_session: Session | None = None,
) -> ClaimedSatelliteJob | None:
    normalized_worker_id = _normalize_worker_id(worker_id)
    owns_session = db_session is None
    session = db_session or get_db_session()
    if session is None:
        raise RuntimeError("Servicio de base de datos no disponible.")

    try:
        if not _supports_postgresql_worker_claim_function(session):
            raise RuntimeError(
                "Atomic satellite job claiming requires PostgreSQL worker SQL support."
            )

        row = session.execute(
            text(
                """
                SELECT *
                FROM public.worker_claim_next_satellite_job(
                    :requested_worker_id
                )
                """
            ),
            {
                "requested_worker_id": normalized_worker_id,
            },
        ).mappings().one_or_none()

        if row is None:
            if owns_session:
                session.rollback()
            return None

        claimed_job = _map_claimed_job_row(row)
        if owns_session:
            session.commit()
        return claimed_job
    except Exception:
        if owns_session:
            session.rollback()
        raise
    finally:
        if owns_session:
            session.close()


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
            job.lease_token = None
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
