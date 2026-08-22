"""Shared NDVI normalization and tenant-scoped persistence helpers."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from sqlalchemy import Integer, bindparam, select, text, update
from sqlalchemy.orm import Session

from litoral_trace.db.models import SatelliteJob, SatelliteNdviObservation
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.services.gee import ALGORITHM_VERSION


@dataclass(frozen=True)
class NdviObservationRecord:
    observation_date: date
    ndvi_mean: float
    ndvi_min: float | None
    ndvi_max: float | None
    ndvi_std: float | None
    scene_cloud_percentage: float
    valid_pixel_count: int | None
    valid_pixel_percentage: float | None
    satellite: str
    collection: str
    geometry_hash: str
    algorithm_version: str
    aoi_cloud_percentage: float | None = None
    processing_date: datetime | None = None


@dataclass(frozen=True)
class NormalizedNdviExecutionResult:
    geometry_hash: str
    algorithm_version: str
    observations: tuple[NdviObservationRecord, ...]


@dataclass(frozen=True)
class PersistedNdviResult:
    observation_count: int


@dataclass(frozen=True)
class RetryScheduleResult:
    next_attempt_at: datetime


class SatelliteJobLeaseLostError(RuntimeError):
    """Raised when a worker no longer owns the running lease it is trying to finalize."""

    def __init__(
        self,
        *,
        job_id: int,
        organization_id: int,
        worker_id: str,
        operation: str,
    ):
        super().__init__(
            "Satellite job lease lost before terminal transition."
        )
        self.job_id = int(job_id)
        self.organization_id = int(organization_id)
        self.worker_id = str(worker_id).strip()
        self.operation = str(operation).strip() or "terminal_transition"


def _normalize_observation_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value).strip())


def _normalize_processing_date(value: str | datetime | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        normalized_value = value
    else:
        normalized_value = datetime.fromisoformat(str(value).strip())
    if normalized_value.tzinfo is None:
        normalized_value = normalized_value.replace(tzinfo=timezone.utc)
    return normalized_value.astimezone(timezone.utc)


def _normalize_worker_id(worker_id: str) -> str:
    normalized_worker_id = str(worker_id or "").strip()
    if not normalized_worker_id:
        raise ValueError("worker_id no puede ser nulo, vacio o whitespace.")
    if len(normalized_worker_id) > 255:
        raise ValueError("worker_id no puede superar 255 caracteres.")
    return normalized_worker_id


def _normalize_lease_token(lease_token: str | UUID) -> str:
    if isinstance(lease_token, UUID):
        return str(lease_token)

    normalized_lease_token = str(lease_token or "").strip()
    if not normalized_lease_token:
        raise ValueError("lease_token no puede ser nulo, vacio o whitespace.")
    return str(UUID(normalized_lease_token))


def _normalize_error_code(error_code: str) -> str:
    return error_code.strip()[:100] or "worker_execution_failed"


def _normalize_error_message(error_message: str) -> str:
    return error_message.strip()[:1024] or "worker execution failed"


def _load_satellite_job_for_tenant(
    db_session: Session,
    *,
    organization_id: int,
    job_id: int,
) -> SatelliteJob:
    return db_session.execute(
        select(SatelliteJob).where(
            SatelliteJob.id == int(job_id),
            SatelliteJob.organization_id == int(organization_id),
        )
    ).scalar_one()


def _raise_lease_lost(
    *,
    organization_id: int,
    job_id: int,
    worker_id: str,
    operation: str,
) -> None:
    raise SatelliteJobLeaseLostError(
        job_id=job_id,
        organization_id=organization_id,
        worker_id=worker_id,
        operation=operation,
    )


def _finalize_satellite_job_with_lease_fencing(
    db_session: Session,
    *,
    organization_id: int,
    job_id: int,
    worker_id: str,
    lease_token: str | UUID,
    operation: str,
    values: dict[str, Any],
) -> SatelliteJob:
    normalized_organization_id = int(organization_id)
    normalized_job_id = int(job_id)
    normalized_worker_id = _normalize_worker_id(worker_id)
    normalized_lease_token = _normalize_lease_token(lease_token)

    set_tenant_db_context(
        db_session,
        normalized_organization_id,
    )

    result = db_session.execute(
        update(SatelliteJob)
        .where(
            SatelliteJob.id == normalized_job_id,
            SatelliteJob.organization_id == normalized_organization_id,
            SatelliteJob.status == "running",
            SatelliteJob.locked_by == normalized_worker_id,
            SatelliteJob.lease_token == normalized_lease_token,
        )
        .values(**values)
    )

    if result.rowcount != 1:
        _raise_lease_lost(
            organization_id=normalized_organization_id,
            job_id=normalized_job_id,
            worker_id=normalized_worker_id,
            operation=operation,
        )

    db_session.flush()
    return _load_satellite_job_for_tenant(
        db_session,
        organization_id=normalized_organization_id,
        job_id=normalized_job_id,
    )


def normalize_ndvi_execution_result(
    raw_result: dict[str, Any],
) -> NormalizedNdviExecutionResult:
    observations: list[NdviObservationRecord] = []
    for raw_observation in raw_result.get("observations", []):
        observations.append(
            NdviObservationRecord(
                observation_date=_normalize_observation_date(
                    raw_observation["observation_date"]
                ),
                ndvi_mean=float(raw_observation["ndvi_mean"]),
                ndvi_min=(
                    float(raw_observation["ndvi_min"])
                    if raw_observation.get("ndvi_min") is not None
                    else None
                ),
                ndvi_max=(
                    float(raw_observation["ndvi_max"])
                    if raw_observation.get("ndvi_max") is not None
                    else None
                ),
                ndvi_std=(
                    float(raw_observation["ndvi_std"])
                    if raw_observation.get("ndvi_std") is not None
                    else None
                ),
                scene_cloud_percentage=float(
                    raw_observation.get("scene_cloud_percentage", 0.0)
                ),
                valid_pixel_count=(
                    int(raw_observation["valid_pixel_count"])
                    if raw_observation.get("valid_pixel_count") is not None
                    else None
                ),
                valid_pixel_percentage=(
                    float(raw_observation["valid_pixel_percentage"])
                    if raw_observation.get("valid_pixel_percentage") is not None
                    else None
                ),
                satellite=str(raw_observation.get("satellite", "Sentinel-2")),
                collection=str(
                    raw_observation.get(
                        "collection",
                        "COPERNICUS/S2_SR_HARMONIZED",
                    )
                ),
                geometry_hash=str(raw_observation["geometry_hash"]),
                algorithm_version=str(
                    raw_observation.get("algorithm_version", ALGORITHM_VERSION)
                ),
                aoi_cloud_percentage=(
                    float(raw_observation["aoi_cloud_percentage"])
                    if raw_observation.get("aoi_cloud_percentage") is not None
                    else None
                ),
                processing_date=_normalize_processing_date(
                    raw_observation.get("processing_date")
                ),
            )
        )

    geometry_hash = str(raw_result.get("geometry_hash") or "")
    algorithm_version = str(raw_result.get("algorithm_version") or ALGORITHM_VERSION)
    if not geometry_hash and observations:
        geometry_hash = observations[0].geometry_hash
    if not algorithm_version and observations:
        algorithm_version = observations[0].algorithm_version

    return NormalizedNdviExecutionResult(
        geometry_hash=geometry_hash,
        algorithm_version=algorithm_version,
        observations=tuple(observations),
    )


def _load_existing_observations(
    db_session: Session,
    *,
    organization_id: int,
    lote_id: int,
    geometry_hash: str,
    observation_dates: set[date],
) -> dict[tuple[date, str], SatelliteNdviObservation]:
    if not observation_dates:
        return {}

    rows = db_session.execute(
        select(SatelliteNdviObservation).where(
            SatelliteNdviObservation.organization_id == organization_id,
            SatelliteNdviObservation.lote_id == lote_id,
            SatelliteNdviObservation.geometry_hash == geometry_hash,
            SatelliteNdviObservation.observation_date.in_(observation_dates),
        )
    ).scalars().all()

    return {
        (row.observation_date, row.geometry_hash): row
        for row in rows
    }


def persist_ndvi_execution_result(
    db_session: Session,
    *,
    organization_id: int,
    lote_id: int,
    satellite_job_id: int | None,
    result: NormalizedNdviExecutionResult,
) -> PersistedNdviResult:
    set_tenant_db_context(db_session, organization_id)

    if satellite_job_id is not None:
        stale_rows = db_session.execute(
            select(SatelliteNdviObservation).where(
                SatelliteNdviObservation.organization_id == organization_id,
                SatelliteNdviObservation.satellite_job_id == satellite_job_id,
            )
        ).scalars().all()
        for stale_row in stale_rows:
            db_session.delete(stale_row)
        db_session.flush()

    observation_dates = {observation.observation_date for observation in result.observations}
    existing_rows = _load_existing_observations(
        db_session,
        organization_id=organization_id,
        lote_id=lote_id,
        geometry_hash=result.geometry_hash,
        observation_dates=observation_dates,
    )

    for observation in result.observations:
        observation_key = (
            observation.observation_date,
            observation.geometry_hash,
        )
        existing_row = existing_rows.get(observation_key)
        if existing_row is None:
            existing_row = SatelliteNdviObservation(
                organization_id=organization_id,
                lote_id=lote_id,
                satellite_job_id=satellite_job_id,
                observation_date=observation.observation_date,
                ndvi_mean=observation.ndvi_mean,
                ndvi_min=observation.ndvi_min,
                ndvi_max=observation.ndvi_max,
                ndvi_std=observation.ndvi_std,
                cloud_percentage=observation.scene_cloud_percentage,
                valid_pixel_count=observation.valid_pixel_count,
                valid_pixel_percentage=observation.valid_pixel_percentage,
                satellite=observation.satellite,
                collection=observation.collection,
                geometry_hash=observation.geometry_hash,
                algorithm_version=observation.algorithm_version,
                processing_date=observation.processing_date,
            )
            db_session.add(existing_row)
            # GEE may return multiple Sentinel-2 scenes for one calendar day.
            # The canonical table has one row per date+geometry, so keep pending
            # inserts in the in-memory identity map and let later same-day
            # observations update that row instead of scheduling a duplicate.
            existing_rows[observation_key] = existing_row
            continue

        if satellite_job_id is not None or existing_row.satellite_job_id is None:
            existing_row.satellite_job_id = satellite_job_id
        existing_row.ndvi_mean = observation.ndvi_mean
        existing_row.ndvi_min = observation.ndvi_min
        existing_row.ndvi_max = observation.ndvi_max
        existing_row.ndvi_std = observation.ndvi_std
        existing_row.cloud_percentage = observation.scene_cloud_percentage
        existing_row.valid_pixel_count = observation.valid_pixel_count
        existing_row.valid_pixel_percentage = observation.valid_pixel_percentage
        existing_row.satellite = observation.satellite
        existing_row.collection = observation.collection
        existing_row.algorithm_version = observation.algorithm_version
        if observation.processing_date is not None:
            existing_row.processing_date = observation.processing_date

    db_session.flush()
    return PersistedNdviResult(observation_count=len(result.observations))


def mark_satellite_job_succeeded(
    db_session: Session,
    *,
    organization_id: int,
    job_id: int,
    worker_id: str,
    lease_token: str | UUID,
) -> SatelliteJob:
    now = datetime.now(timezone.utc)
    return _finalize_satellite_job_with_lease_fencing(
        db_session,
        organization_id=organization_id,
        job_id=job_id,
        worker_id=worker_id,
        lease_token=lease_token,
        operation="succeeded",
        values={
            "status": "succeeded",
            "finished_at": now,
            "locked_at": None,
            "locked_by": None,
            "heartbeat_at": None,
            "error_code": None,
            "error_message": None,
            "updated_at": now,
        },
    )


def mark_satellite_job_failed(
    db_session: Session,
    *,
    organization_id: int,
    job_id: int,
    worker_id: str,
    lease_token: str | UUID,
    error_code: str,
    error_message: str,
) -> SatelliteJob:
    now = datetime.now(timezone.utc)
    return _finalize_satellite_job_with_lease_fencing(
        db_session,
        organization_id=organization_id,
        job_id=job_id,
        worker_id=worker_id,
        lease_token=lease_token,
        operation="failed",
        values={
            "status": "failed",
            "finished_at": now,
            "locked_at": None,
            "locked_by": None,
            "heartbeat_at": None,
            "error_code": _normalize_error_code(error_code),
            "error_message": _normalize_error_message(error_message),
            "updated_at": now,
        },
    )


def schedule_satellite_job_retry(
    db_session: Session,
    *,
    organization_id: int,
    job_id: int,
    worker_id: str,
    lease_token: str | UUID,
    retry_delay_seconds: int,
) -> RetryScheduleResult:
    normalized_retry_delay_seconds = int(retry_delay_seconds)
    if normalized_retry_delay_seconds <= 0:
        raise ValueError("retry_delay_seconds debe ser positivo.")

    bind = db_session.get_bind()
    dialect_name = bind.dialect.name if bind is not None else ""

    if dialect_name == "postgresql":
        updated_at_value = text("statement_timestamp()")
        next_attempt_at_value = text(
            "statement_timestamp() + make_interval(secs => :retry_delay_seconds)"
        ).bindparams(
            bindparam(
                "retry_delay_seconds",
                value=normalized_retry_delay_seconds,
                type_=Integer(),
            )
        )
    else:
        fallback_now = datetime.now(timezone.utc)
        next_attempt_at_value = fallback_now + timedelta(
            seconds=normalized_retry_delay_seconds
        )
        updated_at_value = fallback_now

    job = _finalize_satellite_job_with_lease_fencing(
        db_session,
        organization_id=organization_id,
        job_id=job_id,
        worker_id=worker_id,
        lease_token=lease_token,
        operation="retry_scheduled",
        values={
            "status": "queued",
            "locked_at": None,
            "locked_by": None,
            "heartbeat_at": None,
            "lease_token": None,
            "finished_at": None,
            "next_attempt_at": next_attempt_at_value,
            "error_code": None,
            "error_message": None,
            "updated_at": updated_at_value,
        },
    )
    return RetryScheduleResult(next_attempt_at=job.next_attempt_at)


def update_satellite_job_heartbeat(
    db_session: Session,
    *,
    organization_id: int,
    job_id: int,
    worker_id: str,
    lease_token: str | UUID,
) -> datetime:
    now = datetime.now(timezone.utc)
    normalized_organization_id = int(organization_id)
    normalized_job_id = int(job_id)
    normalized_worker_id = _normalize_worker_id(worker_id)
    normalized_lease_token = _normalize_lease_token(lease_token)

    set_tenant_db_context(
        db_session,
        normalized_organization_id,
    )

    result = db_session.execute(
        update(SatelliteJob)
        .where(
            SatelliteJob.id == normalized_job_id,
            SatelliteJob.organization_id == normalized_organization_id,
            SatelliteJob.status == "running",
            SatelliteJob.locked_by == normalized_worker_id,
            SatelliteJob.lease_token == normalized_lease_token,
        )
        .values(
            heartbeat_at=now,
            updated_at=now,
        )
    )

    if result.rowcount != 1:
        _raise_lease_lost(
            organization_id=normalized_organization_id,
            job_id=normalized_job_id,
            worker_id=normalized_worker_id,
            operation="heartbeat",
        )

    db_session.flush()
    return now
