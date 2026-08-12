"""Immutable per-job satellite result snapshot services."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from litoral_trace.db.models import SatelliteJobResult
from litoral_trace.db.tenant import get_tenant_scoped_db_session, set_tenant_db_context
from litoral_trace.services.satellite_ndvi_processing import (
    NormalizedNdviExecutionResult,
    NdviObservationRecord,
)


NDVI_TIMESERIES_RESULT_SCHEMA_VERSION = "ndvi_timeseries.v1"


@dataclass(frozen=True)
class SatelliteJobResultSnapshot:
    satellite_job_id: int
    organization_id: int
    lote_id: int
    result_schema_version: str
    geometry_hash: str
    algorithm_version: str
    result_payload: dict[str, Any]
    payload_sha256: str


def _normalize_non_empty_string(value: str, *, field_name: str) -> str:
    normalized_value = str(value or "").strip()
    if not normalized_value:
        raise ValueError(f"{field_name} no puede ser vacio.")
    return normalized_value


def _serialize_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def _serialize_observation(
    observation: NdviObservationRecord,
    *,
    result_geometry_hash: str,
    result_algorithm_version: str,
) -> dict[str, Any]:
    if observation.geometry_hash != result_geometry_hash:
        raise ValueError(
            "Todas las observaciones deben conservar el mismo geometry_hash del resultado."
        )
    if observation.algorithm_version != result_algorithm_version:
        raise ValueError(
            "Todas las observaciones deben conservar la misma algorithm_version del resultado."
        )

    return {
        "observation_date": observation.observation_date.isoformat(),
        "ndvi_mean": observation.ndvi_mean,
        "ndvi_min": observation.ndvi_min,
        "ndvi_max": observation.ndvi_max,
        "ndvi_std": observation.ndvi_std,
        "scene_cloud_percentage": observation.scene_cloud_percentage,
        "aoi_cloud_percentage": observation.aoi_cloud_percentage,
        "valid_pixel_count": observation.valid_pixel_count,
        "valid_pixel_percentage": observation.valid_pixel_percentage,
        "satellite": observation.satellite,
        "collection": observation.collection,
        "processing_date": _serialize_datetime(observation.processing_date),
    }


def _observation_sort_key(
    observation: NdviObservationRecord,
) -> tuple[str, str, str, str]:
    return (
        observation.observation_date.isoformat(),
        _serialize_datetime(observation.processing_date) or "",
        observation.satellite,
        observation.collection,
    )


def canonicalize_satellite_job_result_payload(
    payload: dict[str, Any],
) -> bytes:
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def compute_satellite_job_result_payload_sha256(
    payload: dict[str, Any],
) -> str:
    return hashlib.sha256(
        canonicalize_satellite_job_result_payload(payload)
    ).hexdigest()


def build_satellite_job_result_snapshot(
    *,
    satellite_job_id: int,
    organization_id: int,
    lote_id: int,
    result: NormalizedNdviExecutionResult,
) -> SatelliteJobResultSnapshot:
    normalized_geometry_hash = _normalize_non_empty_string(
        result.geometry_hash,
        field_name="geometry_hash",
    )
    normalized_algorithm_version = _normalize_non_empty_string(
        result.algorithm_version,
        field_name="algorithm_version",
    )

    observations_payload = [
        _serialize_observation(
            observation,
            result_geometry_hash=normalized_geometry_hash,
            result_algorithm_version=normalized_algorithm_version,
        )
        for observation in sorted(
            result.observations,
            key=_observation_sort_key,
        )
    ]

    result_payload = {
        "schema_version": NDVI_TIMESERIES_RESULT_SCHEMA_VERSION,
        "job_id": int(satellite_job_id),
        "lote_id": int(lote_id),
        "geometry_hash": normalized_geometry_hash,
        "algorithm_version": normalized_algorithm_version,
        "total_observations": len(observations_payload),
        "observations": observations_payload,
    }

    return SatelliteJobResultSnapshot(
        satellite_job_id=int(satellite_job_id),
        organization_id=int(organization_id),
        lote_id=int(lote_id),
        result_schema_version=NDVI_TIMESERIES_RESULT_SCHEMA_VERSION,
        geometry_hash=normalized_geometry_hash,
        algorithm_version=normalized_algorithm_version,
        result_payload=result_payload,
        payload_sha256=compute_satellite_job_result_payload_sha256(result_payload),
    )


def persist_satellite_job_result(
    db_session: Session,
    *,
    snapshot: SatelliteJobResultSnapshot,
) -> SatelliteJobResult:
    set_tenant_db_context(
        db_session,
        snapshot.organization_id,
    )

    existing_row = db_session.execute(
        select(SatelliteJobResult).where(
            SatelliteJobResult.satellite_job_id == snapshot.satellite_job_id,
            SatelliteJobResult.organization_id == snapshot.organization_id,
        )
    ).scalar_one_or_none()
    if existing_row is not None:
        raise RuntimeError(
            "Immutable satellite job result snapshot already exists for this job."
        )

    row = SatelliteJobResult(
        satellite_job_id=snapshot.satellite_job_id,
        organization_id=snapshot.organization_id,
        lote_id=snapshot.lote_id,
        result_schema_version=snapshot.result_schema_version,
        geometry_hash=snapshot.geometry_hash,
        algorithm_version=snapshot.algorithm_version,
        result_payload=snapshot.result_payload,
        payload_sha256=snapshot.payload_sha256,
    )
    db_session.add(row)
    db_session.flush()
    return row


def get_satellite_job_result(
    *,
    organization_id: int | str,
    job_id: int,
) -> SatelliteJobResult | None:
    session = get_tenant_scoped_db_session(organization_id)
    if session is None:
        raise RuntimeError("Servicio de base de datos no disponible.")

    try:
        row = session.execute(
            select(SatelliteJobResult).where(
                SatelliteJobResult.satellite_job_id == int(job_id),
                SatelliteJobResult.organization_id == int(organization_id),
            )
        ).scalar_one_or_none()
        if row is not None:
            session.expunge(row)
        return row
    finally:
        session.close()
