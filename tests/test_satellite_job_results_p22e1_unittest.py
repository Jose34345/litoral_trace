from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone

import pytest
from sqlalchemy import create_engine, delete, select

from litoral_trace.db.base import Base
from litoral_trace.db.engine import get_db_session
from litoral_trace.db.models import (
    Lote,
    Organization,
    SatelliteJob,
    SatelliteJobResult,
    SatelliteNdviObservation,
)
from litoral_trace.services.gee import ALGORITHM_VERSION, generate_geometry_hash
from litoral_trace.services.satellite_job_results import (
    NDVI_TIMESERIES_RESULT_SCHEMA_VERSION,
    build_satellite_job_result_snapshot,
    canonicalize_satellite_job_result_payload,
    compute_satellite_job_result_payload_sha256,
    persist_satellite_job_result,
)
from litoral_trace.services.satellite_jobs import SatelliteJobType
from litoral_trace.services.satellite_ndvi_processing import (
    NormalizedNdviExecutionResult,
    NdviObservationRecord,
    normalize_ndvi_execution_result,
    persist_ndvi_execution_result,
)


@dataclass(frozen=True)
class _ResultFixture:
    organization_id: int
    lote_id: int
    job_id: int
    geometry_hash: str
    algorithm_version: str


@pytest.fixture(autouse=True)
def cleanup_p22e1_entities():
    _cleanup_p22e1_entities()
    yield
    _cleanup_p22e1_entities()


def _cleanup_p22e1_entities() -> None:
    session = get_db_session()
    org_ids = session.execute(
        select(Organization.id).where(
            Organization.slug.like("worker-e1-org-%")
        )
    ).scalars().all()

    if org_ids:
        session.execute(
            delete(SatelliteJobResult).where(
                SatelliteJobResult.organization_id.in_(org_ids)
            )
        )
        session.execute(
            delete(SatelliteNdviObservation).where(
                SatelliteNdviObservation.organization_id.in_(org_ids)
            )
        )
        session.execute(
            delete(SatelliteJob).where(
                SatelliteJob.organization_id.in_(org_ids)
            )
        )
        session.execute(
            delete(Lote).where(
                Lote.organization_id.in_(org_ids)
            )
        )
        session.execute(
            delete(Organization).where(
                Organization.id.in_(org_ids)
            )
        )
        session.commit()

    session.close()


def _create_result_fixture() -> _ResultFixture:
    session = get_db_session()
    suffix = datetime.now(timezone.utc).strftime("%H%M%S%f")
    polygon_wkt_snapshot = (
        "POLYGON(("
        "-58.91 -27.46, "
        "-58.89 -27.46, "
        "-58.89 -27.44, "
        "-58.91 -27.44, "
        "-58.91 -27.46"
        "))"
    )
    organization = Organization(
        name=f"Worker E1 Org {suffix}",
        slug=f"worker-e1-org-{suffix}",
        tax_id=f"89-{suffix[-8:]}",
        tier="pro",
        is_active=True,
    )
    session.add(organization)
    session.flush()

    lote = Lote(
        organization_id=organization.id,
        identificador=f"WORKER-E1-LOTE-{suffix}",
        productor_id=f"39-{suffix[-8:]}",
        producto_forestal="Madera Aserrada (Pino)",
        hectareas=10.0,
        latitud=-27.45,
        longitud=-58.90,
        polygon_wkt=polygon_wkt_snapshot,
        estatus="Pendiente",
        volumen_ingresado_ton=20.0,
        volumen_exportar_ton=5.0,
    )
    session.add(lote)
    session.flush()

    geometry_hash = generate_geometry_hash(polygon_wkt_snapshot)
    job = SatelliteJob(
        organization_id=organization.id,
        lote_id=lote.id,
        job_type=SatelliteJobType.NDVI_TIMESERIES.value,
        status="running",
        attempt_count=1,
        max_attempts=3,
        request_start_date=date(2026, 7, 1),
        request_end_date=date(2026, 8, 1),
        max_cloud_pct=20.0,
        geometry_hash=geometry_hash,
        algorithm_version=ALGORITHM_VERSION,
        polygon_wkt_snapshot=polygon_wkt_snapshot,
        locked_at=datetime.now(timezone.utc),
        locked_by="worker-e1",
        heartbeat_at=datetime.now(timezone.utc),
        lease_token="00000000-0000-4000-8000-000000000001",
        started_at=datetime.now(timezone.utc),
    )
    session.add(job)
    session.commit()

    fixture = _ResultFixture(
        organization_id=organization.id,
        lote_id=lote.id,
        job_id=job.id,
        geometry_hash=geometry_hash,
        algorithm_version=ALGORITHM_VERSION,
    )
    session.close()
    return fixture


def _build_result(
    fixture: _ResultFixture,
    *,
    observation_dates: tuple[date, ...] = (date(2026, 8, 1),),
    ndvi_mean: float = 0.61,
    aoi_cloud_percentage: float | None = 1.0,
) -> NormalizedNdviExecutionResult:
    observations = tuple(
        NdviObservationRecord(
            observation_date=observation_date,
            ndvi_mean=ndvi_mean,
            ndvi_min=0.55,
            ndvi_max=0.68,
            ndvi_std=0.03,
            scene_cloud_percentage=5.0,
            valid_pixel_count=10,
            valid_pixel_percentage=98.0,
            satellite="Sentinel-2",
            collection="COPERNICUS/S2_SR_HARMONIZED",
            geometry_hash=fixture.geometry_hash,
            algorithm_version=fixture.algorithm_version,
            aoi_cloud_percentage=aoi_cloud_percentage,
            processing_date=datetime(2026, 8, 1, 12, 0, tzinfo=timezone.utc),
        )
        for observation_date in observation_dates
    )
    return NormalizedNdviExecutionResult(
        geometry_hash=fixture.geometry_hash,
        algorithm_version=fixture.algorithm_version,
        observations=observations,
    )


def test_normalize_ndvi_execution_result_preserves_aoi_cloud_percentage():
    normalized = normalize_ndvi_execution_result(
        {
            "geometry_hash": "g" * 64,
            "algorithm_version": ALGORITHM_VERSION,
            "observations": [
                {
                    "observation_date": "2026-08-01",
                    "ndvi_mean": 0.61,
                    "ndvi_min": 0.55,
                    "ndvi_max": 0.68,
                    "ndvi_std": 0.03,
                    "scene_cloud_percentage": 5.0,
                    "aoi_cloud_percentage": 1.25,
                    "valid_pixel_count": 10,
                    "valid_pixel_percentage": 98.0,
                    "satellite": "Sentinel-2",
                    "collection": "COPERNICUS/S2_SR_HARMONIZED",
                    "geometry_hash": "g" * 64,
                    "algorithm_version": ALGORITHM_VERSION,
                    "processing_date": "2026-08-01T12:00:00+00:00",
                }
            ],
        }
    )

    assert normalized.observations[0].aoi_cloud_percentage == 1.25


def test_build_snapshot_uses_expected_schema_and_omits_sensitive_fields():
    fixture = _create_result_fixture()
    snapshot = build_satellite_job_result_snapshot(
        satellite_job_id=fixture.job_id,
        organization_id=fixture.organization_id,
        lote_id=fixture.lote_id,
        result=_build_result(fixture),
    )

    assert snapshot.result_schema_version == NDVI_TIMESERIES_RESULT_SCHEMA_VERSION
    assert snapshot.result_payload["schema_version"] == NDVI_TIMESERIES_RESULT_SCHEMA_VERSION
    assert "organization_id" not in snapshot.result_payload
    assert "worker_id" not in snapshot.result_payload
    assert "lease_token" not in snapshot.result_payload
    assert "locked_by" not in snapshot.result_payload
    assert "heartbeat_at" not in snapshot.result_payload
    assert "polygon_wkt_snapshot" not in snapshot.result_payload


def test_satellite_job_result_foreign_keys_are_explicitly_restrictive():
    foreign_keys = {
        constraint.name: constraint.ondelete
        for constraint in SatelliteJobResult.__table__.foreign_key_constraints
    }

    assert foreign_keys["fk_satellite_job_results_organization_id"] == "RESTRICT"
    assert foreign_keys["fk_satellite_job_results_job_tenant"] == "RESTRICT"
    assert foreign_keys["fk_satellite_job_results_lote_tenant"] == "RESTRICT"


def test_satellite_job_result_relationship_has_no_delete_cascade():
    relationship = SatelliteJob.__mapper__.relationships["result_snapshot"]

    assert "delete" not in relationship.cascade
    assert "delete-orphan" not in relationship.cascade
    assert relationship.passive_deletes is False


def test_zero_observation_snapshot_is_supported():
    fixture = _create_result_fixture()
    snapshot = build_satellite_job_result_snapshot(
        satellite_job_id=fixture.job_id,
        organization_id=fixture.organization_id,
        lote_id=fixture.lote_id,
        result=NormalizedNdviExecutionResult(
            geometry_hash=fixture.geometry_hash,
            algorithm_version=fixture.algorithm_version,
            observations=(),
        ),
    )

    assert snapshot.result_payload["total_observations"] == 0
    assert snapshot.result_payload["observations"] == []


def test_payload_canonicalization_and_sha256_are_deterministic():
    fixture = _create_result_fixture()
    first_snapshot = build_satellite_job_result_snapshot(
        satellite_job_id=fixture.job_id,
        organization_id=fixture.organization_id,
        lote_id=fixture.lote_id,
        result=_build_result(
            fixture,
            observation_dates=(date(2026, 8, 2), date(2026, 8, 1)),
        ),
    )
    second_snapshot = build_satellite_job_result_snapshot(
        satellite_job_id=fixture.job_id,
        organization_id=fixture.organization_id,
        lote_id=fixture.lote_id,
        result=_build_result(
            fixture,
            observation_dates=(date(2026, 8, 1), date(2026, 8, 2)),
        ),
    )

    assert first_snapshot.result_payload == second_snapshot.result_payload
    assert first_snapshot.payload_sha256 == second_snapshot.payload_sha256
    assert canonicalize_satellite_job_result_payload(
        first_snapshot.result_payload
    ) == canonicalize_satellite_job_result_payload(
        second_snapshot.result_payload
    )


def test_payload_sha256_changes_when_payload_changes():
    fixture = _create_result_fixture()
    base_snapshot = build_satellite_job_result_snapshot(
        satellite_job_id=fixture.job_id,
        organization_id=fixture.organization_id,
        lote_id=fixture.lote_id,
        result=_build_result(fixture, ndvi_mean=0.61),
    )
    changed_snapshot = build_satellite_job_result_snapshot(
        satellite_job_id=fixture.job_id,
        organization_id=fixture.organization_id,
        lote_id=fixture.lote_id,
        result=_build_result(fixture, ndvi_mean=0.72),
    )

    assert base_snapshot.payload_sha256 != changed_snapshot.payload_sha256
    assert compute_satellite_job_result_payload_sha256(
        base_snapshot.result_payload
    ) == base_snapshot.payload_sha256


def test_persist_satellite_job_result_refuses_duplicate_snapshot_overwrite():
    fixture = _create_result_fixture()
    first_snapshot = build_satellite_job_result_snapshot(
        satellite_job_id=fixture.job_id,
        organization_id=fixture.organization_id,
        lote_id=fixture.lote_id,
        result=_build_result(fixture, ndvi_mean=0.61),
    )
    second_snapshot = build_satellite_job_result_snapshot(
        satellite_job_id=fixture.job_id,
        organization_id=fixture.organization_id,
        lote_id=fixture.lote_id,
        result=_build_result(fixture, ndvi_mean=0.72),
    )

    session = get_db_session()
    persist_satellite_job_result(session, snapshot=first_snapshot)
    session.commit()
    session.close()

    session = get_db_session()
    with pytest.raises(
        RuntimeError,
        match="Immutable satellite job result snapshot already exists",
    ):
        persist_satellite_job_result(session, snapshot=second_snapshot)
    session.rollback()

    stored_row = session.execute(
        select(SatelliteJobResult).where(
            SatelliteJobResult.satellite_job_id == fixture.job_id
        )
    ).scalar_one()
    session.close()

    assert stored_row.payload_sha256 == first_snapshot.payload_sha256
    assert stored_row.result_payload["observations"][0]["ndvi_mean"] == 0.61


def test_canonical_ndvi_persistence_remains_compatible_with_snapshot_persistence():
    fixture = _create_result_fixture()
    normalized_result = _build_result(fixture, aoi_cloud_percentage=1.0)
    snapshot = build_satellite_job_result_snapshot(
        satellite_job_id=fixture.job_id,
        organization_id=fixture.organization_id,
        lote_id=fixture.lote_id,
        result=normalized_result,
    )

    session = get_db_session()
    persist_ndvi_execution_result(
        session,
        organization_id=fixture.organization_id,
        lote_id=fixture.lote_id,
        satellite_job_id=fixture.job_id,
        result=normalized_result,
    )
    persist_satellite_job_result(session, snapshot=snapshot)
    session.commit()

    canonical_row = session.execute(
        select(SatelliteNdviObservation).where(
            SatelliteNdviObservation.satellite_job_id == fixture.job_id
        )
    ).scalar_one()
    immutable_row = session.execute(
        select(SatelliteJobResult).where(
            SatelliteJobResult.satellite_job_id == fixture.job_id
        )
    ).scalar_one()
    session.close()

    assert canonical_row.cloud_percentage == 5.0
    assert immutable_row.result_payload["observations"][0]["aoi_cloud_percentage"] == 1.0


def test_sqlite_metadata_create_all_supports_satellite_job_results():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    assert "satellite_job_results" in Base.metadata.tables
