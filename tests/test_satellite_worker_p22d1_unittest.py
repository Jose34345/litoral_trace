from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timezone
from uuid import UUID, uuid4

import pytest
from sqlalchemy import delete, select

import litoral_trace.workers.satellite_worker as satellite_worker_module
from litoral_trace.db.engine import get_db_session
from litoral_trace.db.models import (
    Lote,
    Organization,
    SatelliteJob,
    SatelliteNdviObservation,
)
from litoral_trace.services.gee import ALGORITHM_VERSION, generate_geometry_hash
from litoral_trace.services.satellite_jobs import (
    ClaimedSatelliteJob,
    SatelliteJobType,
    serialize_satellite_job,
)
from litoral_trace.services.satellite_ndvi_processing import (
    NormalizedNdviExecutionResult,
    NdviObservationRecord,
    SatelliteJobLeaseLostError,
    mark_satellite_job_failed,
    mark_satellite_job_succeeded,
    persist_ndvi_execution_result,
)
from litoral_trace.workers.satellite_worker import (
    SatelliteWorker,
    SatelliteWorkerExecutionError,
    WorkerRunStatus,
)


@dataclass(frozen=True)
class _JobFixture:
    organization_id: int
    lote_id: int
    job_id: int
    worker_id: str
    lease_token: str
    claimed_job: ClaimedSatelliteJob


class _RecordingClaimSession:
    def commit(self):
        return None

    def rollback(self):
        return None

    def close(self):
        return None


class _FakeGeeAdapter:
    def __init__(self, *, result=None, error: Exception | None = None):
        self.result = result
        self.error = error

    def execute(self, request):
        if self.error is not None:
            raise self.error
        return self.result


class _FailureCountingWorker(SatelliteWorker):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.persist_failure_calls = 0

    def _persist_failure(self, context, *, error_code: str, error_message: str) -> None:
        self.persist_failure_calls += 1
        return super()._persist_failure(
            context,
            error_code=error_code,
            error_message=error_message,
        )


@pytest.fixture(autouse=True)
def cleanup_p22d1_entities():
    _cleanup_p22d1_entities()
    yield
    _cleanup_p22d1_entities()


def _cleanup_p22d1_entities() -> None:
    session = get_db_session()

    d1_org_ids = session.execute(
        select(Organization.id).where(
            Organization.slug.like("worker-d1-org-%")
        )
    ).scalars().all()

    if d1_org_ids:
        session.execute(
            delete(SatelliteNdviObservation).where(
                SatelliteNdviObservation.organization_id.in_(d1_org_ids)
            )
        )
        session.execute(
            delete(SatelliteJob).where(
                SatelliteJob.organization_id.in_(d1_org_ids)
            )
        )
        session.execute(
            delete(Lote).where(
                Lote.organization_id.in_(d1_org_ids)
            )
        )
        session.execute(
            delete(Organization).where(
                Organization.id.in_(d1_org_ids)
            )
        )
        session.commit()

    session.close()


def _create_job_fixture(
    *,
    status: str = "running",
    worker_id: str = "worker-d1",
    lease_token: str | None = None,
) -> _JobFixture:
    session = get_db_session()
    suffix = uuid4().hex[:8]
    now = datetime.now(timezone.utc)

    organization = Organization(
        name=f"Worker D1 Org {suffix}",
        slug=f"worker-d1-org-{suffix}",
        tax_id=f"82-{suffix}",
        tier="pro",
        is_active=True,
    )
    session.add(organization)
    session.flush()

    polygon_wkt_snapshot = (
        "POLYGON(("
        "-58.91 -27.46, "
        "-58.89 -27.46, "
        "-58.89 -27.44, "
        "-58.91 -27.44, "
        "-58.91 -27.46"
        "))"
    )

    lote = Lote(
        organization_id=organization.id,
        identificador=f"WORKER-D1-LOTE-{suffix}",
        productor_id=f"32-{suffix}",
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

    normalized_lease_token = lease_token or str(uuid4())
    geometry_hash = generate_geometry_hash(polygon_wkt_snapshot)

    job = SatelliteJob(
        organization_id=organization.id,
        lote_id=lote.id,
        job_type=SatelliteJobType.NDVI_TIMESERIES.value,
        status=status,
        attempt_count=1 if status != "queued" else 0,
        max_attempts=3,
        request_start_date=date(2026, 7, 1),
        request_end_date=date(2026, 8, 1),
        max_cloud_pct=20.0,
        geometry_hash=geometry_hash,
        algorithm_version=ALGORITHM_VERSION,
        polygon_wkt_snapshot=polygon_wkt_snapshot,
        locked_at=now if status == "running" else None,
        locked_by=worker_id if status == "running" else None,
        heartbeat_at=now if status == "running" else None,
        lease_token=normalized_lease_token,
        started_at=now,
        finished_at=now if status in {"succeeded", "failed"} else None,
    )
    session.add(job)
    session.commit()
    session.refresh(job)

    claimed_job = ClaimedSatelliteJob(
        id=job.id,
        organization_id=job.organization_id,
        lote_id=job.lote_id,
        job_type=job.job_type,
        status=job.status,
        attempt_count=job.attempt_count,
        max_attempts=job.max_attempts,
        next_attempt_at=job.next_attempt_at,
        locked_by=job.locked_by or worker_id,
        locked_at=job.locked_at or now,
        heartbeat_at=job.heartbeat_at or now,
        lease_token=UUID(str(job.lease_token)),
        started_at=job.started_at,
        request_start_date=job.request_start_date,
        request_end_date=job.request_end_date,
        max_cloud_pct=job.max_cloud_pct,
        geometry_hash=job.geometry_hash,
        algorithm_version=job.algorithm_version,
        polygon_wkt_snapshot=job.polygon_wkt_snapshot,
    )

    fixture = _JobFixture(
        organization_id=job.organization_id,
        lote_id=int(job.lote_id),
        job_id=job.id,
        worker_id=worker_id,
        lease_token=str(job.lease_token),
        claimed_job=claimed_job,
    )

    session.close()
    return fixture


def _load_job(job_id: int) -> SatelliteJob:
    session = get_db_session()
    job = session.execute(
        select(SatelliteJob).where(
            SatelliteJob.id == job_id
        )
    ).scalar_one()
    session.expunge(job)
    session.close()
    return job


def _replace_job_lease(job_id: int, *, new_worker_id: str | None = None) -> str:
    session = get_db_session()
    job = session.execute(
        select(SatelliteJob).where(
            SatelliteJob.id == job_id
        )
    ).scalar_one()
    new_lease_token = str(uuid4())
    job.lease_token = new_lease_token
    if new_worker_id is not None:
        job.locked_by = new_worker_id
    session.commit()
    session.close()
    return new_lease_token


def _load_observations(job_id: int) -> list[SatelliteNdviObservation]:
    session = get_db_session()
    rows = session.execute(
        select(SatelliteNdviObservation).where(
            SatelliteNdviObservation.satellite_job_id == job_id
        )
    ).scalars().all()
    session.close()
    return rows


def _normalized_result_from_fixture(
    fixture: _JobFixture,
) -> NormalizedNdviExecutionResult:
    return NormalizedNdviExecutionResult(
        geometry_hash=fixture.claimed_job.geometry_hash or "",
        algorithm_version=fixture.claimed_job.algorithm_version or ALGORITHM_VERSION,
        observations=(
            NdviObservationRecord(
                observation_date=date(2026, 8, 1),
                ndvi_mean=0.61,
                ndvi_min=0.55,
                ndvi_max=0.68,
                ndvi_std=0.03,
                scene_cloud_percentage=5.0,
                valid_pixel_count=10,
                valid_pixel_percentage=98.0,
                satellite="Sentinel-2",
                collection="COPERNICUS/S2_SR_HARMONIZED",
                geometry_hash=fixture.claimed_job.geometry_hash or "",
                algorithm_version=fixture.claimed_job.algorithm_version or ALGORITHM_VERSION,
                processing_date=datetime.now(timezone.utc),
            ),
        ),
    )


def test_mark_satellite_job_succeeded_accepts_matching_running_worker_and_lease():
    fixture = _create_job_fixture()
    session = get_db_session()

    mark_satellite_job_succeeded(
        session,
        organization_id=fixture.organization_id,
        job_id=fixture.job_id,
        worker_id=fixture.worker_id,
        lease_token=fixture.lease_token,
    )
    session.commit()
    session.close()

    job = _load_job(fixture.job_id)

    assert job.status == "succeeded"
    assert job.finished_at is not None


def test_mark_satellite_job_succeeded_rejects_incorrect_lease_and_leaves_job_intact():
    fixture = _create_job_fixture()
    session = get_db_session()

    with pytest.raises(SatelliteJobLeaseLostError):
        mark_satellite_job_succeeded(
            session,
            organization_id=fixture.organization_id,
            job_id=fixture.job_id,
            worker_id=fixture.worker_id,
            lease_token=str(uuid4()),
        )

    session.rollback()
    session.close()

    job = _load_job(fixture.job_id)

    assert job.status == "running"
    assert job.locked_by == fixture.worker_id
    assert str(job.lease_token) == fixture.lease_token


def test_mark_satellite_job_succeeded_rejects_incorrect_worker():
    fixture = _create_job_fixture()
    session = get_db_session()

    with pytest.raises(SatelliteJobLeaseLostError):
        mark_satellite_job_succeeded(
            session,
            organization_id=fixture.organization_id,
            job_id=fixture.job_id,
            worker_id="wrong-worker",
            lease_token=fixture.lease_token,
        )

    session.rollback()
    session.close()

    job = _load_job(fixture.job_id)

    assert job.status == "running"
    assert job.locked_by == fixture.worker_id


def test_mark_satellite_job_succeeded_rejects_non_running_job():
    fixture = _create_job_fixture(status="succeeded")
    session = get_db_session()

    with pytest.raises(SatelliteJobLeaseLostError):
        mark_satellite_job_succeeded(
            session,
            organization_id=fixture.organization_id,
            job_id=fixture.job_id,
            worker_id=fixture.worker_id,
            lease_token=fixture.lease_token,
        )

    session.rollback()
    session.close()

    job = _load_job(fixture.job_id)

    assert job.status == "succeeded"
    assert str(job.lease_token) == fixture.lease_token


def test_valid_succeeded_transition_preserves_terminal_lease_and_clears_active_lock_fields():
    fixture = _create_job_fixture()
    session = get_db_session()

    mark_satellite_job_succeeded(
        session,
        organization_id=fixture.organization_id,
        job_id=fixture.job_id,
        worker_id=fixture.worker_id,
        lease_token=fixture.lease_token,
    )
    session.commit()
    session.close()

    job = _load_job(fixture.job_id)

    assert str(job.lease_token) == fixture.lease_token
    assert job.locked_at is None
    assert job.locked_by is None
    assert job.heartbeat_at is None


def test_mark_satellite_job_failed_accepts_matching_running_worker_and_lease():
    fixture = _create_job_fixture()
    session = get_db_session()

    mark_satellite_job_failed(
        session,
        organization_id=fixture.organization_id,
        job_id=fixture.job_id,
        worker_id=fixture.worker_id,
        lease_token=fixture.lease_token,
        error_code="gee_execution_failed",
        error_message="bounded message",
    )
    session.commit()
    session.close()

    job = _load_job(fixture.job_id)

    assert job.status == "failed"
    assert job.finished_at is not None
    assert job.error_code == "gee_execution_failed"
    assert job.error_message == "bounded message"


def test_mark_satellite_job_failed_rejects_incorrect_lease():
    fixture = _create_job_fixture()
    session = get_db_session()

    with pytest.raises(SatelliteJobLeaseLostError):
        mark_satellite_job_failed(
            session,
            organization_id=fixture.organization_id,
            job_id=fixture.job_id,
            worker_id=fixture.worker_id,
            lease_token=str(uuid4()),
            error_code="gee_execution_failed",
            error_message="bounded message",
        )

    session.rollback()
    session.close()

    job = _load_job(fixture.job_id)

    assert job.status == "running"
    assert str(job.lease_token) == fixture.lease_token


def test_valid_failed_transition_preserves_terminal_lease_and_clears_active_lock_fields():
    fixture = _create_job_fixture()
    session = get_db_session()

    mark_satellite_job_failed(
        session,
        organization_id=fixture.organization_id,
        job_id=fixture.job_id,
        worker_id=fixture.worker_id,
        lease_token=fixture.lease_token,
        error_code=" gee_execution_failed ",
        error_message=" bounded failure message ",
    )
    session.commit()
    session.close()

    job = _load_job(fixture.job_id)

    assert job.status == "failed"
    assert str(job.lease_token) == fixture.lease_token
    assert job.locked_at is None
    assert job.locked_by is None
    assert job.heartbeat_at is None


def test_worker_lease_loss_during_success_rolls_back_observations_and_skips_failure_persistence():
    fixture = _create_job_fixture()
    _replace_job_lease(fixture.job_id)

    worker = _FailureCountingWorker(
        worker_id=fixture.worker_id,
        claim_session_factory=_RecordingClaimSession,
        tenant_session_factory=get_db_session,
        claim_job_func=lambda **_: fixture.claimed_job,
        gee_ndvi_adapter=_FakeGeeAdapter(
            result=_normalized_result_from_fixture(fixture)
        ),
    )

    result = worker.run_once()

    job = _load_job(fixture.job_id)
    observations = _load_observations(fixture.job_id)

    assert result.status is WorkerRunStatus.LEASE_LOST
    assert result.error_code == "lease_lost"
    assert worker.persist_failure_calls == 0
    assert observations == []
    assert job.status == "running"


def test_worker_generic_failure_then_fenced_failed_lease_loss_returns_lease_lost_without_second_terminal_attempt():
    fixture = _create_job_fixture()
    _replace_job_lease(fixture.job_id)

    worker = _FailureCountingWorker(
        worker_id=fixture.worker_id,
        claim_session_factory=_RecordingClaimSession,
        tenant_session_factory=get_db_session,
        claim_job_func=lambda **_: fixture.claimed_job,
        gee_ndvi_adapter=_FakeGeeAdapter(
            error=SatelliteWorkerExecutionError(
                "gee_execution_failed",
                "remote gee execution failed",
            )
        ),
    )

    result = worker.run_once()

    job = _load_job(fixture.job_id)

    assert result.status is WorkerRunStatus.LEASE_LOST
    assert result.error_code == "lease_lost"
    assert worker.persist_failure_calls == 1
    assert job.status == "running"
    assert job.finished_at is None


def test_lease_lost_result_logs_and_public_views_do_not_expose_lease_token(caplog):
    fixture = _create_job_fixture()
    replacement_lease_token = _replace_job_lease(
        fixture.job_id,
        new_worker_id="worker-d1-b",
    )

    worker = _FailureCountingWorker(
        worker_id=fixture.worker_id,
        claim_session_factory=_RecordingClaimSession,
        tenant_session_factory=get_db_session,
        claim_job_func=lambda **_: fixture.claimed_job,
        gee_ndvi_adapter=_FakeGeeAdapter(
            result=_normalized_result_from_fixture(fixture)
        ),
    )

    with caplog.at_level(
        logging.WARNING,
        logger=satellite_worker_module.__name__,
    ):
        result = worker.run_once()

    job = _load_job(fixture.job_id)
    serialized = serialize_satellite_job(job)

    assert result.status is WorkerRunStatus.LEASE_LOST
    assert result.error_code == "lease_lost"
    assert not hasattr(result, "lease_token")
    assert "lease_token" not in serialized

    lease_lost_records = [
        record
        for record in caplog.records
        if record.getMessage() == "satellite_worker_job_lease_lost"
    ]

    assert len(lease_lost_records) == 1
    assert "lease_token" not in lease_lost_records[0].__dict__

    for secret_value in (
        fixture.lease_token,
        replacement_lease_token,
    ):
        assert secret_value not in lease_lost_records[0].getMessage()
