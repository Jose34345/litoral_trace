from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest
from sqlalchemy import delete, select

import litoral_trace.workers.satellite_worker as satellite_worker_module
from litoral_trace.db.engine import get_db_session
from litoral_trace.db.models import Lote, Organization, SatelliteJob
from litoral_trace.services.gee import ALGORITHM_VERSION, generate_geometry_hash
from litoral_trace.services.satellite_jobs import ClaimedSatelliteJob, SatelliteJobType
from litoral_trace.services.satellite_ndvi_processing import (
    NormalizedNdviExecutionResult,
    NdviObservationRecord,
    SatelliteJobLeaseLostError,
    update_satellite_job_heartbeat,
)
from litoral_trace.workers.satellite_worker import (
    SatelliteWorker,
    SatelliteWorkerExecutionError,
    WorkerRunStatus,
    resolve_satellite_worker_heartbeat_seconds,
)


@dataclass(frozen=True)
class _DbJobFixture:
    organization_id: int
    lote_id: int
    job_id: int
    worker_id: str
    lease_token: str


class _RecordingClaimSession:
    def __init__(self):
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        self.closed = True


class _RecordingTenantSession:
    def __init__(self, name: str):
        self.name = name
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


class _RecordingTenantSessionFactory:
    def __init__(self):
        self.sessions: list[_RecordingTenantSession] = []

    def __call__(self):
        session = _RecordingTenantSession(
            name=f"tenant-session-{len(self.sessions) + 1}"
        )
        self.sessions.append(session)
        return session


class _FakeGeeAdapter:
    def __init__(self, *, result=None, error: Exception | None = None, on_execute=None):
        self.result = result
        self.error = error
        self.on_execute = on_execute
        self.calls = []

    def execute(self, request):
        self.calls.append(request)
        if self.on_execute is not None:
            self.on_execute(request)
        if self.error is not None:
            raise self.error
        return self.result


class _FakeHeartbeatController:
    def __init__(self, *, order: list[str] | None = None, on_start=None):
        self.order = order if order is not None else []
        self.on_start = on_start
        self.lease_lost = False
        self.started = False
        self.stop_calls = 0
        self.join_calls = 0
        self.alive = False

    def start(self):
        self.started = True
        self.alive = True
        self.order.append("start")
        if self.on_start is not None:
            self.on_start()

    def stop(self):
        self.stop_calls += 1
        self.order.append("stop")

    def join(self):
        self.join_calls += 1
        self.alive = False
        self.order.append("join")

    def is_alive(self):
        return self.alive

    def has_lease_lost(self):
        return self.lease_lost


class _InjectedHeartbeatWorker(SatelliteWorker):
    def __init__(self, *, heartbeat_controller, **kwargs):
        super().__init__(**kwargs)
        self._injected_heartbeat_controller = heartbeat_controller

    def _create_heartbeat_controller(self, context):
        self.injected_context = context
        return self._injected_heartbeat_controller


class _OrderRecordingWorker(_InjectedHeartbeatWorker):
    def __init__(self, *, heartbeat_controller, order: list[str], **kwargs):
        super().__init__(heartbeat_controller=heartbeat_controller, **kwargs)
        self.order = order

    def _persist_success(self, context, result):
        self.order.append("persist_success")

    def _persist_failure(self, context, *, error_code: str, error_message: str):
        self.order.append("persist_failure")


class _RealHeartbeatWorker(SatelliteWorker):
    def __init__(self, *, order: list[str], **kwargs):
        super().__init__(heartbeat_seconds=1, **kwargs)
        self.order = order
        self.persistence_session = None
        self.created_heartbeat_controller = None

    def _create_heartbeat_controller(self, context):
        controller = super()._create_heartbeat_controller(context)
        controller._heartbeat_seconds = 0.01
        self.created_heartbeat_controller = controller
        return controller

    def _persist_success(self, context, result):
        self.order.append("persist_success")
        session = self._tenant_session_factory()
        self.persistence_session = session
        session.commit()
        session.close()


@pytest.fixture(autouse=True)
def cleanup_p22d2_entities():
    _cleanup_p22d2_entities()
    yield
    _cleanup_p22d2_entities()


def _cleanup_p22d2_entities() -> None:
    session = get_db_session()
    d2_org_ids = session.execute(
        select(Organization.id).where(
            Organization.slug.like("worker-d2-org-%")
        )
    ).scalars().all()

    if d2_org_ids:
        session.execute(
            delete(SatelliteJob).where(
                SatelliteJob.organization_id.in_(d2_org_ids)
            )
        )
        session.execute(
            delete(Lote).where(
                Lote.organization_id.in_(d2_org_ids)
            )
        )
        session.execute(
            delete(Organization).where(
                Organization.id.in_(d2_org_ids)
            )
        )
        session.commit()

    session.close()


def _create_db_job_fixture(
    *,
    status: str = "running",
    worker_id: str = "worker-d2",
    lease_token: str | None = None,
) -> _DbJobFixture:
    session = get_db_session()
    suffix = uuid4().hex[:8]
    now = datetime.now(timezone.utc) - timedelta(minutes=5)
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
        name=f"Worker D2 Org {suffix}",
        slug=f"worker-d2-org-{suffix}",
        tax_id=f"83-{suffix}",
        tier="pro",
        is_active=True,
    )
    session.add(organization)
    session.flush()

    lote = Lote(
        organization_id=organization.id,
        identificador=f"WORKER-D2-LOTE-{suffix}",
        productor_id=f"33-{suffix}",
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

    job = SatelliteJob(
        organization_id=organization.id,
        lote_id=lote.id,
        job_type=SatelliteJobType.NDVI_TIMESERIES.value,
        status=status,
        attempt_count=1,
        max_attempts=3,
        request_start_date=date(2026, 7, 1),
        request_end_date=date(2026, 8, 1),
        max_cloud_pct=20.0,
        geometry_hash=generate_geometry_hash(polygon_wkt_snapshot),
        algorithm_version=ALGORITHM_VERSION,
        polygon_wkt_snapshot=polygon_wkt_snapshot,
        locked_at=now,
        locked_by=worker_id,
        heartbeat_at=now,
        lease_token=normalized_lease_token,
        started_at=now,
        finished_at=now if status != "running" else None,
    )
    session.add(job)
    session.commit()
    session.refresh(job)
    session.close()

    return _DbJobFixture(
        organization_id=job.organization_id,
        lote_id=int(job.lote_id),
        job_id=job.id,
        worker_id=worker_id,
        lease_token=str(job.lease_token),
    )


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


def _build_claimed_job(
    *,
    worker_id: str = "worker-test",
    lease_token: str | None = None,
) -> ClaimedSatelliteJob:
    now = datetime.now(timezone.utc)
    polygon_wkt_snapshot = (
        "POLYGON(("
        "-58.91 -27.46, "
        "-58.89 -27.46, "
        "-58.89 -27.44, "
        "-58.91 -27.44, "
        "-58.91 -27.46"
        "))"
    )
    return ClaimedSatelliteJob(
        id=1,
        organization_id=1,
        lote_id=1,
        job_type=SatelliteJobType.NDVI_TIMESERIES.value,
        status="running",
        attempt_count=1,
        max_attempts=3,
        next_attempt_at=now,
        locked_by=worker_id,
        locked_at=now,
        heartbeat_at=now,
        lease_token=UUID(lease_token or str(uuid4())),
        started_at=now,
        request_start_date=date(2026, 7, 1),
        request_end_date=date(2026, 8, 1),
        max_cloud_pct=20.0,
        geometry_hash=generate_geometry_hash(
            polygon_wkt_snapshot
        ),
        algorithm_version=ALGORITHM_VERSION,
        polygon_wkt_snapshot=polygon_wkt_snapshot,
    )


def _normalized_result_from_claimed_job(
    claimed_job: ClaimedSatelliteJob,
) -> NormalizedNdviExecutionResult:
    return NormalizedNdviExecutionResult(
        geometry_hash=claimed_job.geometry_hash or "",
        algorithm_version=claimed_job.algorithm_version or ALGORITHM_VERSION,
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
                geometry_hash=claimed_job.geometry_hash or "",
                algorithm_version=claimed_job.algorithm_version or ALGORITHM_VERSION,
                processing_date=datetime.now(timezone.utc),
            ),
        ),
    )


def test_update_satellite_job_heartbeat_updates_heartbeat_at_with_valid_lease():
    fixture = _create_db_job_fixture()
    before = _load_job(fixture.job_id)
    session = get_db_session()

    heartbeat_timestamp = update_satellite_job_heartbeat(
        session,
        organization_id=fixture.organization_id,
        job_id=fixture.job_id,
        worker_id=fixture.worker_id,
        lease_token=fixture.lease_token,
    )
    session.commit()
    session.close()

    after = _load_job(fixture.job_id)

    assert heartbeat_timestamp.tzinfo is not None
    assert after.status == "running"
    assert after.locked_by == fixture.worker_id
    assert str(after.lease_token) == fixture.lease_token
    assert after.finished_at is None
    assert after.heartbeat_at is not None
    assert after.heartbeat_at > before.heartbeat_at


def test_update_satellite_job_heartbeat_rejects_wrong_lease():
    fixture = _create_db_job_fixture()
    session = get_db_session()

    with pytest.raises(SatelliteJobLeaseLostError):
        update_satellite_job_heartbeat(
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
    assert str(job.lease_token) == fixture.lease_token


def test_update_satellite_job_heartbeat_rejects_wrong_worker():
    fixture = _create_db_job_fixture()
    session = get_db_session()

    with pytest.raises(SatelliteJobLeaseLostError):
        update_satellite_job_heartbeat(
            session,
            organization_id=fixture.organization_id,
            job_id=fixture.job_id,
            worker_id="wrong-worker",
            lease_token=fixture.lease_token,
        )

    session.rollback()
    session.close()

    job = _load_job(fixture.job_id)
    assert job.locked_by == fixture.worker_id


def test_update_satellite_job_heartbeat_rejects_non_running_job():
    fixture = _create_db_job_fixture(status="failed")
    session = get_db_session()

    with pytest.raises(SatelliteJobLeaseLostError):
        update_satellite_job_heartbeat(
            session,
            organization_id=fixture.organization_id,
            job_id=fixture.job_id,
            worker_id=fixture.worker_id,
            lease_token=fixture.lease_token,
        )

    session.rollback()
    session.close()

    job = _load_job(fixture.job_id)
    assert job.status == "failed"


def test_heartbeat_thread_starts_after_claim_commit_and_stops_before_success_persistence():
    claim_session = _RecordingClaimSession()
    claimed_job = _build_claimed_job()
    order: list[str] = []

    def _assert_claim_committed_before_heartbeat_start():
        assert claim_session.committed is True
        assert claim_session.closed is True

    controller = _FakeHeartbeatController(
        order=order,
        on_start=_assert_claim_committed_before_heartbeat_start,
    )

    worker = _OrderRecordingWorker(
        heartbeat_controller=controller,
        order=order,
        worker_id="worker-test",
        claim_session_factory=lambda: claim_session,
        tenant_session_factory=_RecordingTenantSessionFactory(),
        claim_job_func=lambda **_: claimed_job,
        gee_ndvi_adapter=_FakeGeeAdapter(
            result=_normalized_result_from_claimed_job(claimed_job)
        ),
    )

    result = worker.run_once()

    assert result.status is WorkerRunStatus.SUCCEEDED
    assert order == ["start", "stop", "join", "persist_success"]
    assert controller.started is True
    assert controller.join_calls == 1
    assert controller.alive is False


def test_heartbeat_thread_stops_before_failure_persistence():
    claimed_job = _build_claimed_job()
    order: list[str] = []
    controller = _FakeHeartbeatController(order=order)

    worker = _OrderRecordingWorker(
        heartbeat_controller=controller,
        order=order,
        worker_id="worker-test",
        claim_session_factory=_RecordingClaimSession,
        tenant_session_factory=_RecordingTenantSessionFactory(),
        claim_job_func=lambda **_: claimed_job,
        gee_ndvi_adapter=_FakeGeeAdapter(
            error=SatelliteWorkerExecutionError(
                "gee_execution_failed",
                "controlled gee failure",
            )
        ),
    )

    result = worker.run_once()

    assert result.status is WorkerRunStatus.FAILED
    assert order == ["start", "stop", "join", "persist_failure"]


def test_heartbeat_uses_new_tenant_session_and_not_main_persistence_session(monkeypatch):
    claimed_job = _build_claimed_job()
    order: list[str] = []
    session_factory = _RecordingTenantSessionFactory()
    heartbeat_called = threading.Event()
    heartbeat_sessions: list[_RecordingTenantSession] = []

    def _record_heartbeat(session, **kwargs):
        heartbeat_sessions.append(session)
        heartbeat_called.set()
        return datetime.now(timezone.utc)

    def _wait_for_heartbeat(_request):
        assert heartbeat_called.wait(1)

    monkeypatch.setattr(
        satellite_worker_module,
        "update_satellite_job_heartbeat",
        _record_heartbeat,
    )

    worker = _RealHeartbeatWorker(
        order=order,
        worker_id="worker-test",
        poll_seconds=5,
        claim_session_factory=_RecordingClaimSession,
        tenant_session_factory=session_factory,
        claim_job_func=lambda **_: claimed_job,
        gee_ndvi_adapter=_FakeGeeAdapter(
            result=_normalized_result_from_claimed_job(claimed_job),
            on_execute=_wait_for_heartbeat,
        ),
    )

    result = worker.run_once()

    assert result.status is WorkerRunStatus.SUCCEEDED
    assert len(heartbeat_sessions) >= 1
    assert worker.persistence_session is not None
    assert all(
        heartbeat_session is not worker.persistence_session
        for heartbeat_session in heartbeat_sessions
    )


def test_heartbeat_lease_lost_during_gee_discards_result_without_persisting():
    claimed_job = _build_claimed_job()
    controller = _FakeHeartbeatController()
    order: list[str] = []

    def _lose_lease(_request):
        controller.lease_lost = True

    worker = _OrderRecordingWorker(
        heartbeat_controller=controller,
        order=order,
        worker_id="worker-test",
        claim_session_factory=_RecordingClaimSession,
        tenant_session_factory=_RecordingTenantSessionFactory(),
        claim_job_func=lambda **_: claimed_job,
        gee_ndvi_adapter=_FakeGeeAdapter(
            result=_normalized_result_from_claimed_job(claimed_job),
            on_execute=_lose_lease,
        ),
    )

    result = worker.run_once()

    assert result.status is WorkerRunStatus.LEASE_LOST
    assert result.error_code == "lease_lost"
    assert order == []
    assert controller.started is True
    assert controller.stop_calls == 1
    assert controller.join_calls == 1
    assert controller.alive is False


def test_execution_failure_with_heartbeat_lease_lost_does_not_attempt_failed():
    claimed_job = _build_claimed_job()
    controller = _FakeHeartbeatController()
    order: list[str] = []

    def _lose_lease(_request):
        controller.lease_lost = True

    worker = _OrderRecordingWorker(
        heartbeat_controller=controller,
        order=order,
        worker_id="worker-test",
        claim_session_factory=_RecordingClaimSession,
        tenant_session_factory=_RecordingTenantSessionFactory(),
        claim_job_func=lambda **_: claimed_job,
        gee_ndvi_adapter=_FakeGeeAdapter(
            error=SatelliteWorkerExecutionError(
                "gee_execution_failed",
                "controlled gee failure",
            ),
            on_execute=_lose_lease,
        ),
    )

    result = worker.run_once()

    assert result.status is WorkerRunStatus.LEASE_LOST
    assert result.error_code == "lease_lost"
    assert order == []
    assert controller.started is True
    assert controller.stop_calls == 1
    assert controller.join_calls == 1
    assert controller.alive is False


def test_generic_heartbeat_db_error_is_sanitized_and_retried(monkeypatch, caplog):
    session_factory = _RecordingTenantSessionFactory()
    successful_retry = threading.Event()
    secret_lease_token = str(uuid4())
    call_count = {"value": 0}

    def _heartbeat_once_then_retry(session, **kwargs):
        call_count["value"] += 1
        if call_count["value"] == 1:
            raise RuntimeError(
                "postgresql+psycopg://user:secret@host/db "
                "Bearer super-secret-token"
            )
        successful_retry.set()
        return datetime.now(timezone.utc)

    monkeypatch.setattr(
        satellite_worker_module,
        "update_satellite_job_heartbeat",
        _heartbeat_once_then_retry,
    )

    controller = satellite_worker_module._SatelliteJobHeartbeatController(
        organization_id=1,
        job_id=7,
        job_type=SatelliteJobType.NDVI_TIMESERIES.value,
        worker_id="worker-test",
        lease_token=secret_lease_token,
        heartbeat_seconds=1,
        tenant_session_factory=session_factory,
    )
    controller._heartbeat_seconds = 0.01

    with caplog.at_level(
        logging.WARNING,
        logger=satellite_worker_module.__name__,
    ):
        controller.start()
        assert successful_retry.wait(1)
        controller.stop()
        controller.join()

    assert controller.has_lease_lost() is False
    assert len(session_factory.sessions) >= 2
    assert session_factory.sessions[0].rollbacks == 1
    assert session_factory.sessions[1].commits == 1

    heartbeat_error_records = [
        record
        for record in caplog.records
        if record.getMessage() == "satellite_worker_heartbeat_error"
    ]

    assert len(heartbeat_error_records) == 1
    assert "lease_token" not in heartbeat_error_records[0].__dict__
    assert secret_lease_token not in heartbeat_error_records[0].getMessage()
    assert "secret@host" not in heartbeat_error_records[0].error_message
    assert "Bearer" not in heartbeat_error_records[0].error_message


def test_request_shutdown_during_active_job_does_not_stop_heartbeat_prematurely():
    claimed_job = _build_claimed_job()
    controller = _FakeHeartbeatController()
    started_execution = threading.Event()
    allow_completion = threading.Event()
    results: list = []

    def _block_until_released(_request):
        started_execution.set()
        assert allow_completion.wait(1)

    worker = _OrderRecordingWorker(
        heartbeat_controller=controller,
        order=[],
        worker_id="worker-test",
        claim_session_factory=_RecordingClaimSession,
        tenant_session_factory=_RecordingTenantSessionFactory(),
        claim_job_func=lambda **_: claimed_job,
        gee_ndvi_adapter=_FakeGeeAdapter(
            result=_normalized_result_from_claimed_job(claimed_job),
            on_execute=_block_until_released,
        ),
    )

    thread = threading.Thread(
        target=lambda: results.append(worker.run_once())
    )
    thread.start()

    assert started_execution.wait(1)
    worker.request_shutdown()
    assert controller.stop_calls == 0

    allow_completion.set()
    thread.join(timeout=1)

    assert len(results) == 1
    assert results[0].status is WorkerRunStatus.SUCCEEDED
    assert controller.stop_calls == 1
    assert controller.join_calls == 1


def test_lease_lost_result_and_logs_do_not_expose_lease_token(caplog):
    secret_lease_token = str(uuid4())
    claimed_job = _build_claimed_job(
        lease_token=secret_lease_token
    )
    controller = _FakeHeartbeatController()
    controller.lease_lost = True

    worker = _OrderRecordingWorker(
        heartbeat_controller=controller,
        order=[],
        worker_id="worker-test",
        claim_session_factory=_RecordingClaimSession,
        tenant_session_factory=_RecordingTenantSessionFactory(),
        claim_job_func=lambda **_: claimed_job,
        gee_ndvi_adapter=_FakeGeeAdapter(
            result=_normalized_result_from_claimed_job(claimed_job)
        ),
    )

    with caplog.at_level(
        logging.WARNING,
        logger=satellite_worker_module.__name__,
    ):
        result = worker.run_once()

    assert result.status is WorkerRunStatus.LEASE_LOST
    assert not hasattr(result, "lease_token")

    lease_lost_records = [
        record
        for record in caplog.records
        if record.getMessage() == "satellite_worker_job_lease_lost"
    ]

    assert len(lease_lost_records) == 1
    assert "lease_token" not in lease_lost_records[0].__dict__
    assert secret_lease_token not in lease_lost_records[0].getMessage()


def test_resolve_satellite_worker_heartbeat_seconds_reads_default_and_env(monkeypatch):
    monkeypatch.delenv(
        "SATELLITE_WORKER_HEARTBEAT_SECONDS",
        raising=False,
    )
    assert resolve_satellite_worker_heartbeat_seconds() == 15

    monkeypatch.setenv(
        "SATELLITE_WORKER_HEARTBEAT_SECONDS",
        "27",
    )
    assert resolve_satellite_worker_heartbeat_seconds() == 27
