from __future__ import annotations

import logging
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from sqlalchemy import delete, select
from sqlalchemy.dialects import postgresql

import litoral_trace.services.gee as gee_module
import litoral_trace.services.satellite_ndvi_processing as satellite_ndvi_processing_module
import litoral_trace.workers.satellite_worker as satellite_worker_module
from litoral_trace.db.engine import get_db_session
from litoral_trace.db.models import (
    Lote,
    Organization,
    SatelliteJob,
    SatelliteJobResult,
    SatelliteNdviObservation,
)
from litoral_trace.services.gee import ALGORITHM_VERSION, generate_geometry_hash
from litoral_trace.services.satellite_jobs import ClaimedSatelliteJob, SatelliteJobType
from litoral_trace.services.satellite_ndvi_processing import RetryScheduleResult
from litoral_trace.workers.satellite_worker import (
    EarthEngineGeeNdviAdapter,
    NdviExecutionRequest,
    RetryDisposition,
    SatelliteWorker,
    SatelliteWorkerExecutionError,
    WorkerRunResult,
    WorkerRunStatus,
    main,
    resolve_satellite_worker_retry_base_seconds,
    resolve_satellite_worker_retry_max_seconds,
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
    def __init__(self, *, order: list[str] | None = None):
        self.order = order if order is not None else []
        self.lease_lost = False
        self.started = False
        self.stop_calls = 0
        self.join_calls = 0
        self.alive = False

    def start(self):
        self.started = True
        self.alive = True
        self.order.append("start")

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
        self._heartbeat_controller = heartbeat_controller

    def _create_heartbeat_controller(self, context):
        self.injected_context = context
        return self._heartbeat_controller


class _OrderRecordingRetryWorker(_InjectedHeartbeatWorker):
    def __init__(self, *, heartbeat_controller, order: list[str], **kwargs):
        super().__init__(heartbeat_controller=heartbeat_controller, **kwargs)
        self.order = order

    def _schedule_retry(self, context, *, retry_delay_seconds: int):
        self.order.append("schedule_retry")
        return SimpleNamespace(
            next_attempt_at=datetime.now(timezone.utc)
            + timedelta(seconds=retry_delay_seconds)
        )


def _cleanup_p22d4_entities() -> None:
    session = get_db_session()
    org_ids = session.execute(
        select(Organization.id).where(
            Organization.slug.like("worker-d4-org-%")
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


@pytest.fixture(autouse=True)
def cleanup_p22d4_entities():
    _cleanup_p22d4_entities()
    yield
    _cleanup_p22d4_entities()


def _normalize_dt(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _create_job_fixture(
    *,
    status: str = "running",
    attempt_count: int = 1,
    max_attempts: int = 3,
    worker_id: str = "worker-d4",
    lease_token: str | None = None,
    error_code: str | None = None,
    error_message: str | None = None,
) -> _JobFixture:
    session = get_db_session()
    suffix = uuid4().hex[:8]
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

    organization = Organization(
        name=f"Worker D4 Org {suffix}",
        slug=f"worker-d4-org-{suffix}",
        tax_id=f"84-{suffix}",
        tier="pro",
        is_active=True,
    )
    session.add(organization)
    session.flush()

    lote = Lote(
        organization_id=organization.id,
        identificador=f"WORKER-D4-LOTE-{suffix}",
        productor_id=f"34-{suffix}",
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

    locked_by = worker_id if status == "running" else None
    locked_at = now if status == "running" else None
    heartbeat_at = now if status == "running" else None
    started_at = now - timedelta(minutes=5)
    finished_at = now if status in {"succeeded", "failed"} else None

    job = SatelliteJob(
        organization_id=organization.id,
        lote_id=lote.id,
        job_type=SatelliteJobType.NDVI_TIMESERIES.value,
        status=status,
        attempt_count=attempt_count,
        max_attempts=max_attempts,
        request_start_date=date(2026, 7, 1),
        request_end_date=date(2026, 8, 1),
        max_cloud_pct=20.0,
        geometry_hash=geometry_hash,
        algorithm_version=ALGORITHM_VERSION,
        polygon_wkt_snapshot=polygon_wkt_snapshot,
        next_attempt_at=now - timedelta(minutes=1),
        locked_at=locked_at,
        locked_by=locked_by,
        heartbeat_at=heartbeat_at,
        lease_token=normalized_lease_token if status == "running" else None,
        started_at=started_at,
        finished_at=finished_at,
        error_code=error_code,
        error_message=error_message,
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
        lease_token=UUID(str(job.lease_token or normalized_lease_token)),
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
        lease_token=str(job.lease_token or normalized_lease_token),
        claimed_job=claimed_job,
    )
    session.close()
    return fixture


def _load_job(job_id: int) -> SatelliteJob:
    session = get_db_session()
    job = session.execute(
        select(SatelliteJob).where(SatelliteJob.id == job_id)
    ).scalar_one()
    session.expunge(job)
    session.close()
    return job


def _load_observations(job_id: int) -> list[SatelliteNdviObservation]:
    session = get_db_session()
    rows = session.execute(
        select(SatelliteNdviObservation).where(
            SatelliteNdviObservation.satellite_job_id == job_id
        )
    ).scalars().all()
    for row in rows:
        session.expunge(row)
    session.close()
    return rows


def _replace_job_lease(
    job_id: int,
    *,
    new_worker_id: str | None = None,
    new_status: str | None = None,
) -> str:
    session = get_db_session()
    job = session.execute(
        select(SatelliteJob).where(SatelliteJob.id == job_id)
    ).scalar_one()
    new_lease_token = str(uuid4())
    job.lease_token = new_lease_token
    if new_worker_id is not None:
        job.locked_by = new_worker_id
    if new_status is not None:
        job.status = new_status
    session.commit()
    session.close()
    return new_lease_token


def _retryable_error(
    error_code: str = "gee_temporary_service_failure",
    message: str = "temporary upstream failure",
) -> SatelliteWorkerExecutionError:
    return SatelliteWorkerExecutionError(
        error_code,
        message,
        retry_disposition=RetryDisposition.RETRYABLE,
    )


def test_initialize_earth_engine_keeps_legacy_tuple_contract(monkeypatch):
    monkeypatch.setattr(
        gee_module,
        "_initialize_earth_engine_detailed",
        lambda: gee_module.GeeInitializationResult(
            success=True,
            detail_message="legacy contract",
            initialization_time_ms=42,
        ),
    )

    result = gee_module.initialize_earth_engine()

    assert isinstance(result, tuple)
    assert result == (True, "legacy contract", 42)


def test_detailed_initialize_preserves_machine_readable_failure(monkeypatch):
    fake_settings = SimpleNamespace(
        gee=SimpleNamespace(
            project_id="test-project",
            service_account_json=None,
        )
    )

    class _FakeEeModule:
        @staticmethod
        def Initialize(*args, **kwargs):
            raise PermissionError("structured permission failure")

    monkeypatch.setattr(gee_module, "get_settings", lambda: fake_settings)
    monkeypatch.setitem(sys.modules, "ee", _FakeEeModule)

    result = gee_module._initialize_earth_engine_detailed()

    assert result.success is False
    assert result.failure is not None
    assert result.failure.category == gee_module.GeeFailureCategory.PERMISSION
    assert result.failure.error_code == "gee_permission_denied"


def test_legacy_gee_error_dict_fields_are_preserved_and_new_fields_are_additive(monkeypatch):
    monkeypatch.setattr(
        gee_module,
        "_initialize_earth_engine_detailed",
        lambda: gee_module.GeeInitializationResult(
            success=False,
            detail_message="structured init failure",
            initialization_time_ms=17,
            failure=gee_module.GeeFailureInfo(
                category=gee_module.GeeFailureCategory.TEMPORARY_SERVICE,
                error_code="gee_temporary_service_failure",
                detail_message="structured init failure",
            ),
        ),
    )
    monkeypatch.setattr(
        gee_module,
        "get_settings",
        lambda: SimpleNamespace(
            is_test=False,
            gee=SimpleNamespace(test_mode=False),
        ),
    )

    result = gee_module.consultar_serie_temporal_ndvi_gee(
        polygon_wkt="POLYGON((-58 -27, -57 -27, -57 -26, -58 -26, -58 -27))"
    )

    assert result["status"] == "error"
    assert result["gee_connected"] is False
    assert "error_detail" in result
    assert "geometry_hash" in result
    assert result["total_observations"] == 0
    assert result["gee_initialization_ms"] == 17
    assert result["gee_query_ms"] == 0
    assert result["observations"] == []
    assert result["error_code"] == "gee_temporary_service_failure"
    assert result["error_category"] == "temporary_service"


def test_unknown_structured_gee_error_remains_non_retryable_even_if_message_looks_transient(monkeypatch):
    request = NdviExecutionRequest(
        polygon_wkt_snapshot="POLYGON((-58 -27, -57 -27, -57 -26, -58 -26, -58 -27))",
        start_date="2026-07-01",
        end_date="2026-08-01",
        max_cloud_pct=20.0,
        geometry_hash="a" * 64,
        algorithm_version=ALGORITHM_VERSION,
    )

    monkeypatch.setattr(
        satellite_worker_module,
        "consultar_serie_temporal_ndvi_gee",
        lambda **_: {
            "status": "error",
            "gee_connected": True,
            "error_code": "gee_execution_failed",
            "error_category": "unknown",
            "error_detail": "429 timeout quota temporary",
        },
    )

    with pytest.raises(SatelliteWorkerExecutionError) as exc_info:
        EarthEngineGeeNdviAdapter().execute(request)

    assert exc_info.value.retry_disposition == RetryDisposition.NON_RETRYABLE


def test_retry_schedule_uses_statement_timestamp_and_parameterized_delay(monkeypatch):
    captured = {}

    class _FakeBind:
        dialect = SimpleNamespace(name="postgresql")

    class _FakeSession:
        def get_bind(self):
            return _FakeBind()

    def _capture_finalize(db_session, **kwargs):
        captured["values"] = kwargs["values"]
        return SimpleNamespace(
            next_attempt_at=datetime.now(timezone.utc) + timedelta(seconds=30)
        )

    monkeypatch.setattr(
        satellite_ndvi_processing_module,
        "_finalize_satellite_job_with_lease_fencing",
        _capture_finalize,
    )

    satellite_ndvi_processing_module.schedule_satellite_job_retry(
        _FakeSession(),
        organization_id=1,
        job_id=2,
        worker_id="worker-d4",
        lease_token=str(uuid4()),
        retry_delay_seconds=30,
    )

    compiled_next_attempt = str(
        captured["values"]["next_attempt_at"].compile(
            dialect=postgresql.dialect()
        )
    )
    compiled_updated_at = str(
        captured["values"]["updated_at"].compile(
            dialect=postgresql.dialect()
        )
    )

    assert "statement_timestamp()" in compiled_next_attempt
    assert "make_interval" in compiled_next_attempt
    assert "retry_delay_seconds" in compiled_next_attempt
    assert "30 seconds" not in compiled_next_attempt
    assert compiled_updated_at == "statement_timestamp()"


def test_retry_disposition_is_explicit_and_unknown_defaults_to_non_retryable():
    retryable_error = SatelliteWorkerExecutionError(
        "gee_temporary_service_failure",
        "temporary failure",
        retry_disposition=RetryDisposition.RETRYABLE,
    )
    default_error = SatelliteWorkerExecutionError(
        "gee_execution_failed",
        "unclassified failure",
    )

    assert retryable_error.retry_disposition == RetryDisposition.RETRYABLE
    assert default_error.retry_disposition == RetryDisposition.NON_RETRYABLE


def test_adapter_uses_machine_readable_error_category_without_string_matching(monkeypatch):
    request = NdviExecutionRequest(
        polygon_wkt_snapshot="POLYGON((-58 -27, -57 -27, -57 -26, -58 -26, -58 -27))",
        start_date="2026-07-01",
        end_date="2026-08-01",
        max_cloud_pct=20.0,
        geometry_hash="a" * 64,
        algorithm_version=ALGORITHM_VERSION,
    )

    monkeypatch.setattr(
        satellite_worker_module,
        "consultar_serie_temporal_ndvi_gee",
        lambda **_: {
            "status": "error",
            "gee_connected": True,
            "error_code": "gee_execution_failed",
            "error_category": "unknown",
            "error_detail": "timeout text but unclassified",
        },
    )

    adapter = EarthEngineGeeNdviAdapter()

    with pytest.raises(SatelliteWorkerExecutionError) as exc_info:
        adapter.execute(request)

    assert exc_info.value.retry_disposition == RetryDisposition.NON_RETRYABLE


def test_adapter_marks_explicit_transient_category_as_retryable(monkeypatch):
    request = NdviExecutionRequest(
        polygon_wkt_snapshot="POLYGON((-58 -27, -57 -27, -57 -26, -58 -26, -58 -27))",
        start_date="2026-07-01",
        end_date="2026-08-01",
        max_cloud_pct=20.0,
        geometry_hash="a" * 64,
        algorithm_version=ALGORITHM_VERSION,
    )

    monkeypatch.setattr(
        satellite_worker_module,
        "consultar_serie_temporal_ndvi_gee",
        lambda **_: {
            "status": "error",
            "gee_connected": True,
            "error_code": "gee_temporary_service_failure",
            "error_category": "temporary_service",
            "error_detail": "503 upstream failure",
        },
    )

    adapter = EarthEngineGeeNdviAdapter()

    with pytest.raises(SatelliteWorkerExecutionError) as exc_info:
        adapter.execute(request)

    assert exc_info.value.error_code == "gee_temporary_service_failure"
    assert exc_info.value.retry_disposition == RetryDisposition.RETRYABLE


def test_simulated_fallback_remains_non_retryable(monkeypatch):
    request = NdviExecutionRequest(
        polygon_wkt_snapshot="POLYGON((-58 -27, -57 -27, -57 -26, -58 -26, -58 -27))",
        start_date="2026-07-01",
        end_date="2026-08-01",
        max_cloud_pct=20.0,
        geometry_hash="a" * 64,
        algorithm_version=ALGORITHM_VERSION,
    )

    monkeypatch.setattr(
        satellite_worker_module,
        "consultar_serie_temporal_ndvi_gee",
        lambda **_: {
            "status": "success",
            "gee_connected": False,
            "geometry_hash": "a" * 64,
            "algorithm_version": ALGORITHM_VERSION,
            "observations": (),
        },
    )

    adapter = EarthEngineGeeNdviAdapter()

    with pytest.raises(SatelliteWorkerExecutionError) as exc_info:
        adapter.execute(request)

    assert exc_info.value.error_code == "gee_execution_failed"
    assert exc_info.value.retry_disposition == RetryDisposition.NON_RETRYABLE


def test_backoff_progression_and_cap_are_exact():
    worker = SatelliteWorker(
        worker_id="worker-d4-test",
        retry_base_seconds=30,
        retry_max_seconds=900,
        claim_session_factory=_RecordingClaimSession,
        claim_job_func=lambda **_: None,
    )

    assert worker._calculate_retry_delay_seconds(attempt_count=1) == 30
    assert worker._calculate_retry_delay_seconds(attempt_count=2) == 60
    assert worker._calculate_retry_delay_seconds(attempt_count=3) == 120
    assert worker._calculate_retry_delay_seconds(attempt_count=1000) == 900


def test_absurdly_large_attempt_count_remains_overflow_safe():
    worker = SatelliteWorker(
        worker_id="worker-d4-test",
        retry_base_seconds=30,
        retry_max_seconds=900,
        claim_session_factory=_RecordingClaimSession,
        claim_job_func=lambda **_: None,
    )

    assert worker._calculate_retry_delay_seconds(attempt_count=10**9) == 900


def test_custom_retry_config_and_invariant_validation(monkeypatch):
    monkeypatch.delenv("SATELLITE_WORKER_RETRY_BASE_SECONDS", raising=False)
    monkeypatch.delenv("SATELLITE_WORKER_RETRY_MAX_SECONDS", raising=False)
    assert resolve_satellite_worker_retry_base_seconds() == 30
    assert resolve_satellite_worker_retry_max_seconds() == 900

    monkeypatch.setenv("SATELLITE_WORKER_RETRY_BASE_SECONDS", "45")
    monkeypatch.setenv("SATELLITE_WORKER_RETRY_MAX_SECONDS", "720")
    assert resolve_satellite_worker_retry_base_seconds() == 45
    assert resolve_satellite_worker_retry_max_seconds() == 720

    with pytest.raises(RuntimeError, match="mayor o igual"):
        SatelliteWorker(
            worker_id="worker-d4-test",
            retry_base_seconds=120,
            retry_max_seconds=60,
            claim_session_factory=_RecordingClaimSession,
            claim_job_func=lambda **_: None,
        )


def test_retryable_attempt_remaining_schedules_retry_and_clears_active_state(caplog):
    fixture = _create_job_fixture(attempt_count=1, max_attempts=3)
    heartbeat_controller = _FakeHeartbeatController()
    claim_session = _RecordingClaimSession()

    worker = _InjectedHeartbeatWorker(
        heartbeat_controller=heartbeat_controller,
        worker_id=fixture.worker_id,
        claim_session_factory=lambda: claim_session,
        tenant_session_factory=get_db_session,
        claim_job_func=lambda **_: fixture.claimed_job,
        gee_ndvi_adapter=_FakeGeeAdapter(
            error=_retryable_error(
                message=(
                    "postgresql+psycopg://user:secret@host/db "
                    "Bearer hidden-secret"
                )
            )
        ),
    )

    with caplog.at_level(logging.INFO, logger=satellite_worker_module.__name__):
        result = worker.run_once()

    job = _load_job(fixture.job_id)
    retry_logs = [
        record for record in caplog.records
        if record.getMessage() == "satellite_worker_retry_scheduled"
    ]

    assert result.status is WorkerRunStatus.RETRY_SCHEDULED
    assert result.error_code == "gee_temporary_service_failure"
    assert claim_session.committed is True
    assert heartbeat_controller.stop_calls == 1
    assert heartbeat_controller.join_calls == 1
    assert job.status == "queued"
    assert job.attempt_count == 1
    assert job.started_at == fixture.claimed_job.started_at
    assert job.locked_at is None
    assert job.locked_by is None
    assert job.heartbeat_at is None
    assert job.lease_token is None
    assert job.error_code is None
    assert job.error_message is None
    assert _normalize_dt(job.next_attempt_at) > datetime.now(timezone.utc)
    assert _load_observations(fixture.job_id) == []
    assert len(retry_logs) == 1
    assert retry_logs[0].retry_delay_seconds == 30
    assert "lease_token" not in retry_logs[0].__dict__
    assert "secret@host" not in retry_logs[0].getMessage()
    assert "Bearer" not in retry_logs[0].getMessage()


def test_retry_scheduled_sets_future_next_attempt_at():
    fixture = _create_job_fixture(attempt_count=2, max_attempts=4)
    heartbeat_controller = _FakeHeartbeatController()
    before_run = datetime.now(timezone.utc)

    worker = _InjectedHeartbeatWorker(
        heartbeat_controller=heartbeat_controller,
        worker_id=fixture.worker_id,
        claim_session_factory=_RecordingClaimSession,
        tenant_session_factory=get_db_session,
        claim_job_func=lambda **_: fixture.claimed_job,
        gee_ndvi_adapter=_FakeGeeAdapter(error=_retryable_error()),
    )

    result = worker.run_once()
    job = _load_job(fixture.job_id)

    assert result.status is WorkerRunStatus.RETRY_SCHEDULED
    assert _normalize_dt(job.next_attempt_at) > before_run


def test_retry_scheduling_with_wrong_lease_returns_lease_lost():
    fixture = _create_job_fixture(attempt_count=1, max_attempts=3)
    _replace_job_lease(fixture.job_id)
    heartbeat_controller = _FakeHeartbeatController()

    worker = _InjectedHeartbeatWorker(
        heartbeat_controller=heartbeat_controller,
        worker_id=fixture.worker_id,
        claim_session_factory=_RecordingClaimSession,
        tenant_session_factory=get_db_session,
        claim_job_func=lambda **_: fixture.claimed_job,
        gee_ndvi_adapter=_FakeGeeAdapter(error=_retryable_error()),
    )

    result = worker.run_once()
    job = _load_job(fixture.job_id)

    assert result.status is WorkerRunStatus.LEASE_LOST
    assert result.error_code == "lease_lost"
    assert job.status == "running"
    assert job.finished_at is None


def test_retry_scheduling_with_wrong_worker_returns_lease_lost():
    fixture = _create_job_fixture(attempt_count=1, max_attempts=3)
    _replace_job_lease(
        fixture.job_id,
        new_worker_id="worker-d4-other",
    )
    heartbeat_controller = _FakeHeartbeatController()

    worker = _InjectedHeartbeatWorker(
        heartbeat_controller=heartbeat_controller,
        worker_id=fixture.worker_id,
        claim_session_factory=_RecordingClaimSession,
        tenant_session_factory=get_db_session,
        claim_job_func=lambda **_: fixture.claimed_job,
        gee_ndvi_adapter=_FakeGeeAdapter(error=_retryable_error()),
    )

    result = worker.run_once()

    assert result.status is WorkerRunStatus.LEASE_LOST
    assert result.error_code == "lease_lost"


def test_retry_scheduling_on_non_running_job_returns_lease_lost():
    fixture = _create_job_fixture(attempt_count=1, max_attempts=3)
    _replace_job_lease(
        fixture.job_id,
        new_status="failed",
    )
    heartbeat_controller = _FakeHeartbeatController()

    worker = _InjectedHeartbeatWorker(
        heartbeat_controller=heartbeat_controller,
        worker_id=fixture.worker_id,
        claim_session_factory=_RecordingClaimSession,
        tenant_session_factory=get_db_session,
        claim_job_func=lambda **_: fixture.claimed_job,
        gee_ndvi_adapter=_FakeGeeAdapter(error=_retryable_error()),
    )

    result = worker.run_once()

    assert result.status is WorkerRunStatus.LEASE_LOST
    assert result.error_code == "lease_lost"


def test_non_retryable_failure_is_terminal_even_when_attempts_remain():
    fixture = _create_job_fixture(attempt_count=1, max_attempts=3)
    heartbeat_controller = _FakeHeartbeatController()

    worker = _InjectedHeartbeatWorker(
        heartbeat_controller=heartbeat_controller,
        worker_id=fixture.worker_id,
        claim_session_factory=_RecordingClaimSession,
        tenant_session_factory=get_db_session,
        claim_job_func=lambda **_: fixture.claimed_job,
        gee_ndvi_adapter=_FakeGeeAdapter(
            error=SatelliteWorkerExecutionError(
                "invalid_job_payload",
                "payload inconsistente",
                retry_disposition=RetryDisposition.NON_RETRYABLE,
            )
        ),
    )

    result = worker.run_once()
    job = _load_job(fixture.job_id)

    assert result.status is WorkerRunStatus.FAILED
    assert result.error_code == "invalid_job_payload"
    assert job.status == "failed"
    assert str(job.lease_token) == fixture.lease_token


def test_retryable_exhausted_becomes_terminal_failed_and_retains_terminal_lease(caplog):
    fixture = _create_job_fixture(attempt_count=3, max_attempts=3)
    heartbeat_controller = _FakeHeartbeatController()

    worker = _InjectedHeartbeatWorker(
        heartbeat_controller=heartbeat_controller,
        worker_id=fixture.worker_id,
        claim_session_factory=_RecordingClaimSession,
        tenant_session_factory=get_db_session,
        claim_job_func=lambda **_: fixture.claimed_job,
        gee_ndvi_adapter=_FakeGeeAdapter(error=_retryable_error()),
    )

    with caplog.at_level(logging.WARNING, logger=satellite_worker_module.__name__):
        result = worker.run_once()

    job = _load_job(fixture.job_id)
    exhausted_logs = [
        record for record in caplog.records
        if record.getMessage() == "satellite_worker_retry_exhausted"
    ]

    assert result.status is WorkerRunStatus.FAILED
    assert result.error_code == "gee_temporary_service_failure"
    assert job.status == "failed"
    assert str(job.lease_token) == fixture.lease_token
    assert job.error_code == "gee_temporary_service_failure"
    assert len(exhausted_logs) == 1


def test_heartbeat_stops_before_retry_persistence():
    fixture = _create_job_fixture(attempt_count=1, max_attempts=3)
    order: list[str] = []
    heartbeat_controller = _FakeHeartbeatController(order=order)

    worker = _OrderRecordingRetryWorker(
        heartbeat_controller=heartbeat_controller,
        order=order,
        worker_id=fixture.worker_id,
        claim_session_factory=_RecordingClaimSession,
        tenant_session_factory=get_db_session,
        claim_job_func=lambda **_: fixture.claimed_job,
        gee_ndvi_adapter=_FakeGeeAdapter(error=_retryable_error()),
    )

    result = worker.run_once()

    assert result.status is WorkerRunStatus.RETRY_SCHEDULED
    assert order == ["start", "stop", "join", "schedule_retry"]


def test_heartbeat_lease_loss_beats_retry_scheduling():
    fixture = _create_job_fixture(attempt_count=1, max_attempts=3)
    heartbeat_controller = _FakeHeartbeatController()

    def _lose_lease(_request):
        heartbeat_controller.lease_lost = True

    worker = _InjectedHeartbeatWorker(
        heartbeat_controller=heartbeat_controller,
        worker_id=fixture.worker_id,
        claim_session_factory=_RecordingClaimSession,
        tenant_session_factory=get_db_session,
        claim_job_func=lambda **_: fixture.claimed_job,
        gee_ndvi_adapter=_FakeGeeAdapter(
            error=_retryable_error(),
            on_execute=_lose_lease,
        ),
    )

    result = worker.run_once()
    job = _load_job(fixture.job_id)

    assert result.status is WorkerRunStatus.LEASE_LOST
    assert result.error_code == "lease_lost"
    assert job.status == "running"
    assert job.finished_at is None


def test_generic_exception_remains_non_retryable_and_sanitized():
    fixture = _create_job_fixture(attempt_count=1, max_attempts=3)
    heartbeat_controller = _FakeHeartbeatController()

    worker = _InjectedHeartbeatWorker(
        heartbeat_controller=heartbeat_controller,
        worker_id=fixture.worker_id,
        claim_session_factory=_RecordingClaimSession,
        tenant_session_factory=get_db_session,
        claim_job_func=lambda **_: fixture.claimed_job,
        gee_ndvi_adapter=_FakeGeeAdapter(
            error=RuntimeError(
                "postgresql+psycopg://user:secret@host/db Bearer super-secret"
            )
        ),
    )

    result = worker.run_once()
    job = _load_job(fixture.job_id)

    assert result.status is WorkerRunStatus.FAILED
    assert result.error_code == "worker_execution_failed"
    assert job.error_code == "worker_execution_failed"
    assert "secret@host" not in (job.error_message or "")
    assert "Bearer" not in (job.error_message or "")


def test_graceful_shutdown_behavior_remains_unchanged():
    claim_calls: list[str] = []
    worker = SatelliteWorker(
        worker_id="worker-d4-shutdown",
        retry_base_seconds=30,
        retry_max_seconds=900,
        claim_session_factory=_RecordingClaimSession,
        claim_job_func=lambda **_: claim_calls.append("claim") or None,
    )
    worker.request_shutdown()

    result = worker.run_once()

    assert result.status is WorkerRunStatus.STOPPED
    assert claim_calls == []


def test_once_mode_returns_zero_for_retry_scheduled(monkeypatch):
    fake_worker = SimpleNamespace(
        run_once=lambda: WorkerRunResult(
            status=WorkerRunStatus.RETRY_SCHEDULED,
            error_code="gee_temporary_service_failure",
        )
    )

    monkeypatch.setattr(
        satellite_worker_module,
        "build_satellite_worker",
        lambda: fake_worker,
    )
    monkeypatch.setattr(
        satellite_worker_module,
        "_install_signal_handlers",
        lambda worker: None,
    )

    assert main(["--once"]) == 0
