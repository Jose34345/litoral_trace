from __future__ import annotations

from types import SimpleNamespace

import pytest

import litoral_trace.workers.satellite_worker as worker_module
from litoral_trace.services.audit import (
    AuditAction,
    sanitize_audit_detail,
    sanitize_audit_metadata,
)
from litoral_trace.workers.satellite_worker import (
    SatelliteWorker,
    SatelliteWorkerExecutionError,
    WorkerExecutionContext,
    WorkerRunStatus,
    _SatelliteJobHeartbeatController,
    build_satellite_job_failure_audit_metadata,
    build_satellite_job_success_audit_metadata,
    build_satellite_worker_audit_actor,
)


class _RecordingSession:
    def __init__(self, events: list[str] | None = None, *, fail_commit=False):
        self.events = events if events is not None else []
        self.fail_commit = fail_commit
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def commit(self):
        self.events.append("commit")
        if self.fail_commit:
            raise RuntimeError("commit blocked")
        self.committed = True

    def rollback(self):
        self.events.append("rollback")
        self.rolled_back = True

    def close(self):
        self.events.append("close")
        self.closed = True


class _Heartbeat:
    def start(self):
        return None

    def stop(self):
        return None

    def join(self):
        return None

    def has_lease_lost(self):
        return False


def _claimed_job(**overrides):
    values = {
        "id": 41,
        "organization_id": 7,
        "lote_id": 19,
        "job_type": "ndvi_timeseries",
        "attempt_count": 2,
        "max_attempts": 4,
        "lease_token": "11111111-1111-4111-8111-111111111111",
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _context(**overrides):
    claimed_job = overrides.pop("claimed_job", _claimed_job())
    values = {
        "job_id": claimed_job.id,
        "organization_id": claimed_job.organization_id,
        "job_type": claimed_job.job_type,
        "worker_id": "worker-ephemeral-1",
        "lease_token": str(claimed_job.lease_token),
        "claimed_job": claimed_job,
    }
    values.update(overrides)
    return WorkerExecutionContext(**values)


def _result(observation_count: int = 2):
    return SimpleNamespace(
        geometry_hash="safe-hash",
        algorithm_version="v1",
        observations=tuple(object() for _ in range(observation_count)),
    )


def _worker(*, claim_session_factory=lambda: _RecordingSession(), claim_func=None):
    return SatelliteWorker(
        worker_id="worker-ephemeral-1",
        claim_session_factory=claim_session_factory,
        tenant_session_factory=lambda: _RecordingSession(),
        claim_job_func=claim_func or (lambda **_kwargs: None),
        stale_recovery_interval_seconds=None,
    )


def _patch_success_dependencies(monkeypatch, events, audit_callback):
    monkeypatch.setattr(
        worker_module,
        "persist_ndvi_execution_result",
        lambda session, **kwargs: events.append("observations"),
    )
    monkeypatch.setattr(
        worker_module,
        "build_satellite_job_result_snapshot",
        lambda **kwargs: object(),
    )
    monkeypatch.setattr(
        worker_module,
        "persist_satellite_job_result",
        lambda session, **kwargs: events.append("result"),
    )
    monkeypatch.setattr(
        worker_module,
        "mark_satellite_job_succeeded",
        lambda session, **kwargs: events.append("succeeded"),
    )
    monkeypatch.setattr(worker_module, "record_audit_event", audit_callback)


def test_terminal_worker_audit_actions_are_explicit():
    assert AuditAction.SATELLITE_JOB_SUCCEEDED.value == "satellite.job.succeeded"
    assert AuditAction.SATELLITE_JOB_FAILED.value == "satellite.job.failed"


def test_machine_actor_is_stable_and_contains_no_process_identity():
    actor = build_satellite_worker_audit_actor(7)

    assert actor.organization_id == 7
    assert actor.user_id is None
    assert actor.username == "satellite-worker"
    assert actor.role == "system_worker"


def test_worker_terminal_metadata_uses_strict_event_whitelists():
    context = _context()

    success = build_satellite_job_success_audit_metadata(context, _result(3))
    failure = build_satellite_job_failure_audit_metadata(
        context,
        error_code="provider_timeout",
    )

    assert set(success) == {
        "job_type",
        "attempt_count",
        "max_attempts",
        "observation_count",
    }
    assert set(failure) == {
        "job_type",
        "attempt_count",
        "max_attempts",
        "error_code",
    }
    forbidden = {
        "lease_token",
        "idempotency_key",
        "polygon_wkt_snapshot",
        "credentials",
        "worker_id",
        "error_message",
        "algorithm_version",
    }
    assert forbidden.isdisjoint(success)
    assert forbidden.isdisjoint(failure)


def test_central_metadata_sanitizer_removes_new_sensitive_keys_recursively():
    sanitized = sanitize_audit_metadata(
        {
            "safe": "retained",
            "lease_token": "lease-secret",
            "WORKER_DATABASE_URL": "postgresql://secret",
            "private_key": "private-secret",
            "service_account_json": {"secret": "value"},
            "credentials": "credential-secret",
            "polygon_wkt_snapshot": "POLYGON((secret))",
            "idempotency_key": "idempotency-secret",
            "nested": {"Lease_Token": "nested-secret", "count": 2},
        }
    )

    assert sanitized == {"safe": "retained", "nested": {"count": 2}}


@pytest.mark.parametrize(
    "unsafe_detail",
    [
        "postgresql://runtime:password@db.example/app",
        "Authorization=Bearer abc.def.secret",
        "password=secret-value",
        "-----BEGIN PRIVATE KEY-----secret-----END PRIVATE KEY-----",
        "Traceback (most recent call last):\nRuntimeError: secret",
        "OperationalError('server rejected password=secret')",
    ],
)
def test_detail_sanitizer_never_retains_obvious_secret_bearing_text(unsafe_detail):
    sanitized = sanitize_audit_detail(unsafe_detail)

    assert sanitized is not None
    assert "secret" not in sanitized.lower()
    assert "password@" not in sanitized.lower()


def test_success_audit_uses_same_session_before_commit(monkeypatch):
    events: list[str] = []
    session = _RecordingSession(events)

    def audit(audit_session, **kwargs):
        assert audit_session is session
        assert kwargs["action"] is AuditAction.SATELLITE_JOB_SUCCEEDED
        assert kwargs["entity_type"] == "satellite_job"
        assert kwargs["entity_id"] == 41
        assert kwargs["detail"] == "Satellite job completed successfully."
        events.append("audit")

    _patch_success_dependencies(monkeypatch, events, audit)
    worker = _worker()
    worker._tenant_session_factory = lambda: session

    worker._persist_success(_context(), _result())

    assert events == ["observations", "result", "succeeded", "audit", "commit", "close"]


def test_failure_audit_uses_same_session_before_commit(monkeypatch):
    events: list[str] = []
    session = _RecordingSession(events)
    monkeypatch.setattr(
        worker_module,
        "mark_satellite_job_failed",
        lambda failed_session, **kwargs: (
            failed_session is session and events.append("failed")
        ),
    )

    def audit(audit_session, **kwargs):
        assert audit_session is session
        assert kwargs["action"] is AuditAction.SATELLITE_JOB_FAILED
        assert kwargs["metadata"]["error_code"] == "provider_error"
        assert kwargs["detail"] == "Satellite job failed."
        events.append("audit")

    monkeypatch.setattr(worker_module, "record_audit_event", audit)
    worker = _worker()
    worker._tenant_session_factory = lambda: session

    worker._persist_failure(
        _context(),
        error_code="provider_error",
        error_message="safe provider failure",
    )

    assert events == ["failed", "audit", "commit", "close"]


def test_success_audit_failure_rolls_back_entire_transaction(monkeypatch):
    events: list[str] = []
    session = _RecordingSession(events)

    def fail_audit(_session, **_kwargs):
        events.append("audit")
        raise RuntimeError("audit insert blocked")

    _patch_success_dependencies(monkeypatch, events, fail_audit)
    worker = _worker()
    worker._tenant_session_factory = lambda: session

    with pytest.raises(RuntimeError, match="audit insert blocked"):
        worker._persist_success(_context(), _result())

    assert events == [
        "observations",
        "result",
        "succeeded",
        "audit",
        "rollback",
        "close",
    ]
    assert session.committed is False


def test_failure_audit_failure_rolls_back_terminal_transition(monkeypatch):
    events: list[str] = []
    session = _RecordingSession(events)
    monkeypatch.setattr(
        worker_module,
        "mark_satellite_job_failed",
        lambda _session, **_kwargs: events.append("failed"),
    )

    def fail_audit(_session, **_kwargs):
        events.append("audit")
        raise RuntimeError("audit insert blocked")

    monkeypatch.setattr(worker_module, "record_audit_event", fail_audit)
    worker = _worker()
    worker._tenant_session_factory = lambda: session

    with pytest.raises(RuntimeError, match="audit insert blocked"):
        worker._persist_failure(
            _context(),
            error_code="provider_error",
            error_message="safe provider failure",
        )

    assert events == ["failed", "audit", "rollback", "close"]
    assert session.committed is False


def test_claim_log_is_emitted_after_commit_and_omits_lease(monkeypatch):
    session = _RecordingSession()
    claimed_job = _claimed_job()
    logged = []
    worker = _worker(
        claim_session_factory=lambda: session,
        claim_func=lambda **_kwargs: claimed_job,
    )

    def capture_log(message, *, extra):
        assert session.committed is True
        logged.append((message, extra))

    monkeypatch.setattr(worker_module.logger, "info", capture_log)
    monkeypatch.setattr(
        worker,
        "_build_context",
        lambda _job: (_ for _ in ()).throw(RuntimeError("stop after claim log")),
    )

    with pytest.raises(RuntimeError, match="stop after claim log"):
        worker.run_once()

    assert logged[0][0] == "satellite_worker_job_claimed"
    assert set(logged[0][1]) == {
        "job_id",
        "organization_id",
        "job_type",
        "worker_id",
        "attempt_count",
        "max_attempts",
    }
    assert "lease_token" not in logged[0][1]


def test_failed_claim_commit_emits_no_claim_log(monkeypatch):
    session = _RecordingSession(fail_commit=True)
    worker = _worker(
        claim_session_factory=lambda: session,
        claim_func=lambda **_kwargs: _claimed_job(),
    )
    logged = []
    monkeypatch.setattr(
        worker_module.logger,
        "info",
        lambda *args, **kwargs: logged.append((args, kwargs)),
    )

    with pytest.raises(RuntimeError, match="commit blocked"):
        worker.run_once()

    assert logged == []
    assert session.rolled_back is True


@pytest.mark.parametrize(
    ("handler_error", "expected_message"),
    [
        (None, "satellite_worker_job_succeeded"),
        (
            SatelliteWorkerExecutionError("invalid_request", "safe failure"),
            "satellite_worker_job_failed",
        ),
    ],
)
def test_terminal_operational_logs_include_attempt_limits(
    monkeypatch,
    handler_error,
    expected_message,
):
    claimed_job = _claimed_job()
    worker = _worker(claim_func=lambda **_kwargs: claimed_job)
    monkeypatch.setattr(worker, "_create_heartbeat_controller", lambda _ctx: _Heartbeat())
    monkeypatch.setattr(worker, "_persist_success", lambda *_args: None)
    monkeypatch.setattr(worker, "_persist_failure", lambda *_args, **_kwargs: None)

    def handler(_context):
        if handler_error is not None:
            raise handler_error
        return _result()

    worker._handlers[claimed_job.job_type] = handler
    logged = []
    monkeypatch.setattr(
        worker_module.logger,
        "info",
        lambda message, *, extra: logged.append((message, extra)),
    )
    monkeypatch.setattr(
        worker_module.logger,
        "warning",
        lambda message, *, extra: logged.append((message, extra)),
    )

    result = worker.run_once()

    terminal = next(item for item in logged if item[0] == expected_message)
    assert terminal[1]["attempt_count"] == 2
    assert terminal[1]["max_attempts"] == 4
    assert result.status in {WorkerRunStatus.SUCCEEDED, WorkerRunStatus.FAILED}


def test_retry_transition_remains_operational_only(monkeypatch):
    session = _RecordingSession()
    worker = _worker()
    worker._tenant_session_factory = lambda: session
    audit_calls = []
    monkeypatch.setattr(
        worker_module,
        "schedule_satellite_job_retry",
        lambda *_args, **_kwargs: SimpleNamespace(next_attempt_at=None),
    )
    monkeypatch.setattr(
        worker_module,
        "record_audit_event",
        lambda *_args, **_kwargs: audit_calls.append(kwargs),
    )

    worker._schedule_retry(_context(), retry_delay_seconds=30)

    assert audit_calls == []
    assert session.committed is True


def test_successful_heartbeat_has_no_log_or_durable_audit(monkeypatch):
    session = _RecordingSession()
    controller = _SatelliteJobHeartbeatController(
        organization_id=7,
        job_id=41,
        job_type="ndvi_timeseries",
        worker_id="worker-ephemeral-1",
        lease_token="11111111-1111-4111-8111-111111111111",
        heartbeat_seconds=1,
        tenant_session_factory=lambda: session,
    )

    class _SingleIterationStop:
        def __init__(self):
            self.calls = 0

        def wait(self, _seconds):
            self.calls += 1
            return self.calls > 1

        def set(self):
            return None

    controller._stop_event = _SingleIterationStop()
    logs = []
    audits = []
    monkeypatch.setattr(
        worker_module,
        "update_satellite_job_heartbeat",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        worker_module.logger,
        "info",
        lambda *args, **kwargs: logs.append((args, kwargs)),
    )
    monkeypatch.setattr(
        worker_module,
        "record_audit_event",
        lambda *args, **kwargs: audits.append((args, kwargs)),
    )

    controller._run()

    assert session.committed is True
    assert logs == []
    assert audits == []


def test_idle_worker_emits_no_lifecycle_event(monkeypatch):
    worker = _worker()
    logs = []
    audits = []
    monkeypatch.setattr(
        worker_module.logger,
        "info",
        lambda *args, **kwargs: logs.append((args, kwargs)),
    )
    monkeypatch.setattr(
        worker_module,
        "record_audit_event",
        lambda *args, **kwargs: audits.append((args, kwargs)),
    )

    result = worker.run_once()

    assert result.status is WorkerRunStatus.IDLE
    assert logs == []
    assert audits == []
