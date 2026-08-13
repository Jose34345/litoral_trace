from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from prometheus_client import CollectorRegistry, generate_latest

import main as main_module
from litoral_trace.observability.satellite_worker import (
    SatelliteQueueSnapshot,
    SatelliteQueueSnapshotCache,
    SatelliteWorkerJsonFormatter,
    SatelliteWorkerMetrics,
    get_satellite_queue_snapshot,
    normalize_error_code,
    start_satellite_metrics_server,
)
from litoral_trace.config.settings import WorkersSettings
from litoral_trace.workers.satellite_worker import (
    RetryDisposition,
    SatelliteWorker,
    SatelliteWorkerExecutionError,
    WorkerRunStatus,
)


class _MappingResult:
    def __init__(self, row):
        self.row = row

    def mappings(self):
        return self

    def one(self):
        return self.row


class _Session:
    def __init__(self, row=None, error: Exception | None = None):
        self.row = row
        self.error = error
        self.commits = 0
        self.rollbacks = 0
        self.closes = 0

    def execute(self, _statement):
        if self.error is not None:
            raise self.error
        return _MappingResult(self.row)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closes += 1


def _row(**overrides):
    values = {
        "snapshot_time": datetime(2026, 8, 12, tzinfo=timezone.utc),
        "queued_ready_count": 2,
        "queued_delayed_count": 3,
        "running_count": 4,
        "running_stale_count": 1,
        "running_invalid_count": 0,
        "oldest_ready_age_seconds": 12.5,
        "oldest_active_lease_age_seconds": 8.0,
        "oldest_heartbeat_age_seconds": 3.0,
        "next_delayed_ready_in_seconds": 30.0,
    }
    values.update(overrides)
    return values


def _snapshot(**overrides):
    values = _row()
    values.update(overrides)
    return SatelliteQueueSnapshot(**values)


def _metric_value(registry, name, labels=None):
    return registry.get_sample_value(name, labels or {})


def test_queue_snapshot_maps_typed_values_and_null_durations():
    session = _Session(_row(oldest_ready_age_seconds=None))

    snapshot = get_satellite_queue_snapshot(session)

    assert snapshot.queued_ready_count == 2
    assert snapshot.oldest_ready_age_seconds == 0.0
    assert snapshot.snapshot_time.tzinfo is not None
    assert session.closes == 0


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("queued_ready_count", -1),
        ("running_count", -1),
        ("oldest_heartbeat_age_seconds", -0.1),
        ("next_delayed_ready_in_seconds", float("nan")),
    ],
)
def test_queue_snapshot_rejects_invalid_nonnegative_contract(field, value):
    with pytest.raises(ValueError):
        get_satellite_queue_snapshot(_Session(_row(**{field: value})))


def test_owned_queue_snapshot_session_rolls_back_and_closes_on_error(monkeypatch):
    session = _Session(error=RuntimeError("private-driver-detail"))
    monkeypatch.setattr(
        "litoral_trace.observability.satellite_worker.get_worker_db_session",
        lambda: session,
    )

    with pytest.raises(RuntimeError):
        get_satellite_queue_snapshot()

    assert session.rollbacks == 1
    assert session.closes == 1


def test_snapshot_cache_hit_and_expiration_use_injected_monotonic_clock():
    clock = {"now": 10.0}
    calls = []
    cache = SatelliteQueueSnapshotCache(
        refresh_seconds=30,
        loader=lambda: calls.append("load") or _snapshot(),
        monotonic_func=lambda: clock["now"],
    )

    first = cache.get()
    clock["now"] = 39.9
    second = cache.get()
    clock["now"] = 40.0
    third = cache.get()

    assert first == second == third
    assert calls == ["load", "load"]


def test_snapshot_refresh_failure_retains_last_good_and_timestamp():
    clock = {"now": 0.0}
    calls = {"count": 0}
    errors = []

    def _load():
        calls["count"] += 1
        if calls["count"] == 1:
            return _snapshot()
        raise RuntimeError("postgresql://private")

    cache = SatelliteQueueSnapshotCache(
        refresh_seconds=1,
        loader=_load,
        monotonic_func=lambda: clock["now"],
        on_error=errors.append,
    )
    first = cache.get()
    timestamp = cache.last_success_timestamp_seconds
    clock["now"] = 1.0

    assert cache.get() == first
    assert cache.last_success_timestamp_seconds == timestamp
    assert len(errors) == 1


def test_first_snapshot_failure_is_safe_zero_state():
    cache = SatelliteQueueSnapshotCache(
        loader=lambda: (_ for _ in ()).throw(RuntimeError("secret")),
    )

    snapshot = cache.get()

    assert snapshot == SatelliteQueueSnapshot.empty()
    assert cache.last_success_timestamp_seconds == 0.0


def test_prometheus_exact_names_labels_and_error_allowlist():
    registry = CollectorRegistry()
    metrics = SatelliteWorkerMetrics(registry)
    metrics.record_claimed("ndvi_timeseries")
    metrics.record_succeeded("ndvi_timeseries")
    metrics.record_failed("ndvi_timeseries", "attacker-controlled-code")
    metrics.record_retry_scheduled("ndvi_timeseries", "gee_timeout", 30)
    metrics.record_lease_lost("ndvi_timeseries")
    metrics.record_stale_recoveries(2, 1)
    metrics.record_heartbeat_failure()
    metrics.observe_claim_duration(0.2)
    metrics.observe_gee_duration("ndvi_timeseries", 2)
    metrics.observe_execution_duration("ndvi_timeseries", 3)

    assert _metric_value(
        registry,
        "litoral_trace_satellite_jobs_claimed_total",
        {"job_type": "ndvi_timeseries"},
    ) == 1
    assert _metric_value(
        registry,
        "litoral_trace_satellite_jobs_failed_total",
        {"job_type": "ndvi_timeseries", "error_code": "other"},
    ) == 1
    assert _metric_value(
        registry,
        "litoral_trace_satellite_stale_recoveries_total",
        {"outcome": "requeued"},
    ) == 2
    assert b"organization_id" not in generate_latest(registry)
    assert normalize_error_code("gee_timeout") == "gee_timeout"
    assert normalize_error_code("raw-new-code") == "other"


def test_queue_gauges_and_snapshot_error_semantics(caplog):
    registry = CollectorRegistry()
    metrics = SatelliteWorkerMetrics(registry)
    cache = SatelliteQueueSnapshotCache(
        loader=lambda: (_ for _ in ()).throw(RuntimeError("secret@host")),
        on_error=metrics.record_snapshot_error,
    )
    with caplog.at_level(logging.WARNING):
        snapshot = cache.get()
    metrics.update_queue_snapshot(
        snapshot,
        last_success_timestamp_seconds=cache.last_success_timestamp_seconds,
    )

    assert _metric_value(
        registry, "litoral_trace_satellite_jobs_queued_ready"
    ) == 0
    assert _metric_value(
        registry,
        "litoral_trace_satellite_queue_snapshot_last_success_timestamp_seconds",
    ) == 0
    assert _metric_value(
        registry,
        "litoral_trace_satellite_queue_snapshot_errors_total",
    ) == 1
    assert "secret@host" not in caplog.text


def test_separate_registries_allow_duplicate_metrics_objects():
    first = SatelliteWorkerMetrics(CollectorRegistry())
    second = SatelliteWorkerMetrics(CollectorRegistry())
    first.record_claimed("ndvi_timeseries")
    second.record_claimed("ndvi_timeseries")

    assert first.registry is not second.registry


def test_bound_queue_gauges_refresh_on_collection_and_share_cache():
    clock = {"now": 0.0}
    calls = []
    registry = CollectorRegistry()
    metrics = SatelliteWorkerMetrics(registry)
    cache = SatelliteQueueSnapshotCache(
        refresh_seconds=30,
        loader=lambda: calls.append("load") or _snapshot(),
        monotonic_func=lambda: clock["now"],
        on_error=metrics.record_snapshot_error,
    )
    metrics.bind_queue_snapshot_cache(cache)

    generate_latest(registry)
    generate_latest(registry)

    assert calls == ["load"]
    assert _metric_value(
        registry, "litoral_trace_satellite_jobs_queued_ready"
    ) == 2


def test_metrics_settings_defaults_are_disabled_and_validate_enabled_host():
    settings = WorkersSettings()
    assert settings.satellite_metrics_enabled is False
    assert settings.satellite_metrics_host == "127.0.0.1"
    assert settings.satellite_metrics_port == 9108
    assert settings.satellite_queue_metrics_refresh_seconds == 30

    with pytest.raises(ValueError):
        WorkersSettings(
            satellite_metrics_enabled=True,
            satellite_metrics_host="   ",
        )


def test_enabled_metrics_server_failure_is_sanitized(monkeypatch):
    monkeypatch.setattr(
        "litoral_trace.observability.satellite_worker.start_wsgi_server",
        lambda **_kwargs: (_ for _ in ()).throw(
            RuntimeError("bind private-host:secret")
        ),
    )

    with pytest.raises(RuntimeError) as exc_info:
        start_satellite_metrics_server(
            host="127.0.0.1",
            port=9108,
            registry=CollectorRegistry(),
        )

    assert "private-host" not in str(exc_info.value)


def test_json_formatter_allowlists_fields_and_omits_secrets_and_traceback():
    formatter = SatelliteWorkerJsonFormatter()
    record = logging.LogRecord(
        "worker.test",
        logging.WARNING,
        __file__,
        1,
        "satellite_worker_job_failed",
        (),
        (RuntimeError, RuntimeError("traceback-secret"), None),
    )
    record.job_id = 7
    record.error_message = "sanitized failure"
    record.lease_token = "malicious-lease-secret"
    record.unknown_extra = "unknown-secret"

    payload = json.loads(formatter.format(record))

    assert payload["event"] == "satellite_worker_job_failed"
    assert payload["job_id"] == 7
    assert payload["error_message"] == "sanitized failure"
    rendered = json.dumps(payload)
    assert "malicious-lease-secret" not in rendered
    assert "unknown-secret" not in rendered
    assert "traceback-secret" not in rendered


@pytest.mark.parametrize("failure_mode", ["missing", "execute"])
def test_ready_returns_safe_503_and_closes_when_session_exists(
    monkeypatch,
    failure_mode,
):
    session = None if failure_mode == "missing" else _Session(
        error=RuntimeError("postgresql://user:password@private-host/db")
    )
    monkeypatch.setattr(main_module, "get_db_session", lambda: session)

    response = asyncio.run(main_module.readiness_check())

    assert response.status_code == 503
    assert b"private-host" not in response.body
    if session is not None:
        assert session.rollbacks == 1
        assert session.closes == 1


def test_ready_returns_200_closes_session_and_health_is_unchanged(monkeypatch):
    session = _Session(row={})
    monkeypatch.setattr(main_module, "get_db_session", lambda: session)

    ready = asyncio.run(main_module.readiness_check())
    health = asyncio.run(main_module.health_check())

    assert ready.status_code == 200
    assert json.loads(ready.body) == {"status": "ready"}
    assert session.closes == 1
    assert health.status_code == 200
    assert json.loads(health.body)["status"] == "healthy"


def test_claim_metric_occurs_only_after_successful_commit():
    events = []

    class _Metrics:
        def record_claimed(self, _job_type):
            events.append("metric")

        def observe_claim_duration(self, _seconds):
            events.append("duration")

    class _ClaimSession(_Session):
        def commit(self):
            events.append("commit")

        def close(self):
            events.append("close")

    claimed = SimpleNamespace(job_type="ndvi_timeseries")
    worker = SatelliteWorker(
        worker_id="worker-f2",
        claim_session_factory=_ClaimSession,
        claim_job_func=lambda **_kwargs: claimed,
        stale_recovery_interval_seconds=None,
        metrics=_Metrics(),
    )
    worker._handlers = {}
    worker._create_heartbeat_controller = lambda _context: SimpleNamespace(
        start=lambda: None,
        stop=lambda: None,
        join=lambda: None,
        has_lease_lost=lambda: False,
    )
    worker._build_context = lambda _job: (_ for _ in ()).throw(
        RuntimeError("stop after claim")
    )

    with pytest.raises(RuntimeError):
        worker.run_once()

    assert events.index("commit") < events.index("metric")


@pytest.mark.parametrize(
    ("outcome", "expected_status", "durable_event", "metric_event"),
    [
        ("success", WorkerRunStatus.SUCCEEDED, "durable_success", "record_succeeded"),
        ("failed", WorkerRunStatus.FAILED, "durable_failed", "record_failed"),
        ("retry", WorkerRunStatus.RETRY_SCHEDULED, "durable_retry", "record_retry_scheduled"),
    ],
)
def test_lifecycle_metrics_follow_durable_transition(
    outcome,
    expected_status,
    durable_event,
    metric_event,
):
    events = []

    class _Metrics:
        registry = CollectorRegistry()

        def __getattr__(self, name):
            return lambda *_args, **_kwargs: events.append(name)

    claimed = SimpleNamespace(
        id=41,
        organization_id=7,
        lote_id=19,
        job_type="ndvi_timeseries",
        attempt_count=1,
        max_attempts=3,
        lease_token="11111111-1111-4111-8111-111111111111",
    )
    worker = SatelliteWorker(
        worker_id="worker-f2-lifecycle",
        claim_session_factory=_Session,
        claim_job_func=lambda **_kwargs: claimed,
        stale_recovery_interval_seconds=None,
        metrics=_Metrics(),
    )
    heartbeat = SimpleNamespace(
        start=lambda: None,
        stop=lambda: None,
        join=lambda: None,
        has_lease_lost=lambda: False,
    )
    worker._create_heartbeat_controller = lambda _context: heartbeat
    if outcome == "success":
        worker._handlers = {"ndvi_timeseries": lambda _context: object()}
        worker._persist_success = (
            lambda _context, _result: events.append("durable_success")
        )
    elif outcome == "failed":
        worker._handlers = {
            "ndvi_timeseries": lambda _context: (_ for _ in ()).throw(
                SatelliteWorkerExecutionError("invalid_job_payload", "safe")
            )
        }
        worker._persist_failure = lambda _context, **_kwargs: events.append(
            "durable_failed"
        )
    else:
        worker._handlers = {
            "ndvi_timeseries": lambda _context: (_ for _ in ()).throw(
                SatelliteWorkerExecutionError(
                    "gee_timeout",
                    "safe",
                    retry_disposition=RetryDisposition.RETRYABLE,
                )
            )
        }
        worker._schedule_retry = lambda _context, **_kwargs: events.append(
            "durable_retry"
        )

    result = worker.run_once()

    assert result.status is expected_status
    assert events.index(durable_event) < events.index(metric_event)
