from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest

from litoral_trace.workers import satellite_worker as worker_module
from litoral_trace.workers.satellite_worker import (
    WorkerRunResult,
    WorkerRunStatus,
    check_satellite_worker_readiness,
    main,
)


class _Session:
    def __init__(
        self,
        *,
        execute_error: Exception | None = None,
        rollback_error: Exception | None = None,
        close_error: Exception | None = None,
    ) -> None:
        self.execute_error = execute_error
        self.rollback_error = rollback_error
        self.close_error = close_error
        self.execute_calls: list[str] = []
        self.commits = 0
        self.rollbacks = 0
        self.closes = 0

    def execute(self, statement):
        self.execute_calls.append(str(statement))
        if self.execute_error is not None:
            raise self.execute_error
        return object()

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1
        if self.rollback_error is not None:
            raise self.rollback_error

    def close(self) -> None:
        self.closes += 1
        if self.close_error is not None:
            raise self.close_error


def _settings(*, metrics_enabled: bool = False):
    return SimpleNamespace(
        observability=SimpleNamespace(log_level="INFO"),
        workers=SimpleNamespace(
            satellite_metrics_enabled=metrics_enabled,
            satellite_metrics_host="127.0.0.1",
            satellite_metrics_port=9108,
        ),
    )


def _disable_cli_side_effects(monkeypatch, *, metrics_enabled: bool = False):
    monkeypatch.setattr(
        worker_module,
        "get_settings",
        lambda: _settings(metrics_enabled=metrics_enabled),
    )
    monkeypatch.setattr(
        worker_module,
        "configure_satellite_worker_json_logging",
        lambda _level: None,
    )


def test_readiness_succeeds_with_worker_snapshot_and_runtime_select(monkeypatch):
    worker_session = _Session()
    runtime_session = _Session()
    snapshot_sessions = []

    monkeypatch.setattr(
        worker_module,
        "get_satellite_queue_snapshot",
        lambda session: snapshot_sessions.append(session) or object(),
    )

    result = check_satellite_worker_readiness(
        worker_session_factory=lambda: worker_session,
        runtime_session_factory=lambda: runtime_session,
    )

    assert result is True
    assert snapshot_sessions == [worker_session]

    assert worker_session.commits == 0
    assert worker_session.rollbacks == 1
    assert worker_session.closes == 1
    assert worker_session.execute_calls == []

    assert runtime_session.commits == 0
    assert runtime_session.rollbacks == 1
    assert runtime_session.closes == 1
    assert runtime_session.execute_calls == ["SELECT 1"]


def test_readiness_returns_false_when_worker_session_is_missing(monkeypatch):
    runtime_factory_calls = []

    monkeypatch.setattr(
        worker_module,
        "get_satellite_queue_snapshot",
        lambda _session: pytest.fail(
            "queue snapshot must not run without a worker session"
        ),
    )

    result = check_satellite_worker_readiness(
        worker_session_factory=lambda: None,
        runtime_session_factory=lambda: runtime_factory_calls.append(True),
    )

    assert result is False
    assert runtime_factory_calls == []


def test_readiness_returns_false_when_worker_snapshot_fails_and_closes_session(
    monkeypatch,
    caplog,
):
    worker_session = _Session()
    runtime_factory_calls = []

    monkeypatch.setattr(
        worker_module,
        "get_satellite_queue_snapshot",
        lambda _session: (_ for _ in ()).throw(
            RuntimeError(
                "postgresql+psycopg://worker:secret@private-worker-host/db"
            )
        ),
    )

    with caplog.at_level(logging.WARNING, logger=worker_module.logger.name):
        result = check_satellite_worker_readiness(
            worker_session_factory=lambda: worker_session,
            runtime_session_factory=lambda: runtime_factory_calls.append(True),
        )

    assert result is False
    assert runtime_factory_calls == []
    assert worker_session.commits == 0
    assert worker_session.rollbacks == 1
    assert worker_session.closes == 1

    record = next(
        item
        for item in caplog.records
        if item.getMessage()
        == "satellite_worker_readiness_worker_db_unavailable"
    )
    assert record.error_type == "RuntimeError"
    assert record.error_message == "[REDACTED]"
    assert "private-worker-host" not in record.error_message
    assert "secret" not in record.error_message


def test_readiness_returns_false_when_runtime_session_is_missing(monkeypatch):
    worker_session = _Session()
    snapshot_calls = []

    monkeypatch.setattr(
        worker_module,
        "get_satellite_queue_snapshot",
        lambda session: snapshot_calls.append(session) or object(),
    )

    result = check_satellite_worker_readiness(
        worker_session_factory=lambda: worker_session,
        runtime_session_factory=lambda: None,
    )

    assert result is False
    assert snapshot_calls == [worker_session]
    assert worker_session.rollbacks == 1
    assert worker_session.closes == 1


def test_readiness_returns_false_when_runtime_select_fails_and_redacts_log(
    monkeypatch,
    caplog,
):
    worker_session = _Session()
    runtime_session = _Session(
        execute_error=RuntimeError(
            "postgresql://runtime:secret@private-runtime-host/db"
        )
    )

    monkeypatch.setattr(
        worker_module,
        "get_satellite_queue_snapshot",
        lambda _session: object(),
    )

    with caplog.at_level(logging.WARNING, logger=worker_module.logger.name):
        result = check_satellite_worker_readiness(
            worker_session_factory=lambda: worker_session,
            runtime_session_factory=lambda: runtime_session,
        )

    assert result is False

    assert worker_session.commits == 0
    assert worker_session.rollbacks == 1
    assert worker_session.closes == 1

    assert runtime_session.commits == 0
    assert runtime_session.rollbacks == 1
    assert runtime_session.closes == 1
    assert runtime_session.execute_calls == ["SELECT 1"]

    record = next(
        item
        for item in caplog.records
        if item.getMessage()
        == "satellite_worker_readiness_runtime_db_unavailable"
    )
    assert record.error_type == "RuntimeError"
    assert record.error_message == "[REDACTED]"
    assert "private-runtime-host" not in record.error_message
    assert "secret" not in record.error_message


def test_readiness_cleanup_failures_do_not_escape(monkeypatch):
    worker_session = _Session(
        rollback_error=RuntimeError("rollback cleanup failure"),
        close_error=RuntimeError("close cleanup failure"),
    )
    runtime_session = _Session(
        rollback_error=RuntimeError("rollback cleanup failure"),
        close_error=RuntimeError("close cleanup failure"),
    )

    monkeypatch.setattr(
        worker_module,
        "get_satellite_queue_snapshot",
        lambda _session: object(),
    )

    result = check_satellite_worker_readiness(
        worker_session_factory=lambda: worker_session,
        runtime_session_factory=lambda: runtime_session,
    )

    assert result is True
    assert worker_session.rollbacks == 1
    assert worker_session.closes == 1
    assert runtime_session.rollbacks == 1
    assert runtime_session.closes == 1


def test_check_mode_returns_zero_without_constructing_or_running_worker(
    monkeypatch,
):
    _disable_cli_side_effects(monkeypatch, metrics_enabled=True)
    calls = []

    monkeypatch.setattr(
        worker_module,
        "check_satellite_worker_readiness",
        lambda: calls.append("check") or True,
    )
    monkeypatch.setattr(
        worker_module,
        "build_satellite_worker",
        lambda: pytest.fail("check mode must not build SatelliteWorker"),
    )
    monkeypatch.setattr(
        worker_module,
        "start_satellite_metrics_server",
        lambda **_kwargs: pytest.fail(
            "check mode must not start the metrics server"
        ),
    )
    monkeypatch.setattr(
        worker_module,
        "_install_signal_handlers",
        lambda _worker: pytest.fail(
            "check mode must not install worker signal handlers"
        ),
    )

    assert main(["--check"]) == 0
    assert calls == ["check"]


def test_check_mode_returns_one_when_readiness_fails(monkeypatch):
    _disable_cli_side_effects(monkeypatch)
    monkeypatch.setattr(
        worker_module,
        "check_satellite_worker_readiness",
        lambda: False,
    )
    monkeypatch.setattr(
        worker_module,
        "build_satellite_worker",
        lambda: pytest.fail("failed check must not build SatelliteWorker"),
    )

    assert main(["--check"]) == 1


def test_check_mode_does_not_reach_claim_gee_audit_or_worker_loops(monkeypatch):
    _disable_cli_side_effects(monkeypatch, metrics_enabled=True)

    monkeypatch.setattr(
        worker_module,
        "check_satellite_worker_readiness",
        lambda: True,
    )

    forbidden = {
        "claim": "claim_next_satellite_job",
        "gee": "consultar_serie_temporal_ndvi_gee",
        "audit": "record_audit_event",
        "build": "build_satellite_worker",
    }

    for label, attribute in forbidden.items():
        monkeypatch.setattr(
            worker_module,
            attribute,
            lambda *args, _label=label, **kwargs: pytest.fail(
                f"--check reached forbidden path: {_label}"
            ),
        )

    monkeypatch.setattr(
        worker_module,
        "start_satellite_metrics_server",
        lambda **_kwargs: pytest.fail(
            "--check must not start Prometheus HTTP serving"
        ),
    )
    monkeypatch.setattr(
        worker_module,
        "_install_signal_handlers",
        lambda _worker: pytest.fail(
            "--check must not install signal handlers"
        ),
    )

    assert main(["--check"]) == 0


def test_once_mode_still_calls_run_once_and_not_run_forever(monkeypatch):
    _disable_cli_side_effects(monkeypatch)

    calls = []

    class _Worker:
        metrics = SimpleNamespace(registry=object())

        def run_once(self):
            calls.append("run_once")
            return WorkerRunResult(status=WorkerRunStatus.SUCCEEDED)

        def run_forever(self):
            pytest.fail("--once must not call run_forever")

    monkeypatch.setattr(worker_module, "build_satellite_worker", _Worker)
    monkeypatch.setattr(
        worker_module,
        "_install_signal_handlers",
        lambda _worker: calls.append("signals"),
    )

    assert main(["--once"]) == 0
    assert calls == ["signals", "run_once"]


@pytest.mark.parametrize(
    ("status", "expected_exit"),
    [
        (WorkerRunStatus.IDLE, 0),
        (WorkerRunStatus.SUCCEEDED, 0),
        (WorkerRunStatus.RETRY_SCHEDULED, 0),
        (WorkerRunStatus.FAILED, 1),
        (WorkerRunStatus.LEASE_LOST, 1),
        (WorkerRunStatus.STOPPED, 1),
    ],
)
def test_once_mode_preserves_existing_exit_status_contract(
    monkeypatch,
    status,
    expected_exit,
):
    _disable_cli_side_effects(monkeypatch)

    class _Worker:
        metrics = SimpleNamespace(registry=object())

        def run_once(self):
            return WorkerRunResult(status=status)

        def run_forever(self):
            pytest.fail("--once must not call run_forever")

    monkeypatch.setattr(worker_module, "build_satellite_worker", _Worker)
    monkeypatch.setattr(
        worker_module,
        "_install_signal_handlers",
        lambda _worker: None,
    )

    assert main(["--once"]) == expected_exit


def test_default_mode_still_calls_run_forever_and_not_run_once(monkeypatch):
    _disable_cli_side_effects(monkeypatch)

    calls = []

    class _Worker:
        metrics = SimpleNamespace(registry=object())

        def run_once(self):
            pytest.fail("default worker mode must not call run_once directly")

        def run_forever(self):
            calls.append("run_forever")

    monkeypatch.setattr(worker_module, "build_satellite_worker", _Worker)
    monkeypatch.setattr(
        worker_module,
        "_install_signal_handlers",
        lambda _worker: calls.append("signals"),
    )

    assert main([]) == 0
    assert calls == ["signals", "run_forever"]


def test_check_and_once_are_mutually_exclusive(monkeypatch):
    _disable_cli_side_effects(monkeypatch)

    with pytest.raises(SystemExit) as exc_info:
        main(["--check", "--once"])

    assert exc_info.value.code == 2