from __future__ import annotations

import time

from fastapi import Response

from litoral_trace.web import us_lacey_worker_app as worker_app


class _AliveThread:
    @staticmethod
    def is_alive() -> bool:
        return True


def _prime_worker_state(*, uptime_seconds: float) -> None:
    worker_app.app.state.thread = _AliveThread()
    worker_app.app.state.last_worker_success = time.monotonic()
    worker_app.app.state.started_monotonic = time.monotonic() - uptime_seconds


def test_worker_health_allows_cleanup_startup_grace(monkeypatch) -> None:
    _prime_worker_state(uptime_seconds=30)
    called = False

    def probe(**_kwargs) -> bool:
        nonlocal called
        called = True
        return True

    monkeypatch.setattr(worker_app, "has_overdue_sandbox_purge_backlog", probe)

    response = Response()
    payload = worker_app.health(response)

    assert response.status_code == 200
    assert payload["sandbox_cleanup"] == "warming_up"
    assert called is False


def test_worker_health_fails_when_cleanup_backlog_is_overdue(monkeypatch) -> None:
    _prime_worker_state(
        uptime_seconds=worker_app._SANDBOX_CLEANUP_HEALTH_GRACE_SECONDS + 5
    )
    monkeypatch.setattr(
        worker_app,
        "has_overdue_sandbox_purge_backlog",
        lambda **_kwargs: True,
    )

    response = Response()
    payload = worker_app.health(response)

    assert response.status_code == 503
    assert payload == {
        "status": "not_ready",
        "service": "us-lacey-worker",
        "sandbox_cleanup": "overdue",
    }


def test_worker_health_reports_healthy_cleanup_after_grace(monkeypatch) -> None:
    _prime_worker_state(
        uptime_seconds=worker_app._SANDBOX_CLEANUP_HEALTH_GRACE_SECONDS + 5
    )

    monkeypatch.setattr(
        worker_app,
        "has_overdue_sandbox_purge_backlog",
        lambda **_kwargs: False,
    )

    response = Response()
    payload = worker_app.health(response)

    assert response.status_code == 200
    assert payload == {
        "status": "healthy",
        "service": "us-lacey-worker",
        "sandbox_cleanup": "healthy",
    }
