from __future__ import annotations

import threading
from types import SimpleNamespace

from litoral_trace.web import us_lacey_free_app as free_app
from litoral_trace.web import us_lacey_pilot_app as pilot_app


def test_free_entrypoint_reuses_hardened_portal_app() -> None:
    assert free_app.app is pilot_app.app


def test_inline_worker_disabled_by_default(monkeypatch) -> None:
    monkeypatch.delenv("US_LACEY_INLINE_WORKER_ENABLED", raising=False)

    free_app._start_inline_worker()

    thread = getattr(free_app.app.state, "us_lacey_inline_worker_thread", None)
    assert thread is None or not thread.is_alive()


def test_schema_bootstrap_is_disabled_by_default(monkeypatch) -> None:
    monkeypatch.delenv("US_LACEY_BOOTSTRAP_SCHEMA_ON_STARTUP", raising=False)
    called: list[tuple[object, str]] = []
    monkeypatch.setattr(
        free_app.command,
        "upgrade",
        lambda config, target: called.append((config, target)),
    )

    free_app._bootstrap_schema_if_requested()

    assert called == []


def test_schema_bootstrap_migrates_and_binds_dedicated_logins(monkeypatch) -> None:
    monkeypatch.setenv("US_LACEY_BOOTSTRAP_SCHEMA_ON_STARTUP", "1")
    monkeypatch.setenv(
        "MIGRATION_DATABASE_URL",
        "postgresql://owner:secret@example/neondb",
    )
    calls: list[object] = []
    statements: list[str] = []

    class _Connection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def exec_driver_sql(self, statement: str) -> None:
            statements.append(statement)

    class _Engine:
        def connect(self):
            return _Connection()

        def dispose(self) -> None:
            calls.append("dispose")

    monkeypatch.setattr(free_app, "Config", lambda path: ("config", path))
    monkeypatch.setattr(
        free_app.command,
        "upgrade",
        lambda config, target: calls.append(("upgrade", config, target)),
    )
    monkeypatch.setattr(
        free_app,
        "create_engine",
        lambda *args, **kwargs: _Engine(),
    )
    monkeypatch.setattr(
        free_app,
        "normalize_database_url",
        lambda value: value,
    )

    free_app._bootstrap_schema_if_requested()

    assert calls[0] == ("upgrade", ("config", "alembic.ini"), "head")
    assert calls[-1] == "dispose"
    assert any("litoral_trace_worker_executor" in item for item in statements)
    assert any("litoral_trace_impersonation_reader" in item for item in statements)


def test_worker_keeps_interactive_poll_cadence_when_queue_is_idle(monkeypatch) -> None:
    waits: list[float] = []

    class _StopEvent:
        def is_set(self) -> bool:
            return False

        def wait(self, seconds: float) -> bool:
            waits.append(seconds)
            return len(waits) >= 4

    monkeypatch.setenv("US_LACEY_WORKER_POLL_SECONDS", "2")
    monkeypatch.setenv("US_LACEY_WORKER_MAX_BACKOFF_SECONDS", "360")
    monkeypatch.setenv("US_LACEY_WORKER_RECOVERY_EVERY_SECONDS", "3600")
    monkeypatch.setattr(
        free_app,
        "recover_stale_us_lacey_jobs",
        lambda **_: (0, 0),
    )
    monkeypatch.setattr(
        free_app,
        "process_one_us_lacey_job",
        lambda **_: SimpleNamespace(claimed=False),
    )

    free_app._inline_worker_loop(_StopEvent())

    assert waits == [2.0, 2.0, 2.0, 2.0]


def test_worker_database_errors_use_exponential_backoff(monkeypatch) -> None:
    waits: list[float] = []

    class _StopEvent:
        def is_set(self) -> bool:
            return False

        def wait(self, seconds: float) -> bool:
            waits.append(seconds)
            return len(waits) >= 3

    monkeypatch.setenv("US_LACEY_WORKER_POLL_SECONDS", "2")
    monkeypatch.setenv("US_LACEY_WORKER_MAX_BACKOFF_SECONDS", "360")
    monkeypatch.setenv("US_LACEY_WORKER_RECOVERY_EVERY_SECONDS", "3600")
    monkeypatch.setattr(
        free_app,
        "recover_stale_us_lacey_jobs",
        lambda **_: (0, 0),
    )

    def fail_claim(**_: object):
        raise free_app.UsLaceyJobError("database unavailable")

    monkeypatch.setattr(free_app, "process_one_us_lacey_job", fail_claim)

    free_app._inline_worker_loop(_StopEvent())

    assert waits == [2.0, 4.0, 8.0]


def test_worker_backoff_caps_at_six_minutes() -> None:
    current = 256.0
    next_wait = free_app._next_worker_backoff_seconds(
        current,
        base=2.0,
        cap=360.0,
    )
    assert next_wait == 360.0
    assert (
        free_app._next_worker_backoff_seconds(
            next_wait,
            base=2.0,
            cap=360.0,
        )
        == 360.0
    )


def test_inline_worker_lifecycle(monkeypatch) -> None:
    monkeypatch.setenv("US_LACEY_INLINE_WORKER_ENABLED", "1")
    monkeypatch.setattr(
        free_app,
        "get_us_lacey_worker_database_url",
        lambda: "postgresql+psycopg://worker:secret@example/neondb",
    )

    started = threading.Event()

    def fake_loop(stop_event: threading.Event) -> None:
        started.set()
        stop_event.wait(2.0)

    monkeypatch.setattr(free_app, "_inline_worker_loop", fake_loop)

    free_app._start_inline_worker()
    thread = free_app.app.state.us_lacey_inline_worker_thread

    assert started.wait(1.0)
    assert thread.is_alive()

    free_app._stop_inline_worker()

    assert not thread.is_alive()


def test_free_tier_worker_readiness_allows_healthy_idle_backoff(monkeypatch) -> None:
    class _Thread:
        @staticmethod
        def is_alive() -> bool:
            return True

    monkeypatch.setenv("US_LACEY_WORKER_POLL_SECONDS", "2")
    us_lacey_free_app = free_app
    us_lacey_free_app.app.state.us_lacey_inline_worker_thread = _Thread()
    us_lacey_free_app.app.state.us_lacey_inline_worker_current_wait_seconds = 360.0
    us_lacey_free_app.app.state.us_lacey_inline_worker_last_success_monotonic = 100.0

    monkeypatch.setattr(us_lacey_free_app.time, "monotonic", lambda: 459.0)
    assert us_lacey_free_app._inline_worker_ready() is True

    monkeypatch.setattr(us_lacey_free_app.time, "monotonic", lambda: 466.0)
    assert us_lacey_free_app._inline_worker_ready() is False
