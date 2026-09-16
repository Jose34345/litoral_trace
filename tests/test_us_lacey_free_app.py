from __future__ import annotations

import threading

from litoral_trace.web import us_lacey_free_app as free_app
from litoral_trace.web import us_lacey_pilot_app as pilot_app


def test_free_entrypoint_reuses_hardened_portal_app() -> None:
    assert free_app.app is pilot_app.app


def test_inline_worker_disabled_by_default(monkeypatch) -> None:
    monkeypatch.delenv("US_LACEY_INLINE_WORKER_ENABLED", raising=False)

    free_app._start_inline_worker()

    thread = getattr(free_app.app.state, "us_lacey_inline_worker_thread", None)
    assert thread is None or not thread.is_alive()


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
