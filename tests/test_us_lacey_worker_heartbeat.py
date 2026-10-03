from __future__ import annotations

import time
from threading import Event

from litoral_trace.us_lacey import worker


def test_active_job_heartbeat_refreshes_lease_until_stopped(monkeypatch) -> None:
    calls: list[tuple[object, str]] = []
    fired = Event()

    # Match the real queue API: both arguments are keyword-only. A permissive
    # positional fake previously hid a TypeError that prevented lease renewal.
    def fake_heartbeat(*, job_id: int, worker_id: str):
        calls.append((job_id, worker_id))
        fired.set()
        return True

    monkeypatch.setattr(worker, "heartbeat_us_lacey_job", fake_heartbeat, raising=False)
    job_id = 42
    heartbeat = worker._UsLaceyJobHeartbeat(
        job_id=job_id,
        worker_id="heartbeat-test",
        interval_seconds=0.01,
    )
    heartbeat.start()
    try:
        assert fired.wait(timeout=1.0)
    finally:
        heartbeat.stop()

    count_after_stop = len(calls)
    time.sleep(0.03)
    assert len(calls) == count_after_stop
    assert calls
    assert all(call == (job_id, "heartbeat-test") for call in calls)

def test_heartbeat_watchdog_lease_loss_reconciles_operation(monkeypatch) -> None:
    fired = Event()
    refreshed: list[tuple[int, int]] = []

    def timed_out(*, job_id: int, worker_id: str) -> bool:
        del job_id, worker_id
        fired.set()
        return False

    monkeypatch.setattr(worker, "heartbeat_us_lacey_job", timed_out)
    monkeypatch.setattr(
        worker,
        "_refresh_operation",
        lambda *, organization_id, operation_id: refreshed.append(
            (organization_id, operation_id)
        )
        or "FAILED",
    )

    heartbeat = worker._UsLaceyJobHeartbeat(
        job_id=77,
        worker_id="watchdog-test",
        interval_seconds=0.01,
        organization_id=5,
        operation_id=9,
    )
    heartbeat.start()
    try:
        assert fired.wait(timeout=1.0)
        deadline = time.monotonic() + 1.0
        while not refreshed and time.monotonic() < deadline:
            time.sleep(0.01)
    finally:
        heartbeat.stop()

    assert refreshed == [(5, 9)]

