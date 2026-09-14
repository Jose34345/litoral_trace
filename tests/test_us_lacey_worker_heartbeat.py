from __future__ import annotations

import time
from threading import Event
from uuid import uuid4

from litoral_trace.us_lacey import worker


def test_active_job_heartbeat_refreshes_lease_until_stopped(monkeypatch) -> None:
    calls: list[tuple[object, str]] = []
    fired = Event()

    def fake_heartbeat(job_id, worker_id):
        calls.append((job_id, worker_id))
        fired.set()
        return True

    monkeypatch.setattr(worker, "heartbeat_us_lacey_job", fake_heartbeat, raising=False)
    job_id = uuid4()
    heartbeat = worker._UsLaceyJobHeartbeat(
        job_id=job_id,
        worker_id="heartbeat-test",
        interval_seconds=0.01,
    )
    heartbeat.start()
    assert fired.wait(timeout=1.0)
    heartbeat.stop()

    count_after_stop = len(calls)
    time.sleep(0.03)
    assert len(calls) == count_after_stop
    assert calls
    assert all(call == (job_id, "heartbeat-test") for call in calls)
