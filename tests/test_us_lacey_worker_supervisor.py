from __future__ import annotations

import signal
import sys
import threading
from types import SimpleNamespace

from litoral_trace.us_lacey import worker_once, worker_runner


class _ImmediateProcess:
    def __init__(self, returncode: int = 0, pid: int = 4321) -> None:
        self.returncode = returncode
        self.pid = pid
        self.wait_calls = 0

    def poll(self):
        return self.returncode

    def wait(self, timeout=None):
        self.wait_calls += 1
        return self.returncode


def test_worker_once_exits_without_claim_when_queue_is_empty(monkeypatch) -> None:
    fake_worker = SimpleNamespace(
        process_one_us_lacey_job=lambda **_: SimpleNamespace(claimed=False)
    )
    monkeypatch.setitem(sys.modules, "litoral_trace.us_lacey.worker", fake_worker)

    assert worker_once.run_one(worker_id="child-test") == worker_once.EXIT_NO_JOB


def test_worker_once_returns_success_after_one_claimed_job(monkeypatch) -> None:
    fake_worker = SimpleNamespace(
        process_one_us_lacey_job=lambda **_: SimpleNamespace(
            claimed=True,
            job_id=11,
            job_status="COMPLETED",
            document_status="NEEDS_REVIEW",
            operation_status="READY_FOR_REVIEW",
            projected_count=5,
            conflict_count=1,
        )
    )
    monkeypatch.setitem(sys.modules, "litoral_trace.us_lacey.worker", fake_worker)

    assert worker_once.run_one(worker_id="child-test") == worker_once.EXIT_OK


def test_supervisor_spawns_one_child_only_when_queue_is_claimable(monkeypatch) -> None:
    stop = threading.Event()
    probes = 0
    spawned: list[str] = []
    process = _ImmediateProcess(returncode=worker_once.EXIT_OK)

    monkeypatch.setattr(worker_runner, "recover_stale_us_lacey_jobs", lambda **_: (0, 0))

    def probe() -> bool:
        nonlocal probes
        probes += 1
        if probes == 1:
            return True
        stop.set()
        return False

    monkeypatch.setattr(worker_runner, "has_claimable_us_lacey_job", probe)

    def spawn(*, worker_id: str):
        spawned.append(worker_id)
        return process

    monkeypatch.setattr(worker_runner, "_spawn_child", spawn)

    worker_runner.run_supervisor(stop_event=stop)

    assert len(spawned) == 1
    assert process.wait_calls >= 1


def test_supervisor_does_not_spawn_child_for_idle_queue(monkeypatch) -> None:
    stop = threading.Event()
    spawned = False

    monkeypatch.setattr(worker_runner, "recover_stale_us_lacey_jobs", lambda **_: (0, 0))

    def probe() -> bool:
        stop.set()
        return False

    monkeypatch.setattr(worker_runner, "has_claimable_us_lacey_job", probe)

    def spawn(**_):
        nonlocal spawned
        spawned = True
        return _ImmediateProcess()

    monkeypatch.setattr(worker_runner, "_spawn_child", spawn)

    worker_runner.run_supervisor(stop_event=stop)

    assert spawned is False


def test_terminate_child_reaps_after_graceful_signal(monkeypatch) -> None:
    class _RunningProcess(_ImmediateProcess):
        def __init__(self) -> None:
            super().__init__(returncode=0, pid=9876)
            self.running = True

        def poll(self):
            return None if self.running else self.returncode

        def wait(self, timeout=None):
            self.wait_calls += 1
            self.running = False
            self.returncode = -signal.SIGTERM
            return self.returncode

    process = _RunningProcess()
    signaled: list[int] = []

    monkeypatch.setattr(
        worker_runner,
        "_signal_child_group",
        lambda _process, sig: signaled.append(sig),
    )

    rc = worker_runner._terminate_child(process, grace_seconds=1.0)

    assert rc == -signal.SIGTERM
    assert signaled == [signal.SIGTERM]
    assert process.wait_calls == 1
