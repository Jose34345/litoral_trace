"""Long-lived supervisor for isolated single-job U.S. Lacey worker children.

Heavy document processing never runs in this process. Every claimable job is
executed by a fresh Python child and the child exits after at most one job,
allowing the operating system to reclaim RSS retained by native PDF/OCR stacks.
"""
from __future__ import annotations

import logging
import os
import signal
import socket
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from uuid import uuid4

from litoral_trace.us_lacey.jobs import (
    has_claimable_us_lacey_job,
    recover_stale_us_lacey_jobs,
)
from litoral_trace.us_lacey.worker_once import EXIT_NO_JOB, EXIT_OK


_LOG = logging.getLogger("litoral_trace.us_lacey.worker_runner")
_STOP_EVENT = threading.Event()


def _float_env(name: str, default: float, *, minimum: float, maximum: float) -> float:
    raw = str(os.environ.get(name, default)).strip()
    try:
        value = float(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be numeric.") from exc
    if value < minimum or value > maximum:
        raise RuntimeError(f"{name} is outside the supported range.")
    return value


def _int_env(name: str, default: int, *, minimum: int, maximum: int) -> int:
    raw = str(os.environ.get(name, default)).strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer.") from exc
    if value < minimum or value > maximum:
        raise RuntimeError(f"{name} is outside the supported range.")
    return value


def _request_stop(signum, _frame) -> None:
    _LOG.info("worker_supervisor_stop_requested signal=%s", signum)
    _STOP_EVENT.set()


def _spawn_child(*, worker_id: str) -> subprocess.Popen[bytes]:
    command = [
        sys.executable,
        "-m",
        "litoral_trace.us_lacey.worker_once",
        "--worker-id",
        worker_id,
    ]
    return subprocess.Popen(
        command,
        stdin=subprocess.DEVNULL,
        start_new_session=(os.name == "posix"),
    )


def _signal_child_group(process: subprocess.Popen[bytes], sig: int) -> None:
    if process.poll() is not None:
        return
    if os.name == "posix":
        try:
            os.killpg(process.pid, sig)
            return
        except ProcessLookupError:
            return
        except OSError:
            _LOG.warning(
                "worker_child_group_signal_failed pid=%s signal=%s",
                process.pid,
                sig,
                exc_info=True,
            )
    if sig == signal.SIGKILL:
        process.kill()
    else:
        process.terminate()


def _terminate_child(
    process: subprocess.Popen[bytes],
    *,
    grace_seconds: float,
) -> int:
    """Terminate a child and always reap it so no zombie remains."""
    if process.poll() is not None:
        return int(process.wait())

    _signal_child_group(process, signal.SIGTERM)
    try:
        return int(process.wait(timeout=grace_seconds))
    except subprocess.TimeoutExpired:
        _LOG.warning(
            "worker_child_grace_expired pid=%s grace_seconds=%s",
            process.pid,
            grace_seconds,
        )
        _signal_child_group(process, signal.SIGKILL)
        return int(process.wait())


def _wait_for_child(
    process: subprocess.Popen[bytes],
    *,
    stop_event: threading.Event,
    grace_seconds: float,
) -> int:
    """Wait for a child while remaining responsive to supervisor shutdown."""
    while True:
        returncode = process.poll()
        if returncode is not None:
            return int(process.wait())
        if stop_event.wait(0.25):
            return _terminate_child(process, grace_seconds=grace_seconds)


def _notify(callback: Callable[[], None] | None) -> None:
    if callback is None:
        return
    try:
        callback()
    except Exception:
        _LOG.exception("worker_supervisor_heartbeat_callback_failed")


def run_supervisor(
    *,
    stop_event: threading.Event | None = None,
    on_healthy_iteration: Callable[[], None] | None = None,
) -> None:
    """Supervise isolated one-job children until shutdown is requested."""
    stop = stop_event or _STOP_EVENT
    poll_seconds = _float_env(
        "US_LACEY_WORKER_POLL_SECONDS", 2.0, minimum=0.25, maximum=30.0
    )
    recovery_every = _int_env(
        "US_LACEY_WORKER_RECOVERY_EVERY_SECONDS", 60, minimum=30, maximum=3600
    )
    stale_after = _int_env(
        "US_LACEY_WORKER_STALE_AFTER_SECONDS", 120, minimum=60, maximum=86400
    )
    child_grace_seconds = _float_env(
        "US_LACEY_WORKER_CHILD_GRACE_SECONDS", 15.0, minimum=1.0, maximum=120.0
    )
    supervisor_id = f"supervisor-{socket.gethostname()}-{uuid4().hex[:10]}"
    next_recovery = 0.0

    _LOG.info(
        "us_lacey_worker_supervisor_started supervisor_id=%s stale_after_seconds=%s",
        supervisor_id,
        stale_after,
    )
    while not stop.is_set():
        now = time.monotonic()
        if now >= next_recovery:
            try:
                retried, failed = recover_stale_us_lacey_jobs(
                    stale_after_seconds=stale_after
                )
                if retried or failed:
                    _LOG.warning(
                        "stale_jobs_recovered retried=%s failed=%s",
                        retried,
                        failed,
                    )
                _notify(on_healthy_iteration)
            except Exception:
                _LOG.exception("stale_job_recovery_failed")
            next_recovery = now + recovery_every

        if stop.is_set():
            break

        try:
            claimable = has_claimable_us_lacey_job()
            _notify(on_healthy_iteration)
        except Exception:
            _LOG.exception("worker_queue_probe_failed")
            stop.wait(min(5.0, max(1.0, poll_seconds * 2.0)))
            continue

        if not claimable:
            stop.wait(poll_seconds)
            continue

        child_worker_id = f"{supervisor_id}-child-{uuid4().hex[:10]}"
        try:
            process = _spawn_child(worker_id=child_worker_id)
        except Exception:
            _LOG.exception("worker_child_spawn_failed worker_id=%s", child_worker_id)
            stop.wait(min(5.0, max(1.0, poll_seconds * 2.0)))
            continue

        _LOG.info(
            "worker_child_started pid=%s worker_id=%s",
            process.pid,
            child_worker_id,
        )
        returncode = _wait_for_child(
            process,
            stop_event=stop,
            grace_seconds=child_grace_seconds,
        )
        _notify(on_healthy_iteration)

        if stop.is_set():
            _LOG.info(
                "worker_child_stopped_for_shutdown pid=%s returncode=%s",
                process.pid,
                returncode,
            )
            break

        if returncode == EXIT_OK:
            _LOG.info(
                "worker_child_completed pid=%s worker_id=%s",
                process.pid,
                child_worker_id,
            )
            continue
        if returncode == EXIT_NO_JOB:
            # Queue-probe/claim races are expected with more than one supervisor.
            _LOG.debug("worker_child_found_no_job pid=%s", process.pid)
            stop.wait(poll_seconds)
            continue

        if returncode < 0:
            _LOG.error(
                "worker_child_terminated_by_signal pid=%s signal=%s worker_id=%s",
                process.pid,
                -returncode,
                child_worker_id,
            )
        else:
            _LOG.error(
                "worker_child_failed pid=%s exit_code=%s worker_id=%s",
                process.pid,
                returncode,
                child_worker_id,
            )
        # The durable lease remains authoritative. If the child died before it
        # could fail/retry the job, stale recovery will recycle it after the
        # configured 120-second default rather than the previous 10 minutes.
        stop.wait(min(5.0, max(1.0, poll_seconds * 2.0)))

    _LOG.info("us_lacey_worker_supervisor_stopped supervisor_id=%s", supervisor_id)


def run() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    signal.signal(signal.SIGTERM, _request_stop)
    signal.signal(signal.SIGINT, _request_stop)
    run_supervisor(stop_event=_STOP_EVENT)


if __name__ == "__main__":
    run()
