"""Free-tier ASGI entrypoint for the private U.S. Lacey pilot.

Render's free tier does not provide a separate background-worker service. This
entrypoint keeps the customer portal unchanged and runs the durable U.S. Lacey
queue consumer in a daemon thread while the web instance is awake.

The queue still uses ``US_LACEY_WORKER_DATABASE_URL`` and therefore preserves
the dedicated least-privilege PostgreSQL worker role. The free deployment is a
private-beta convenience only: when Render spins the web service down, queue
processing pauses and resumes on the next wake-up.
"""
from __future__ import annotations

import logging
import os
import socket
import threading
import time
from uuid import uuid4

from litoral_trace.us_lacey.jobs import recover_stale_us_lacey_jobs
from litoral_trace.us_lacey.worker import process_one_us_lacey_job
from litoral_trace.us_lacey.worker_db import get_us_lacey_worker_database_url
from litoral_trace.web.us_lacey_pilot_app import app


_LOG = logging.getLogger("litoral_trace.us_lacey.inline_worker")


def _bool_env(name: str, default: bool = False) -> bool:
    raw = str(os.environ.get(name, "1" if default else "0")).strip().lower()
    return raw in {"1", "true", "yes", "on"}


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


def _inline_worker_loop(stop_event: threading.Event) -> None:
    poll_seconds = _float_env(
        "US_LACEY_WORKER_POLL_SECONDS", 2.0, minimum=0.25, maximum=30.0
    )
    recovery_every = _int_env(
        "US_LACEY_WORKER_RECOVERY_EVERY_SECONDS", 60, minimum=30, maximum=3600
    )
    stale_after = _int_env(
        "US_LACEY_WORKER_STALE_AFTER_SECONDS", 600, minimum=60, maximum=86400
    )
    worker_id = f"inline-{socket.gethostname()}-{uuid4().hex[:12]}"
    next_recovery = 0.0

    _LOG.info("us_lacey_inline_worker_started worker_id=%s", worker_id)
    while not stop_event.is_set():
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
            except Exception:
                _LOG.exception("stale_job_recovery_failed")
            next_recovery = now + recovery_every

        try:
            result = process_one_us_lacey_job(worker_id=worker_id)
            if result.claimed:
                _LOG.info(
                    "job_processed job_id=%s job_status=%s document_status=%s "
                    "operation_status=%s projected=%s conflicts=%s",
                    result.job_id,
                    result.job_status,
                    result.document_status,
                    result.operation_status,
                    result.projected_count,
                    result.conflict_count,
                )
                continue
        except Exception:
            _LOG.exception("inline_worker_iteration_failed")
            if stop_event.wait(min(5.0, max(1.0, poll_seconds * 2.0))):
                break
            continue

        stop_event.wait(poll_seconds)

    _LOG.info("us_lacey_inline_worker_stopped worker_id=%s", worker_id)


def _start_inline_worker() -> None:
    if not _bool_env("US_LACEY_INLINE_WORKER_ENABLED", default=False):
        _LOG.info("us_lacey_inline_worker_disabled")
        return

    # Fail closed before accepting traffic if the dedicated worker URL is absent,
    # points at another database, or reuses the web runtime role.
    get_us_lacey_worker_database_url()

    existing = getattr(app.state, "us_lacey_inline_worker_thread", None)
    if existing is not None and existing.is_alive():
        return

    stop_event = threading.Event()
    thread = threading.Thread(
        target=_inline_worker_loop,
        args=(stop_event,),
        name="us-lacey-inline-worker",
        daemon=True,
    )
    app.state.us_lacey_inline_worker_stop = stop_event
    app.state.us_lacey_inline_worker_thread = thread
    thread.start()


def _stop_inline_worker() -> None:
    stop_event = getattr(app.state, "us_lacey_inline_worker_stop", None)
    thread = getattr(app.state, "us_lacey_inline_worker_thread", None)
    if stop_event is not None:
        stop_event.set()
    if thread is not None and thread.is_alive():
        thread.join(timeout=10.0)


app.add_event_handler("startup", _start_inline_worker)
app.add_event_handler("shutdown", _stop_inline_worker)
