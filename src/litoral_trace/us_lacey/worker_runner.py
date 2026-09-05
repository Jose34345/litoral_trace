"""Continuous process entrypoint for the production U.S. Lacey worker."""
from __future__ import annotations

import logging
import os
import signal
import socket
import time
from uuid import uuid4

from litoral_trace.us_lacey.jobs import recover_stale_us_lacey_jobs
from litoral_trace.us_lacey.worker import process_one_us_lacey_job


_LOG = logging.getLogger("litoral_trace.us_lacey.worker_runner")
_STOP = False


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
    global _STOP
    _STOP = True
    _LOG.info("worker_stop_requested signal=%s", signum)


def run() -> None:
    global _STOP
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    signal.signal(signal.SIGTERM, _request_stop)
    signal.signal(signal.SIGINT, _request_stop)

    poll_seconds = _float_env(
        "US_LACEY_WORKER_POLL_SECONDS", 2.0, minimum=0.25, maximum=30.0
    )
    recovery_every = _int_env(
        "US_LACEY_WORKER_RECOVERY_EVERY_SECONDS", 60, minimum=30, maximum=3600
    )
    stale_after = _int_env(
        "US_LACEY_WORKER_STALE_AFTER_SECONDS", 600, minimum=60, maximum=86400
    )
    worker_id = f"{socket.gethostname()}-{uuid4().hex[:12]}"
    next_recovery = 0.0

    _LOG.info("us_lacey_worker_started worker_id=%s", worker_id)
    while not _STOP:
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
                    "job_processed job_id=%s job_status=%s document_status=%s operation_status=%s projected=%s conflicts=%s",
                    result.job_id,
                    result.job_status,
                    result.document_status,
                    result.operation_status,
                    result.projected_count,
                    result.conflict_count,
                )
                continue
        except Exception:
            _LOG.exception("worker_iteration_failed")
            time.sleep(min(5.0, max(1.0, poll_seconds * 2.0)))
            continue

        time.sleep(poll_seconds)

    _LOG.info("us_lacey_worker_stopped worker_id=%s", worker_id)


if __name__ == "__main__":
    run()
