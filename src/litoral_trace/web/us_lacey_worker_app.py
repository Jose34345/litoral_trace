"""Dedicated U.S. Lacey worker service entrypoint.

Deploy this ASGI app as a separate Render service with the same U.S. database
and Vault credentials. Customer web traffic never enters this process.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
import logging
import os
import socket
import threading
import time
from uuid import uuid4

from fastapi import FastAPI, Response, status

from litoral_trace.us_lacey.jobs import recover_stale_us_lacey_jobs
from litoral_trace.us_lacey.worker import process_one_us_lacey_job
from litoral_trace.us_lacey.worker_db import get_us_lacey_worker_database_url


_LOG = logging.getLogger("litoral_trace.us_lacey.dedicated_worker")


def _loop(stop: threading.Event, app: FastAPI) -> None:
    worker_id = f"dedicated-{socket.gethostname()}-{uuid4().hex[:12]}"
    poll = max(0.25, min(30.0, float(os.getenv("US_LACEY_WORKER_POLL_SECONDS", "2"))))
    recovery_every = max(30, min(3600, int(os.getenv("US_LACEY_WORKER_RECOVERY_EVERY_SECONDS", "60"))))
    stale_after = max(60, min(86400, int(os.getenv("US_LACEY_WORKER_STALE_AFTER_SECONDS", "600"))))
    next_recovery = 0.0
    _LOG.info("dedicated_worker_started worker_id=%s", worker_id)
    while not stop.is_set():
        now = time.monotonic()
        if now >= next_recovery:
            try:
                recover_stale_us_lacey_jobs(stale_after_seconds=stale_after)
            except Exception:
                _LOG.exception("dedicated_stale_recovery_failed")
            next_recovery = now + recovery_every
        try:
            result = process_one_us_lacey_job(worker_id=worker_id)
            app.state.last_worker_success = time.monotonic()
            if result.claimed:
                continue
        except Exception:
            _LOG.exception("dedicated_worker_iteration_failed")
        stop.wait(poll)
    _LOG.info("dedicated_worker_stopped worker_id=%s", worker_id)


@asynccontextmanager
async def lifespan(app: FastAPI):
    get_us_lacey_worker_database_url()
    stop = threading.Event()
    thread = threading.Thread(target=_loop, args=(stop, app), daemon=True, name="us-lacey-dedicated-worker")
    app.state.stop = stop
    app.state.thread = thread
    app.state.last_worker_success = 0.0
    thread.start()
    try:
        yield
    finally:
        stop.set()
        thread.join(timeout=10)


app = FastAPI(title="Litoral Trace U.S. Lacey Worker", docs_url=None, redoc_url=None, openapi_url=None, lifespan=lifespan)


@app.get("/health")
def health(response: Response) -> dict[str, str]:
    thread = getattr(app.state, "thread", None)
    if thread is None or not thread.is_alive():
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {"status": "not_ready", "service": "us-lacey-worker"}
    return {"status": "healthy", "service": "us-lacey-worker"}
