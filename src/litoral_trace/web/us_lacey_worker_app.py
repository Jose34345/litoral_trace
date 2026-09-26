"""Dedicated U.S. Lacey worker service entrypoint.

The ASGI process is only a lightweight supervisor/health endpoint. Heavy
document work executes in one fresh child process per durable job.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
import logging
import threading
import time

from fastapi import FastAPI, Response, status

from litoral_trace.us_lacey.worker_db import get_us_lacey_worker_database_url
from litoral_trace.us_lacey.worker_runner import run_supervisor


_LOG = logging.getLogger("litoral_trace.us_lacey.dedicated_worker")


def _mark_healthy(app: FastAPI) -> None:
    app.state.last_worker_success = time.monotonic()


def _loop(stop: threading.Event, app: FastAPI) -> None:
    _LOG.info("dedicated_worker_supervisor_thread_started")
    try:
        run_supervisor(
            stop_event=stop,
            on_healthy_iteration=lambda: _mark_healthy(app),
        )
    except Exception:
        _LOG.exception("dedicated_worker_supervisor_failed")
    finally:
        _LOG.info("dedicated_worker_supervisor_thread_stopped")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Fail closed if the dedicated least-privilege worker database credential is
    # missing or misconfigured before we advertise a healthy worker endpoint.
    get_us_lacey_worker_database_url()
    stop = threading.Event()
    thread = threading.Thread(
        target=_loop,
        args=(stop, app),
        daemon=True,
        name="us-lacey-worker-supervisor",
    )
    app.state.stop = stop
    app.state.thread = thread
    app.state.last_worker_success = 0.0
    thread.start()
    try:
        yield
    finally:
        stop.set()
        thread.join(timeout=30.0)


app = FastAPI(
    title="Litoral Trace U.S. Lacey Worker",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
    lifespan=lifespan,
)


@app.get("/health")
def health(response: Response) -> dict[str, str]:
    thread = getattr(app.state, "thread", None)
    if thread is None or not thread.is_alive():
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {"status": "not_ready", "service": "us-lacey-worker"}
    last_success = float(getattr(app.state, "last_worker_success", 0.0))
    if last_success <= 0.0:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {"status": "starting", "service": "us-lacey-worker"}
    return {"status": "healthy", "service": "us-lacey-worker"}
