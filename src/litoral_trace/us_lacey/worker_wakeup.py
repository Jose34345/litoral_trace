"""In-process wake signal for the free-tier U.S. Lacey inline worker.

The durable PostgreSQL queue remains authoritative. This module only removes
idle polling latency: upload paths can wake the co-located worker immediately
while an empty queue is allowed to back off long enough for Neon scale-to-zero.
"""
from __future__ import annotations

import threading
import time


_WAKE_EVENT = threading.Event()
_WAKE_POLL_SECONDS = 0.25


def wake_us_lacey_worker() -> None:
    """Hint the co-located inline worker that durable queue work is available."""
    _WAKE_EVENT.set()


def wait_for_us_lacey_worker_wakeup(
    *,
    stop_event: threading.Event,
    timeout_seconds: float,
) -> bool:
    """Wait for upload wake, timeout or shutdown.

    Returns True only when shutdown was requested. A wake signal or timeout both
    return False so the worker loop performs another durable queue claim.
    """
    timeout = max(0.0, float(timeout_seconds))
    deadline = time.monotonic() + timeout

    while True:
        if stop_event.is_set():
            return True
        if _WAKE_EVENT.is_set():
            _WAKE_EVENT.clear()
            return False

        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False

        if stop_event.wait(min(_WAKE_POLL_SECONDS, remaining)):
            return True
