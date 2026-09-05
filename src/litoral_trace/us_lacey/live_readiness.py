"""Coarse live-readiness probes for the isolated U.S. Lacey pilot.

These probes deliberately return only ready/not-ready state. They must never
expose connection strings, bucket names, object keys, credentials, or provider
error details to HTTP clients.
"""
from __future__ import annotations

import logging
from uuid import uuid4

from litoral_trace.us_lacey.storage import (
    build_us_lacey_storage_settings,
    get_us_lacey_storage_client,
)

_LOG = logging.getLogger("litoral_trace.us_lacey.live_readiness")


def probe_storage_roundtrip() -> str:
    """Write, read, verify, and remove one tiny object in the U.S. prefix."""
    payload = b"us-lacey-live-readiness"
    storage = None
    object_key: str | None = None
    version_id: str | None = None
    wrote_object = False
    roundtrip_ok = False
    cleanup_ok = True

    try:
        settings = build_us_lacey_storage_settings()
        storage = get_us_lacey_storage_client()
        object_key = (
            f"{settings.normalized_key_prefix}/healthchecks/"
            f"{uuid4().hex}.txt"
        )
        write = storage.put_object(
            key=object_key,
            body=payload,
            content_type="text/plain",
            content_length=len(payload),
            metadata={"purpose": "live-readiness"},
        )
        wrote_object = True
        version_id = write.version_id

        head = storage.head_object(key=object_key, version_id=version_id)
        if head.size_bytes != len(payload):
            raise RuntimeError("storage readiness size mismatch")

        with storage.get_object_stream(key=object_key, version_id=version_id) as stream:
            if stream.read() != payload:
                raise RuntimeError("storage readiness payload mismatch")

        roundtrip_ok = True
    except Exception:
        _LOG.exception("us_lacey_storage_roundtrip_failed")
    finally:
        if wrote_object and storage is not None and object_key is not None:
            try:
                storage.delete_object(key=object_key, version_id=version_id)
            except Exception:
                cleanup_ok = False
                _LOG.exception("us_lacey_storage_roundtrip_cleanup_failed")

    if roundtrip_ok and cleanup_ok:
        _LOG.info("us_lacey_storage_roundtrip_ready")
        return "ready"
    return "not_ready"


def live_runtime_status(*, worker_ready: bool, storage_roundtrip: str) -> dict[str, str]:
    """Return a secret-free status document for the deployed free-tier workload."""
    inline_worker = "ready" if worker_ready else "not_ready"
    storage = "ready" if storage_roundtrip == "ready" else "not_ready"
    overall = "ready" if inline_worker == "ready" and storage == "ready" else "not_ready"
    return {
        "status": overall,
        "inline_worker": inline_worker,
        "storage_roundtrip": storage,
    }
