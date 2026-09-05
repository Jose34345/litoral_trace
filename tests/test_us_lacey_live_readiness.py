from __future__ import annotations

from types import SimpleNamespace

from litoral_trace.us_lacey import live_readiness


class _Stream:
    def __init__(self, payload: bytes):
        self._payload = payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return None

    def read(self) -> bytes:
        return self._payload


class _Storage:
    def __init__(self, *, payload: bytes, cleanup_fails: bool = False):
        self.payload = payload
        self.cleanup_fails = cleanup_fails
        self.put_key: str | None = None
        self.delete_key: str | None = None

    def put_object(self, *, key, body, content_type, content_length, metadata=None):
        assert body == self.payload
        assert content_type == "text/plain"
        assert content_length == len(self.payload)
        assert metadata == {"purpose": "live-readiness"}
        self.put_key = key
        return SimpleNamespace(version_id="version-1")

    def head_object(self, *, key, version_id=None):
        assert key == self.put_key
        assert version_id == "version-1"
        return SimpleNamespace(size_bytes=len(self.payload))

    def get_object_stream(self, *, key, version_id=None):
        assert key == self.put_key
        assert version_id == "version-1"
        return _Stream(self.payload)

    def delete_object(self, *, key, version_id=None):
        self.delete_key = key
        assert version_id == "version-1"
        if self.cleanup_fails:
            raise RuntimeError("simulated cleanup failure")
        return SimpleNamespace(delete_marker=False, version_id=version_id)


def test_storage_roundtrip_uses_isolated_healthcheck_prefix_and_cleans(monkeypatch):
    payload = b"us-lacey-live-readiness"
    storage = _Storage(payload=payload)
    settings = SimpleNamespace(normalized_key_prefix="us-lacey/pilot")
    monkeypatch.setattr(live_readiness, "build_us_lacey_storage_settings", lambda: settings)
    monkeypatch.setattr(live_readiness, "get_us_lacey_storage_client", lambda: storage)

    assert live_readiness.probe_storage_roundtrip() == "ready"
    assert storage.put_key is not None
    assert storage.put_key.startswith("us-lacey/pilot/healthchecks/")
    assert storage.put_key.endswith(".txt")
    assert storage.delete_key == storage.put_key


def test_storage_roundtrip_fails_closed_when_cleanup_fails(monkeypatch):
    payload = b"us-lacey-live-readiness"
    storage = _Storage(payload=payload, cleanup_fails=True)
    settings = SimpleNamespace(normalized_key_prefix="us-lacey/pilot")
    monkeypatch.setattr(live_readiness, "build_us_lacey_storage_settings", lambda: settings)
    monkeypatch.setattr(live_readiness, "get_us_lacey_storage_client", lambda: storage)

    assert live_readiness.probe_storage_roundtrip() == "not_ready"
    assert storage.delete_key == storage.put_key


def test_live_runtime_status_requires_successful_worker_heartbeat_and_storage():
    assert live_readiness.live_runtime_status(
        worker_ready=True,
        storage_roundtrip="ready",
    ) == {
        "status": "ready",
        "inline_worker": "ready",
        "storage_roundtrip": "ready",
    }


def test_live_runtime_status_fails_closed_without_worker_heartbeat():
    assert live_readiness.live_runtime_status(
        worker_ready=False,
        storage_roundtrip="ready",
    ) == {
        "status": "not_ready",
        "inline_worker": "not_ready",
        "storage_roundtrip": "ready",
    }


def test_free_tier_worker_readiness_requires_fresh_successful_db_heartbeat(monkeypatch):
    from litoral_trace.web import us_lacey_free_app

    class _Thread:
        @staticmethod
        def is_alive() -> bool:
            return True

    monkeypatch.setenv("US_LACEY_WORKER_POLL_SECONDS", "2")
    monkeypatch.setattr(us_lacey_free_app.time, "monotonic", lambda: 100.0)
    us_lacey_free_app.app.state.us_lacey_inline_worker_thread = _Thread()

    us_lacey_free_app.app.state.us_lacey_inline_worker_last_success_monotonic = 99.0
    assert us_lacey_free_app._inline_worker_ready() is True

    us_lacey_free_app.app.state.us_lacey_inline_worker_last_success_monotonic = 80.0
    assert us_lacey_free_app._inline_worker_ready() is False
