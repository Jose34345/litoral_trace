from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from litoral_trace.lacey_engine.ai_providers import AIProviderConfig
from litoral_trace.lacey_engine.domain import DocumentResolution
from litoral_trace.us_lacey.lacey_engine_service import UsLaceyEngine2Service


class _Download:
    def __init__(self, content: bytes) -> None:
        self._content = content

    def iter_chunks(self):
        return iter((self._content,))


class _Vault:
    def __init__(self, by_public_id: dict[object, bytes]) -> None:
        self._by_public_id = by_public_id

    @contextmanager
    def materialize_verified_download(self, *, document_id, **_):
        yield _Download(self._by_public_id[document_id])


def _resolution() -> DocumentResolution:
    return DocumentResolution(
        engine_version="test-engine",
        filename="ok.pdf",
        document_type="UNKNOWN",
        role_hint="UNKNOWN",
        text="Bill of Lading: MAEU274342495",
        pages=(),
        fields=(),
        conflicts=(),
        warnings=(),
    )


def test_engine2_continues_after_one_document_failure_and_reports_partial(monkeypatch):
    """One unreadable source must not erase usable evidence from sibling sources."""
    service = UsLaceyEngine2Service(session_factory=lambda: None, vault_service=object())
    processed: list[str] = []

    def fake_process_document(*, filename: str, **_):
        processed.append(filename)
        if filename == "broken-scan.pdf":
            raise RuntimeError("synthetic OCR failure")
        return _resolution()

    monkeypatch.setattr("litoral_trace.us_lacey.lacey_engine_service.process_document", fake_process_document)

    # Contract-level helper introduced by Reliability V2: process every current
    # document independently and make the aggregate state explicit.
    outcome = service._process_engine2_document_batch(
        documents=(
            SimpleNamespace(filename="broken-scan.pdf"),
            SimpleNamespace(filename="invoice.pdf"),
            SimpleNamespace(filename="packing-list.pdf"),
        ),
        process_one=lambda document: fake_process_document(filename=document.filename),
    )

    assert processed == ["broken-scan.pdf", "invoice.pdf", "packing-list.pdf"]
    assert outcome.status == "PARTIAL"
    assert len(outcome.succeeded) == 2
    assert len(outcome.failed) == 1
    assert outcome.failed[0].safe_error_code == "ENGINE2_SHADOW_FAILED"


def test_engine2_failed_document_retry_identity_is_idempotent(monkeypatch):
    """The same immutable source failure may be retried without a duplicate FAILED row."""
    service = UsLaceyEngine2Service(session_factory=lambda: None, vault_service=object())
    existing = SimpleNamespace(id=41, status="FAILED")

    identity = service._engine2_document_run_identity(
        organization_id=7,
        assurance_document_id=11,
        source_sha256="a" * 64,
        role_hint="UNKNOWN",
        status="FAILED",
    )
    assert identity == {
        "organization_id": 7,
        "assurance_document_id": 11,
        "source_sha256": "a" * 64,
        "engine_version": service._engine_version,
        "schema_version": "lacey_document_resolution_v1",
        "role_hint": "UNKNOWN",
        "status": "FAILED",
    }

    # The persistence helper must return the existing immutable failure instead
    # of attempting another INSERT with the same unique identity.
    fake_session = SimpleNamespace()
    monkeypatch.setattr(service, "_find_engine2_document_run", lambda *_args, **_kwargs: existing)
    result = service._get_or_create_failed_engine2_run(
        fake_session,
        operation_id=13,
        operation_document_id=17,
        identity=identity,
    )
    assert result is existing
