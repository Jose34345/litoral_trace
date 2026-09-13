from __future__ import annotations

from types import SimpleNamespace

from litoral_trace.lacey_engine.serialization import DOCUMENT_RESOLUTION_SCHEMA_VERSION
from litoral_trace.us_lacey.lacey_engine_service import UsLaceyEngine2Service


def test_engine2_continues_after_one_document_failure_and_reports_partial(monkeypatch):
    """One unreadable source must not erase usable evidence from sibling sources."""
    service = UsLaceyEngine2Service(session_factory=lambda: None, vault_service=object())
    processed: list[str] = []

    def process_one(document):
        processed.append(document.filename)
        if document.filename == "broken-scan.pdf":
            raise RuntimeError("synthetic OCR failure")
        return f"resolution:{document.filename}"

    outcome = service._process_engine2_document_batch(
        documents=(
            SimpleNamespace(filename="broken-scan.pdf"),
            SimpleNamespace(filename="invoice.pdf"),
            SimpleNamespace(filename="packing-list.pdf"),
        ),
        process_one=process_one,
    )

    assert processed == ["broken-scan.pdf", "invoice.pdf", "packing-list.pdf"]
    assert outcome.status == "PARTIAL"
    assert [item.result for item in outcome.succeeded] == [
        "resolution:invoice.pdf",
        "resolution:packing-list.pdf",
    ]
    assert len(outcome.failed) == 1
    assert outcome.failed[0].document.filename == "broken-scan.pdf"
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
        "schema_version": DOCUMENT_RESOLUTION_SCHEMA_VERSION,
        "role_hint": "UNKNOWN",
        "status": "FAILED",
    }

    fake_session = SimpleNamespace()
    monkeypatch.setattr(service, "_find_engine2_document_run", lambda *_args, **_kwargs: existing)
    result = service._get_or_create_failed_engine2_run(
        fake_session,
        operation_id=13,
        operation_document_id=17,
        identity=identity,
    )
    assert result is existing
