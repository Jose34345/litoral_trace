from __future__ import annotations

from types import SimpleNamespace
from uuid import UUID

from litoral_trace.api import assurance as assurance_api
from litoral_trace.assurance.domain import DocumentProcessingStatus


def test_background_wrapper_contains_one_document_failure(monkeypatch):
    def boom(**kwargs):
        raise RuntimeError("synthetic background failure")

    monkeypatch.setattr(assurance_api, "_process_and_reconcile", boom)

    result = assurance_api._process_and_reconcile_safely(
        organization_id=70,
        assurance_public_id="00000000-0000-0000-0000-000000000001",
        force_reprocess=False,
    )

    assert result == DocumentProcessingStatus.FAILED.value


def test_only_incomplete_or_failed_duplicates_are_reprocessed():
    assert assurance_api._duplicate_requires_reprocess(
        DocumentProcessingStatus.UPLOADED.value
    ) is True
    assert assurance_api._duplicate_requires_reprocess(
        DocumentProcessingStatus.FAILED.value
    ) is True
    assert assurance_api._duplicate_requires_reprocess(
        DocumentProcessingStatus.PROCESSING.value
    ) is False
    assert assurance_api._duplicate_requires_reprocess(
        DocumentProcessingStatus.EXTRACTED.value
    ) is False
    assert assurance_api._duplicate_requires_reprocess(
        DocumentProcessingStatus.NEEDS_REVIEW.value
    ) is False


def _duplicate_result(processing_status: str):
    return SimpleNamespace(
        assurance_public_id=UUID("00000000-0000-0000-0000-000000000001"),
        vault_public_id=UUID("00000000-0000-0000-0000-000000000002"),
        filename="invoice.pdf",
        content_type="application/pdf",
        size_bytes=128,
        sha256="a" * 64,
        duplicate=True,
        processing_status=processing_status,
    )


def test_scheduled_duplicate_retry_is_not_serialized_as_terminal_duplicate():
    payload = assurance_api._serialize_ingestion(
        _duplicate_result(DocumentProcessingStatus.FAILED.value),
        reprocess_scheduled=True,
        poll_required=True,
    )

    assert payload["duplicate"] is False
    assert payload["reused_original"] is True
    assert payload["reprocess_scheduled"] is True
    assert payload["progress_url"].endswith("/progress")


def test_active_processing_duplicate_stays_attached_to_progress_without_new_work():
    result = _duplicate_result(DocumentProcessingStatus.PROCESSING.value)

    reprocess, poll = assurance_api._duplicate_processing_decision(
        organization_id=70,
        result=result,
    )
    payload = assurance_api._serialize_ingestion(
        result,
        reprocess_scheduled=reprocess,
        poll_required=poll,
    )

    assert reprocess is False
    assert poll is True
    assert payload["duplicate"] is False
    assert payload["reused_original"] is True
    assert payload["reprocess_scheduled"] is False


def test_ocr_required_review_is_reprocessed_after_runtime_fix(monkeypatch):
    result = _duplicate_result(DocumentProcessingStatus.NEEDS_REVIEW.value)

    def fake_progress(self, *, organization_id, assurance_public_id):
        assert organization_id == 70
        assert assurance_public_id == result.assurance_public_id
        return {
            "processing_status": DocumentProcessingStatus.NEEDS_REVIEW.value,
            "last_error_code": "OCR_REQUIRED",
        }

    monkeypatch.setattr(
        assurance_api.AssuranceProcessingService,
        "progress",
        fake_progress,
    )

    reprocess, poll = assurance_api._duplicate_processing_decision(
        organization_id=70,
        result=result,
    )

    assert reprocess is True
    assert poll is True


def test_non_ocr_review_remains_terminal_reuse(monkeypatch):
    result = _duplicate_result(DocumentProcessingStatus.NEEDS_REVIEW.value)

    monkeypatch.setattr(
        assurance_api.AssuranceProcessingService,
        "progress",
        lambda self, **kwargs: {
            "processing_status": DocumentProcessingStatus.NEEDS_REVIEW.value,
            "last_error_code": "REQUIRED_FIELDS_MISSING",
        },
    )

    reprocess, poll = assurance_api._duplicate_processing_decision(
        organization_id=70,
        result=result,
    )
    payload = assurance_api._serialize_ingestion(
        result,
        reprocess_scheduled=reprocess,
        poll_required=poll,
    )

    assert reprocess is False
    assert poll is False
    assert payload["duplicate"] is True
    assert payload["reused_original"] is True


def test_completed_duplicate_remains_terminal_for_workspace_reuse():
    payload = assurance_api._serialize_ingestion(
        _duplicate_result(DocumentProcessingStatus.NEEDS_REVIEW.value),
        reprocess_scheduled=False,
        poll_required=False,
    )

    assert payload["duplicate"] is True
    assert payload["reused_original"] is True
    assert payload["reprocess_scheduled"] is False
