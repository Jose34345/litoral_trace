from __future__ import annotations

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
