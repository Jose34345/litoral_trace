from __future__ import annotations

from types import SimpleNamespace

import pytest

import litoral_trace.api.assurance as assurance_api
from litoral_trace.assurance.domain import DocumentProcessingStatus


def _flags(*, reconciliation: bool = True):
    # These orchestration regressions isolate the historical process→reconcile
    # contract. Supplier resolution has its own service/E2E acceptance tests, so
    # keep Document Intelligence explicitly disabled here instead of relying on
    # an incomplete feature-flag mock.
    return SimpleNamespace(
        assurance_v1=True,
        document_intelligence=False,
        reconciliation=reconciliation,
        operational_exceptions=False,
    )


def test_pipeline_is_marked_complete_only_with_reconciliation_outcome(monkeypatch):
    events: list[str] = []
    captured: dict[str, object] = {}

    class Processing:
        def process(self, **kwargs):
            del kwargs
            events.append("process")
            return DocumentProcessingStatus.EXTRACTED.value

    class Reconciliation:
        def reconcile_document(self, **kwargs):
            del kwargs
            events.append("reconcile")
            return SimpleNamespace(
                operation_count=1,
                finding_count=2,
                created_count=1,
                refreshed_count=1,
                reopened_count=0,
                auto_resolved_count=0,
            )

    def mark(**kwargs):
        events.append("complete")
        captured.update(kwargs["metadata"])

    monkeypatch.setattr(assurance_api, "AssuranceProcessingService", Processing)
    monkeypatch.setattr(assurance_api, "AssuranceReconciliationService", Reconciliation)
    monkeypatch.setattr(assurance_api, "get_assurance_feature_flags", lambda: _flags())
    monkeypatch.setattr(assurance_api, "mark_pipeline_completed", mark)

    result = assurance_api._process_and_reconcile(
        organization_id=7,
        assurance_public_id="00000000-0000-0000-0000-000000000007",
    )

    assert result == DocumentProcessingStatus.EXTRACTED.value
    assert events == ["process", "reconcile", "complete"]
    assert captured["reconciliation_operation_count"] == 1
    assert captured["reconciliation_finding_count"] == 2
    assert captured["reconciliation_created_count"] == 1
    assert captured["reconciliation_refreshed_count"] == 1


def test_pipeline_failure_is_published_instead_of_leaving_workspace_polling(monkeypatch):
    markers: list[dict[str, object]] = []

    class Processing:
        def process(self, **kwargs):
            del kwargs
            return DocumentProcessingStatus.NEEDS_REVIEW.value

    class BrokenReconciliation:
        def reconcile_document(self, **kwargs):
            del kwargs
            raise RuntimeError("reconciliation unavailable")

    monkeypatch.setattr(assurance_api, "AssuranceProcessingService", Processing)
    monkeypatch.setattr(assurance_api, "AssuranceReconciliationService", BrokenReconciliation)
    monkeypatch.setattr(assurance_api, "get_assurance_feature_flags", lambda: _flags())
    monkeypatch.setattr(
        assurance_api,
        "mark_pipeline_completed",
        lambda **kwargs: markers.append(dict(kwargs["metadata"])),
    )

    with pytest.raises(RuntimeError, match="reconciliation unavailable"):
        assurance_api._process_and_reconcile(
            organization_id=7,
            assurance_public_id="00000000-0000-0000-0000-000000000007",
        )

    assert len(markers) == 1
    assert markers[0]["pipeline_error_code"] == "RuntimeError"


def test_failed_extraction_does_not_require_reconciliation_marker(monkeypatch):
    markers: list[dict[str, object]] = []

    class Processing:
        def process(self, **kwargs):
            del kwargs
            return DocumentProcessingStatus.FAILED.value

    monkeypatch.setattr(assurance_api, "AssuranceProcessingService", Processing)
    monkeypatch.setattr(assurance_api, "get_assurance_feature_flags", lambda: _flags())
    monkeypatch.setattr(assurance_api, "mark_pipeline_completed", lambda **kwargs: markers.append(kwargs))

    result = assurance_api._process_and_reconcile(
        organization_id=7,
        assurance_public_id="00000000-0000-0000-0000-000000000007",
    )
    assert result == DocumentProcessingStatus.FAILED.value
    assert markers == []
