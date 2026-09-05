"""Jinja-backed operational workspace views for the U.S. Lacey portal."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from litoral_trace.web.templates import templates


def _render(request, name: str, **context: object) -> str:
    return templates.get_template(f"us_lacey/{name}.html").render(request=request, **context)


@dataclass(frozen=True, slots=True)
class ProcessingView:
    """Small, customer-safe operation progress projection.

    This intentionally derives progress only from durable document/job states;
    it is not a client timer and it does not wait for optional AI shadow work.
    """

    percent: int
    state: str
    message: str
    terminal: bool
    failed: bool


def processing_view(detail) -> ProcessingView:
    documents = tuple(detail.documents)
    statuses = {str(document.job_status or "").upper() for document in documents}
    document_states = {str(document.processing_status or "").upper() for document in documents}
    if "FAILED" in statuses or "FAILED" in document_states:
        return ProcessingView(100, "FAILED", "We couldn't finish processing this document.", True, True)
    if detail.status == "COMPLETED":
        return ProcessingView(100, "COMPLETED", "Preparation complete.", True, False)
    if "RUNNING" in statuses:
        return ProcessingView(60, "RUNNING", "Extracting shipment information", False, False)
    if statuses & {"QUEUED", "RETRY"}:
        return ProcessingView(35, "QUEUED", "Document queued for secure analysis", False, False)
    if "COMPLETED" in statuses:
        return ProcessingView(100, "READY_FOR_REVIEW", "Document analysis complete", True, False)
    if documents and document_states & {"EXTRACTED", "EXTRACTION_COMPLETE", "RECONCILED"}:
        return ProcessingView(90, "RECONCILING", "Reconciling document evidence", False, False)
    if documents and not statuses:
        return ProcessingView(20, "STORED", "Document securely stored", False, False)
    if documents:
        return ProcessingView(100, "READY_FOR_REVIEW", "Document analysis complete", True, False)
    return ProcessingView(0, "WAITING_FOR_DOCUMENT", "Add a document to begin analysis", True, False)


def render_operations(*, request, identity, operations: Sequence, entitlement) -> str:
    return _render(request, "operations", identity=identity, operations=operations, entitlement=entitlement)


def render_new_operation(*, request, identity, entitlement, csrf_token: str, error: str | None = None) -> str:
    return _render(request, "new_operation", identity=identity, entitlement=entitlement, csrf_token=csrf_token, error=error)


def render_operation_detail(*, request, identity, detail, engine2_dossier, upload_csrf: str, complete_csrf: str, review_csrf: Mapping[int, str], error: str | None = None, notice: str | None = None) -> str:
    exception_fields = [field for field in detail.fields if field.status in {"MISSING", "REVIEW"}]
    settled_fields = [field for field in detail.fields if field.status not in {"MISSING", "REVIEW"}]
    progress = processing_view(detail)
    return _render(request, "operation_detail", identity=identity, detail=detail, engine2_dossier=engine2_dossier, upload_csrf=upload_csrf, complete_csrf=complete_csrf, review_csrf=review_csrf, exception_fields=exception_fields, settled_fields=settled_fields, processing=progress, error=error, notice=notice)


def render_processing_fragment(*, request, detail) -> str:
    return _render(request, "fragments/processing_fragment", detail=detail, processing=processing_view(detail))


def render_operation_workspace(*, request, identity, detail, engine2_dossier, complete_csrf: str, review_csrf: Mapping[int, str]) -> str:
    exception_fields = [field for field in detail.fields if field.status in {"MISSING", "REVIEW"}]
    settled_fields = [field for field in detail.fields if field.status not in {"MISSING", "REVIEW"}]
    return _render(request, "fragments/operation_workspace", identity=identity, detail=detail, engine2_dossier=engine2_dossier, complete_csrf=complete_csrf, review_csrf=review_csrf, exception_fields=exception_fields, settled_fields=settled_fields)
