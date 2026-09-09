"""Jinja-backed operational workspace views for the U.S. Lacey portal."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace

from litoral_trace.us_lacey.candidate_normalization import group_candidate_evidence
from litoral_trace.us_lacey.ppq505 import PPQ505_FIELDS_BY_KEY
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


def _field_has_displayable_resolution(field) -> bool:
    """Only count/display a settled field when it actually contains a resolution.

    Optional PPQ fields can be initialized in a non-review status with no value so
    that they do not block completion. Those empty placeholders are operationally
    settled, but presenting them as customer-confirmed data is misleading.
    """
    if str(field.status or "").upper() == "NOT_REQUIRED":
        return True
    value = field.effective_value
    return value is not None and bool(str(value).strip())


def _present_review_field(field):
    """Collapse same-value candidate metadata for the customer review card.

    Database candidate rows remain intact. The view exposes one representative per
    canonical value, using the highest confidence while merging page references into
    the representative page label. Thus page/confidence differences do not render as
    separate conflicting choices.

    ``_review_field_sets`` is also exercised with deliberately lightweight view
    doubles in contract tests. Candidate presentation is optional enrichment, so a
    field without the full candidate shape must retain the legacy behavior unchanged.
    """
    field_name = getattr(field, "field_name", None)
    candidates = getattr(field, "candidates", ())
    if not field_name or not candidates:
        return field
    groups = group_candidate_evidence(field_name, candidates)
    if not groups:
        return field
    presented = []
    for group in groups:
        representative = group.representative
        page_value = representative.source_page
        if len(group.source_pages) > 1:
            page_value = ", ".join(str(page) for page in group.source_pages)
        elif len(group.source_pages) == 1:
            page_value = group.source_pages[0]
        presented.append(
            replace(
                representative,
                confidence=float(group.confidence),
                source_page=page_value,
            )
        )
    return replace(field, candidates=tuple(presented))


_OPEN_REVIEW_STATUSES = frozenset({"MISSING", "REVIEW", "FOUND"})


def _is_customer_ppq_field(field) -> bool:
    """Hide internal reconciliation evidence from the PPQ review queue.

    Lightweight contract-test doubles intentionally omit ``field_name``; preserve
    their legacy behavior while production rows must belong to the published PPQ
    contract to appear as customer-editable preparation fields.
    """
    field_name = getattr(field, "field_name", None)
    return field_name is None or field_name in PPQ505_FIELDS_BY_KEY


def _review_field_sets(detail):
    # FOUND means the pipeline has a supported proposal but no human has accepted it
    # yet. Keeping FOUND in the review queue makes the UI truthful and enables a safe
    # one-click confirmation workflow without presenting AI/extraction as final data.
    customer_fields = tuple(field for field in detail.fields if _is_customer_ppq_field(field))
    article_components = {
        str(getattr(field, "line_reference", "")): field
        for field in customer_fields
        if getattr(field, "field_name", None) == "article_component"
    }

    def is_open_review_field(field) -> bool:
        if getattr(field, "status", None) not in _OPEN_REVIEW_STATUSES:
            return False
        if getattr(field, "field_name", None) != "percent_recycled":
            return True
        article = article_components.get(str(getattr(field, "line_reference", "")))
        return article is None or getattr(article, "status", None) not in _OPEN_REVIEW_STATUSES

    exception_fields = [
        _present_review_field(field)
        for field in customer_fields
        if is_open_review_field(field)
    ]
    settled_fields = [
        field
        for field in customer_fields
        if field.status not in _OPEN_REVIEW_STATUSES
        and _field_has_displayable_resolution(field)
    ]
    return exception_fields, settled_fields


def render_operations(*, request, identity, operations: Sequence, entitlement) -> str:
    return _render(request, "operations", identity=identity, operations=operations, entitlement=entitlement)


def render_new_operation(*, request, identity, entitlement, csrf_token: str, error: str | None = None) -> str:
    return _render(request, "new_operation", identity=identity, entitlement=entitlement, csrf_token=csrf_token, error=error)


def render_operation_detail(*, request, identity, detail, engine2_dossier, upload_csrf: str, complete_csrf: str, review_csrf: Mapping[int, str], error: str | None = None, notice: str | None = None) -> str:
    exception_fields, settled_fields = _review_field_sets(detail)
    progress = processing_view(detail)
    return _render(request, "operation_detail", identity=identity, detail=detail, engine2_dossier=engine2_dossier, upload_csrf=upload_csrf, complete_csrf=complete_csrf, review_csrf=review_csrf, exception_fields=exception_fields, settled_fields=settled_fields, processing=progress, error=error, notice=notice)


def render_processing_fragment(*, request, detail) -> str:
    return _render(request, "fragments/processing_fragment", detail=detail, processing=processing_view(detail))


def render_operation_workspace(*, request, identity, detail, engine2_dossier, complete_csrf: str, review_csrf: Mapping[int, str], error: str | None = None) -> str:
    exception_fields, settled_fields = _review_field_sets(detail)
    return _render(request, "fragments/operation_workspace", identity=identity, detail=detail, engine2_dossier=engine2_dossier, complete_csrf=complete_csrf, review_csrf=review_csrf, exception_fields=exception_fields, settled_fields=settled_fields, error=error)
