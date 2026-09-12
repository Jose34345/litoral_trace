"""Jinja-backed operational workspace views for the U.S. Lacey portal."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
import logging

from markupsafe import Markup, escape

from litoral_trace.us_lacey.candidate_normalization import group_candidate_evidence
from litoral_trace.us_lacey.ppq505 import PPQ505_FIELDS_BY_KEY
from litoral_trace.us_lacey.semantic_evidence_read import (
    EvidenceTextView,
    SemanticEvidenceReadService,
)
from litoral_trace.web.templates import templates


LOGGER = logging.getLogger(__name__)


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
        # Filtering is a deliberate customer-safety decision. Never fall back to the
        # original candidate tuple when every value was rejected as structural noise.
        return replace(field, candidates=())
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


def _semantic_evidence_for_detail(identity, detail) -> dict[str, tuple[EvidenceTextView, ...]]:
    """Read optional Phase D evidence without making the legacy UI depend on it."""
    organization_id = getattr(identity, "organization_id", None)
    operation_public_id = getattr(detail, "public_id", None)
    if not organization_id or operation_public_id is None:
        return {}
    try:
        return SemanticEvidenceReadService().get_operation_evidence(
            organization_id=int(organization_id),
            operation_public_id=operation_public_id,
        )
    except Exception:
        # The semantic layer remains additive during Phase D. A read-side problem must
        # never hide the authoritative human-review workflow.
        LOGGER.exception(
            "us_lacey_semantic_evidence_read_failed",
            extra={"organization_id": int(organization_id)},
        )
        return {}


def _evidence_for_field(field, evidence_by_field: Mapping[str, tuple[EvidenceTextView, ...]]):
    items = tuple(evidence_by_field.get(str(getattr(field, "field_name", "")), ()))
    if not items:
        return ()
    source_document_id = getattr(field, "source_assurance_document_id", None)
    source_page = getattr(field, "source_page", None)
    preferred = tuple(
        item
        for item in items
        if (source_document_id is None or item.source_assurance_document_id == source_document_id)
        and (source_page is None or str(item.source_page) == str(source_page))
    )
    return preferred or items


def _semantic_evidence_markup(evidence: EvidenceTextView) -> Markup:
    """Inline-safe evidence UI for the existing Jinja review card.

    The translated interpretation is visible by default. The badge's native tooltip
    always exposes the immutable original source text, including on a no-JS page.
    """
    display = escape(evidence.display_text)
    source_meta = Markup(
        '<span class="text-xs text-slate-500">Document evidence · page {}</span>'
    ).format(escape(str(evidence.source_page)))
    if evidence.is_translated:
        badge = Markup(
            '<span class="ml-2 inline-flex rounded-full bg-sky-50 px-2 py-0.5 text-xs font-semibold text-sky-800" '
            'data-translation-badge data-original-language="{}" title="Original evidence: {}">Translated from {}</span>'
        ).format(
            escape(evidence.original_language),
            escape(evidence.original_text),
            escape(evidence.original_language_label),
        )
    else:
        badge = Markup("")
    return Markup(
        '<br><span class="mt-2 inline-block text-sm text-slate-700" data-semantic-evidence '
        'data-source-span-id="{}"><span class="font-semibold">Evidence:</span> {}</span> {} {}'
    ).format(
        escape(str(evidence.source_span_id)),
        display,
        badge,
        source_meta,
    )


def _decorate_review_fields(fields, evidence_by_field: Mapping[str, tuple[EvidenceTextView, ...]]):
    """Attach display_text to review cards without mutating evidence or form values."""
    decorated = []
    for field in fields:
        proposed_value = getattr(field, "proposed_value", None)
        if not proposed_value:
            decorated.append(field)
            continue
        evidence_items = _evidence_for_field(field, evidence_by_field)
        if not evidence_items:
            decorated.append(field)
            continue
        presentation = Markup("{}").format(escape(str(proposed_value)))
        for evidence in evidence_items:
            presentation += _semantic_evidence_markup(evidence)
        decorated.append(replace(field, proposed_value=presentation))
    return decorated


def _review_field_sets_with_semantic_evidence(identity, detail):
    exception_fields, settled_fields = _review_field_sets(detail)
    evidence_by_field = _semantic_evidence_for_detail(identity, detail)
    return (
        _decorate_review_fields(exception_fields, evidence_by_field),
        _decorate_review_fields(settled_fields, evidence_by_field),
    )


def render_operations(*, request, identity, operations: Sequence, entitlement) -> str:
    return _render(request, "operations", identity=identity, operations=operations, entitlement=entitlement)


def render_new_operation(*, request, identity, entitlement, csrf_token: str, error: str | None = None) -> str:
    return _render(request, "new_operation", identity=identity, entitlement=entitlement, csrf_token=csrf_token, error=error)


def render_operation_detail(*, request, identity, detail, engine2_dossier, upload_csrf: str, complete_csrf: str, review_csrf: Mapping[int, str], error: str | None = None, notice: str | None = None) -> str:
    exception_fields, settled_fields = _review_field_sets_with_semantic_evidence(identity, detail)
    progress = processing_view(detail)
    return _render(request, "operation_detail", identity=identity, detail=detail, engine2_dossier=engine2_dossier, upload_csrf=upload_csrf, complete_csrf=complete_csrf, review_csrf=review_csrf, exception_fields=exception_fields, settled_fields=settled_fields, processing=progress, error=error, notice=notice)


def render_processing_fragment(*, request, detail) -> str:
    return _render(request, "fragments/processing_fragment", detail=detail, processing=processing_view(detail))


def render_operation_workspace(*, request, identity, detail, engine2_dossier, complete_csrf: str, review_csrf: Mapping[int, str], error: str | None = None, is_oob_update: bool | None = None) -> str:
    exception_fields, settled_fields = _review_field_sets_with_semantic_evidence(identity, detail)
    # Initial workspace hydration is a GET and must render its own summary/banner/final
    # confirmation normally. Review mutations are POSTs and update those regions OOB.
    if is_oob_update is None:
        is_oob_update = str(getattr(request, "method", "GET")).upper() == "POST"
    return _render(request, "fragments/operation_workspace", identity=identity, detail=detail, engine2_dossier=engine2_dossier, complete_csrf=complete_csrf, review_csrf=review_csrf, exception_fields=exception_fields, settled_fields=settled_fields, error=error, is_oob_update=is_oob_update)
