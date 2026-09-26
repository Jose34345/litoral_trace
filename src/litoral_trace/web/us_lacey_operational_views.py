"""Jinja-backed operational workspace views for the U.S. Lacey portal."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
import logging

from markupsafe import Markup, escape

from litoral_trace.us_lacey.candidate_normalization import (
    TaxonomicComparisonContext,
    group_candidate_evidence,
)
from litoral_trace.us_lacey.ppq505 import (
    PPQ505_FIELDS_BY_KEY,
    canonical_ppq_value_key,
)
from litoral_trace.us_lacey.product_intelligence_snapshot import (
    get_current_product_intelligence_view,
)
from litoral_trace.us_lacey.regulatory_assessment_snapshot import (
    get_current_regulatory_assessment_view,
)
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
    error_codes = {
        str(getattr(document, "last_error_code", None) or "").upper()
        for document in documents
    }
    if "UNSUPPORTED_DOMAIN" in error_codes:
        return ProcessingView(
            100,
            "UNSUPPORTED_DOMAIN",
            (
                "This file does not appear to be a commercial invoice, packing list, "
                "or transport document. Litoral Trace cannot process legal or "
                "administrative files."
            ),
            True,
            True,
        )
    if "FAILED" in statuses or "FAILED" in document_states:
        return ProcessingView(100, "FAILED", "We couldn't finish processing this document.", True, True)
    if detail.status == "COMPLETED":
        return ProcessingView(100, "COMPLETED", "Preparation complete.", True, False)
    if "RUNNING" in statuses:
        completed_jobs = sum(
            str(document.job_status or "").upper() == "COMPLETED"
            for document in documents
        )
        durable_extraction_states = {"EXTRACTED", "EXTRACTION_COMPLETE", "RECONCILED"}
        all_documents_extracted = bool(documents) and all(
            str(document.processing_status or "").upper() in durable_extraction_states
            for document in documents
        )
        if all_documents_extracted and completed_jobs >= max(1, len(documents) - 1):
            return ProcessingView(90, "RECONCILING", "Reconciling shipment evidence", False, False)
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



_ACTION_REQUIRED_STATUSES = frozenset({"MISSING", "CONFLICT", "REVIEW", "REVIEW_REQUIRED"})
_AUTO_SUPPORTED_STATUSES = frozenset({"SUPPORTED", "FOUND", "SUPPORTED MULTIPLE", "SUPPORTED_MULTIPLE"})
_SETTLED_STATUSES = frozenset({"MATCHED", "NOT_REQUIRED"})

_ENGINE2_TO_PREPARATION_FIELD = {
    "estimated_arrival_date": "estimated_arrival_date",
    "bill_of_lading": "bill_of_lading",
    "container_number": "container_number",
    "importer_name": "importer_name",
    "importer_address": "importer_address",
    "consignee_name": "consignee_name",
    "consignee_address": "consignee_address",
    "shipper_name": "shipper_name",
    "supplier_name": "supplier_name",
    "manufacturer_name": "manufacturer_name",
    "manufacturer_id": "manufacturer_id",
    "filing_entry_reference": "filing_entry_reference",
    "country_of_origin": "country_of_origin",
    "description": "merchandise_description",
    "hts_code": "hts_code",
    "entered_value": "entered_value",
    "article_component": "article_component",
    "genus": "genus",
    "species": "species",
    "country_of_harvest": "country_of_harvest",
    "plant_quantity": "plant_quantity",
    "metric_unit": "metric_unit",
    "percent_recycled": "percent_recycled",
}

_DOWNSTREAM_RESOLVED_STATUSES = (
    _AUTO_SUPPORTED_STATUSES
    | _SETTLED_STATUSES
)


def _engine2_downstream_annotations(detail) -> dict[str, str]:
    """Explain when canonical/preparation stages resolved an Engine 2 gap.

    Engine 2's dossier is intentionally a direct-evidence audit surface. A field can
    therefore be MISSING there while the authoritative preparation record is safely
    resolved downstream. Expose that distinction without mutating either source.
    """
    fields_by_name: dict[str, list[object]] = {}
    for field in tuple(getattr(detail, "fields", ()) or ()):
        field_name = str(getattr(field, "field_name", "") or "").strip()
        if field_name:
            fields_by_name.setdefault(field_name, []).append(field)

    annotations: dict[str, str] = {}
    for engine_key, preparation_field in _ENGINE2_TO_PREPARATION_FIELD.items():
        matching = fields_by_name.get(preparation_field, ())
        if not matching:
            continue

        statuses = {
            str(getattr(field, "status", "") or "").upper()
            for field in matching
        }
        if statuses and statuses <= {"NOT_REQUIRED"}:
            annotations[engine_key] = "Not required by preparation rule"
            continue

        if (
            statuses
            and not (statuses & _ACTION_REQUIRED_STATUSES)
            and statuses <= _DOWNSTREAM_RESOLVED_STATUSES
            and all(_field_has_displayable_resolution(field) for field in matching)
        ):
            annotations[engine_key] = "Resolved downstream by Canonical Truth"

    return annotations

_REGULATORY_RULE_TITLES = {
    "HTS_APPLICABILITY": "HTS Schedule Coverage",
    "DE_MINIMIS": "De Minimis Exemption Assessment",
    "SPECIAL_COMPOSITE": "Special Composite Wood Pathway",
    "SPECIAL_RECYCLED": "Recycled Material Exception",
}

_REGULATORY_STATUS_LABELS = {
    "PASS": "Check passed",
    "FAIL": "Needs review",
    "INDETERMINATE": "Needs information",
    "NOT_APPLICABLE": "Not applicable",
}

_REGULATORY_ACTION_GUIDANCE = {
    "HTS_APPLICABILITY": (
        "To evaluate APHIS Lacey schedule coverage, add or correct the 10-digit "
        "HTS code in the Action Required tab."
    ),
    "DE_MINIMIS": (
        "To evaluate the De Minimis exemption, provide the missing quantities "
        "or values in the Action Required tab."
    ),
    "SPECIAL_COMPOSITE": (
        "To evaluate the special composite wood pathway, provide the missing "
        "product-composition or component evidence in the Action Required tab."
    ),
    "SPECIAL_RECYCLED": (
        "To evaluate the recycled-material exception, provide the missing "
        "recycled-content or material evidence in the Action Required tab."
    ),
}


def _regulatory_customer_message(assessment: Mapping[str, object]) -> str:
    """Translate deterministic rule output into customer-facing review guidance."""
    rule_id = str(assessment.get("rule_id") or "").upper()
    status = str(assessment.get("status") or "").upper()
    explanation = str(assessment.get("explanation") or "").strip()

    if status == "INDETERMINATE":
        return _REGULATORY_ACTION_GUIDANCE.get(
            rule_id,
            "Additional supported information is required. Review the missing "
            "items in the Action Required tab.",
        )
    if status == "FAIL":
        return explanation or (
            "This rule needs human review before the declaration package can be finalized."
        )
    return explanation or "No additional action is required for this rule."


def _present_regulatory_assessment(view):
    """Add stable presentation labels without mutating deterministic rule output."""
    if view is None:
        return None
    payload = dict(getattr(view, "payload", {}) or {})
    presented_assessments: list[dict[str, object]] = []
    for raw in payload.get("assessments", ()):
        if not isinstance(raw, Mapping):
            continue
        item = dict(raw)
        rule_id = str(item.get("rule_id") or "")
        status = str(item.get("status") or "")
        subject_ref = str(item.get("subject_ref") or "").strip()
        item["display_title"] = _REGULATORY_RULE_TITLES.get(
            rule_id,
            rule_id.replace("_", " ").title() or "Regulatory check",
        )
        item["display_status"] = _REGULATORY_STATUS_LABELS.get(
            status,
            status.replace("_", " ").title() or "Review",
        )
        item["display_subject"] = (
            f"Plant line {subject_ref}" if subject_ref else "Shipment"
        )
        item["customer_message"] = _regulatory_customer_message(item)
        presented_assessments.append(item)
    payload["assessments"] = presented_assessments
    return replace(view, payload=payload)



def _is_customer_ppq_field(field) -> bool:
    field_name = getattr(field, "field_name", None)
    return field_name is None or field_name in PPQ505_FIELDS_BY_KEY


def _taxonomic_context_for_presented_field(
    field,
    all_fields,
) -> TaxonomicComparisonContext | None:
    """Return same-line genus context when presenting species alternatives."""
    if getattr(field, "field_name", None) != "species":
        return None

    line_reference = str(getattr(field, "line_reference", ""))
    genus_field = next(
        (
            candidate
            for candidate in all_fields
            if (
                str(getattr(candidate, "line_reference", "")) == line_reference
                and getattr(candidate, "field_name", None) == "genus"
            )
        ),
        None,
    )
    if genus_field is None:
        return None

    genus = (
        getattr(genus_field, "human_value", None)
        or getattr(genus_field, "normalized_value", None)
        or getattr(genus_field, "original_value", None)
    )
    if genus is None or not str(genus).strip():
        return None
    return TaxonomicComparisonContext(genus=str(genus).strip())


def _present_review_field(field, *, all_fields=()):
    """Collapse semantically equivalent evidence for customer presentation."""
    field_name = getattr(field, "field_name", None)
    candidates = getattr(field, "candidates", ())
    if not field_name or not candidates:
        return field

    comparison_fields = tuple(all_fields) or (field,)
    groups = group_candidate_evidence(
        field_name,
        candidates,
        comparison_context=_taxonomic_context_for_presented_field(
            field,
            comparison_fields,
        ),
    )
    if not groups:
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


def _review_field_groups(detail):
    """Partition the customer workflow into exception-first presentation buckets."""
    customer_fields = tuple(
        field for field in detail.fields if _is_customer_ppq_field(field)
    )
    presented = tuple(
        _present_review_field(field, all_fields=customer_fields)
        for field in customer_fields
    )

    attention_fields = tuple(
        field
        for field in presented
        if getattr(field, "status", None) in _ACTION_REQUIRED_STATUSES
    )
    auto_supported_fields = tuple(
        field
        for field in presented
        if (
            getattr(field, "status", None) in _AUTO_SUPPORTED_STATUSES
            and bool(
                getattr(field, "proposed_value", None)
                or getattr(field, "effective_value", None)
            )
        )
    )
    settled_fields = tuple(
        field
        for field in presented
        if (
            getattr(field, "status", None) in _SETTLED_STATUSES
            and _field_has_displayable_resolution(field)
        )
    )
    return attention_fields, auto_supported_fields, settled_fields


def _semantic_evidence_for_detail(identity, detail) -> dict[str, tuple[EvidenceTextView, ...]]:
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
        LOGGER.exception(
            "us_lacey_semantic_evidence_read_failed",
            extra={"organization_id": int(organization_id)},
        )
        return {}


def _evidence_for_field(field, evidence_by_field: Mapping[str, tuple[EvidenceTextView, ...]]):
    """Return evidence that is safe to render on one review card.

    Shipment-scoped fields keep the legacy document/page preference. Plant-line
    fields fail closed: evidence must support the same value on the same source
    document/page, or match the exact source locator. This prevents evidence for
    one botanical line from appearing on another line's review card.
    """
    field_name = str(getattr(field, "field_name", "") or "")
    items = tuple(evidence_by_field.get(field_name, ()))
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
    candidates = preferred or items

    if str(getattr(field, "scope", "") or "").upper() != "PLANT_LINE":
        return candidates

    value = (
        getattr(field, "effective_value", None)
        or getattr(field, "proposed_value", None)
    )
    if value is not None:
        target_key = canonical_ppq_value_key(field_name, value)
        if target_key:
            value_matches = tuple(
                item
                for item in candidates
                if canonical_ppq_value_key(
                    field_name,
                    item.normalized_value
                    or item.display_text
                    or item.original_text,
                )
                == target_key
            )
            if value_matches:
                return value_matches

    source_locator = str(getattr(field, "source_locator", "") or "").strip()
    if source_locator:
        locator_matches = tuple(
            item
            for item in candidates
            if str(item.source_locator or "").strip() == source_locator
        )
        if locator_matches:
            return locator_matches

    # Showing no semantic evidence is safer than leaking evidence from another
    # merchandise line. The card's own source metadata remains available.
    return ()


def _semantic_evidence_markup(evidence: EvidenceTextView) -> Markup:
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
    """Preserve rich semantic evidence where detailed human review needs it."""
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


def _auto_resolved_evidence_summary(fields):
    """Collapse repeated Auto-Resolved evidence into unique source-page references."""
    summarized = []
    for field in fields:
        pages: set[int] = set()
        for candidate in tuple(getattr(field, "candidates", ()) or ()):
            raw_page = getattr(candidate, "source_page", None)
            if raw_page is None:
                continue
            for part in str(raw_page).split(","):
                token = part.strip()
                if token.isdigit():
                    pages.add(int(token))

        if not pages:
            raw_page = getattr(field, "source_page", None)
            if raw_page is not None:
                for part in str(raw_page).split(","):
                    token = part.strip()
                    if token.isdigit():
                        pages.add(int(token))

        if not pages:
            summarized.append(field)
            continue

        summarized.append(
            replace(field, source_page=", ".join(str(page) for page in sorted(pages)))
        )
    return summarized


def _review_field_groups_with_semantic_evidence(identity, detail):
    attention_fields, auto_supported_fields, settled_fields = _review_field_groups(detail)
    evidence_by_field = _semantic_evidence_for_detail(identity, detail)
    return (
        _decorate_review_fields(attention_fields, evidence_by_field),
        _auto_resolved_evidence_summary(auto_supported_fields),
        _decorate_review_fields(settled_fields, evidence_by_field),
    )


def _product_intelligence_for_detail(identity, detail, explicit_view=None):
    """Best-effort additive read; canonical review UI must remain available on failure."""
    if explicit_view is not None:
        return explicit_view
    organization_id = getattr(identity, "organization_id", None)
    operation_public_id = getattr(detail, "public_id", None)
    if not organization_id or operation_public_id is None:
        return None
    try:
        return get_current_product_intelligence_view(
            organization_id=int(organization_id),
            operation_public_id=operation_public_id,
        )
    except Exception:
        LOGGER.exception(
            "us_lacey_product_intelligence_read_failed",
            extra={"organization_id": int(organization_id)},
        )
        return None


def _regulatory_assessment_for_detail(identity, detail, explicit_view=None):
    """Best-effort non-canonical rules read; never block the authoritative review UI."""
    if explicit_view is not None:
        return explicit_view
    organization_id = getattr(identity, "organization_id", None)
    operation_public_id = getattr(detail, "public_id", None)
    if not organization_id or operation_public_id is None:
        return None
    try:
        return get_current_regulatory_assessment_view(
            organization_id=int(organization_id),
            operation_public_id=operation_public_id,
        )
    except Exception:
        LOGGER.exception(
            "us_lacey_regulatory_assessment_read_failed",
            extra={"organization_id": int(organization_id)},
        )
        return None


def render_operations(*, request, identity, operations: Sequence, entitlement) -> str:
    return _render(request, "operations", identity=identity, operations=operations, entitlement=entitlement)


def render_new_operation(*, request, identity, entitlement, csrf_token: str, error: str | None = None) -> str:
    return _render(request, "new_operation", identity=identity, entitlement=entitlement, csrf_token=csrf_token, error=error)


def render_operation_detail(*, request, identity, detail, engine2_dossier, upload_csrf: str, complete_csrf: str, review_csrf: Mapping[int, str], alias_csrf: str = "", retry_csrf: str = "", product_intelligence=None, regulatory_assessment=None, error: str | None = None, notice: str | None = None, field_errors: Mapping[int, str] | None = None, field_input_values: Mapping[int, str] | None = None) -> str:
    attention_fields, auto_supported_fields, settled_fields = _review_field_groups_with_semantic_evidence(identity, detail)
    progress = processing_view(detail)
    product_intelligence = _product_intelligence_for_detail(identity, detail, product_intelligence)
    regulatory_assessment = _present_regulatory_assessment(
        _regulatory_assessment_for_detail(identity, detail, regulatory_assessment)
    )
    return _render(
        request,
        "operation_detail",
        identity=identity,
        detail=detail,
        engine2_dossier=engine2_dossier,
        product_intelligence=product_intelligence,
        regulatory_assessment=regulatory_assessment,
        upload_csrf=upload_csrf,
        complete_csrf=complete_csrf,
        review_csrf=review_csrf,
        alias_csrf=alias_csrf,
        retry_csrf=retry_csrf,
        field_errors=dict(field_errors or {}),
        field_input_values=dict(field_input_values or {}),
        attention_fields=attention_fields,
        auto_supported_fields=auto_supported_fields,
        settled_fields=settled_fields,
        engine2_downstream_annotations=_engine2_downstream_annotations(detail),
        processing=progress,
        error=error,
        notice=notice,
    )


def render_operation_alias(
    *,
    request,
    detail,
    alias_csrf: str,
    alias_error: str | None = None,
    alias_input_value: str | None = None,
    alias_saved: bool = False,
) -> str:
    return _render(
        request,
        "fragments/operation_alias",
        detail=detail,
        alias_csrf=alias_csrf,
        alias_error=alias_error,
        alias_input_value=alias_input_value,
        alias_saved=alias_saved,
    )


def render_processing_fragment(*, request, detail, retry_csrf: str = "") -> str:
    return _render(
        request,
        "fragments/processing_fragment",
        detail=detail,
        processing=processing_view(detail),
        retry_csrf=retry_csrf,
    )


def render_operation_workspace(*, request, identity, detail, engine2_dossier, complete_csrf: str, review_csrf: Mapping[int, str], error: str | None = None, field_errors: Mapping[int, str] | None = None, field_input_values: Mapping[int, str] | None = None, is_oob_update: bool | None = None) -> str:
    attention_fields, auto_supported_fields, settled_fields = _review_field_groups_with_semantic_evidence(identity, detail)
    if is_oob_update is None:
        is_oob_update = str(getattr(request, "method", "GET")).upper() == "POST"

    return _render(
        request,
        "fragments/operation_workspace",
        identity=identity,
        detail=detail,
        engine2_dossier=engine2_dossier,
        product_intelligence=_product_intelligence_for_detail(identity, detail),
        regulatory_assessment=_present_regulatory_assessment(
            _regulatory_assessment_for_detail(identity, detail)
        ),
        complete_csrf=complete_csrf,
        review_csrf=review_csrf,
        field_errors=dict(field_errors or {}),
        field_input_values=dict(field_input_values or {}),
        attention_fields=attention_fields,
        auto_supported_fields=auto_supported_fields,
        settled_fields=settled_fields,
        engine2_downstream_annotations=_engine2_downstream_annotations(detail),
        error=error,
        is_oob_update=is_oob_update,
    )
