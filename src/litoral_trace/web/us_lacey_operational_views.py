"""Jinja-backed operational workspace views for the U.S. Lacey portal."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timezone
import logging

from markupsafe import Markup, escape

from litoral_trace.us_lacey.evidence_catalog import UsLaceyEvidenceCatalogService
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
    blocking_regulatory_assessments,
    get_current_regulatory_assessment_view,
    refresh_current_regulatory_assessment_view,
)
from litoral_trace.us_lacey.audit_trail import list_operation_events
from litoral_trace.us_lacey.semantic_evidence_read import (
    EvidenceTextView,
    SemanticEvidenceReadService,
)
from litoral_trace.web.templates import templates


LOGGER = logging.getLogger(__name__)


def _render(request, name: str, **context: object) -> str:
    return templates.get_template(f"us_lacey/{name}.html").render(request=request, **context)


@dataclass(frozen=True, slots=True)
class ReviewProvenanceSummary:
    current_shipment_count: int
    reused_evidence_count: int
    review_required_count: int


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
    "NOT_EVALUATED": "Not evaluated",
    "NOT_APPLICABLE": "Not applicable",
}

_REGULATORY_REASON_LABELS = {
    "HTS10_MISSING": "HTS missing",
    "HTS10_INVALID": "HTS invalid",
    "HTS_NOT_IN_PARTIAL_CATALOG": "HTS coverage unavailable",
    "MISSING_REQUIRED_INPUTS": "Required quantity or mass inputs are missing",
    "INVALID_HTS10": "HTS invalid",
    "INVALID_PLANT_UNIT_MASS": "Plant mass per unit is invalid",
    "INVALID_TOTAL_UNIT_MASS": "Total unit mass is invalid",
    "INVALID_ENTRY_PLANT_MASS": "Entry plant mass is invalid",
    "PROTECTED_STATUS_UNKNOWN": "Protected-plant status is unknown",
    "EXEMPTION_NOT_CLAIMED": (
        "De Minimis exemption was not claimed. Does not block the declaration package."
    ),
    "MISSING_OPTIONAL_EXEMPTION_INPUTS": (
        "De Minimis exemption was not claimed. Does not block the declaration package."
    ),
}

_REGULATORY_ACTION_LABELS = {
    "HTS_APPLICABILITY": "Provide HTS",
    "DE_MINIMIS": "Provide quantity / mass evidence",
    "SPECIAL_COMPOSITE": "Provide composition evidence",
    "SPECIAL_RECYCLED": "Provide recycled-content evidence",
}

_REGULATORY_ACTION_FIELDS = {
    "HTS_APPLICABILITY": "hts_code",
    "SPECIAL_COMPOSITE": "article_component",
    "SPECIAL_RECYCLED": "percent_recycled",
}

_REGULATORY_BLOCK_MESSAGES = {
    "HTS_APPLICABILITY": (
        "Missing or invalid HTS Code. Required to determine APHIS schedule coverage."
    ),
    "DE_MINIMIS": (
        "Missing quantity or mass information. Required to evaluate the De Minimis pathway."
    ),
    "SPECIAL_COMPOSITE": (
        "Missing product-composition evidence. Required to evaluate the special composite wood pathway."
    ),
    "SPECIAL_RECYCLED": (
        "Missing recycled-content evidence. Required to evaluate the recycled-material exception."
    ),
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
    review_required = bool(assessment.get("review_required"))

    if status == "INDETERMINATE" and not review_required:
        base = explanation or "This optional check cannot be evaluated with the current evidence."
        return f"{base} Does not block the current preparation package."
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
    for index, raw in enumerate(payload.get("assessments", ()), start=1):
        if not isinstance(raw, Mapping):
            continue
        item = dict(raw)
        rule_id = str(item.get("rule_id") or "").upper()
        status = str(item.get("status") or "").upper()
        subject_ref = str(item.get("subject_ref") or "").strip()
        review_required = bool(item.get("review_required"))
        reason_codes = tuple(str(code or "") for code in item.get("reason_codes", ()) or ())

        item["display_title"] = _REGULATORY_RULE_TITLES.get(
            rule_id,
            rule_id.replace("_", " ").title() or "Regulatory check",
        )
        item["display_status"] = (
            "Not evaluated"
            if status == "INDETERMINATE" and not review_required
            else _REGULATORY_STATUS_LABELS.get(
                status,
                status.replace("_", " ").title() or "Review",
            )
        )
        item["display_reason"] = "; ".join(
            _REGULATORY_REASON_LABELS.get(
                code,
                code.replace("_", " ").capitalize(),
            )
            for code in reason_codes
            if code
        ) or "No additional reason provided"
        item["display_subject"] = (
            f"Plant line {subject_ref}" if subject_ref else "Shipment"
        )
        item["customer_message"] = _regulatory_customer_message(item)
        item["blocks_package"] = review_required
        item["action_label"] = _REGULATORY_ACTION_LABELS.get(
            rule_id,
            "Provide information",
        )
        item["action_field_name"] = _REGULATORY_ACTION_FIELDS.get(rule_id)
        item["blocking_message"] = _REGULATORY_BLOCK_MESSAGES.get(
            rule_id,
            "Additional supported information is required before the preparation package can be finalized.",
        )
        item["action_id"] = (
            f"{rule_id.lower() or 'rule'}-{subject_ref or 'shipment'}-{index}"
        )
        presented_assessments.append(item)
    payload["assessments"] = presented_assessments
    return replace(view, payload=payload)


def _regulatory_action_items(regulatory_assessment, *, attention_fields=()):
    """Project blocking regulatory checks into the Action Required workflow."""
    if regulatory_assessment is None:
        return ()

    attention = tuple(attention_fields or ())
    items: list[dict[str, object]] = []
    for index, assessment in enumerate(
        regulatory_assessment.payload.get("assessments", ()),
        start=1,
    ):
        if not isinstance(assessment, Mapping) or not bool(
            assessment.get("blocks_package")
        ):
            continue
        item = dict(assessment)
        line_reference = str(item.get("subject_ref") or "").strip()
        target_field_name = str(item.get("action_field_name") or "").strip()
        target_field = next(
            (
                field
                for field in attention
                if target_field_name
                and str(getattr(field, "field_name", "") or "") == target_field_name
                and (
                    not line_reference
                    or str(getattr(field, "line_reference", "") or "") == line_reference
                )
            ),
            None,
        )
        item["action_id"] = str(
            item.get("action_id")
            or (
                f"{str(item.get('rule_id') or 'rule').lower()}-"
                f"{line_reference or 'shipment'}-{index}"
            )
        )
        item["line_reference"] = line_reference
        item["target_field_id"] = (
            None if target_field is None else int(getattr(target_field, "id"))
        )
        item["request_guidance"] = str(
            item.get("blocking_message")
            or item.get("customer_message")
            or "Provide supporting information for this regulatory check."
        )
        items.append(item)
    return tuple(items)



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


def _review_provenance_summary(detail) -> ReviewProvenanceSummary:
    """Count customer-facing field provenance without double-counting placeholders."""
    current = 0
    reused = 0
    review_required = 0
    for field in tuple(getattr(detail, "fields", ()) or ()):
        if not _is_customer_ppq_field(field):
            continue
        status = str(getattr(field, "status", "") or "").upper()
        provenance = str(getattr(field, "provenance", "current_shipment") or "current_shipment")
        if status in _ACTION_REQUIRED_STATUSES:
            review_required += 1
            continue
        if not _field_has_displayable_resolution(field):
            continue
        if provenance == "reused_evidence":
            reused += 1
        else:
            current += 1
    return ReviewProvenanceSummary(
        current_shipment_count=current,
        reused_evidence_count=reused,
        review_required_count=review_required,
    )


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
        current = get_current_regulatory_assessment_view(
            organization_id=int(organization_id),
            operation_public_id=operation_public_id,
        )
        if current is not None:
            return current
        return refresh_current_regulatory_assessment_view(
            organization_id=int(organization_id),
            operation_public_id=operation_public_id,
        )
    except Exception:
        LOGGER.exception(
            "us_lacey_regulatory_assessment_read_failed",
            extra={"organization_id": int(organization_id)},
        )
        return None


def _relative_time(value: datetime | None, *, now: datetime | None = None) -> str:
    if value is None:
        return "—"
    current = now or datetime.now(timezone.utc)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    delta = current - value.astimezone(timezone.utc)
    seconds = max(0, int(delta.total_seconds()))
    if seconds < 60:
        return "just now"
    if seconds < 3600:
        minutes = seconds // 60
        return f"{minutes} min ago"
    if seconds < 86400:
        hours = seconds // 3600
        return f"{hours} hour{'s' if hours != 1 else ''} ago"
    if seconds < 7 * 86400:
        days = seconds // 86400
        return f"{days} day{'s' if days != 1 else ''} ago"
    return value.strftime("%b %d, %Y")


def _business_reference(detail) -> str:
    client_reference = str(getattr(detail, "client_reference", "") or "").strip()
    if client_reference and not client_reference.upper().startswith("INTAKE-"):
        return client_reference
    fields = tuple(getattr(detail, "fields", ()) or ())
    by_name = {}
    for field in fields:
        value = getattr(field, "effective_value", None)
        cleaned = str(value or "").strip()
        if cleaned:
            by_name.setdefault(str(getattr(field, "field_name", "")), cleaned)
    if by_name.get("filing_entry_reference"):
        return f"Entry {by_name['filing_entry_reference']}"
    if by_name.get("bill_of_lading"):
        return f"B/L {by_name['bill_of_lading']}"
    return f"Shipment {str(getattr(detail, 'public_id', '')).split('-')[0].upper()}"


def _business_supplier_name(detail) -> str | None:
    direct = str(getattr(detail, "supplier_name", "") or "").strip()
    if direct:
        return direct
    for field in tuple(getattr(detail, "fields", ()) or ()):
        if str(getattr(field, "field_name", "")) != "supplier_name":
            continue
        value = str(getattr(field, "effective_value", "") or "").strip()
        if value:
            return value
    return None


def _business_title(detail) -> str:
    reference = _business_reference(detail)
    supplier = _business_supplier_name(detail) or "Supplier unresolved"
    business_date = getattr(detail, "operation_date", None)
    if business_date is None:
        created_at = getattr(detail, "created_at", None)
        business_date = created_at.date() if created_at is not None else None
    date_label = business_date.strftime("%b %d") if business_date is not None else "Date pending"
    return f"{reference} · {supplier} · {date_label}"


def _readiness_summary(
    detail,
    *,
    attention_fields,
    processing,
    regulatory_action_items=(),
):
    """Derive the single customer-facing preparation readiness invariant."""
    def field_state(field_name: str) -> str:
        matches = [
            field for field in getattr(detail, "fields", ())
            if str(getattr(field, "field_name", "")) == field_name
        ]
        if not matches:
            return "MISSING"
        if any(
            getattr(field, "effective_value", None)
            and str(getattr(field, "status", "")).upper()
            not in {"MISSING", "REVIEW", "REVIEW_REQUIRED", "CONFLICT"}
            for field in matches
        ):
            return "VERIFIED"
        if any(str(getattr(field, "status", "")).upper() == "CONFLICT" for field in matches):
            return "CONFLICT"
        return "REVIEW REQUIRED"

    field_exception_count = len(tuple(attention_fields or ()))
    regulatory_exception_count = len(tuple(regulatory_action_items or ()))
    exception_count = field_exception_count + regulatory_exception_count
    processing_failed = bool(getattr(processing, "failed", False))
    processing_terminal = bool(getattr(processing, "terminal", False))
    completed = str(getattr(detail, "status", "") or "").upper() == "COMPLETED"
    package_ready = (
        completed
        and processing_terminal
        and not processing_failed
        and exception_count == 0
    )

    if processing_failed:
        overall = "PROCESSING FAILED"
    elif exception_count:
        overall = "NOT READY"
    elif package_ready:
        overall = "PACKAGE READY"
    elif processing_terminal:
        overall = "READY FOR FINAL CONFIRMATION"
    else:
        overall = "IN PREPARATION"

    return {
        "overall": overall,
        "package_ready": package_ready,
        "exception_count": exception_count,
        "field_exception_count": field_exception_count,
        "regulatory_exception_count": regulatory_exception_count,
        "species": field_state("species"),
        "country_of_harvest": field_state("country_of_harvest"),
    }


def _reuse_summary(identity, detail):
    fields = tuple(getattr(detail, "fields", ()) or ())
    if not any(getattr(field, "provenance", "") == "reused_evidence" for field in fields):
        return None
    try:
        return UsLaceyEvidenceCatalogService().reuse_summary(
            organization_id=int(identity.organization_id),
            fields=fields,
        )
    except Exception:
        LOGGER.exception(
            "us_lacey_reused_evidence_summary_read_failed",
            extra={"organization_id": int(identity.organization_id)},
        )
        return None


def render_operations(*, request, identity, operations: Sequence, entitlement) -> str:
    now = datetime.now(timezone.utc)
    open_count = sum(
        item.status in {"NEW", "PROCESSING", "REVIEW_REQUIRED", "READY_FOR_REVIEW"}
        for item in operations
    )
    needs_review = sum(
        item.exception_count > 0 or item.status == "REVIEW_REQUIRED"
        for item in operations
    )
    ready_to_export = sum(
        item.status == "READY_FOR_REVIEW" and item.exception_count == 0
        for item in operations
    )
    completed_this_month = sum(
        item.status == "COMPLETED"
        and item.updated_at.year == now.year
        and item.updated_at.month == now.month
        for item in operations
    )
    metrics = {
        "open": open_count,
        "needs_review": needs_review,
        "ready_to_export": ready_to_export,
        "completed_this_month": completed_this_month,
    }
    return _render(
        request,
        "operations",
        identity=identity,
        operations=operations,
        entitlement=entitlement,
        metrics=metrics,
        relative_time=_relative_time,
    )


def render_evidence_catalog(*, request, identity, entitlement, catalog) -> str:
    return _render(
        request,
        "evidence",
        identity=identity,
        entitlement=entitlement,
        catalog=catalog,
        relative_time=_relative_time,
    )


def render_new_operation(*, request, identity, entitlement, csrf_token: str, error: str | None = None) -> str:
    return _render(request, "new_operation", identity=identity, entitlement=entitlement, csrf_token=csrf_token, error=error)


def render_operation_detail(*, request, identity, detail, engine2_dossier, upload_csrf: str, complete_csrf: str, review_csrf: Mapping[int, str], alias_csrf: str = "", retry_csrf: str = "", product_intelligence=None, regulatory_assessment=None, error: str | None = None, notice: str | None = None, field_errors: Mapping[int, str] | None = None, field_input_values: Mapping[int, str] | None = None) -> str:
    attention_fields, auto_supported_fields, settled_fields = _review_field_groups_with_semantic_evidence(identity, detail)
    progress = processing_view(detail)
    product_intelligence = _product_intelligence_for_detail(identity, detail, product_intelligence)
    regulatory_assessment = _present_regulatory_assessment(
        _regulatory_assessment_for_detail(identity, detail, regulatory_assessment)
    )
    regulatory_action_items = _regulatory_action_items(
        regulatory_assessment,
        attention_fields=attention_fields,
    )
    readiness_summary = _readiness_summary(
        detail,
        attention_fields=attention_fields,
        processing=progress,
        regulatory_action_items=regulatory_action_items,
    )
    reused_evidence_summary = _reuse_summary(identity, detail)
    audit_events = list_operation_events(
        organization_id=identity.organization_id,
        operation_public_id=detail.public_id,
        limit=5,
    )
    return _render(
        request,
        "operation_detail",
        identity=identity,
        detail=detail,
        engine2_dossier=engine2_dossier,
        product_intelligence=product_intelligence,
        regulatory_assessment=regulatory_assessment,
        regulatory_action_items=regulatory_action_items,
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
        provenance_summary=_review_provenance_summary(detail),
        reused_evidence_summary=reused_evidence_summary,
        audit_events=audit_events,
        readiness_summary=readiness_summary,
        business_reference=_business_reference(detail),
        business_supplier_name=_business_supplier_name(detail),
        business_title=_business_title(detail),
        relative_time=_relative_time,
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
        business_reference=_business_reference(detail),
        business_title=_business_title(detail),
    )


def render_processing_fragment(*, request, detail, retry_csrf: str = "") -> str:
    return _render(
        request,
        "fragments/processing_fragment",
        detail=detail,
        processing=processing_view(detail),
        retry_csrf=retry_csrf,
    )


def render_operation_audit_log(*, request, detail, audit_events) -> str:
    return _render(
        request,
        "fragments/activity_audit_log",
        detail=detail,
        audit_events=audit_events,
    )


def render_operation_workspace(*, request, identity, detail, engine2_dossier, complete_csrf: str, review_csrf: Mapping[int, str], error: str | None = None, field_errors: Mapping[int, str] | None = None, field_input_values: Mapping[int, str] | None = None, is_oob_update: bool | None = None) -> str:
    attention_fields, auto_supported_fields, settled_fields = _review_field_groups_with_semantic_evidence(identity, detail)
    if is_oob_update is None:
        is_oob_update = str(getattr(request, "method", "GET")).upper() == "POST"

    progress = processing_view(detail)
    regulatory_assessment = _present_regulatory_assessment(
        _regulatory_assessment_for_detail(identity, detail)
    )
    regulatory_action_items = _regulatory_action_items(
        regulatory_assessment,
        attention_fields=attention_fields,
    )
    return _render(
        request,
        "fragments/operation_workspace",
        identity=identity,
        detail=detail,
        engine2_dossier=engine2_dossier,
        product_intelligence=_product_intelligence_for_detail(identity, detail),
        regulatory_assessment=regulatory_assessment,
        regulatory_action_items=regulatory_action_items,
        complete_csrf=complete_csrf,
        review_csrf=review_csrf,
        field_errors=dict(field_errors or {}),
        field_input_values=dict(field_input_values or {}),
        attention_fields=attention_fields,
        auto_supported_fields=auto_supported_fields,
        settled_fields=settled_fields,
        provenance_summary=_review_provenance_summary(detail),
        reused_evidence_summary=_reuse_summary(identity, detail),
        readiness_summary=_readiness_summary(
            detail,
            attention_fields=attention_fields,
            processing=progress,
            regulatory_action_items=regulatory_action_items,
        ),
        business_reference=_business_reference(detail),
        business_supplier_name=_business_supplier_name(detail),
        business_title=_business_title(detail),
        relative_time=_relative_time,
        engine2_downstream_annotations=_engine2_downstream_annotations(detail),
        error=error,
        is_oob_update=is_oob_update,
        processing=progress,
    )
