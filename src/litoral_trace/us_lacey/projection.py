"""Conservative projection from Assurance evidence into the U.S. review schema.

Only semantically safe generic mappings and explicit U.S. headers are accepted.
Ambiguous business concepts (origin vs. country of harvest, supplier vs.
manufacturer, generic quantity vs. plant quantity) intentionally remain missing.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import re
import unicodedata
from uuid import UUID

from sqlalchemy import func, select

from litoral_trace.db.models import (
    AssuranceDocument,
    DocumentExtractionRun,
    ExtractedDocumentField,
    ReconciliationIssue,
    UsLaceyFieldCandidate,
    UsLaceyOperation,
    UsLaceyOperationField,
    UsLaceyPlantDeclaration,
    UsLaceyPpqPlantLine,
    UsLaceyProcessingJob,
)
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.us_lacey.db import get_us_lacey_db_session
from litoral_trace.us_lacey.ppq505 import (
    PPQ505_FIELDS_BY_KEY,
    PPQ505_PLANT_FIELDS,
    PPQ505_SHIPMENT_REFERENCE,
    PpqScope,
    is_paper_or_paperboard,
    validate_ppq_value,
)
from litoral_trace.us_lacey.reconciliation_invariants import (
    reconcile_entered_value_invariant,
    upsert_shipment_total_entered_value,
)


class UsLaceyProjectionError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class UsLaceyProjectionResult:
    projected_count: int
    matched_count: int
    review_count: int
    conflict_count: int
    operation_status: str


_SAFE_GENERIC_MAP = {
    "hs_code": "hts_code",
    "product": "merchandise_description",
    "species": "species",
}

_EXPLICIT_HEADER_ALIASES = {
    "estimated date of arrival": "estimated_arrival_date",
    "estimated arrival date": "estimated_arrival_date",
    "eta": "estimated_arrival_date",
    "filing entry reference": "filing_entry_reference",
    "entry reference": "filing_entry_reference",
    "entry type": "entry_type",
    "importer name": "importer_name",
    "importer identification": "importer_identifier",
    "importer identifier": "importer_identifier",
    "importer id": "importer_identifier",
    "importer address": "importer_address",
    "consignee": "consignee_name",
    "consignee name": "consignee_name",
    "consignee address": "consignee_address",
    "broker": "filer_name",
    "customs broker": "filer_name",
    "filer": "filer_name",
    "filer name": "filer_name",
    "filer contact": "filer_contact",
    "bill of lading": "bill_of_lading",
    "bol": "bill_of_lading",
    "master bol": "bill_of_lading",
    "master bill of lading": "bill_of_lading",
    "container": "container_number",
    "container number": "container_number",
    "manufacturer id": "manufacturer_id",
    "manufacturer identification": "manufacturer_id",
    "shipment description": "merchandise_description",
    "commodity description": "merchandise_description",
    "cargo description": "merchandise_description",
    "description of goods": "merchandise_description",
    "goods description": "merchandise_description",
    "hts": "hts_code",
    "hts code": "hts_code",
    "hts number": "hts_code",
    "article component": "article_component",
    "article": "article_component",
    "component": "article_component",
    "merchandise description": "merchandise_description",
    "entered value": "entered_value",
    "genus": "genus",
    "species": "species",
    "country of harvest": "country_of_harvest",
    "harvest country": "country_of_harvest",
    "plant quantity": "plant_quantity",
    "plant qty": "plant_quantity",
    "metric unit": "metric_unit",
    "plant unit": "metric_unit",
    "percent recycled": "percent_recycled",
    "recycled percentage": "percent_recycled",
}

_RAW_TABLE_FIELD = re.compile(r"^raw\.table\.(?P<table>\d+)\.(?P<header>.+)$")
_DATA_ROW = re.compile(r"(?:^|;)data_row:(?P<row>\d+)(?:;|$)")
_CONTAINER_TOKEN = re.compile(
    r"(?<![A-Z0-9])[A-Z]{4}(?:[ -]?\d){7}(?![A-Z0-9])",
    re.IGNORECASE,
)
_URLISH = re.compile(r"(?:https?://|www\.)", re.IGNORECASE)
_EMAIL = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")
_PHONE_ONLY = re.compile(r"^[+()\-\s.\d]{7,}$")
_NUMBERED_DESCRIPTION_HEADER = re.compile(
    r"^(?:commodity description|cargo description|description of goods|goods description) \d+$"
)
_WEIGHT_DESCRIPTION = re.compile(
    r"^\s*[0-9][0-9,.]*\s*(?:KG|KGS?|G|GRAMS?|LB|LBS?|POUNDS?|MT|METRIC\s+TONS?|TONNES?)\s*$",
    re.IGNORECASE,
)
_NOT_PAPER_REASON_CODE = "NOT_PAPER_OR_PAPERBOARD"
_PLANT_DECLARATION_SIGNATURE = frozenset(
    {"genus", "species", "country of harvest", "plant quantity"}
)
_LINE_ALLOCATION_IDENTITY_HEADERS = frozenset(
    {
        "component",
        "article component",
        "article",
        "genus",
        "species",
        "country of harvest",
        "plant quantity",
    }
)

_STRUCTURAL_ARTIFACTS_BY_TARGET = {
    "container_number": frozenset(
        {
            "seal number",
            "equipment description code",
            "equipment description",
            "container length",
            "container height",
            "container width",
            "container type",
            "load status",
            "url",
        }
    ),
    "consignee_name": frozenset(
        {
            "address line",
            "city",
            "state province",
            "zip code",
            "country code",
            "comm number",
            "comm number qualifier",
            "consignee name",
        }
    ),
    "importer_name": frozenset(
        {
            "address line",
            "city",
            "state province",
            "zip code",
            "country code",
            "comm number",
            "comm number qualifier",
            "importer name",
        }
    ),
    "merchandise_description": frozenset(
        {
            "marks and numbers",
            "equipment description",
            "equipment description code",
            "url",
        }
    ),
}

_PARTY_NON_NAMES = frozenset(
    {
        "eeuu",
        "us",
        "usa",
        "united states",
        "united states of america",
        "nz",
        "new zealand",
    }
)

_TAXONOMY_COMMERCIAL_STOP_WORDS = frozenset(
    {
        "board",
        "boards",
        "cutting",
        "lumber",
        "pallet",
        "pallets",
        "wood",
        "wooden",
    }
)


_PLANT_ROW_IDENTITY_TARGETS = frozenset(
    {
        "article_component",
        "genus",
        "species",
        "country_of_harvest",
        "plant_quantity",
        "metric_unit",
        "percent_recycled",
    }
)
_MAX_AUTO_PLANT_LINES = 500


def _fold(value: object) -> str:
    text = unicodedata.normalize("NFKD", str(value or "").lower())
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return re.sub(r"[^a-z0-9]+", " ", text).strip()


def _is_structural_artifact(target: str, value: object) -> bool:
    """Reject known vertical-table labels only for the target they can corrupt."""
    folded = _fold(value)
    for artifact in _STRUCTURAL_ARTIFACTS_BY_TARGET.get(target, ()):
        if folded == artifact:
            return True
        suffix = folded.removeprefix(f"{artifact} ")
        if suffix != folded and suffix.isdigit():
            return True
    return False


def _explicit_header_target(header: object) -> str | None:
    folded = _fold(header)
    direct = _EXPLICIT_HEADER_ALIASES.get(folded)
    if direct:
        return direct
    if _NUMBERED_DESCRIPTION_HEADER.fullmatch(folded):
        return "merchandise_description"
    return None


def _table_header_context(row: ExtractedDocumentField, table_headers) -> frozenset[str]:
    """Return headers for this raw table, while preserving legacy flat-call tests."""
    if isinstance(table_headers, dict):
        match = _RAW_TABLE_FIELD.match(str(row.field_name or ""))
        if match is not None:
            return frozenset(table_headers.get(int(match.group("table")), frozenset()))
        merged: set[str] = set()
        for headers in table_headers.values():
            merged.update(headers)
        return frozenset(merged)
    return frozenset(table_headers or ())


def _is_plant_declaration_table(headers: frozenset[str]) -> bool:
    return _PLANT_DECLARATION_SIGNATURE.issubset(headers)


def _is_line_allocation_table(headers: frozenset[str]) -> bool:
    return "entered value" in headers and bool(headers & _LINE_ALLOCATION_IDENTITY_HEADERS)


def _description_candidate_role(row: ExtractedDocumentField, value: object) -> str | None:
    """Classify obvious non-commercial-description evidence before candidate admission."""
    raw = " ".join(str(value or "").split()).strip()
    field_folded = _fold(row.field_name)
    locator_folded = _fold(row.source_locator)
    value_folded = _fold(raw)
    semantic_context = f"{field_folded} {locator_folded}".strip()

    if _WEIGHT_DESCRIPTION.fullmatch(raw) or re.search(r"\b(?:gross|net)?\s*weight\b", semantic_context):
        return "WEIGHT"
    if (
        re.search(r"\b(?:plant|article) component(?: description)?\b", semantic_context)
        or value_folded.startswith("plant component description ")
        or value_folded.startswith("article component description ")
        or value_folded.startswith("component description ")
    ):
        return "PLANT_COMPONENT_DESCRIPTION"
    if (
        re.search(r"\b(?:packaging|packing|package) description\b", semantic_context)
        or value_folded.startswith("packaging description ")
        or value_folded.startswith("packing description ")
        or value_folded.startswith("package description ")
    ):
        return "PACKAGING_DESCRIPTION"
    if (
        re.search(r"\bsupplier (?:statement|declaration|certification)\b", semantic_context)
        or value_folded.startswith("supplier statement ")
        or value_folded.startswith("supplier declares ")
        or value_folded.startswith("supplier certifies ")
        or re.search(r"\bsupplier s production plant\b", value_folded)
    ):
        return "SUPPLIER_STATEMENT"
    return None


def _is_candidate_admissible(
    target: str,
    value: object,
    *,
    table_headers: frozenset[str] = frozenset(),
) -> bool:
    """Apply conservative type semantics before a candidate can reach human review.

    This guard deliberately rejects obvious parser-structure artifacts and impossible
    field types. It never turns an inferred value into evidence; rejected values simply
    leave the preparation field missing for human review.
    """
    raw = str(value or "").strip()
    if not raw:
        return False
    folded = _fold(raw)
    if target in {"genus", "species"}:
        taxonomy_tokens = frozenset(folded.split())
        if taxonomy_tokens & _TAXONOMY_COMMERCIAL_STOP_WORDS:
            return False
    if _is_structural_artifact(target, raw):
        return False
    if folded and folded in table_headers:
        return False
    if target == "container_number":
        if _URLISH.search(raw):
            return False
        return bool(_CONTAINER_TOKEN.search(raw.upper()))
    if target in {"importer_name", "consignee_name"}:
        if folded in _PARTY_NON_NAMES:
            return False
        if _URLISH.search(raw) or _EMAIL.fullmatch(raw) or _PHONE_ONLY.fullmatch(raw):
            return False
    if target == "merchandise_description" and _URLISH.search(raw):
        return False
    return True


def _target_field(
    row: ExtractedDocumentField,
    *,
    table_headers=frozenset(),
) -> tuple[str | None, int]:
    context_headers = _table_header_context(row, table_headers)
    generic = _SAFE_GENERIC_MAP.get(str(row.field_name or "").lower())
    if generic:
        value = row.normalized_value or row.original_value
        if generic == "merchandise_description" and _description_candidate_role(row, value):
            return None, 0
        if _is_candidate_admissible(generic, value, table_headers=context_headers):
            return generic, 2
        return None, 0
    raw_match = _RAW_TABLE_FIELD.match(str(row.field_name or ""))
    if raw_match:
        header = _fold(raw_match.group("header"))
        if header == "unit" and _is_plant_declaration_table(context_headers):
            target = "metric_unit"
        else:
            target = _explicit_header_target(raw_match.group("header"))
        value = row.normalized_value or row.original_value
        if target == "entered_value" and context_headers and not _is_line_allocation_table(context_headers):
            # A commercial invoice/shipment total is reconciliation evidence, not a
            # PPQ plant-line allocation. It is persisted separately by the projector.
            return None, 0
        if target == "merchandise_description" and _description_candidate_role(row, value):
            return None, 0
        if target and _is_candidate_admissible(target, value, table_headers=context_headers):
            return target, 3
    return None, 0


def _line_reference(
    *, target: str, source_locator: str | None, line_references: tuple[str, ...]
) -> str:
    contract = PPQ505_FIELDS_BY_KEY[target]
    if contract.scope is PpqScope.SHIPMENT:
        return PPQ505_SHIPMENT_REFERENCE
    if not line_references:
        return ""
    match = _DATA_ROW.search(str(source_locator or ""))
    if match:
        row_number = int(match.group("row"))
        if 1 <= row_number <= len(line_references):
            return line_references[row_number - 1]
    return line_references[0] if len(line_references) == 1 else ""


def _has_explicit_line_entered_value(
    extracted: tuple[ExtractedDocumentField, ...] | list[ExtractedDocumentField],
    *,
    table_headers,
) -> bool:
    for row in extracted:
        target, _priority = _target_field(row, table_headers=table_headers)
        if target == "entered_value" and _DATA_ROW.search(str(row.source_locator or "")):
            return True
    return False


def _shipment_total_entered_value_source(
    extracted: tuple[ExtractedDocumentField, ...] | list[ExtractedDocumentField],
    *,
    table_headers,
) -> ExtractedDocumentField | None:
    """Return one unambiguous non-line Entered Value source, otherwise fail closed."""
    distinct: dict[str, ExtractedDocumentField] = {}
    for row in extracted:
        raw_match = _RAW_TABLE_FIELD.match(str(row.field_name or ""))
        if raw_match is None or _fold(raw_match.group("header")) != "entered value":
            continue
        headers = _table_header_context(row, table_headers)
        if _is_line_allocation_table(headers):
            continue
        raw = row.normalized_value or row.original_value
        validation = validate_ppq_value("entered_value", raw)
        if validation.status.value != "VALID" or validation.normalized_value is None:
            continue
        distinct.setdefault(validation.normalized_value, row)
    if len(distinct) != 1:
        return None
    return next(iter(distinct.values()))


def _explicit_plant_data_rows(
    extracted: tuple[ExtractedDocumentField, ...] | list[ExtractedDocumentField],
    *,
    table_headers,
) -> tuple[int, ...]:
    """Return explicit table rows that carry plant-line identity evidence.

    Row numbers are evidence locators, not inferred declaration facts. Restricting
    materialization to plant identity fields prevents shipment totals or generic
    invoice rows from manufacturing declaration lines.
    """
    rows: set[int] = set()
    for source in extracted:
        target, _priority = _target_field(source, table_headers=table_headers)
        if target not in _PLANT_ROW_IDENTITY_TARGETS:
            continue
        match = _DATA_ROW.search(str(source.source_locator or ""))
        if match is None:
            continue
        row_number = int(match.group("row"))
        if 1 <= row_number <= _MAX_AUTO_PLANT_LINES:
            rows.add(row_number)
    return tuple(sorted(rows))


def _new_line_reference(*, ordinal: int, used: set[str]) -> str:
    preferred = str(ordinal)
    if preferred not in used:
        return preferred
    base = f"auto-{ordinal}"
    candidate = base
    suffix = 2
    while candidate in used:
        candidate = f"{base}-{suffix}"
        suffix += 1
    return candidate


def _materialize_explicit_plant_lines(
    session,
    *,
    organization_id: int,
    operation: UsLaceyOperation,
    extracted: tuple[ExtractedDocumentField, ...] | list[ExtractedDocumentField],
    table_headers,
) -> tuple[str, ...]:
    """Create only consecutively evidenced plant rows missing from an operation.

    Upload-first intentionally starts with a minimal operation. When extraction
    explicitly identifies data_row:2, data_row:3, ... for plant identity fields,
    those rows must become independent PPQ plant lines before projection. This keeps
    parallel components from being collapsed into line 1 while refusing large row
    jumps or shipment-only evidence as a basis for creating regulatory rows.
    """
    plant_lines = list(
        session.scalars(
            select(UsLaceyPpqPlantLine)
            .where(
                UsLaceyPpqPlantLine.organization_id == organization_id,
                UsLaceyPpqPlantLine.operation_id == operation.id,
            )
            .order_by(UsLaceyPpqPlantLine.ordinal.asc(), UsLaceyPpqPlantLine.id.asc())
        ).all()
    )
    explicit_rows = set(_explicit_plant_data_rows(extracted, table_headers=table_headers))
    used = {str(row.line_reference) for row in plant_lines}
    next_ordinal = len(plant_lines) + 1
    while next_ordinal <= _MAX_AUTO_PLANT_LINES and next_ordinal in explicit_rows:
        line_reference = _new_line_reference(ordinal=next_ordinal, used=used)
        plant_line = UsLaceyPpqPlantLine(
            organization_id=organization_id,
            operation_id=operation.id,
            line_reference=line_reference,
            ordinal=next_ordinal,
        )
        session.add(plant_line)
        session.flush()
        session.add(
            UsLaceyPlantDeclaration(
                organization_id=organization_id,
                plant_line_id=plant_line.id,
                ordinal=1,
            )
        )
        for field_contract in PPQ505_PLANT_FIELDS:
            session.add(
                UsLaceyOperationField(
                    organization_id=organization_id,
                    operation_id=operation.id,
                    merchandise_line_reference=line_reference,
                    field_name=field_contract.key,
                    field_scope="PLANT_LINE",
                    plant_line_id=plant_line.id,
                    field_status="MISSING",
                    validation_status="MISSING",
                    confidence=0.0,
                )
            )
        plant_lines.append(plant_line)
        used.add(line_reference)
        next_ordinal += 1

    operation.merchandise_line_count = len(plant_lines)
    session.flush()
    return tuple(str(row.line_reference) for row in plant_lines)


def _fingerprint(*parts: object) -> str:
    canonical = "\x1f".join(str(part if part is not None else "") for part in parts)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _upsert_conflict(
    session,
    *,
    organization_id: int,
    operation: UsLaceyOperation,
    field: UsLaceyOperationField,
    new_document_id: int,
    new_value: str,
    new_locator: str | None,
    new_confidence: float,
) -> None:
    left_value = field.human_value or field.normalized_value or field.original_value
    left_document_id = field.source_assurance_document_id
    fingerprint = _fingerprint(
        "US_LACEY_FIELD_CONFLICT",
        operation.public_id,
        field.merchandise_line_reference,
        field.field_name,
        left_document_id,
        new_document_id,
        left_value,
        new_value,
    )
    existing = session.scalar(
        select(ReconciliationIssue).where(
            ReconciliationIssue.organization_id == organization_id,
            ReconciliationIssue.fingerprint == fingerprint,
        )
    )
    evidence = {
        "left_locator": field.source_locator,
        "right_locator": new_locator,
        "left_confidence": float(field.confidence),
        "right_confidence": float(new_confidence),
        "source": "us_lacey_projection",
    }
    if existing is None:
        session.add(
            ReconciliationIssue(
                organization_id=organization_id,
                operation_reference=f"us_lacey:{operation.public_id}",
                fingerprint=fingerprint,
                rule_code="US_LACEY_FIELD_CONFLICT",
                severity="BLOCKING",
                status="OPEN",
                field_name=field.field_name,
                us_lacey_operation_field_id=field.id,
                left_document_id=left_document_id,
                right_document_id=new_document_id,
                left_source=(
                    f"assurance:{left_document_id}:{field.source_locator or 'unknown'}"
                    if left_document_id
                    else "existing_operation_value"
                ),
                right_source=f"assurance:{new_document_id}:{new_locator or 'unknown'}",
                left_value=left_value,
                right_value=new_value,
                explanation=(
                    "Two source documents provide different values for the same U.S. preparation field. "
                    "Human review is required; no value was silently overwritten."
                ),
                evidence_json=evidence,
            )
        )
    else:
        existing.status = "OPEN"
        existing.severity = "BLOCKING"
        existing.left_value = left_value
        existing.right_value = new_value
        existing.evidence_json = evidence
        existing.us_lacey_operation_field_id = field.id
        existing.resolution_justification = None
        existing.resolved_at = None


def _field_evidence_value(field: UsLaceyOperationField) -> str:
    return str(field.human_value or field.normalized_value or field.original_value or "").strip()


def _apply_percent_recycled_condition(
    session,
    *,
    organization_id: int,
    operation: UsLaceyOperation,
) -> None:
    """Resolve Percent Recycled as not-required for non-paper plant lines.

    Extracted values remain on the field as provenance; only the regulatory state is
    changed. If the article later becomes paper/paperboard, the automatic reason is
    removed and the evidence returns to human review instead of being silently used.
    """
    rows = session.scalars(
        select(UsLaceyOperationField).where(
            UsLaceyOperationField.organization_id == organization_id,
            UsLaceyOperationField.operation_id == operation.id,
            UsLaceyOperationField.field_name.in_(("article_component", "percent_recycled")),
        )
    ).all()
    by_line = {
        (str(row.merchandise_line_reference), row.field_name): row
        for row in rows
    }
    for (line_reference, field_name), percent_field in tuple(by_line.items()):
        if field_name != "percent_recycled":
            continue
        article_field = by_line.get((line_reference, "article_component"))
        article_value = _field_evidence_value(article_field) if article_field is not None else ""
        if not article_value:
            continue
        if not is_paper_or_paperboard(article_value):
            percent_field.field_status = "NOT_REQUIRED"
            percent_field.validation_status = "VALID"
            percent_field.validation_error = None
            percent_field.not_required_reason_code = _NOT_PAPER_REASON_CODE
            percent_field.human_value = None
            continue
        if percent_field.not_required_reason_code != _NOT_PAPER_REASON_CODE:
            continue
        percent_field.not_required_reason_code = None
        source_value = str(percent_field.normalized_value or percent_field.original_value or "").strip()
        if not source_value:
            percent_field.field_status = "MISSING"
            percent_field.validation_status = "MISSING"
            percent_field.validation_error = None
            continue
        validation = validate_ppq_value("percent_recycled", source_value)
        percent_field.validation_status = validation.status.value
        percent_field.validation_error = validation.error
        percent_field.field_status = "REVIEW"


def refresh_us_lacey_operation_status(
    session,
    *,
    organization_id: int,
    operation: UsLaceyOperation,
) -> str:
    """Derive operational state from the current unit of work."""
    # U.S. Lacey sessions intentionally use autoflush=False. State derivation must
    # therefore flush pending field/conflict review decisions before counting them.
    session.flush()
    _apply_percent_recycled_condition(
        session,
        organization_id=organization_id,
        operation=operation,
    )
    session.flush()
    job_statuses = session.scalars(
        select(UsLaceyProcessingJob.status).where(
            UsLaceyProcessingJob.organization_id == organization_id,
            UsLaceyProcessingJob.operation_id == operation.id,
        )
    ).all()
    if any(status == "FAILED" for status in job_statuses):
        operation.status = "FAILED"
        operation.review_result = "PROCESSING_FAILED"
        return operation.status
    if any(status in {"QUEUED", "RUNNING", "RETRY"} for status in job_statuses):
        operation.status = "PROCESSING"
        operation.review_result = None
        return operation.status

    unresolved = session.scalar(
        select(func.count(UsLaceyOperationField.id)).where(
            UsLaceyOperationField.organization_id == organization_id,
            UsLaceyOperationField.operation_id == operation.id,
            UsLaceyOperationField.field_status.in_(("MISSING", "REVIEW")),
        )
    ) or 0
    open_conflicts = session.scalar(
        select(func.count(ReconciliationIssue.id)).where(
            ReconciliationIssue.organization_id == organization_id,
            ReconciliationIssue.operation_reference == f"us_lacey:{operation.public_id}",
            ReconciliationIssue.status == "OPEN",
        )
    ) or 0
    if int(unresolved) or int(open_conflicts):
        operation.status = "REVIEW_REQUIRED"
        operation.review_result = "NEEDS_HUMAN_REVIEW"
    elif int(operation.document_count) > 0:
        operation.status = "READY_FOR_REVIEW"
        operation.review_result = "READY_FOR_HUMAN_CONFIRMATION"
    else:
        operation.status = "NEW"
        operation.review_result = None
    return operation.status


def project_assurance_document_to_us_lacey(
    *,
    organization_id: int,
    operation_id: int,
    assurance_document_id: int,
) -> UsLaceyProjectionResult:
    org_id = int(organization_id)
    session = get_us_lacey_db_session()
    try:
        set_tenant_db_context(session, org_id)
        operation = session.scalar(
            select(UsLaceyOperation).where(
                UsLaceyOperation.organization_id == org_id,
                UsLaceyOperation.id == int(operation_id),
            )
        )
        document = session.scalar(
            select(AssuranceDocument).where(
                AssuranceDocument.organization_id == org_id,
                AssuranceDocument.id == int(assurance_document_id),
            )
        )
        if operation is None or document is None:
            raise UsLaceyProjectionError("Operation or processed document was not found.")
        latest_run = session.scalar(
            select(DocumentExtractionRun)
            .where(
                DocumentExtractionRun.organization_id == org_id,
                DocumentExtractionRun.assurance_document_id == document.id,
            )
            .order_by(DocumentExtractionRun.id.desc())
        )
        if latest_run is None:
            raise UsLaceyProjectionError("Processed document has no extraction run.")

        extracted = session.scalars(
            select(ExtractedDocumentField)
            .where(
                ExtractedDocumentField.organization_id == org_id,
                ExtractedDocumentField.assurance_document_id == document.id,
                ExtractedDocumentField.extraction_run_id == latest_run.id,
            )
            .order_by(ExtractedDocumentField.id.asc())
        ).all()
        table_header_sets: dict[int, set[str]] = {}
        for row in extracted:
            match = _RAW_TABLE_FIELD.match(str(row.field_name or ""))
            if match is None:
                continue
            table_header_sets.setdefault(int(match.group("table")), set()).add(
                _fold(match.group("header"))
            )
        table_headers = {
            table_id: frozenset(headers)
            for table_id, headers in table_header_sets.items()
        }

        shipment_total_source = _shipment_total_entered_value_source(
            extracted,
            table_headers=table_headers,
        )
        if shipment_total_source is not None:
            shipment_total_raw = (
                shipment_total_source.normalized_value or shipment_total_source.original_value
            )
            upsert_shipment_total_entered_value(
                session,
                organization_id=org_id,
                operation=operation,
                raw_value=shipment_total_raw,
                source_assurance_document_id=document.id,
                source_page=shipment_total_source.source_page,
                source_locator=shipment_total_source.source_locator,
                extractor=latest_run.engine,
                extractor_version=latest_run.engine_version,
                confidence=float(shipment_total_source.confidence),
            )

        line_references = _materialize_explicit_plant_lines(
            session,
            organization_id=org_id,
            operation=operation,
            extracted=extracted,
            table_headers=table_headers,
        )
        operation_fields = session.scalars(
            select(UsLaceyOperationField).where(
                UsLaceyOperationField.organization_id == org_id,
                UsLaceyOperationField.operation_id == operation.id,
            )
        ).all()
        indexed = {
            (row.merchandise_line_reference, row.field_name): row
            for row in operation_fields
        }
        has_entered_value_allocations = _has_explicit_line_entered_value(
            extracted,
            table_headers=table_headers,
        )

        candidates: dict[tuple[str, str], list[tuple[int, ExtractedDocumentField]]] = {}
        for row in extracted:
            target, priority = _target_field(row, table_headers=table_headers)
            value = row.normalized_value or row.original_value
            if target is None or value is None or not str(value).strip():
                continue
            if (
                target == "entered_value"
                and has_entered_value_allocations
                and _DATA_ROW.search(str(row.source_locator or "")) is None
            ):
                # Preserve non-line totals as evidence, but never project them into
                # a plant line when explicit allocations are present.
                continue
            line = _line_reference(
                target=target,
                source_locator=row.source_locator,
                line_references=line_references,
            )
            if not line:
                continue
            key = (line, target)
            candidates.setdefault(key, []).append((priority, row))

        projected = matched = review = conflicts = 0
        species_for_genus: list[tuple[str, ExtractedDocumentField]] = []
        for (line, target), sources in candidates.items():
            field = indexed.get((line, target))
            if field is None:
                continue
            distinct: dict[str, ExtractedDocumentField] = {}
            for _priority, source in sources:
                raw = str(source.original_value or source.normalized_value or "").strip()
                validation = validate_ppq_value(target, raw)
                fingerprint = _fingerprint(
                    "US_LACEY_FIELD_CANDIDATE", operation.public_id, field.id,
                    document.id, source.id, raw, source.source_locator,
                )
                candidate = session.scalar(
                    select(UsLaceyFieldCandidate).where(
                        UsLaceyFieldCandidate.organization_id == org_id,
                        UsLaceyFieldCandidate.fingerprint == fingerprint,
                    )
                )
                if candidate is None:
                    session.add(UsLaceyFieldCandidate(
                        organization_id=org_id,
                        operation_id=operation.id,
                        operation_field_id=field.id,
                        source_assurance_document_id=document.id,
                        original_value=raw,
                        normalized_value=validation.normalized_value,
                        validation_status=validation.status.value,
                        validation_error=validation.error,
                        confidence=float(source.confidence),
                        source_page=source.source_page,
                        source_locator=source.source_locator,
                        extractor=latest_run.engine,
                        extractor_version=latest_run.engine_version,
                        fingerprint=fingerprint,
                    ))
                candidate_value = validation.normalized_value or raw
                distinct.setdefault(candidate_value, source)

            if len(distinct) > 1:
                alternatives = list(distinct.items())
                left_value, left_source = alternatives[0]
                field.original_value = str(left_source.original_value or left_value)
                field.normalized_value = None
                field.field_status = "REVIEW"
                field.validation_status = "REVIEW_REQUIRED"
                field.validation_error = "Multiple supported source candidates require a human decision."
                for new_value, new_source in alternatives[1:]:
                    _upsert_conflict(
                        session,
                        organization_id=org_id,
                        operation=operation,
                        field=field,
                        new_document_id=document.id,
                        new_value=new_value,
                        new_locator=new_source.source_locator,
                        new_confidence=float(new_source.confidence),
                    )
                    conflicts += 1
                review += 1
                continue

            if not distinct:
                continue
            source = next(iter(distinct.values()))
            raw_value = str(source.original_value or source.normalized_value).strip()
            validation = validate_ppq_value(target, raw_value)
            new_value = validation.normalized_value or raw_value
            existing_value = field.human_value or field.normalized_value or field.original_value
            human_confirmed = bool(
                field.field_status == "MATCHED"
                and field.human_value is not None
                and str(field.human_value).strip()
            )
            if existing_value and str(existing_value).strip() != new_value:
                _upsert_conflict(
                    session,
                    organization_id=org_id,
                    operation=operation,
                    field=field,
                    new_document_id=document.id,
                    new_value=new_value,
                    new_locator=source.source_locator,
                    new_confidence=float(source.confidence),
                )
                field.field_status = "REVIEW"
                conflicts += 1
                review += 1
                continue

            field.original_value = raw_value
            field.normalized_value = new_value
            field.validation_status = validation.status.value
            field.validation_error = validation.error
            field.confidence = float(source.confidence)
            field.source_assurance_document_id = document.id
            field.source_page = source.source_page
            field.source_locator = source.source_locator
            field.extractor = latest_run.engine
            field.extractor_version = latest_run.engine_version
            if validation.status.value in {"INVALID", "REVIEW_REQUIRED"}:
                field.field_status = "REVIEW"
                review += 1
            elif human_confirmed:
                field.field_status = "MATCHED"
                matched += 1
            elif float(source.confidence) >= 0.90 and not bool(source.needs_review):
                field.field_status = "FOUND"
            else:
                field.field_status = "REVIEW"
                review += 1
            projected += 1
            if target == "species":
                species_for_genus.append((line, source))

        for line, source in species_for_genus:
            genus_field = indexed.get((line, "genus"))
            if genus_field is None or genus_field.human_value:
                continue
            species_value = str(source.normalized_value or source.original_value or "").strip()
            parts = species_value.split()
            if len(parts) < 2 or not parts[0].isalpha():
                continue
            genus = parts[0]
            current_genus = genus_field.normalized_value or genus_field.original_value
            if current_genus and str(current_genus).strip().lower() != genus.lower():
                _upsert_conflict(
                    session,
                    organization_id=org_id,
                    operation=operation,
                    field=genus_field,
                    new_document_id=document.id,
                    new_value=genus,
                    new_locator=source.source_locator,
                    new_confidence=float(source.confidence),
                )
                genus_field.field_status = "REVIEW"
                conflicts += 1
                continue
            if not current_genus:
                genus_field.original_value = species_value
                genus_field.normalized_value = genus
                genus_field.confidence = min(float(source.confidence), 0.89)
                genus_field.source_assurance_document_id = document.id
                genus_field.source_page = source.source_page
                genus_field.source_locator = (
                    f"{source.source_locator or 'unknown'};derived:genus_from_species"
                )
                genus_field.extractor = "us-lacey-deterministic-projector"
                genus_field.extractor_version = "1.0.0"
                genus_field.field_status = "REVIEW"
                projected += 1
                review += 1

        reconcile_entered_value_invariant(
            session,
            organization_id=org_id,
            operation=operation,
        )
        operation_status = refresh_us_lacey_operation_status(
            session,
            organization_id=org_id,
            operation=operation,
        )
        session.commit()
        return UsLaceyProjectionResult(
            projected_count=projected,
            matched_count=matched,
            review_count=review,
            conflict_count=conflicts,
            operation_status=operation_status,
        )
    except UsLaceyProjectionError:
        session.rollback()
        raise
    except Exception as exc:
        session.rollback()
        raise UsLaceyProjectionError(
            "Unable to project extracted evidence into the U.S. review schema."
        ) from exc
    finally:
        session.close()
