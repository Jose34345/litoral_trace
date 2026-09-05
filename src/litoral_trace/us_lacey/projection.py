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
    UsLaceyProcessingJob,
)
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.us_lacey.db import get_us_lacey_db_session
from litoral_trace.us_lacey.ppq505 import (
    PPQ505_FIELDS_BY_KEY,
    PPQ505_SHIPMENT_REFERENCE,
    PpqScope,
    validate_ppq_value,
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
    "container": "container_number",
    "container number": "container_number",
    "manufacturer id": "manufacturer_id",
    "manufacturer identification": "manufacturer_id",
    "shipment description": "merchandise_description",
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

_RAW_TABLE_FIELD = re.compile(r"^raw\.table\.\d+\.(?P<header>.+)$")
_DATA_ROW = re.compile(r"(?:^|;)data_row:(?P<row>\d+)(?:;|$)")


def _fold(value: object) -> str:
    text = unicodedata.normalize("NFKD", str(value or "").lower())
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return re.sub(r"[^a-z0-9]+", " ", text).strip()


def _target_field(row: ExtractedDocumentField) -> tuple[str | None, int]:
    generic = _SAFE_GENERIC_MAP.get(str(row.field_name or "").lower())
    if generic:
        return generic, 2
    raw_match = _RAW_TABLE_FIELD.match(str(row.field_name or ""))
    if raw_match:
        target = _EXPLICIT_HEADER_ALIASES.get(_fold(raw_match.group("header")))
        if target:
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


def refresh_us_lacey_operation_status(
    session,
    *,
    organization_id: int,
    operation: UsLaceyOperation,
) -> str:
    """Derive operational state without making a legal/compliance determination."""
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

        operation_fields = session.scalars(
            select(UsLaceyOperationField).where(
                UsLaceyOperationField.organization_id == org_id,
                UsLaceyOperationField.operation_id == operation.id,
            )
        ).all()
        line_references = tuple(dict.fromkeys(
            row.merchandise_line_reference
            for row in operation_fields
            if row.field_scope == "PLANT_LINE"
        ))
        indexed = {
            (row.merchandise_line_reference, row.field_name): row
            for row in operation_fields
        }
        extracted = session.scalars(
            select(ExtractedDocumentField)
            .where(
                ExtractedDocumentField.organization_id == org_id,
                ExtractedDocumentField.assurance_document_id == document.id,
                ExtractedDocumentField.extraction_run_id == latest_run.id,
            )
            .order_by(ExtractedDocumentField.id.asc())
        ).all()

        candidates: dict[tuple[str, str], list[tuple[int, ExtractedDocumentField]]] = {}
        for row in extracted:
            target, priority = _target_field(row)
            value = row.normalized_value or row.original_value
            if target is None or value is None or not str(value).strip():
                continue
            line = _line_reference(
                target=target,
                source_locator=row.source_locator,
                line_references=line_references,
            )
            if not line:
                # Evidence cannot be assigned to a declaration line safely.
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
            elif existing_value:
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

        # A binomial scientific species can deterministically propose its genus,
        # but the derived value remains REVIEW until a person confirms it.
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
