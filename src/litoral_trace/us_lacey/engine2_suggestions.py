"""Bridge deterministic Engine 2 shipment evidence into the human review queue.

Engine 2 remains non-authoritative. A supported shipment value may prefill a MISSING
PPQ preparation field as FOUND, but it is never silently accepted as MATCHED. The
bridge is deliberately evidence preserving and conservative around multi-line values.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
from typing import Mapping

from sqlalchemy import select

from litoral_trace.db.models import (
    UsLaceyEngineShipmentRun,
    UsLaceyFieldCandidate,
    UsLaceyOperation,
    UsLaceyOperationDocument,
    UsLaceyOperationField,
)
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.lacey_engine.serialization import SHIPMENT_RESOLUTION_SCHEMA_VERSION
from litoral_trace.us_lacey.db import get_us_lacey_db_session
from litoral_trace.us_lacey.ppq505 import validate_ppq_value
from litoral_trace.us_lacey.projection import refresh_us_lacey_operation_status


_ENGINE2_TO_PREPARATION_FIELD = {
    "estimated_arrival_date": "estimated_arrival_date",
    "filing_entry_reference": "filing_entry_reference",
    "container_number": "container_number",
    "bill_of_lading": "bill_of_lading",
    "manufacturer_id": "manufacturer_id",
    "importer_name": "importer_name",
    "consignee_name": "consignee_name",
    "importer_address": "importer_address",
    "consignee_address": "consignee_address",
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
_SUPPORTED_STATES = frozenset({"SUPPORTED", "SUPPORTED_MULTIPLE"})


@dataclass(frozen=True, slots=True)
class Engine2Suggestion:
    field_name: str
    value: str
    operation_document_id: int
    source_text: str
    source_page: int
    evidence_class: str
    confidence: float
    engine_version: str


def _fingerprint(*parts: object) -> str:
    canonical = "\x1f".join(str(part if part is not None else "") for part in parts)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _candidate_payload(evidence: Mapping[str, object]) -> Mapping[str, object] | None:
    candidate = evidence.get("candidate")
    return candidate if isinstance(candidate, Mapping) else None


def _suggestion_from_field(
    field_key: str,
    payload: Mapping[str, object],
    *,
    engine_version: str,
) -> Engine2Suggestion | None:
    """Choose the strongest exact documentary source for one supported field."""
    target = _ENGINE2_TO_PREPARATION_FIELD.get(field_key)
    if target is None or str(payload.get("state") or "") not in _SUPPORTED_STATES:
        return None

    values = payload.get("values")
    evidence_rows = payload.get("supporting_evidence")
    if not isinstance(values, list) or len(values) != 1 or not isinstance(evidence_rows, list):
        # Multiple canonical values are a set/multi-line allocation problem and must
        # stay in human/Terra review rather than being collapsed into one PPQ field.
        return None

    eligible: list[tuple[float, float, int, str, str, int, str]] = []
    for evidence in evidence_rows:
        if not isinstance(evidence, Mapping):
            continue
        candidate = _candidate_payload(evidence)
        if candidate is None:
            continue
        raw = candidate.get("raw")
        provenance = candidate.get("provenance")
        if not isinstance(raw, Mapping) or not isinstance(provenance, Mapping):
            continue
        evidence_class = str(raw.get("evidence_class") or provenance.get("evidence_class") or "")
        if evidence_class not in {"EXPLICIT", "DERIVED"}:
            continue
        value = str(evidence.get("normalized_value") or raw.get("normalized_value") or "").strip()
        source_text = str(provenance.get("source_text") or "").strip()
        try:
            page = int(provenance.get("page") or 0)
            operation_document_id = int(evidence.get("document_id") or 0)
            source_authority = float(evidence.get("source_authority") or 0.0)
            candidate_score = float(evidence.get("candidate_score") or candidate.get("score") or 0.0)
        except (TypeError, ValueError):
            continue
        if not value or not source_text or page < 1 or operation_document_id <= 0:
            continue
        eligible.append(
            (
                source_authority,
                candidate_score,
                operation_document_id,
                value,
                source_text,
                page,
                evidence_class,
            )
        )
    if not eligible:
        return None

    # Source authority breaks ties before extraction score. Confidence is a bounded
    # review-priority signal, never the raw Engine 2 score (which is not 0..1).
    source_authority, candidate_score, document_id, value, source_text, page, evidence_class = max(
        eligible, key=lambda item: (item[0], item[1])
    )
    support_count = len(eligible)
    confidence = 0.94 if support_count > 1 else 0.86
    if evidence_class == "DERIVED":
        confidence = min(confidence, 0.90)
    return Engine2Suggestion(
        field_name=target,
        value=value,
        operation_document_id=document_id,
        source_text=source_text,
        source_page=page,
        evidence_class=evidence_class,
        confidence=confidence,
        engine_version=engine_version,
    )


def supported_engine2_suggestions(payload: Mapping[str, object]) -> tuple[Engine2Suggestion, ...]:
    fields = payload.get("canonical_fields")
    if not isinstance(fields, Mapping):
        return ()
    engine_version = str(payload.get("engine_version") or "lacey-engine-2")
    suggestions: list[Engine2Suggestion] = []
    for field_key, field_payload in fields.items():
        if not isinstance(field_payload, Mapping):
            continue
        suggestion = _suggestion_from_field(
            str(field_key), field_payload, engine_version=engine_version
        )
        if suggestion is not None:
            suggestions.append(suggestion)
    return tuple(suggestions)


def project_engine2_supported_suggestions(*, organization_id: int, operation_id: int) -> int:
    """Prefill MISSING PPQ fields as FOUND from current Engine 2 shipment support."""
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
        if operation is None:
            return 0

        run = session.scalar(
            select(UsLaceyEngineShipmentRun)
            .where(
                UsLaceyEngineShipmentRun.organization_id == org_id,
                UsLaceyEngineShipmentRun.operation_id == operation.id,
                UsLaceyEngineShipmentRun.schema_version == SHIPMENT_RESOLUTION_SCHEMA_VERSION,
            )
            .order_by(UsLaceyEngineShipmentRun.id.desc())
        )
        if run is None or not isinstance(run.resolution_json, Mapping):
            return 0

        fields = session.scalars(
            select(UsLaceyOperationField).where(
                UsLaceyOperationField.organization_id == org_id,
                UsLaceyOperationField.operation_id == operation.id,
            )
        ).all()
        by_name: dict[str, list[UsLaceyOperationField]] = {}
        for field in fields:
            by_name.setdefault(field.field_name, []).append(field)

        links = session.scalars(
            select(UsLaceyOperationDocument).where(
                UsLaceyOperationDocument.organization_id == org_id,
                UsLaceyOperationDocument.operation_id == operation.id,
                UsLaceyOperationDocument.is_current.is_(True),
            )
        ).all()
        assurance_by_link = {link.id: link.assurance_document_id for link in links}

        promoted = 0
        for suggestion in supported_engine2_suggestions(run.resolution_json):
            targets = by_name.get(suggestion.field_name, [])
            if len(targets) != 1:
                # Multi-line plant allocation remains explicit. A shipment-level HTS,
                # species or quantity cannot be guessed onto one of several PPQ lines.
                continue
            field = targets[0]
            if field.field_status != "MISSING" or field.reviewed_at is not None:
                continue
            assurance_document_id = assurance_by_link.get(suggestion.operation_document_id)
            if assurance_document_id is None:
                continue
            validation = validate_ppq_value(field.field_name, suggestion.value)
            if validation.status.value != "VALID" or not validation.normalized_value:
                continue

            fingerprint = _fingerprint(
                "US_LACEY_ENGINE2_SUPPORTED",
                operation.public_id,
                field.id,
                assurance_document_id,
                validation.normalized_value,
                suggestion.source_page,
                suggestion.source_text,
            )
            existing = session.scalar(
                select(UsLaceyFieldCandidate).where(
                    UsLaceyFieldCandidate.organization_id == org_id,
                    UsLaceyFieldCandidate.fingerprint == fingerprint,
                )
            )
            if existing is None:
                session.add(
                    UsLaceyFieldCandidate(
                        organization_id=org_id,
                        operation_id=operation.id,
                        operation_field_id=field.id,
                        source_assurance_document_id=assurance_document_id,
                        original_value=suggestion.value,
                        normalized_value=validation.normalized_value,
                        validation_status="VALID",
                        validation_error=None,
                        confidence=suggestion.confidence,
                        source_page=suggestion.source_page,
                        source_locator=f"engine2-supported:{suggestion.source_text[:1500]}",
                        extractor="engine2-shipment-supported",
                        extractor_version=suggestion.engine_version,
                        fingerprint=fingerprint,
                        decision="PENDING",
                    )
                )

            field.original_value = suggestion.value
            field.normalized_value = validation.normalized_value
            field.field_status = "FOUND"
            field.confidence = suggestion.confidence
            field.source_assurance_document_id = assurance_document_id
            field.source_page = suggestion.source_page
            field.source_locator = f"engine2-supported:{suggestion.source_text[:1500]}"
            field.extractor = "engine2-shipment-supported"
            field.extractor_version = suggestion.engine_version
            field.validation_status = "VALID"
            field.validation_error = None
            promoted += 1

        if promoted:
            refresh_us_lacey_operation_status(
                session,
                organization_id=org_id,
                operation=operation,
            )
        session.commit()
        return promoted
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
