"""Promote verified Engine 2 + AI agreement into non-authoritative review suggestions.

This is the narrow bridge from shadow intelligence to the customer review queue. It is
intentionally conservative: only exact, verified, non-inferred AI evidence that AGREES
with Engine 2 may populate a previously MISSING field as FOUND. Human-reviewed values,
conflicts and existing extracted proposals are never overwritten.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
from typing import Mapping

from sqlalchemy import select

from litoral_trace.db.models import (
    UsLaceyEngineDocumentRun,
    UsLaceyFieldCandidate,
    UsLaceyOperation,
    UsLaceyOperationField,
)
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.lacey_engine.ai_shadow import AI_SHADOW_SCHEMA_VERSION
from litoral_trace.us_lacey.candidate_reconciliation import reconcile_duplicate_field_candidates
from litoral_trace.us_lacey.db import get_us_lacey_db_session
from litoral_trace.us_lacey.ppq505 import PPQ505_SHIPMENT_REFERENCE, validate_ppq_value
from litoral_trace.us_lacey.projection import refresh_us_lacey_operation_status


_AI_TO_PREPARATION_FIELD = {
    "estimated_arrival_date": "estimated_arrival_date",
    "bill_of_lading": "bill_of_lading",
    "container_number": "container_number",
    "consignee_name": "consignee_name",
    "consignee_address": "consignee_address",
    "description": "merchandise_description",
    "species": "species",
    "genus": "genus",
    "filing_entry_reference": "filing_entry_reference",
    "manufacturer_id": "manufacturer_id",
    "hts_code": "hts_code",
    "country_of_harvest": "country_of_harvest",
    "plant_quantity": "plant_quantity",
    "metric_unit": "metric_unit",
}


@dataclass(frozen=True, slots=True)
class VerifiedSuggestion:
    field_name: str
    value: str
    source_text: str
    source_page: int
    confidence: float
    provider: str
    model: str


def _fingerprint(*parts: object) -> str:
    text = "\x1f".join(str(part if part is not None else "") for part in parts)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def verified_agreement_suggestions(payload: Mapping[str, object]) -> tuple[VerifiedSuggestion, ...]:
    """Return only exact verified, non-inferred candidates for AGREEMENT rows."""
    raw_candidates = payload.get("candidates")
    raw_reconciliation = payload.get("reconciliation")
    provider = str(payload.get("provider") or "")
    model = str(payload.get("model") or "")
    if not isinstance(raw_candidates, list) or not isinstance(raw_reconciliation, list):
        return ()

    agreements: dict[str, str] = {}
    for row in raw_reconciliation:
        if not isinstance(row, Mapping) or str(row.get("status") or "") != "AGREEMENT":
            continue
        field_key = str(row.get("field_key") or "")
        ai_value = str(row.get("ai_value") or "").strip()
        if field_key in _AI_TO_PREPARATION_FIELD and ai_value:
            agreements[field_key] = ai_value

    suggestions: list[VerifiedSuggestion] = []
    for field_key, agreed_value in agreements.items():
        eligible = []
        for candidate in raw_candidates:
            if not isinstance(candidate, Mapping):
                continue
            if str(candidate.get("field_key") or "") != field_key:
                continue
            if not bool(candidate.get("evidence_verified")):
                continue
            if str(candidate.get("evidence_class") or "") == "INFERRED":
                continue
            normalized = str(candidate.get("normalized_value") or candidate.get("value") or "").strip()
            if not normalized or normalized.casefold() != agreed_value.casefold():
                continue
            source_text = str(candidate.get("source_text") or "").strip()
            try:
                page = int(candidate.get("page") or 0)
                confidence = float(candidate.get("confidence") or 0.0)
            except (TypeError, ValueError):
                continue
            if not source_text or page < 1 or not 0.0 <= confidence <= 1.0:
                continue
            eligible.append((confidence, source_text, page, normalized))
        if not eligible:
            continue
        confidence, source_text, page, normalized = max(eligible, key=lambda item: item[0])
        suggestions.append(
            VerifiedSuggestion(
                field_name=_AI_TO_PREPARATION_FIELD[field_key],
                value=normalized,
                source_text=source_text,
                source_page=page,
                confidence=confidence,
                provider=provider,
                model=model,
            )
        )
    return tuple(suggestions)


def project_verified_ai_suggestions(*, organization_id: int, operation_id: int) -> int:
    """Populate MISSING fields as FOUND only from Engine2+AI agreement evidence.

    After the optional AI bridge, always run deterministic duplicate-value
    reconciliation.  This keeps page/confidence metadata from creating a false
    conflict even when no AI document run exists.
    """
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

        fields = session.scalars(
            select(UsLaceyOperationField).where(
                UsLaceyOperationField.organization_id == org_id,
                UsLaceyOperationField.operation_id == operation.id,
            )
        ).all()
        by_name: dict[str, list[UsLaceyOperationField]] = {}
        for field in fields:
            by_name.setdefault(field.field_name, []).append(field)

        runs = session.scalars(
            select(UsLaceyEngineDocumentRun)
            .where(
                UsLaceyEngineDocumentRun.organization_id == org_id,
                UsLaceyEngineDocumentRun.operation_id == operation.id,
                UsLaceyEngineDocumentRun.schema_version == AI_SHADOW_SCHEMA_VERSION,
                UsLaceyEngineDocumentRun.status == "SUCCEEDED",
            )
            .order_by(UsLaceyEngineDocumentRun.id.asc())
        ).all()

        promoted = 0
        for run in runs:
            if not isinstance(run.resolution_json, Mapping):
                continue
            for suggestion in verified_agreement_suggestions(run.resolution_json):
                targets = by_name.get(suggestion.field_name, [])
                if len(targets) != 1:
                    # Plant-line values are safe to auto-place only while the intake has
                    # a single line. Multi-line/component allocation needs explicit scope.
                    continue
                field = targets[0]
                if field.field_status != "MISSING" or field.reviewed_at is not None:
                    continue
                validation = validate_ppq_value(field.field_name, suggestion.value)
                if validation.status.value != "VALID" or not validation.normalized_value:
                    continue

                fingerprint = _fingerprint(
                    "US_LACEY_AI_AGREEMENT",
                    operation.public_id,
                    field.id,
                    run.assurance_document_id,
                    suggestion.value,
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
                            source_assurance_document_id=run.assurance_document_id,
                            original_value=suggestion.value,
                            normalized_value=validation.normalized_value,
                            validation_status="VALID",
                            validation_error=None,
                            confidence=suggestion.confidence,
                            source_page=suggestion.source_page,
                            source_locator=f"ai-verified:{suggestion.source_text[:1500]}",
                            extractor=f"engine2+{suggestion.provider or 'ai'}-agreement",
                            extractor_version=suggestion.model or run.engine_version,
                            fingerprint=fingerprint,
                            decision="PENDING",
                        )
                    )

                field.original_value = suggestion.value
                field.normalized_value = validation.normalized_value
                field.field_status = "FOUND"
                field.confidence = suggestion.confidence
                field.source_assurance_document_id = run.assurance_document_id
                field.source_page = suggestion.source_page
                field.source_locator = f"ai-verified:{suggestion.source_text[:1500]}"
                field.extractor = f"engine2+{suggestion.provider or 'ai'}-agreement"
                field.extractor_version = suggestion.model or run.engine_version
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

        duplicate_result = reconcile_duplicate_field_candidates(
            organization_id=org_id,
            operation_id=operation.id,
        )
        return promoted + int(duplicate_result.promoted_count)
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
