"""Deterministic semantic reconciliation between Engine 2 and human review.

This pass resolves only relationships that can be proven from structured evidence:
corroborating values, empty placeholders, and parallel plant-component table rows.
It never marks a value human-confirmed and never invents a regulatory fact.
"""
from __future__ import annotations

import hashlib
from typing import Mapping

from sqlalchemy import select

from litoral_trace.db.models import (
    ReconciliationIssue,
    UsLaceyEngineShipmentRun,
    UsLaceyFieldCandidate,
    UsLaceyOperation,
    UsLaceyOperationDocument,
    UsLaceyOperationField,
    UsLaceyPlantDeclaration,
    UsLaceyPpqPlantLine,
)
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.lacey_engine.semantic_graph import semantic_normalize
from litoral_trace.lacey_engine.serialization import SHIPMENT_RESOLUTION_SCHEMA_VERSION
from litoral_trace.us_lacey.db import get_us_lacey_db_session
from litoral_trace.us_lacey.ppq505 import validate_ppq_value
from litoral_trace.us_lacey.projection import refresh_us_lacey_operation_status


_ENGINE_TO_FIELD = {
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
_SINGLE_SUPPORTED = frozenset({"SUPPORTED", "NEAR_MATCH", "SUPPORTED_MULTIPLE"})
_COMPONENT_FIELDS = frozenset(
    {"article_component", "genus", "species", "country_of_harvest", "plant_quantity", "metric_unit", "percent_recycled"}
)


def _fingerprint(*parts: object) -> str:
    payload = "\x1f".join(str(part if part is not None else "") for part in parts)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _evidence_rows(payload: Mapping[str, object]) -> list[Mapping[str, object]]:
    rows = payload.get("supporting_evidence")
    return [row for row in rows if isinstance(row, Mapping)] if isinstance(rows, list) else []


def _raw_candidate(row: Mapping[str, object]) -> tuple[Mapping[str, object], Mapping[str, object]] | None:
    candidate = row.get("candidate")
    if not isinstance(candidate, Mapping):
        return None
    raw = candidate.get("raw")
    provenance = candidate.get("provenance")
    if not isinstance(raw, Mapping) or not isinstance(provenance, Mapping):
        return None
    return raw, provenance


def _strongest(rows: list[Mapping[str, object]]) -> Mapping[str, object] | None:
    if not rows:
        return None
    def score(row: Mapping[str, object]) -> tuple[float, float]:
        try:
            return float(row.get("source_authority") or 0), float(row.get("candidate_score") or 0)
        except (TypeError, ValueError):
            return 0.0, 0.0
    return max(rows, key=score)


def _engine_values(field_key: str, rows: list[Mapping[str, object]]) -> set[str]:
    values = set()
    for row in rows:
        value = str(row.get("normalized_value") or "").strip()
        normalized = semantic_normalize(field_key, value)
        if normalized:
            values.add(normalized)
    return values


def _resolve_field_issues(session, *, organization_id: int, operation: UsLaceyOperation, field: UsLaceyOperationField, reason: str) -> int:
    issues = session.scalars(
        select(ReconciliationIssue).where(
            ReconciliationIssue.organization_id == organization_id,
            ReconciliationIssue.operation_reference == f"us_lacey:{operation.public_id}",
            ReconciliationIssue.us_lacey_operation_field_id == field.id,
            ReconciliationIssue.status == "OPEN",
        )
    ).all()
    for issue in issues:
        issue.status = "RESOLVED"
        issue.resolution_justification = reason
        issue.evidence_json = {**(issue.evidence_json or {}), "semantic_resolution": reason}
    return len(issues)


def _promote_single_supported(
    session,
    *,
    organization_id: int,
    operation: UsLaceyOperation,
    field: UsLaceyOperationField,
    field_key: str,
    payload: Mapping[str, object],
    assurance_by_link: Mapping[int, int],
    engine_version: str,
) -> bool:
    if field.reviewed_at is not None or field.human_value:
        return False
    state = str(payload.get("state") or "")
    rows = _evidence_rows(payload)
    if state not in _SINGLE_SUPPORTED or not rows:
        return False
    # A SUPPORTED_MULTIPLE set is only safe for a singular review field when all
    # evidence collapses to one semantic value.  Parallel components remain separate.
    semantic_values = _engine_values(field_key, rows)
    if len(semantic_values) != 1:
        return False
    strongest = _strongest(rows)
    if strongest is None:
        return False
    parts = _raw_candidate(strongest)
    if parts is None:
        return False
    raw, provenance = parts
    value = str(strongest.get("normalized_value") or raw.get("normalized_value") or "").strip()
    validation = validate_ppq_value(field.field_name, value)
    if validation.status.value != "VALID" or not validation.normalized_value:
        return False
    try:
        link_id = int(strongest.get("document_id") or 0)
        page = int(provenance.get("page") or 0)
        confidence = min(0.99, max(0.0, float(strongest.get("candidate_score") or 0.0) / 100.0))
    except (TypeError, ValueError):
        return False
    assurance_document_id = assurance_by_link.get(link_id)
    if assurance_document_id is None or page < 1:
        return False
    source_text = str(provenance.get("source_text") or "").strip()
    fingerprint = _fingerprint(
        "US_LACEY_SEMANTIC_SUPPORTED", operation.public_id, field.id,
        assurance_document_id, validation.normalized_value, page, source_text,
    )
    candidate = session.scalar(
        select(UsLaceyFieldCandidate).where(
            UsLaceyFieldCandidate.organization_id == organization_id,
            UsLaceyFieldCandidate.fingerprint == fingerprint,
        )
    )
    if candidate is None:
        session.add(UsLaceyFieldCandidate(
            organization_id=organization_id,
            operation_id=operation.id,
            operation_field_id=field.id,
            source_assurance_document_id=assurance_document_id,
            original_value=value,
            normalized_value=validation.normalized_value,
            validation_status="VALID",
            validation_error=None,
            confidence=confidence,
            source_page=page,
            source_locator=f"semantic-engine2:{source_text[:1500]}",
            extractor="engine2-semantic-reconciler",
            extractor_version=engine_version,
            fingerprint=fingerprint,
            decision="PENDING",
        ))
    field.original_value = value
    field.normalized_value = validation.normalized_value
    field.field_status = "FOUND"
    field.validation_status = "VALID"
    field.validation_error = None
    field.confidence = confidence
    field.source_assurance_document_id = assurance_document_id
    field.source_page = page
    field.source_locator = f"semantic-engine2:{source_text[:1500]}"
    field.extractor = "engine2-semantic-reconciler"
    field.extractor_version = engine_version
    _resolve_field_issues(
        session,
        organization_id=organization_id,
        operation=operation,
        field=field,
        reason="Deterministic semantic corroboration: Engine 2 resolved one canonical value; human confirmation is still required.",
    )
    return True


def _project_component_suggestions(
    session,
    *,
    organization_id: int,
    operation: UsLaceyOperation,
    canonical_fields: Mapping[str, object],
    assurance_by_link: Mapping[int, int],
    engine_version: str,
) -> int:
    plant_line = session.scalar(
        select(UsLaceyPpqPlantLine)
        .where(
            UsLaceyPpqPlantLine.organization_id == organization_id,
            UsLaceyPpqPlantLine.operation_id == operation.id,
        )
        .order_by(UsLaceyPpqPlantLine.ordinal.asc())
    )
    if plant_line is None:
        return 0
    grouped: dict[str, dict[str, list[Mapping[str, object]]]] = {}
    for engine_key in _COMPONENT_FIELDS:
        payload = canonical_fields.get(engine_key)
        if not isinstance(payload, Mapping):
            continue
        for row in _evidence_rows(payload):
            component_key = str(row.get("component_key") or "").strip()
            if not component_key:
                continue
            grouped.setdefault(component_key, {}).setdefault(engine_key, []).append(row)
    complete = []
    for component_key, by_field in grouped.items():
        # Species/genus/harvest/quantity/unit form the minimum coherent Lacey
        # component.  Missing values stay for human/supplier review.
        required = ("genus", "species", "country_of_harvest", "plant_quantity", "metric_unit")
        selected: dict[str, Mapping[str, object]] = {}
        valid = True
        for key in required:
            rows = by_field.get(key, [])
            if len(_engine_values(key, rows)) != 1:
                valid = False
                break
            strongest = _strongest(rows)
            if strongest is None:
                valid = False
                break
            selected[key] = strongest
        if valid:
            complete.append((component_key, by_field, selected))
    complete.sort(key=lambda item: item[0])

    changed = 0
    for ordinal, (component_key, by_field, selected) in enumerate(complete, start=1):
        declaration = session.scalar(
            select(UsLaceyPlantDeclaration).where(
                UsLaceyPlantDeclaration.organization_id == organization_id,
                UsLaceyPlantDeclaration.plant_line_id == plant_line.id,
                UsLaceyPlantDeclaration.ordinal == ordinal,
            )
        )
        if declaration is None:
            declaration = UsLaceyPlantDeclaration(
                organization_id=organization_id,
                plant_line_id=plant_line.id,
                ordinal=ordinal,
            )
            session.add(declaration)
        values = {key: str(row.get("normalized_value") or "").strip() for key, row in selected.items()}
        strongest_all = _strongest([row for rows in by_field.values() for row in rows])
        parts = _raw_candidate(strongest_all) if strongest_all is not None else None
        provenance = parts[1] if parts else {}
        try:
            link_id = int((strongest_all or {}).get("document_id") or 0)
            page = int(provenance.get("page") or 0)
        except (TypeError, ValueError):
            link_id, page = 0, 0
        declaration.genus = values["genus"]
        declaration.species = values["species"]
        declaration.country_of_harvest = values["country_of_harvest"]
        declaration.quantity = values["plant_quantity"]
        declaration.unit = values["metric_unit"]
        declaration.original_values = {
            "review_state": "SUPPORTED_PENDING_HUMAN_CONFIRMATION",
            "component_key": component_key,
            "article_component": next(iter(_engine_values("article_component", by_field.get("article_component", []))), None),
            "percent_recycled": next(iter(_engine_values("percent_recycled", by_field.get("percent_recycled", []))), None),
        }
        declaration.source_assurance_document_id = assurance_by_link.get(link_id)
        declaration.source_page = page or None
        declaration.source_locator = f"engine2-component:{component_key}"
        declaration.extractor = "engine2-semantic-component"
        declaration.extractor_version = engine_version
        declaration.confidence = 0.90
        changed += 1

    # If Engine 2 proves multiple parallel components, generic one-value PPQ fields
    # must not keep misleading blocking conflicts.  Keep them REVIEW (not MATCHED)
    # and explain that component-level suggestions exist.
    if len(complete) > 1:
        fields = session.scalars(
            select(UsLaceyOperationField).where(
                UsLaceyOperationField.organization_id == organization_id,
                UsLaceyOperationField.operation_id == operation.id,
                UsLaceyOperationField.field_name.in_(tuple(_COMPONENT_FIELDS)),
            )
        ).all()
        for field in fields:
            if field.reviewed_at is not None or field.human_value:
                continue
            field.field_status = "REVIEW"
            field.validation_status = "REVIEW_REQUIRED"
            field.validation_error = f"{len(complete)} parallel plant components were reconstructed from table rows; review component-level suggestions rather than choosing one value as a conflict."
            _resolve_field_issues(
                session,
                organization_id=organization_id,
                operation=operation,
                field=field,
                reason="Deterministic semantic graph classified the alternatives as parallel plant components, not contradictory values.",
            )
    return changed


def reconcile_operation_semantics(*, organization_id: int, operation_id: int) -> dict[str, int]:
    """Run the deterministic semantic pass after Engine 2 shipment resolution."""
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
            return {"fields_promoted": 0, "components_reconstructed": 0}
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
            return {"fields_promoted": 0, "components_reconstructed": 0}
        canonical_fields = run.resolution_json.get("canonical_fields")
        if not isinstance(canonical_fields, Mapping):
            return {"fields_promoted": 0, "components_reconstructed": 0}
        links = session.scalars(
            select(UsLaceyOperationDocument).where(
                UsLaceyOperationDocument.organization_id == org_id,
                UsLaceyOperationDocument.operation_id == operation.id,
                UsLaceyOperationDocument.is_current.is_(True),
            )
        ).all()
        assurance_by_link = {link.id: link.assurance_document_id for link in links}
        operation_fields = session.scalars(
            select(UsLaceyOperationField).where(
                UsLaceyOperationField.organization_id == org_id,
                UsLaceyOperationField.operation_id == operation.id,
            )
        ).all()
        by_name: dict[str, list[UsLaceyOperationField]] = {}
        for field in operation_fields:
            by_name.setdefault(field.field_name, []).append(field)
        promoted = 0
        engine_version = str(run.resolution_json.get("engine_version") or "lacey-engine-2")
        for engine_key, target in _ENGINE_TO_FIELD.items():
            payload = canonical_fields.get(engine_key)
            targets = by_name.get(target, [])
            if not isinstance(payload, Mapping) or len(targets) != 1:
                continue
            if _promote_single_supported(
                session,
                organization_id=org_id,
                operation=operation,
                field=targets[0],
                field_key=engine_key,
                payload=payload,
                assurance_by_link=assurance_by_link,
                engine_version=engine_version,
            ):
                promoted += 1
        components = _project_component_suggestions(
            session,
            organization_id=org_id,
            operation=operation,
            canonical_fields=canonical_fields,
            assurance_by_link=assurance_by_link,
            engine_version=engine_version,
        )
        refresh_us_lacey_operation_status(session, organization_id=org_id, operation=operation)
        session.commit()
        return {"fields_promoted": promoted, "components_reconstructed": components}
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
