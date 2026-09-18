"""Canonical shipment and plant-line truth for U.S. Lacey preparation.

Engine 2 owns evidence admission and reconciliation.  This module is the single
machine publication boundary between that evidence graph and customer review.
It preserves line identity, fails closed on ambiguous joins, and never overwrites
human-reviewed work.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
import hashlib
import json
import re
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
from litoral_trace.us_lacey.cross_document_line_identity import (
    reconcile_cross_document_line_identity,
)
from litoral_trace.us_lacey.ppq505 import (
    PPQ505_FIELDS_BY_KEY,
    PPQ505_PLANT_FIELDS,
    PPQ505_SHIPMENT_REFERENCE,
    PpqScope,
    validate_ppq_value,
)


CANONICAL_PUBLISHER_VERSION = "lacey_canonical_shipment_truth_v2"
_CANONICAL_EXTRACTOR = "canonical-shipment-truth"
_CANONICAL_CONFLICT_RESOLUTION = "Superseded by canonical shipment-line reconciliation."


class CanonicalTruthState(str, Enum):
    SUPPORTED = "SUPPORTED"
    SUPPORTED_MULTIPLE = "SUPPORTED_MULTIPLE"
    NEAR_MATCH = "NEAR_MATCH"
    REVIEW_REQUIRED = "REVIEW_REQUIRED"
    CONFLICT = "CONFLICT"
    MISSING = "MISSING"


@dataclass(frozen=True, slots=True)
class CanonicalEvidence:
    candidate_id: str
    document_id: str
    field_key: str
    normalized_value: str
    source_authority: float
    candidate_score: float
    source_page: int | None
    source_text: str
    line_key: str | None
    component_key: str | None
    evidence_class: str


@dataclass(frozen=True, slots=True)
class CanonicalFieldTruth:
    field_name: str
    state: CanonicalTruthState
    values: tuple[str, ...]
    evidence: tuple[CanonicalEvidence, ...]


@dataclass(frozen=True, slots=True)
class CanonicalPlantLineTruth:
    entity_key: str
    ordinal_hint: int | None
    taxon_key: str | None
    fields: Mapping[str, CanonicalFieldTruth]


@dataclass(frozen=True, slots=True)
class CanonicalShipmentTruth:
    shipment_fields: Mapping[str, CanonicalFieldTruth]
    plant_lines: tuple[CanonicalPlantLineTruth, ...]
    unresolved_component_keys: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class CanonicalPublishResult:
    line_count: int
    field_count: int
    review_count: int
    rejected_candidate_count: int
    resolved_conflict_count: int


_ENGINE_TO_PPQ = {
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
_MERCHANDISE_FIELDS = frozenset({"description", "hts_code", "entered_value"})
_COMPONENT_FIELDS = frozenset(
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
_QUANTITATIVE_COMPONENT_FIELDS = frozenset({"plant_quantity", "metric_unit"})
_SHIPMENT_TOTAL_ENTERED_VALUE = "shipment_total_entered_value"
_BOTANICAL_PPQ_FIELDS = frozenset(_ENGINE_TO_PPQ[key] for key in _COMPONENT_FIELDS)
_ROW_ORDINAL = re.compile(r"(?:^|:)row:(\d+)$", re.IGNORECASE)
_TAXON = re.compile(r"^taxon:([^:]+):([^:]+)$", re.IGNORECASE)


def _candidate_payload(row: Mapping) -> Mapping:
    candidate = row.get("candidate")
    return candidate if isinstance(candidate, Mapping) else {}


def _provenance_payload(row: Mapping) -> Mapping:
    provenance = _candidate_payload(row).get("provenance")
    return provenance if isinstance(provenance, Mapping) else {}


def _raw_payload(row: Mapping) -> Mapping:
    raw = _candidate_payload(row).get("raw")
    return raw if isinstance(raw, Mapping) else {}


def _evidence(row: Mapping, *, fallback_field: str) -> CanonicalEvidence:
    provenance = _provenance_payload(row)
    raw = _raw_payload(row)
    page = provenance.get("page")
    try:
        source_page = int(page) if page is not None else None
    except (TypeError, ValueError):
        source_page = None
    return CanonicalEvidence(
        candidate_id=str(row.get("candidate_id") or ""),
        document_id=str(row.get("document_id") or ""),
        field_key=str(row.get("field_key") or fallback_field),
        normalized_value=str(
            row.get("normalized_value") or raw.get("normalized_value") or ""
        ).strip(),
        source_authority=float(row.get("source_authority") or 0.0),
        candidate_score=float(
            row.get("candidate_score") or _candidate_payload(row).get("score") or 0.0
        ),
        source_page=source_page,
        source_text=str(provenance.get("source_text") or ""),
        line_key=str(row.get("line_key")).strip() if row.get("line_key") else None,
        component_key=(
            str(row.get("component_key")).strip() if row.get("component_key") else None
        ),
        evidence_class=str(
            provenance.get("evidence_class") or raw.get("evidence_class") or ""
        ),
    )


def _distinct_values(rows: tuple[CanonicalEvidence, ...]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(row.normalized_value for row in rows if row.normalized_value))


def _field_state(
    *,
    aggregate_state: str,
    rows: tuple[CanonicalEvidence, ...],
    force_review: bool = False,
) -> CanonicalTruthState:
    if not rows:
        return CanonicalTruthState.MISSING
    values = _distinct_values(rows)
    if len(values) > 1:
        return CanonicalTruthState.CONFLICT
    if force_review or aggregate_state == CanonicalTruthState.REVIEW_REQUIRED.value:
        return CanonicalTruthState.REVIEW_REQUIRED
    if aggregate_state == CanonicalTruthState.NEAR_MATCH.value:
        return CanonicalTruthState.NEAR_MATCH
    if len(rows) > 1:
        return CanonicalTruthState.SUPPORTED_MULTIPLE
    return CanonicalTruthState.SUPPORTED


def _field_truth(
    *,
    target_key: str,
    field_payload: Mapping,
    rows: tuple[CanonicalEvidence, ...],
    force_review: bool = False,
) -> CanonicalFieldTruth:
    return CanonicalFieldTruth(
        field_name=target_key,
        state=_field_state(
            aggregate_state=str(field_payload.get("state") or "MISSING"),
            rows=rows,
            force_review=force_review,
        ),
        values=_distinct_values(rows),
        evidence=rows,
    )


def _ordinal(key: str) -> int | None:
    match = _ROW_ORDINAL.search(key)
    return int(match.group(1)) if match else None


def _taxon_parts(key: str) -> tuple[str, str] | None:
    match = _TAXON.fullmatch(key.strip())
    if not match:
        return None
    return match.group(1).casefold(), match.group(2).casefold()


def _contains_taxon(text: str, taxon_key: str) -> bool:
    parts = _taxon_parts(taxon_key)
    if parts is None:
        return False
    normalized = " ".join(re.sub(r"[^a-z0-9]+", " ", text.casefold()).split())
    genus, species = parts
    return bool(
        re.search(rf"\b{re.escape(genus)}\b", normalized)
        and re.search(rf"\b{re.escape(species)}\b", normalized)
    )


def _explicit_component_line_matches(
    *,
    component_key: str,
    merchandise_keys: tuple[str, ...] | list[str],
    evidence_by_field: Mapping[str, tuple[CanonicalEvidence, ...]],
) -> tuple[str, ...] | None:
    """Return explicit SKU/line bindings for one taxon-bearing component.

    Genus/species observations already carrying both component_key and line_key
    are direct identity evidence. Multiple explicit lines remain ambiguous and
    are returned as such so callers fail closed instead of falling back to
    description matching.
    """
    known_lines = frozenset(merchandise_keys)
    lines = {
        row.line_key
        for field_key in ("genus", "species")
        for row in evidence_by_field.get(field_key, ())
        if row.component_key == component_key
        and row.line_key in known_lines
    }
    if not lines:
        return None
    return tuple(sorted(lines))


def _low_authority_review_fields(payload: Mapping) -> frozenset[str]:
    fields: set[str] = set()
    for issue in payload.get("issues") or ():
        if not isinstance(issue, Mapping):
            continue
        if str(issue.get("issue_type") or "") != "LOW_AUTHORITY_ONLY":
            continue
        field = str(issue.get("field_key") or "").strip()
        if field:
            fields.add(field)
    return frozenset(fields)


def _line_has_single_valid_hts(
    *,
    line_key: str,
    evidence_by_field: Mapping[str, tuple[CanonicalEvidence, ...]],
) -> bool:
    values = {
        row.normalized_value
        for row in evidence_by_field.get("hts_code", ())
        if row.line_key == line_key
        and validate_ppq_value("hts_code", row.normalized_value).status.value == "VALID"
    }
    return len(values) == 1


def _quantitative_component_rows(
    *,
    engine_key: str,
    line_key: str,
    component_key: str,
    evidence_by_field: Mapping[str, tuple[CanonicalEvidence, ...]],
) -> tuple[CanonicalEvidence, ...]:
    """Return only quantitative rows with deterministic line-binding proof.

    A derived line_key is produced upstream only by explicit SKU/LINE identity or
    exact HTS+taxon binding. Taxon-scoped rows remain admissible when the canonical
    line itself has one valid HTS and the taxon->line mapping is unique.
    """
    strong_taxon_signature = _line_has_single_valid_hts(
        line_key=line_key,
        evidence_by_field=evidence_by_field,
    )
    return tuple(
        row
        for row in evidence_by_field.get(engine_key, ())
        if row.line_key == line_key
        or (
            strong_taxon_signature
            and row.component_key == component_key
        )
    )


def _unique_shipment_total_entered_value(
    *,
    field_payloads: Mapping[str, Mapping],
    evidence_by_field: Mapping[str, tuple[CanonicalEvidence, ...]],
) -> CanonicalFieldTruth | None:
    field_payload = field_payloads.get(_SHIPMENT_TOTAL_ENTERED_VALUE)
    if field_payload is None:
        return None
    if str(field_payload.get("state") or "") not in {
        CanonicalTruthState.SUPPORTED.value,
        CanonicalTruthState.SUPPORTED_MULTIPLE.value,
    }:
        return None

    raw_rows = tuple(
        row
        for row in (field_payload.get("supporting_evidence") or ())
        if isinstance(row, Mapping)
    )
    if not raw_rows:
        return None
    if any(
        str(row.get("scope") or "").upper() != "SHIPMENT"
        or row.get("line_key")
        or row.get("component_key")
        for row in raw_rows
    ):
        return None

    rows = evidence_by_field.get(_SHIPMENT_TOTAL_ENTERED_VALUE, ())
    if len(_distinct_values(rows)) != 1:
        return None

    return _field_truth(
        target_key="entered_value",
        field_payload=field_payload,
        rows=rows,
        force_review=True,
    )


def _apply_single_line_entered_value_inference(
    *,
    line_truths: list[CanonicalPlantLineTruth],
    unresolved: set[str],
    field_payloads: Mapping[str, Mapping],
    evidence_by_field: Mapping[str, tuple[CanonicalEvidence, ...]],
) -> list[CanonicalPlantLineTruth]:
    """Use one shipment total for one proven line, always as REVIEW_REQUIRED."""
    if len(line_truths) != 1 or unresolved:
        return line_truths

    line = line_truths[0]
    if (
        line.taxon_key is None
        or "hts_code" not in line.fields
        or "entered_value" in line.fields
        or not _line_has_single_valid_hts(
            line_key=line.entity_key,
            evidence_by_field=evidence_by_field,
        )
    ):
        return line_truths

    inferred = _unique_shipment_total_entered_value(
        field_payloads=field_payloads,
        evidence_by_field=evidence_by_field,
    )
    if inferred is None:
        return line_truths

    fields = dict(line.fields)
    fields["entered_value"] = inferred
    return [
        CanonicalPlantLineTruth(
            entity_key=line.entity_key,
            ordinal_hint=line.ordinal_hint,
            taxon_key=line.taxon_key,
            fields=fields,
        )
    ]


def _composed_merchandise_description(
    plant_lines: tuple[CanonicalPlantLineTruth, ...],
) -> CanonicalFieldTruth | None:
    """Collapse line-local descriptions into the shipment-scoped PPQ field.

    PPQ 505 field 10 is shipment-scoped.  Line descriptions remain on canonical
    plant lines for identity and evidence isolation, but their exact supported
    strings are published once, in stable line order, rather than as a false
    cross-line conflict.
    """
    values: list[str] = []
    evidence: list[CanonicalEvidence] = []
    for line in plant_lines:
        description = line.fields.get("merchandise_description")
        if description is None or description.state is CanonicalTruthState.CONFLICT:
            return None
        if len(description.values) != 1:
            return None
        if description.values[0] not in values:
            values.append(description.values[0])
        evidence.extend(description.evidence)
    if not values:
        return None
    state = (
        CanonicalTruthState.SUPPORTED_MULTIPLE
        if len(values) > 1 or len(evidence) > 1
        else CanonicalTruthState.SUPPORTED
    )
    return CanonicalFieldTruth(
        field_name="merchandise_description",
        state=state,
        values=("; ".join(values),),
        evidence=tuple(evidence),
    )


def build_canonical_shipment_truth(payload: Mapping) -> CanonicalShipmentTruth:
    """Build one fail-closed shipment/plant-line truth from Engine 2 JSON."""
    payload = reconcile_cross_document_line_identity(payload)
    fields_payload = payload.get("canonical_fields")
    if not isinstance(fields_payload, Mapping):
        raise ValueError("Shipment resolution has no canonical_fields mapping.")

    low_authority_fields = _low_authority_review_fields(payload)
    evidence_by_field: dict[str, tuple[CanonicalEvidence, ...]] = {}
    field_payloads: dict[str, Mapping] = {}
    for engine_key, raw_field in fields_payload.items():
        if not isinstance(raw_field, Mapping):
            continue
        key = str(engine_key)
        field_payloads[key] = raw_field
        evidence_by_field[key] = tuple(
            _evidence(row, fallback_field=key)
            for row in (raw_field.get("supporting_evidence") or ())
            if isinstance(row, Mapping)
        )

    merchandise_keys = sorted(
        {
            row.line_key
            for key in _MERCHANDISE_FIELDS
            for row in evidence_by_field.get(key, ())
            if row.line_key
        },
        key=lambda value: (_ordinal(value) is None, _ordinal(value) or 10**9, value),
    )
    component_keys = sorted(
        {
            row.component_key
            for key in (_COMPONENT_FIELDS - _QUANTITATIVE_COMPONENT_FIELDS)
            for row in evidence_by_field.get(key, ())
            if row.component_key
        }
    )

    description_text_by_line: dict[str, str] = {}
    for line_key in merchandise_keys:
        description_text_by_line[line_key] = " ".join(
            part
            for row in evidence_by_field.get("description", ())
            if row.line_key == line_key
            for part in (row.normalized_value, row.source_text)
            if part
        )

    matches_by_component: dict[str, tuple[str, ...]] = {}
    for component_key in component_keys:
        explicit_matches = _explicit_component_line_matches(
            component_key=component_key,
            merchandise_keys=merchandise_keys,
            evidence_by_field=evidence_by_field,
        )
        if explicit_matches is not None:
            matches = explicit_matches
        else:
            matches = tuple(
                line_key
                for line_key in merchandise_keys
                if _contains_taxon(
                    description_text_by_line.get(line_key, ""),
                    component_key,
                )
            )
            if (
                not matches
                and len(merchandise_keys) == 1
                and len(component_keys) == 1
            ):
                matches = (merchandise_keys[0],)
        matches_by_component[component_key] = matches

    component_for_line: dict[str, str] = {}
    unresolved: set[str] = set()
    for component_key, matches in matches_by_component.items():
        if len(matches) != 1:
            unresolved.add(component_key)
            continue
        line_key = matches[0]
        if line_key in component_for_line:
            unresolved.add(component_key)
            unresolved.add(component_for_line.pop(line_key))
            continue
        component_for_line[line_key] = component_key

    line_truths: list[CanonicalPlantLineTruth] = []
    for line_key in merchandise_keys:
        line_fields: dict[str, CanonicalFieldTruth] = {}
        for engine_key in _MERCHANDISE_FIELDS:
            field_payload = field_payloads.get(engine_key)
            if field_payload is None:
                continue
            rows = tuple(
                row for row in evidence_by_field.get(engine_key, ()) if row.line_key == line_key
            )
            if not rows:
                continue
            line_fields[_ENGINE_TO_PPQ[engine_key]] = _field_truth(
                target_key=_ENGINE_TO_PPQ[engine_key],
                field_payload=field_payload,
                rows=rows,
            )

        component_key = component_for_line.get(line_key)
        if component_key:
            for engine_key in _COMPONENT_FIELDS:
                field_payload = field_payloads.get(engine_key)
                if field_payload is None:
                    continue
                if engine_key in _QUANTITATIVE_COMPONENT_FIELDS:
                    rows = _quantitative_component_rows(
                        engine_key=engine_key,
                        line_key=line_key,
                        component_key=component_key,
                        evidence_by_field=evidence_by_field,
                    )
                else:
                    rows = tuple(
                        row
                        for row in evidence_by_field.get(engine_key, ())
                        if row.component_key == component_key
                    )
                if not rows:
                    continue
                line_fields[_ENGINE_TO_PPQ[engine_key]] = _field_truth(
                    target_key=_ENGINE_TO_PPQ[engine_key],
                    field_payload=field_payload,
                    rows=rows,
                    force_review=engine_key in low_authority_fields,
                )

        line_truths.append(
            CanonicalPlantLineTruth(
                entity_key=line_key,
                ordinal_hint=_ordinal(line_key),
                taxon_key=component_key,
                fields=line_fields,
            )
        )

    if not merchandise_keys:
        for ordinal, component_key in enumerate(component_keys, start=1):
            line_fields: dict[str, CanonicalFieldTruth] = {}
            for engine_key in _COMPONENT_FIELDS:
                field_payload = field_payloads.get(engine_key)
                if field_payload is None:
                    continue
                rows = tuple(
                    row
                    for row in evidence_by_field.get(engine_key, ())
                    if row.component_key == component_key
                )
                if not rows:
                    continue
                line_fields[_ENGINE_TO_PPQ[engine_key]] = _field_truth(
                    target_key=_ENGINE_TO_PPQ[engine_key],
                    field_payload=field_payload,
                    rows=rows,
                    force_review=engine_key in low_authority_fields,
                )
            line_truths.append(
                CanonicalPlantLineTruth(
                    entity_key=component_key,
                    ordinal_hint=ordinal,
                    taxon_key=component_key,
                    fields=line_fields,
                )
            )

    line_truths = _apply_single_line_entered_value_inference(
        line_truths=line_truths,
        unresolved=unresolved,
        field_payloads=field_payloads,
        evidence_by_field=evidence_by_field,
    )
    plant_lines = tuple(line_truths)
    shipment_fields: dict[str, CanonicalFieldTruth] = {}
    for engine_key, field_payload in field_payloads.items():
        if engine_key in _MERCHANDISE_FIELDS or engine_key in _COMPONENT_FIELDS:
            continue
        target = _ENGINE_TO_PPQ.get(engine_key, engine_key)
        contract = PPQ505_FIELDS_BY_KEY.get(target)
        if contract is None or contract.scope is not PpqScope.SHIPMENT:
            continue
        shipment_fields[target] = _field_truth(
            target_key=target,
            field_payload=field_payload,
            rows=evidence_by_field.get(engine_key, ()),
            force_review=engine_key in low_authority_fields,
        )

    composed_description = _composed_merchandise_description(plant_lines)
    if composed_description is not None:
        shipment_fields["merchandise_description"] = composed_description

    return CanonicalShipmentTruth(
        shipment_fields=shipment_fields,
        plant_lines=plant_lines,
        unresolved_component_keys=tuple(sorted(unresolved)),
    )


def _reviewed(target: UsLaceyOperationField) -> bool:
    return target.reviewed_at is not None or bool(str(target.human_value or "").strip())


def _confidence(score: float) -> float:
    value = float(score or 0.0)
    if value > 1.0:
        value /= 100.0
    return max(0.0, min(1.0, value))


def _primary_evidence(field: CanonicalFieldTruth) -> CanonicalEvidence | None:
    if not field.evidence:
        return None
    return max(
        field.evidence,
        key=lambda row: (row.source_authority, row.candidate_score, row.candidate_id),
    )


def _candidate_fingerprint(
    *,
    organization_id: int,
    operation_id: int,
    target: UsLaceyOperationField,
    source_assurance_document_id: int,
    evidence: CanonicalEvidence,
    normalized_value: str,
) -> str:
    payload = {
        "version": CANONICAL_PUBLISHER_VERSION,
        "organization_id": int(organization_id),
        "operation_id": int(operation_id),
        "operation_field_id": int(target.id),
        "source_assurance_document_id": int(source_assurance_document_id),
        "candidate_id": evidence.candidate_id,
        "normalized_value": normalized_value,
        "source_page": evidence.source_page,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _line_has_human_review(
    session,
    *,
    organization_id: int,
    operation_id: int,
    plant_line_id: int,
) -> bool:
    fields = session.scalars(
        select(UsLaceyOperationField).where(
            UsLaceyOperationField.organization_id == organization_id,
            UsLaceyOperationField.operation_id == operation_id,
            UsLaceyOperationField.plant_line_id == plant_line_id,
        )
    ).all()
    return any(
        field.reviewed_at is not None
        or field.reviewed_by_user_id is not None
        or bool(str(field.human_value or "").strip())
        for field in fields
    )


def _ensure_plant_line_count(
    session,
    *,
    organization_id: int,
    operation: UsLaceyOperation,
    needed: int,
) -> tuple[UsLaceyPpqPlantLine, ...]:
    lines = list(
        session.scalars(
            select(UsLaceyPpqPlantLine)
            .where(
                UsLaceyPpqPlantLine.organization_id == organization_id,
                UsLaceyPpqPlantLine.operation_id == operation.id,
            )
            .order_by(UsLaceyPpqPlantLine.ordinal.asc(), UsLaceyPpqPlantLine.id.asc())
        ).all()
    )

    if len(lines) > needed:
        surplus = lines[needed:]
        for line in surplus:
            if not str(line.line_reference).startswith("CANONICAL-"):
                raise RuntimeError("CANONICAL_STALE_LINE_REQUIRES_REVIEW")
            if _line_has_human_review(
                session,
                organization_id=organization_id,
                operation_id=int(operation.id),
                plant_line_id=int(line.id),
            ):
                raise RuntimeError("CANONICAL_STALE_LINE_REQUIRES_REVIEW")
        for line in surplus:
            session.delete(line)
        session.flush()
        lines = lines[:needed]

    existing_refs = {str(line.line_reference) for line in lines}
    next_ordinal = max((int(line.ordinal) for line in lines), default=0) + 1
    while len(lines) < needed:
        ordinal = next_ordinal
        reference = f"CANONICAL-{ordinal}"
        suffix = 1
        while reference in existing_refs:
            suffix += 1
            reference = f"CANONICAL-{ordinal}-{suffix}"
        line = UsLaceyPpqPlantLine(
            organization_id=organization_id,
            operation_id=operation.id,
            line_reference=reference,
            ordinal=ordinal,
        )
        session.add(line)
        session.flush()
        session.add(
            UsLaceyPlantDeclaration(
                organization_id=organization_id,
                plant_line_id=line.id,
                ordinal=1,
            )
        )
        for contract in PPQ505_PLANT_FIELDS:
            session.add(
                UsLaceyOperationField(
                    organization_id=organization_id,
                    operation_id=operation.id,
                    merchandise_line_reference=reference,
                    field_name=contract.key,
                    field_scope="PLANT_LINE",
                    plant_line_id=line.id,
                    field_status="MISSING",
                    validation_status="MISSING",
                    confidence=0.0,
                )
            )
        lines.append(line)
        existing_refs.add(reference)
        next_ordinal += 1
    operation.merchandise_line_count = len(lines)
    session.flush()
    return tuple(lines)


def _ensure_shipment_field(
    session,
    *,
    organization_id: int,
    operation_id: int,
    field_name: str,
) -> UsLaceyOperationField:
    contract = PPQ505_FIELDS_BY_KEY.get(field_name)
    if contract is None or contract.scope is not PpqScope.SHIPMENT:
        raise RuntimeError("CANONICAL_SHIPMENT_FIELD_CONTRACT_INVALID")
    existing = session.scalar(
        select(UsLaceyOperationField).where(
            UsLaceyOperationField.organization_id == organization_id,
            UsLaceyOperationField.operation_id == operation_id,
            UsLaceyOperationField.merchandise_line_reference == PPQ505_SHIPMENT_REFERENCE,
            UsLaceyOperationField.field_name == field_name,
        )
    )
    if existing is not None:
        return existing
    field = UsLaceyOperationField(
        organization_id=organization_id,
        operation_id=operation_id,
        merchandise_line_reference=PPQ505_SHIPMENT_REFERENCE,
        field_name=field_name,
        field_scope="SHIPMENT",
        plant_line_id=None,
        field_status="MISSING",
        validation_status="MISSING",
        confidence=0.0,
    )
    session.add(field)
    session.flush()
    return field


def _reject_stale_machine_candidates(
    session,
    *,
    organization_id: int,
    target: UsLaceyOperationField,
    accepted_fingerprints: frozenset[str],
) -> int:
    rejected = 0
    rows = session.scalars(
        select(UsLaceyFieldCandidate).where(
            UsLaceyFieldCandidate.organization_id == organization_id,
            UsLaceyFieldCandidate.operation_field_id == target.id,
            UsLaceyFieldCandidate.decision == "PENDING",
        )
    ).all()
    now = datetime.now(timezone.utc)
    for row in rows:
        if row.fingerprint in accepted_fingerprints:
            continue
        row.decision = "REJECTED"
        row.decided_at = now
        rejected += 1
    return rejected


def _candidate_groups(field: CanonicalFieldTruth) -> Mapping[str, tuple[CanonicalEvidence, ...]]:
    if len(field.values) == 1:
        return {field.values[0]: field.evidence}
    groups: dict[str, list[CanonicalEvidence]] = {}
    for evidence in field.evidence:
        if evidence.normalized_value:
            groups.setdefault(evidence.normalized_value, []).append(evidence)
    return {key: tuple(value) for key, value in groups.items()}


def _publish_field(
    session,
    *,
    organization_id: int,
    operation_id: int,
    target: UsLaceyOperationField,
    truth: CanonicalFieldTruth | None,
    assurance_by_operation_document: Mapping[int, int],
    ambiguous_component_binding: bool,
) -> tuple[int, int, int]:
    if _reviewed(target):
        return 0, 0, 0

    if truth is None:
        rejected = _reject_stale_machine_candidates(
            session,
            organization_id=organization_id,
            target=target,
            accepted_fingerprints=frozenset(),
        )
        target.original_value = None
        target.normalized_value = None
        target.source_assurance_document_id = None
        target.source_page = None
        target.source_locator = None
        target.extractor = _CANONICAL_EXTRACTOR
        target.extractor_version = CANONICAL_PUBLISHER_VERSION
        target.confidence = 0.0
        if ambiguous_component_binding and target.field_name in _BOTANICAL_PPQ_FIELDS:
            target.field_status = "REVIEW"
            target.validation_status = "REVIEW_REQUIRED"
            target.validation_error = (
                "Evidence exists, but its plant-line association is ambiguous."
            )
            return 1, 1, rejected
        if target.field_status == "NOT_REQUIRED":
            return 0, 0, rejected
        target.field_status = "MISSING"
        target.validation_status = "MISSING"
        target.validation_error = None
        return 1, 0, rejected

    accepted_fingerprints: set[str] = set()
    for value, evidences in _candidate_groups(truth).items():
        validation = validate_ppq_value(target.field_name, value)
        normalized = validation.normalized_value or value
        for evidence in evidences:
            try:
                operation_document_id = int(evidence.document_id)
            except (TypeError, ValueError) as exc:
                raise RuntimeError("CANONICAL_EVIDENCE_DOCUMENT_ID_INVALID") from exc
            source_assurance = assurance_by_operation_document.get(operation_document_id)
            if source_assurance is None:
                raise RuntimeError("CANONICAL_EVIDENCE_SOURCE_NOT_CURRENT")
            fingerprint = _candidate_fingerprint(
                organization_id=organization_id,
                operation_id=operation_id,
                target=target,
                source_assurance_document_id=source_assurance,
                evidence=evidence,
                normalized_value=normalized,
            )
            accepted_fingerprints.add(fingerprint)
            existing = session.scalar(
                select(UsLaceyFieldCandidate).where(
                    UsLaceyFieldCandidate.organization_id == organization_id,
                    UsLaceyFieldCandidate.fingerprint == fingerprint,
                )
            )
            if existing is None:
                session.add(
                    UsLaceyFieldCandidate(
                        organization_id=organization_id,
                        operation_id=operation_id,
                        operation_field_id=target.id,
                        source_assurance_document_id=source_assurance,
                        original_value=value,
                        normalized_value=normalized,
                        validation_status=(
                            "REVIEW_REQUIRED"
                            if truth.state
                            in {CanonicalTruthState.REVIEW_REQUIRED, CanonicalTruthState.CONFLICT}
                            else validation.status.value
                        ),
                        validation_error=(
                            "Canonical evidence requires human review."
                            if truth.state
                            in {CanonicalTruthState.REVIEW_REQUIRED, CanonicalTruthState.CONFLICT}
                            else validation.error
                        ),
                        confidence=_confidence(evidence.candidate_score),
                        source_page=evidence.source_page,
                        source_locator=(
                            f"canonical:{evidence.line_key or evidence.component_key or 'shipment'}:"
                            f"{evidence.candidate_id}"
                        ),
                        extractor=_CANONICAL_EXTRACTOR,
                        extractor_version=CANONICAL_PUBLISHER_VERSION,
                        fingerprint=fingerprint,
                        decision="PENDING",
                    )
                )
            elif existing.decision == "REJECTED" and existing.decided_by_user_id is None:
                existing.decision = "PENDING"
                existing.decided_at = None

    rejected = _reject_stale_machine_candidates(
        session,
        organization_id=organization_id,
        target=target,
        accepted_fingerprints=frozenset(accepted_fingerprints),
    )

    if truth.state is CanonicalTruthState.CONFLICT or len(truth.values) != 1:
        target.original_value = None
        target.normalized_value = None
        target.field_status = "REVIEW"
        target.validation_status = "REVIEW_REQUIRED"
        target.validation_error = "Canonical evidence contains multiple values for this entity."
        primary = _primary_evidence(truth)
        target.confidence = _confidence(primary.candidate_score) if primary else 0.0
        target.extractor = _CANONICAL_EXTRACTOR
        target.extractor_version = CANONICAL_PUBLISHER_VERSION
        return 1, 1, rejected

    value = truth.values[0]
    validation = validate_ppq_value(target.field_name, value)
    normalized = validation.normalized_value or value
    primary = _primary_evidence(truth)
    if primary is None:
        raise RuntimeError("CANONICAL_FIELD_WITHOUT_EVIDENCE")
    try:
        primary_document = int(primary.document_id)
    except (TypeError, ValueError) as exc:
        raise RuntimeError("CANONICAL_EVIDENCE_DOCUMENT_ID_INVALID") from exc
    source_assurance = assurance_by_operation_document.get(primary_document)
    if source_assurance is None:
        raise RuntimeError("CANONICAL_EVIDENCE_SOURCE_NOT_CURRENT")

    target.original_value = value
    target.normalized_value = normalized
    target.source_assurance_document_id = source_assurance
    target.source_page = primary.source_page
    target.source_locator = (
        f"canonical:{primary.line_key or primary.component_key or 'shipment'}:{primary.candidate_id}"
    )
    target.extractor = _CANONICAL_EXTRACTOR
    target.extractor_version = CANONICAL_PUBLISHER_VERSION
    target.confidence = _confidence(primary.candidate_score)
    target.not_required_reason_code = None

    must_review = (
        truth.state in {CanonicalTruthState.REVIEW_REQUIRED, CanonicalTruthState.NEAR_MATCH}
        or validation.status.value in {"INVALID", "REVIEW_REQUIRED"}
    )
    if must_review:
        target.field_status = "REVIEW"
        target.validation_status = "REVIEW_REQUIRED"
        target.validation_error = validation.error or "Canonical evidence requires human review."
        return 1, 1, rejected

    target.field_status = "FOUND"
    target.validation_status = validation.status.value
    target.validation_error = validation.error
    return 1, 0, rejected


def _resolvable_field_names(truth: CanonicalShipmentTruth) -> frozenset[str]:
    names: set[str] = {
        key
        for key, field in truth.shipment_fields.items()
        if field.state is not CanonicalTruthState.CONFLICT and len(field.values) == 1
    }
    if truth.plant_lines:
        line_names = set(truth.plant_lines[0].fields)
        for line in truth.plant_lines[1:]:
            line_names.intersection_update(line.fields)
        if truth.unresolved_component_keys:
            line_names.difference_update(_BOTANICAL_PPQ_FIELDS)
        names.update(
            field_name
            for field_name in line_names
            if all(
                line.fields[field_name].state is not CanonicalTruthState.CONFLICT
                for line in truth.plant_lines
            )
        )
    return frozenset(names)


def publish_canonical_shipment_truth(
    session,
    *,
    organization_id: int,
    operation_id: int,
) -> CanonicalPublishResult:
    """Publish latest Engine 2 truth into PPQ review in the caller transaction."""
    org_id = int(organization_id)
    op_id = int(operation_id)
    operation = session.scalar(
        select(UsLaceyOperation).where(
            UsLaceyOperation.organization_id == org_id,
            UsLaceyOperation.id == op_id,
        )
    )
    if operation is None:
        raise RuntimeError("CANONICAL_OPERATION_NOT_FOUND")

    shipment_run = session.scalar(
        select(UsLaceyEngineShipmentRun)
        .where(
            UsLaceyEngineShipmentRun.organization_id == org_id,
            UsLaceyEngineShipmentRun.operation_id == op_id,
        )
        .order_by(UsLaceyEngineShipmentRun.id.desc())
    )
    if shipment_run is None:
        raise RuntimeError("CANONICAL_SHIPMENT_RUN_NOT_FOUND")
    truth = build_canonical_shipment_truth(shipment_run.resolution_json)

    lines = _ensure_plant_line_count(
        session,
        organization_id=org_id,
        operation=operation,
        needed=len(truth.plant_lines),
    )
    canonical_lines = lines[: len(truth.plant_lines)]

    operation_documents = session.scalars(
        select(UsLaceyOperationDocument).where(
            UsLaceyOperationDocument.organization_id == org_id,
            UsLaceyOperationDocument.operation_id == op_id,
            UsLaceyOperationDocument.is_current.is_(True),
        )
    ).all()
    assurance_by_operation_document = {
        int(row.id): int(row.assurance_document_id) for row in operation_documents
    }

    fields = session.scalars(
        select(UsLaceyOperationField).where(
            UsLaceyOperationField.organization_id == org_id,
            UsLaceyOperationField.operation_id == op_id,
        )
    ).all()
    indexed = {
        (str(row.merchandise_line_reference), row.field_name): row for row in fields
    }

    field_count = review_count = rejected_count = 0
    ambiguous = bool(truth.unresolved_component_keys)
    for line_row, canonical_line in zip(canonical_lines, truth.plant_lines):
        for contract in PPQ505_PLANT_FIELDS:
            target = indexed.get((str(line_row.line_reference), contract.key))
            if target is None:
                raise RuntimeError("CANONICAL_PPQ_FIELD_SLOT_MISSING")
            published, reviews, rejected = _publish_field(
                session,
                organization_id=org_id,
                operation_id=op_id,
                target=target,
                truth=canonical_line.fields.get(contract.key),
                assurance_by_operation_document=assurance_by_operation_document,
                ambiguous_component_binding=ambiguous,
            )
            field_count += published
            review_count += reviews
            rejected_count += rejected

    for field_name, shipment_truth in truth.shipment_fields.items():
        target = _ensure_shipment_field(
            session,
            organization_id=org_id,
            operation_id=op_id,
            field_name=field_name,
        )
        published, reviews, rejected = _publish_field(
            session,
            organization_id=org_id,
            operation_id=op_id,
            target=target,
            truth=shipment_truth,
            assurance_by_operation_document=assurance_by_operation_document,
            ambiguous_component_binding=False,
        )
        field_count += published
        review_count += reviews
        rejected_count += rejected

    if len(lines) > len(truth.plant_lines):
        canonical_ids = {int(line.id) for line in canonical_lines}
        for row in fields:
            if (
                row.plant_line_id is None
                or int(row.plant_line_id) in canonical_ids
                or _reviewed(row)
            ):
                continue
            rejected_count += _reject_stale_machine_candidates(
                session,
                organization_id=org_id,
                target=row,
                accepted_fingerprints=frozenset(),
            )
            if row.field_status != "NOT_REQUIRED":
                row.original_value = None
                row.normalized_value = None
                row.source_assurance_document_id = None
                row.source_page = None
                row.source_locator = None
                row.extractor = _CANONICAL_EXTRACTOR
                row.extractor_version = CANONICAL_PUBLISHER_VERSION
                row.confidence = 0.0
                row.field_status = "MISSING"
                row.validation_status = "MISSING"
                row.validation_error = None

    resolved_conflicts = 0
    resolvable_fields = _resolvable_field_names(truth)
    if resolvable_fields:
        issues = session.scalars(
            select(ReconciliationIssue).where(
                ReconciliationIssue.organization_id == org_id,
                ReconciliationIssue.operation_reference == f"us_lacey:{operation.public_id}",
                ReconciliationIssue.status == "OPEN",
                ReconciliationIssue.rule_code == "US_LACEY_FIELD_CONFLICT",
                ReconciliationIssue.field_name.in_(tuple(resolvable_fields)),
            )
        ).all()
        now = datetime.now(timezone.utc)
        for issue in issues:
            issue.status = "RESOLVED"
            issue.resolved_at = now
            issue.resolution_justification = _CANONICAL_CONFLICT_RESOLUTION
            resolved_conflicts += 1

    session.flush()
    from litoral_trace.us_lacey.projection import refresh_us_lacey_operation_status

    refresh_us_lacey_operation_status(
        session,
        organization_id=org_id,
        operation=operation,
    )
    return CanonicalPublishResult(
        line_count=len(truth.plant_lines),
        field_count=field_count,
        review_count=review_count,
        rejected_candidate_count=rejected_count,
        resolved_conflict_count=resolved_conflicts,
    )
