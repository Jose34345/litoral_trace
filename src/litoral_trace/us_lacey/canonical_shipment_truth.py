"""Deterministic final truth model for U.S. Lacey shipment preparation.

The extraction/reconciliation engine owns evidence admission.  This module owns the
last semantic step before machine-derived values are published to customer review:
it turns scoped shipment evidence into explicit shipment and plant-line entities.

It intentionally contains no AI calls and never guesses an ambiguous line binding.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import re
from typing import Mapping


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
        page_value = int(page) if page is not None else None
    except (TypeError, ValueError):
        page_value = None
    return CanonicalEvidence(
        candidate_id=str(row.get("candidate_id") or ""),
        document_id=str(row.get("document_id") or ""),
        field_key=str(row.get("field_key") or fallback_field),
        normalized_value=str(row.get("normalized_value") or raw.get("normalized_value") or "").strip(),
        source_authority=float(row.get("source_authority") or 0.0),
        candidate_score=float(row.get("candidate_score") or _candidate_payload(row).get("score") or 0.0),
        source_page=page_value,
        source_text=str(provenance.get("source_text") or ""),
        line_key=(str(row.get("line_key")).strip() if row.get("line_key") else None),
        component_key=(str(row.get("component_key")).strip() if row.get("component_key") else None),
        evidence_class=str(provenance.get("evidence_class") or raw.get("evidence_class") or ""),
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
    engine_key: str,
    target_key: str,
    field_payload: Mapping,
    rows: tuple[CanonicalEvidence, ...],
    force_review: bool = False,
) -> CanonicalFieldTruth:
    state = _field_state(
        aggregate_state=str(field_payload.get("state") or "MISSING"),
        rows=rows,
        force_review=force_review,
    )
    return CanonicalFieldTruth(target_key, state, _distinct_values(rows), rows)


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


def _low_authority_review_fields(payload: Mapping) -> frozenset[str]:
    fields: set[str] = set()
    for issue in payload.get("issues") or ():
        if not isinstance(issue, Mapping):
            continue
        if str(issue.get("issue_type") or "") == "LOW_AUTHORITY_ONLY":
            field = str(issue.get("field_key") or "").strip()
            if field:
                fields.add(field)
    return frozenset(fields)


def build_canonical_shipment_truth(payload: Mapping) -> CanonicalShipmentTruth:
    """Build one fail-closed shipment/plant-line truth from Engine 2 JSON.

    Merchandise rows are never merged merely because their values look similar.
    Plant-component evidence is attached only when a taxon is explicitly present
    in exactly one merchandise line description (or the whole operation has one
    merchandise line and one component).
    """
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
            for key in _COMPONENT_FIELDS
            for row in evidence_by_field.get(key, ())
            if row.component_key
        }
    )

    description_text_by_line: dict[str, str] = {}
    for line_key in merchandise_keys:
        rows = tuple(
            row
            for row in evidence_by_field.get("description", ())
            if row.line_key == line_key
        )
        description_text_by_line[line_key] = " ".join(
            part
            for row in rows
            for part in (row.normalized_value, row.source_text)
            if part
        )

    matches_by_component: dict[str, tuple[str, ...]] = {}
    for component_key in component_keys:
        explicit = tuple(
            line_key
            for line_key in merchandise_keys
            if _contains_taxon(description_text_by_line.get(line_key, ""), component_key)
        )
        if not explicit and len(merchandise_keys) == 1 and len(component_keys) == 1:
            explicit = (merchandise_keys[0],)
        matches_by_component[component_key] = explicit

    component_for_line: dict[str, str] = {}
    unresolved: set[str] = set()
    for component_key, matches in matches_by_component.items():
        if len(matches) != 1:
            unresolved.add(component_key)
            continue
        line_key = matches[0]
        if line_key in component_for_line:
            # Two botanical entities claiming one merchandise row is ambiguous.
            unresolved.add(component_key)
            unresolved.add(component_for_line.pop(line_key))
            continue
        component_for_line[line_key] = component_key

    plant_lines: list[CanonicalPlantLineTruth] = []
    for line_key in merchandise_keys:
        canonical_fields: dict[str, CanonicalFieldTruth] = {}
        for engine_key in _MERCHANDISE_FIELDS:
            field_payload = field_payloads.get(engine_key)
            if field_payload is None:
                continue
            rows = tuple(row for row in evidence_by_field.get(engine_key, ()) if row.line_key == line_key)
            if not rows:
                continue
            canonical_fields[_ENGINE_TO_PPQ[engine_key]] = _field_truth(
                engine_key=engine_key,
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
                rows = tuple(
                    row
                    for row in evidence_by_field.get(engine_key, ())
                    if row.component_key == component_key
                )
                if not rows:
                    continue
                canonical_fields[_ENGINE_TO_PPQ[engine_key]] = _field_truth(
                    engine_key=engine_key,
                    target_key=_ENGINE_TO_PPQ[engine_key],
                    field_payload=field_payload,
                    rows=rows,
                    force_review=engine_key in low_authority_fields,
                )

        plant_lines.append(
            CanonicalPlantLineTruth(
                entity_key=line_key,
                ordinal_hint=_ordinal(line_key),
                taxon_key=component_key,
                fields=canonical_fields,
            )
        )

    # A component-only operation is still representable; a mixed operation with
    # ambiguous component joins is deliberately fail-closed instead of growing
    # invented duplicate PPQ lines.
    if not merchandise_keys:
        for ordinal, component_key in enumerate(component_keys, start=1):
            canonical_fields: dict[str, CanonicalFieldTruth] = {}
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
                canonical_fields[_ENGINE_TO_PPQ[engine_key]] = _field_truth(
                    engine_key=engine_key,
                    target_key=_ENGINE_TO_PPQ[engine_key],
                    field_payload=field_payload,
                    rows=rows,
                    force_review=engine_key in low_authority_fields,
                )
            plant_lines.append(
                CanonicalPlantLineTruth(
                    entity_key=component_key,
                    ordinal_hint=ordinal,
                    taxon_key=component_key,
                    fields=canonical_fields,
                )
            )

    shipment_fields: dict[str, CanonicalFieldTruth] = {}
    for engine_key, field_payload in field_payloads.items():
        if engine_key in _MERCHANDISE_FIELDS or engine_key in _COMPONENT_FIELDS:
            continue
        rows = evidence_by_field.get(engine_key, ())
        target = _ENGINE_TO_PPQ.get(engine_key, engine_key)
        shipment_fields[target] = _field_truth(
            engine_key=engine_key,
            target_key=target,
            field_payload=field_payload,
            rows=rows,
            force_review=engine_key in low_authority_fields,
        )

    return CanonicalShipmentTruth(
        shipment_fields=shipment_fields,
        plant_lines=tuple(plant_lines),
        unresolved_component_keys=tuple(sorted(unresolved)),
    )
