"""Conservative entity matching and field acceptance policy for Assurance v1.

Automatic decisions are intentionally asymmetric: high-confidence, unique,
non-conflicting evidence may be accepted; medium confidence is routed to human
review; low confidence or contradictions never mutate operational records.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
import re
import unicodedata
from typing import Iterable, Sequence

from litoral_trace.assurance.extraction import ExtractedCandidate


HIGH_CONFIDENCE = 0.90
MEDIUM_CONFIDENCE = 0.65


class FieldDecisionStatus(StrEnum):
    AUTO_ACCEPTED = "AUTO_ACCEPTED"
    NEEDS_REVIEW = "NEEDS_REVIEW"
    LOW_CONFIDENCE = "LOW_CONFIDENCE"
    CONFLICT = "CONFLICT"


@dataclass(frozen=True, slots=True)
class FieldDecision:
    candidate: ExtractedCandidate
    status: FieldDecisionStatus
    reason: str


@dataclass(frozen=True, slots=True)
class EntityRecord:
    entity_type: str
    entity_reference: str
    identifiers: tuple[str, ...]
    display_name: str | None = None


@dataclass(frozen=True, slots=True)
class EntityMatch:
    entity_type: str
    entity_reference: str
    confidence: float
    method: str
    matched_value: str
    ambiguous: bool = False


def _fold(value: object) -> str:
    text = unicodedata.normalize("NFKD", str(value or "").strip().lower())
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return re.sub(r"[^a-z0-9]+", "", text)


def _group_conflicts(candidates: Sequence[ExtractedCandidate]) -> set[str]:
    by_field: dict[str, set[str]] = {}
    for candidate in candidates:
        normalized = str(candidate.normalized_value).strip()
        if not normalized:
            continue
        by_field.setdefault(candidate.field_name, set()).add(normalized.casefold())
    return {field_name for field_name, values in by_field.items() if len(values) > 1}


def decide_field_acceptance(
    candidates: Sequence[ExtractedCandidate],
) -> tuple[FieldDecision, ...]:
    """Apply one deterministic acceptance policy to extracted fields."""
    conflicts = _group_conflicts(candidates)
    decisions: list[FieldDecision] = []
    for candidate in candidates:
        if candidate.field_name in conflicts:
            decisions.append(
                FieldDecision(
                    candidate=candidate,
                    status=FieldDecisionStatus.CONFLICT,
                    reason="same_field_has_conflicting_values",
                )
            )
        elif candidate.confidence >= HIGH_CONFIDENCE:
            decisions.append(
                FieldDecision(
                    candidate=candidate,
                    status=FieldDecisionStatus.AUTO_ACCEPTED,
                    reason="high_confidence_without_conflict",
                )
            )
        elif candidate.confidence >= MEDIUM_CONFIDENCE:
            decisions.append(
                FieldDecision(
                    candidate=candidate,
                    status=FieldDecisionStatus.NEEDS_REVIEW,
                    reason="medium_confidence",
                )
            )
        else:
            decisions.append(
                FieldDecision(
                    candidate=candidate,
                    status=FieldDecisionStatus.LOW_CONFIDENCE,
                    reason="low_confidence",
                )
            )
    return tuple(decisions)


def match_entity(
    value: object,
    records: Iterable[EntityRecord],
    *,
    entity_type: str,
) -> EntityMatch | None:
    """Match only exact/normalized identifiers; never fuzzy-write by name."""
    raw = str(value or "").strip()
    folded = _fold(raw)
    if not folded:
        return None

    matches: list[tuple[EntityRecord, str, bool]] = []
    for record in records:
        if record.entity_type != entity_type:
            continue
        for identifier in record.identifiers:
            identifier_raw = str(identifier or "").strip()
            if not identifier_raw:
                continue
            if raw.casefold() == identifier_raw.casefold():
                matches.append((record, identifier_raw, True))
                break
            if folded == _fold(identifier_raw):
                matches.append((record, identifier_raw, False))
                break

    unique_refs = {record.entity_reference for record, _, _ in matches}
    if not matches:
        return None
    if len(unique_refs) != 1:
        return EntityMatch(
            entity_type=entity_type,
            entity_reference="",
            confidence=0.0,
            method="AMBIGUOUS",
            matched_value=raw,
            ambiguous=True,
        )

    record, matched_identifier, exact_raw = matches[0]
    return EntityMatch(
        entity_type=entity_type,
        entity_reference=record.entity_reference,
        confidence=1.0 if exact_raw else 0.95,
        method="EXACT_IDENTIFIER" if exact_raw else "NORMALIZED_IDENTIFIER",
        matched_value=matched_identifier,
        ambiguous=False,
    )


def match_candidate_entities(
    candidates: Sequence[ExtractedCandidate],
    records: Iterable[EntityRecord],
) -> tuple[EntityMatch, ...]:
    """Resolve supported business references from extracted fields."""
    records_tuple = tuple(records)
    field_to_entity = {
        "issuer_cuit": "SUPPLIER",
        "supplier": "SUPPLIER",
        "lot_id": "LOT",
        "shipment_code": "SHIPMENT",
        "order_id": "ORDER",
        "sale_reference": "ORDER",
    }
    results: dict[tuple[str, str], EntityMatch] = {}
    for candidate in candidates:
        entity_type = field_to_entity.get(candidate.field_name)
        if entity_type is None:
            continue
        match = match_entity(
            candidate.normalized_value,
            records_tuple,
            entity_type=entity_type,
        )
        if match is None:
            continue
        effective = EntityMatch(
            entity_type=match.entity_type,
            entity_reference=match.entity_reference,
            confidence=(
                0.0
                if match.ambiguous
                else min(match.confidence, candidate.confidence)
            ),
            method=match.method,
            matched_value=match.matched_value,
            ambiguous=match.ambiguous,
        )
        key = (effective.entity_type, effective.entity_reference or effective.matched_value)
        current = results.get(key)
        if current is None or effective.confidence > current.confidence:
            results[key] = effective
    return tuple(results.values())
