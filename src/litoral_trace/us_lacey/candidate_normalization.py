"""Canonical grouping for U.S. Lacey review evidence.

Evidence identity is the actual field value, not the page, extractor or confidence
that produced it. This module is intentionally pure so projection, bulk review and
presentation code can share exactly the same grouping rule without changing the
human-confirmation authority boundary.
"""
from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Generic, Iterable, TypeVar

from litoral_trace.us_lacey.ppq505 import canonical_ppq_value_key


T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class CandidateEvidenceGroup(Generic[T]):
    canonical_value: str
    representative: T
    evidence: tuple[T, ...]
    confidence: float
    source_pages: tuple[int, ...]
    source_document_ids: tuple[int, ...]


def _candidate_value(candidate: object) -> object:
    normalized = getattr(candidate, "normalized_value", None)
    if normalized is not None and str(normalized).strip():
        return normalized
    return getattr(candidate, "original_value", "")


def _confidence(candidate: object) -> float:
    try:
        return max(0.0, min(1.0, float(getattr(candidate, "confidence", 0.0) or 0.0)))
    except (TypeError, ValueError):
        return 0.0


def _candidate_id(candidate: object) -> int:
    try:
        return int(getattr(candidate, "id", 0) or 0)
    except (TypeError, ValueError):
        return 0


def _source_page(candidate: object) -> int | None:
    try:
        value = getattr(candidate, "source_page", None)
        return None if value in (None, "") else int(value)
    except (TypeError, ValueError):
        return None


def _source_document_id(candidate: object) -> int | None:
    raw = getattr(candidate, "source_assurance_document_id", None)
    if raw is None:
        raw = getattr(candidate, "source_document_id", None)
    try:
        return None if raw in (None, "") else int(raw)
    except (TypeError, ValueError):
        return None


def merchandise_description_candidate_role(value: object) -> str | None:
    """Reject structural/material evidence from the commercial-description pool.

    These rows remain stored as auditable source evidence. They are only excluded
    from the customer-selectable ``Description of Merchandise`` candidate set.
    The rules are deliberately deterministic and narrow: component/base-material,
    accessory/adhesive, and packaging descriptions are not descriptions of the
    merchandise as sold.
    """
    raw = " ".join(str(value or "").split()).strip()
    folded = raw.casefold()
    if not folded:
        return None

    if re.search(r"\bplant material\b", folded):
        return "PLANT_COMPONENT_DESCRIPTION"
    if re.search(
        r"\b(?:metal )?fasteners?\b|\bprotective pads?\b|\badhesives?\b",
        folded,
    ):
        return "PLANT_COMPONENT_DESCRIPTION"
    if re.search(
        r"\b(?:corrugated\s+)?cartons?\b|\bpallets?\b|\bpacking\b|\bpackaging\b",
        folded,
    ):
        return "PACKAGING_DESCRIPTION"
    if re.search(r"\binserts?\b", folded) and re.search(
        r"\b(?:cartons?|pallets?|packing|packaging)\b", folded
    ):
        return "PACKAGING_DESCRIPTION"
    return None


def group_candidate_evidence(
    field_name: str,
    candidates: Iterable[T],
) -> tuple[CandidateEvidenceGroup[T], ...]:
    """Collapse same-value evidence while retaining every provenance row.

    The group key is scoped by the caller's field. In the database each operation
    field already belongs to one shipment/plant line, so this function never merges
    evidence across declaration lines. The highest-confidence row is the display
    representative; ties resolve deterministically to the lowest candidate id.

    Structural evidence may remain persisted for audit while being excluded from
    the customer-selectable commercial-description pool.
    """
    grouped: dict[str, list[T]] = {}
    order: list[str] = []
    normalized_field = str(field_name or "").strip().casefold()
    for candidate in candidates:
        value = _candidate_value(candidate)
        if (
            normalized_field == "merchandise_description"
            and merchandise_description_candidate_role(value) is not None
        ):
            continue
        key = canonical_ppq_value_key(field_name, value)
        if not key:
            continue
        if key not in grouped:
            grouped[key] = []
            order.append(key)
        grouped[key].append(candidate)

    result: list[CandidateEvidenceGroup[T]] = []
    for key in order:
        rows = tuple(grouped[key])
        representative = max(rows, key=lambda row: (_confidence(row), -_candidate_id(row)))
        pages = tuple(sorted({page for row in rows if (page := _source_page(row)) is not None}))
        documents = tuple(
            sorted(
                {
                    document_id
                    for row in rows
                    if (document_id := _source_document_id(row)) is not None
                }
            )
        )
        result.append(
            CandidateEvidenceGroup(
                canonical_value=key,
                representative=representative,
                evidence=rows,
                confidence=max((_confidence(row) for row in rows), default=0.0),
                source_pages=pages,
                source_document_ids=documents,
            )
        )
    return tuple(result)
