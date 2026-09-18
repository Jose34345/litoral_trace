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

from litoral_trace.lacey_engine.multi_agent.semantic_normalization import semantic_value_key
from litoral_trace.us_lacey.ppq505 import canonical_ppq_value_key
from litoral_trace.us_lacey.regulatory.taxonomy.resolver import normalize_taxonomy_query


T = TypeVar("T")

TAXONOMIC_CONTEXT_MIN_CONFIDENCE = 0.90


@dataclass(frozen=True, slots=True)
class TaxonomicComparisonContext:
    """Comparison-only context for one declaration line.

    The genus is never written back to source candidates. It exists solely so
    an epithet-only species observation can be compared with an explicit
    binomial from another source without manufacturing a new evidence value.
    """

    genus: str


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

def derive_taxonomic_comparison_context(
    genus_candidates: Iterable[object],
    *,
    confirmed_genus: object | None = None,
    minimum_confidence: float = TAXONOMIC_CONTEXT_MIN_CONFIDENCE,
) -> TaxonomicComparisonContext | None:
    """Return one fail-closed genus context for a single plant line.

    A human-confirmed genus is sufficient authority. Otherwise only candidate
    observations meeting the confidence threshold participate, and they must
    collapse to exactly one genus identity. Missing, malformed or competing
    high-confidence genera deliberately return None.
    """
    if confirmed_genus is not None and str(confirmed_genus).strip():
        normalized = normalize_taxonomy_query(str(confirmed_genus))
        if len(normalized.split()) == 1:
            return TaxonomicComparisonContext(genus=str(confirmed_genus).strip())
        return None

    by_genus: dict[str, str] = {}
    for candidate in genus_candidates:
        if _confidence(candidate) < float(minimum_confidence):
            continue
        value = str(_candidate_value(candidate) or "").strip()
        normalized = normalize_taxonomy_query(value)
        if len(normalized.split()) != 1:
            continue
        by_genus.setdefault(normalized, value)

    if len(by_genus) != 1:
        return None
    return TaxonomicComparisonContext(genus=next(iter(by_genus.values())))


def candidate_comparison_key(
    field_name: str,
    value: object,
    *,
    comparison_context: TaxonomicComparisonContext | None = None,
) -> str:
    """Return a comparison-only key without mutating caller-owned evidence."""
    normalized_field = str(field_name or "").strip().casefold()
    if normalized_field == "species" and comparison_context is not None:
        genus = normalize_taxonomy_query(comparison_context.genus)
        if len(genus.split()) == 1:
            contextual = semantic_value_key(
                "species",
                value,
                genus_context=frozenset({genus}),
            )
            if contextual.startswith("taxon:"):
                return contextual
    return canonical_ppq_value_key(field_name, value)


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
    *,
    comparison_context: TaxonomicComparisonContext | None = None,
) -> tuple[CandidateEvidenceGroup[T], ...]:
    """Collapse same-value evidence while retaining every provenance row.

    The group key is scoped by the caller's field. In the database each operation
    field already belongs to one shipment/plant line, so this function never merges
    evidence across declaration lines. Species may receive an explicit comparison-only
    genus context from that same line; raw candidate values and provenance remain
    untouched. The highest-confidence row is the display
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
        key = candidate_comparison_key(
            field_name,
            value,
            comparison_context=comparison_context,
        )
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
