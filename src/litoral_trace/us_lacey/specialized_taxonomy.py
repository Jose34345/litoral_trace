"""Non-canonical taxonomy enrichment for verified specialized Lacey candidates."""
from __future__ import annotations

from collections.abc import Iterable

from litoral_trace.lacey_engine.multi_agent.contracts import CandidateEnvelope
from litoral_trace.us_lacey.regulatory.taxonomy import (
    TaxonomyResolution,
    resolve_taxonomy,
)
from litoral_trace.us_lacey.regulatory.taxonomy.catalog import CATALOG_VERSION
from litoral_trace.us_lacey.regulatory.taxonomy.resolver import normalize_taxonomy_query


SPECIALIZED_TAXONOMY_VERSION = f"lacey_specialized_taxonomy_v1:{CATALOG_VERSION}"


def _serialize_resolution(
    resolution: TaxonomyResolution,
    *,
    query: str,
    query_source: str,
) -> dict[str, object]:
    return {
        "version": SPECIALIZED_TAXONOMY_VERSION,
        "query": query,
        "query_source": query_source,
        "status": resolution.status.value,
        "reason": resolution.reason,
        "review_required": resolution.review_required,
        "catalog_version": resolution.catalog_version,
        "candidates": [
            {
                "scientific_name": candidate.scientific_name,
                "rank": candidate.rank.value,
                "genus": candidate.genus,
                "species_epithet": candidate.species_epithet,
                "match_kind": candidate.match_kind.value,
                "confidence": str(candidate.confidence),
                "authority_source": candidate.authority_source,
                "authority_record_url": candidate.authority_record_url,
                "catalog_record_id": candidate.catalog_record_id,
            }
            for candidate in resolution.candidates
        ],
    }


def _context_required_payload(raw_species: str) -> dict[str, object]:
    return {
        "version": SPECIALIZED_TAXONOMY_VERSION,
        "query": raw_species,
        "query_source": "RAW_SPECIES_WITHOUT_UNIQUE_GENUS",
        "status": "REVIEW_REQUIRED",
        "reason": "MISSING_UNAMBIGUOUS_GENUS_CONTEXT",
        "review_required": True,
        "catalog_version": None,
        "candidates": [],
    }


def _same_line(candidate: CandidateEnvelope, other: CandidateEnvelope) -> bool:
    return (
        candidate.line_item_key is not None
        and candidate.line_item_key == other.line_item_key
    )


def _unique_line_genus(
    species: CandidateEnvelope,
    candidates: tuple[CandidateEnvelope, ...],
) -> str | None:
    by_normalized: dict[str, str] = {}
    for candidate in candidates:
        if candidate.candidate.field_key != "genus":
            continue
        if not _same_line(species, candidate):
            continue
        value = str(candidate.candidate.value or "").strip()
        normalized = normalize_taxonomy_query(value)
        if normalized:
            by_normalized.setdefault(normalized, value)

    if len(by_normalized) != 1:
        return None
    return next(iter(by_normalized.values()))


def _taxonomy_payload_for_candidate(
    candidate: CandidateEnvelope,
    *,
    candidates: tuple[CandidateEnvelope, ...],
) -> dict[str, object] | None:
    field_key = candidate.candidate.field_key
    raw_value = str(candidate.candidate.value or "").strip()

    if field_key == "genus":
        resolution = resolve_taxonomy(raw_value)
        return _serialize_resolution(
            resolution,
            query=raw_value,
            query_source="DIRECT",
        )

    if field_key != "species":
        return None

    normalized_species = normalize_taxonomy_query(raw_value)
    if len(normalized_species.split()) >= 2:
        resolution = resolve_taxonomy(raw_value)
        return _serialize_resolution(
            resolution,
            query=raw_value,
            query_source="DIRECT",
        )

    genus = _unique_line_genus(candidate, candidates)
    if genus is None:
        return _context_required_payload(raw_value)

    query = f"{genus} {raw_value}".strip()
    resolution = resolve_taxonomy(query)
    return _serialize_resolution(
        resolution,
        query=query,
        query_source="LINE_GENUS_CONTEXT",
    )


def taxonomy_enrichment_for_candidates(
    candidates: Iterable[CandidateEnvelope],
) -> dict[CandidateEnvelope, dict[str, object]]:
    """Return comparison/enrichment metadata without mutating source candidates."""
    packet = tuple(candidates)
    enriched: dict[CandidateEnvelope, dict[str, object]] = {}
    for candidate in packet:
        payload = _taxonomy_payload_for_candidate(candidate, candidates=packet)
        if payload is not None:
            enriched[candidate] = payload
    return enriched
