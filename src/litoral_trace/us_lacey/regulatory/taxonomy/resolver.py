"""Exact-match, fail-closed taxonomy resolver for U.S. Lacey evidence."""
from __future__ import annotations

from decimal import Decimal
import re
import unicodedata
from collections.abc import Iterable

from litoral_trace.us_lacey.regulatory.taxonomy.catalog import CATALOG_V1, CATALOG_VERSION
from litoral_trace.us_lacey.regulatory.taxonomy.domain import (
    TaxonomyCandidate,
    TaxonomyCatalogRecord,
    TaxonomyMatchKind,
    TaxonomyResolution,
    TaxonomyStatus,
)


_CONFIDENCE_BY_MATCH_KIND = {
    TaxonomyMatchKind.ACCEPTED_SCIENTIFIC_NAME: Decimal("1.00"),
    TaxonomyMatchKind.SYNONYM: Decimal("0.95"),
    TaxonomyMatchKind.COMMON_ALIAS: Decimal("0.90"),
    TaxonomyMatchKind.COMMERCIAL_ALIAS: Decimal("0.85"),
    TaxonomyMatchKind.GENUS_ALIAS: Decimal("0.80"),
}


def normalize_taxonomy_query(value: str) -> str:
    """Normalize only for exact catalog lookup; never fuzzy-match or mutate source text."""
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(char for char in text if not unicodedata.combining(char))
    text = text.casefold().replace("_", " ")
    text = re.sub(r"[^\w]+", " ", text, flags=re.UNICODE)
    return " ".join(text.split())


def _candidate(record: TaxonomyCatalogRecord, match_kind: TaxonomyMatchKind) -> TaxonomyCandidate:
    return TaxonomyCandidate(
        scientific_name=record.scientific_name,
        rank=record.rank,
        genus=record.genus,
        species_epithet=record.species_epithet,
        match_kind=match_kind,
        confidence=_CONFIDENCE_BY_MATCH_KIND[match_kind],
        authority_source=record.authority_source,
        authority_record_url=record.authority_record_url,
        catalog_record_id=record.record_id,
    )


def _record_match_kind(record: TaxonomyCatalogRecord, query: str) -> TaxonomyMatchKind | None:
    if query == normalize_taxonomy_query(record.scientific_name):
        return TaxonomyMatchKind.ACCEPTED_SCIENTIFIC_NAME
    for alias in record.synonyms:
        if query == normalize_taxonomy_query(alias):
            return TaxonomyMatchKind.SYNONYM
    for alias in record.common_aliases:
        if query == normalize_taxonomy_query(alias):
            return TaxonomyMatchKind.COMMON_ALIAS
    for alias in record.commercial_aliases:
        if query == normalize_taxonomy_query(alias):
            return TaxonomyMatchKind.COMMERCIAL_ALIAS
    for alias in record.genus_aliases:
        if query == normalize_taxonomy_query(alias):
            return TaxonomyMatchKind.GENUS_ALIAS
    return None


def resolve_taxonomy(
    name: str,
    *,
    records: Iterable[TaxonomyCatalogRecord] | None = None,
) -> TaxonomyResolution:
    """Return supported exact candidates without manufacturing taxonomic certainty."""
    query = normalize_taxonomy_query(name)
    catalog = tuple(CATALOG_V1 if records is None else records)
    matches: list[TaxonomyCandidate] = []

    if query:
        for record in catalog:
            match_kind = _record_match_kind(record, query)
            if match_kind is not None:
                matches.append(_candidate(record, match_kind))

    ordered = tuple(
        sorted(
            matches,
            key=lambda item: (
                item.scientific_name.casefold(),
                item.rank.value,
                item.catalog_record_id,
            ),
        )
    )

    if not ordered:
        return TaxonomyResolution(
            catalog_version=CATALOG_VERSION,
            query_normalized=query,
            status=TaxonomyStatus.NO_MATCH,
            reason="NO_SUPPORTED_EXACT_MATCH",
            review_required=True,
            candidates=(),
        )

    if len(ordered) > 1:
        return TaxonomyResolution(
            catalog_version=CATALOG_VERSION,
            query_normalized=query,
            status=TaxonomyStatus.AMBIGUOUS,
            reason="MULTIPLE_SUPPORTED_EXACT_CANDIDATES",
            review_required=True,
            candidates=ordered,
        )

    candidate = ordered[0]
    if candidate.match_kind is TaxonomyMatchKind.ACCEPTED_SCIENTIFIC_NAME:
        return TaxonomyResolution(
            catalog_version=CATALOG_VERSION,
            query_normalized=query,
            status=TaxonomyStatus.RESOLVED,
            reason="EXACT_ACCEPTED_SCIENTIFIC_NAME",
            review_required=False,
            candidates=ordered,
        )

    return TaxonomyResolution(
        catalog_version=CATALOG_VERSION,
        query_normalized=query,
        status=TaxonomyStatus.REVIEW_REQUIRED,
        reason=f"EXACT_{candidate.match_kind.value}",
        review_required=True,
        candidates=ordered,
    )
