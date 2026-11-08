"""Immutable contracts for non-canonical U.S. Lacey taxonomy resolution."""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum


class TaxonomyStatus(StrEnum):
    RESOLVED = "RESOLVED"
    REVIEW_REQUIRED = "REVIEW_REQUIRED"
    AMBIGUOUS = "AMBIGUOUS"
    NO_MATCH = "NO_MATCH"


class TaxonomicRank(StrEnum):
    SPECIES = "SPECIES"
    GENUS = "GENUS"


class TaxonomyMatchKind(StrEnum):
    ACCEPTED_SCIENTIFIC_NAME = "ACCEPTED_SCIENTIFIC_NAME"
    SYNONYM = "SYNONYM"
    COMMON_ALIAS = "COMMON_ALIAS"
    COMMERCIAL_ALIAS = "COMMERCIAL_ALIAS"
    GENUS_ALIAS = "GENUS_ALIAS"


@dataclass(frozen=True, slots=True)
class TaxonomyCatalogRecord:
    """One curated taxonomy target plus exact aliases allowed to resolve to it."""

    record_id: str
    scientific_name: str
    rank: TaxonomicRank
    genus: str
    species_epithet: str | None
    authority_source: str
    authority_record_url: str
    synonyms: tuple[str, ...] = ()
    common_aliases: tuple[str, ...] = ()
    commercial_aliases: tuple[str, ...] = ()
    genus_aliases: tuple[str, ...] = ()

    @classmethod
    def species(
        cls,
        *,
        record_id: str,
        scientific_name: str,
        genus: str,
        species_epithet: str,
        authority_source: str,
        authority_record_url: str,
        synonyms: tuple[str, ...] = (),
        common_aliases: tuple[str, ...] = (),
        commercial_aliases: tuple[str, ...] = (),
    ) -> "TaxonomyCatalogRecord":
        return cls(
            record_id=record_id,
            scientific_name=scientific_name,
            rank=TaxonomicRank.SPECIES,
            genus=genus,
            species_epithet=species_epithet,
            authority_source=authority_source,
            authority_record_url=authority_record_url,
            synonyms=tuple(synonyms),
            common_aliases=tuple(common_aliases),
            commercial_aliases=tuple(commercial_aliases),
        )

    @classmethod
    def genus_record(
        cls,
        *,
        record_id: str,
        scientific_name: str,
        authority_source: str,
        authority_record_url: str,
        genus_aliases: tuple[str, ...] = (),
    ) -> "TaxonomyCatalogRecord":
        return cls(
            record_id=record_id,
            scientific_name=scientific_name,
            rank=TaxonomicRank.GENUS,
            genus=scientific_name,
            species_epithet=None,
            authority_source=authority_source,
            authority_record_url=authority_record_url,
            genus_aliases=tuple(genus_aliases),
        )


@dataclass(frozen=True, slots=True)
class TaxonomyCandidate:
    scientific_name: str
    rank: TaxonomicRank
    genus: str
    species_epithet: str | None
    match_kind: TaxonomyMatchKind
    confidence: Decimal
    authority_source: str
    authority_record_url: str
    catalog_record_id: str


@dataclass(frozen=True, slots=True)
class TaxonomyResolution:
    catalog_version: str
    query_normalized: str
    status: TaxonomyStatus
    reason: str
    review_required: bool
    candidates: tuple[TaxonomyCandidate, ...]
