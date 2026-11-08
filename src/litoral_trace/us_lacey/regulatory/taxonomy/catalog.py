"""Small, versioned seed catalog for deterministic U.S. Lacey taxonomy support.

This is intentionally not a comprehensive botanical database. Every alias is
explicitly curated and exact-match only; unknown names must remain unresolved.
"""
from __future__ import annotations

from litoral_trace.us_lacey.regulatory.taxonomy.domain import TaxonomyCatalogRecord


CATALOG_VERSION = "2026-09-17-v1"
_USDA_NAL = "USDA NAL Agricultural Thesaurus"


CATALOG_V1: tuple[TaxonomyCatalogRecord, ...] = (
    TaxonomyCatalogRecord.species(
        record_id="usda-nalt-45477",
        scientific_name="Hevea brasiliensis",
        genus="Hevea",
        species_epithet="brasiliensis",
        authority_source=_USDA_NAL,
        authority_record_url="https://lod.nal.usda.gov/nalt/45477",
        synonyms=("Siphonia brasiliensis",),
        common_aliases=("rubber tree", "rubbertree", "para rubber"),
        # USDA Forest Products Laboratory identifies rubberwood as
        # Hevea brasiliensis. This remains a review-required commercial alias.
        commercial_aliases=("rubberwood", "rubber wood"),
    ),
    TaxonomyCatalogRecord.genus_record(
        record_id="usda-nalt-38577",
        scientific_name="Hevea",
        authority_source=_USDA_NAL,
        authority_record_url="https://lod.nal.usda.gov/nalt/38577",
        genus_aliases=("hevea wood",),
    ),
    TaxonomyCatalogRecord.genus_record(
        record_id="curated-genus-quercus-v1",
        scientific_name="Quercus",
        authority_source=_USDA_NAL,
        authority_record_url="https://lod.nal.usda.gov/nalt/en/",
        genus_aliases=("oak",),
    ),
    TaxonomyCatalogRecord.genus_record(
        record_id="curated-genus-pinus-v1",
        scientific_name="Pinus",
        authority_source=_USDA_NAL,
        authority_record_url="https://lod.nal.usda.gov/nalt/en/",
        genus_aliases=("pine",),
    ),
)
