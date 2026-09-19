from __future__ import annotations

from litoral_trace.us_lacey.regulatory.taxonomy import (
    TaxonomicRank,
    TaxonomyCatalogRecord,
    TaxonomyMatchKind,
    TaxonomyStatus,
    resolve_taxonomy,
)


def test_exact_accepted_scientific_name_resolves_without_publishing_authority():
    result = resolve_taxonomy("Hevea brasiliensis")

    assert result.status is TaxonomyStatus.RESOLVED
    assert result.review_required is False
    assert result.catalog_version == "2026-09-17-v1"
    assert result.query_normalized == "hevea brasiliensis"
    assert len(result.candidates) == 1
    candidate = result.candidates[0]
    assert candidate.scientific_name == "Hevea brasiliensis"
    assert candidate.rank is TaxonomicRank.SPECIES
    assert candidate.genus == "Hevea"
    assert candidate.species_epithet == "brasiliensis"
    assert candidate.match_kind is TaxonomyMatchKind.ACCEPTED_SCIENTIFIC_NAME
    assert candidate.authority_source == "USDA NAL Agricultural Thesaurus"
    assert candidate.authority_record_url == "https://lod.nal.usda.gov/nalt/45477"


def test_accepted_genus_name_remains_review_required_for_species_level_workflow():
    result = resolve_taxonomy("Hevea")

    assert result.status is TaxonomyStatus.REVIEW_REQUIRED
    assert result.review_required is True
    assert result.reason == "EXACT_ACCEPTED_GENUS_NAME"
    assert result.candidates[0].scientific_name == "Hevea"
    assert result.candidates[0].rank is TaxonomicRank.GENUS
    assert result.candidates[0].species_epithet is None


def test_synonym_and_usda_common_aliases_require_review():
    for name, match_kind in (
        ("Siphonia brasiliensis", TaxonomyMatchKind.SYNONYM),
        ("rubber tree", TaxonomyMatchKind.COMMON_ALIAS),
        ("rubbertree", TaxonomyMatchKind.COMMON_ALIAS),
        ("para rubber", TaxonomyMatchKind.COMMON_ALIAS),
    ):
        result = resolve_taxonomy(name)
        assert result.status is TaxonomyStatus.REVIEW_REQUIRED
        assert result.review_required is True
        assert result.candidates[0].scientific_name == "Hevea brasiliensis"
        assert result.candidates[0].rank is TaxonomicRank.SPECIES
        assert result.candidates[0].match_kind is match_kind


def test_commercial_rubberwood_alias_is_candidate_not_final_species_fact():
    result = resolve_taxonomy("  Rubber-wood  ")

    assert result.status is TaxonomyStatus.REVIEW_REQUIRED
    assert result.review_required is True
    assert result.candidates[0].scientific_name == "Hevea brasiliensis"
    assert result.candidates[0].match_kind is TaxonomyMatchKind.COMMERCIAL_ALIAS


def test_hevea_wood_stays_at_genus_level_and_never_invents_brasiliensis():
    result = resolve_taxonomy("Hevea wood")

    assert result.status is TaxonomyStatus.REVIEW_REQUIRED
    assert result.review_required is True
    assert len(result.candidates) == 1
    candidate = result.candidates[0]
    assert candidate.scientific_name == "Hevea"
    assert candidate.rank is TaxonomicRank.GENUS
    assert candidate.genus == "Hevea"
    assert candidate.species_epithet is None
    assert candidate.match_kind is TaxonomyMatchKind.GENUS_ALIAS
    assert all(item.species_epithet != "brasiliensis" for item in result.candidates)


def test_generic_genus_aliases_require_review_instead_of_species_guessing():
    oak = resolve_taxonomy("oak")
    pine = resolve_taxonomy("pine")

    assert oak.status is TaxonomyStatus.REVIEW_REQUIRED
    assert oak.candidates[0].scientific_name == "Quercus"
    assert oak.candidates[0].rank is TaxonomicRank.GENUS
    assert pine.status is TaxonomyStatus.REVIEW_REQUIRED
    assert pine.candidates[0].scientific_name == "Pinus"
    assert pine.candidates[0].rank is TaxonomicRank.GENUS


def test_unknown_and_near_match_fail_closed_without_fuzzy_guessing():
    for name in ("plywood", "wood", "rubberwod", ""):
        result = resolve_taxonomy(name)
        assert result.status is TaxonomyStatus.NO_MATCH
        assert result.review_required is True
        assert result.candidates == ()


def test_multiple_supported_exact_alias_candidates_remain_ambiguous():
    records = (
        TaxonomyCatalogRecord.species(
            record_id="test-a",
            scientific_name="Example alpha",
            genus="Example",
            species_epithet="alpha",
            authority_source="TEST_ONLY",
            authority_record_url="https://example.invalid/a",
            commercial_aliases=("trade mix",),
        ),
        TaxonomyCatalogRecord.species(
            record_id="test-b",
            scientific_name="Example beta",
            genus="Example",
            species_epithet="beta",
            authority_source="TEST_ONLY",
            authority_record_url="https://example.invalid/b",
            commercial_aliases=("trade mix",),
        ),
    )

    result = resolve_taxonomy("trade mix", records=records)

    assert result.status is TaxonomyStatus.AMBIGUOUS
    assert result.review_required is True
    assert [candidate.scientific_name for candidate in result.candidates] == [
        "Example alpha",
        "Example beta",
    ]
    assert all(candidate.match_kind is TaxonomyMatchKind.COMMERCIAL_ALIAS for candidate in result.candidates)
