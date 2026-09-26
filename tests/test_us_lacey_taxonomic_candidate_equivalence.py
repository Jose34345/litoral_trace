from types import SimpleNamespace

from litoral_trace.us_lacey.candidate_normalization import (
    TaxonomicComparisonContext,
    derive_taxonomic_comparison_context,
    group_candidate_evidence,
)


def _candidate(
    candidate_id: int,
    value: str,
    *,
    confidence: float = 0.99,
    page: int = 1,
    document_id: int = 100,
):
    return SimpleNamespace(
        id=candidate_id,
        original_value=value,
        normalized_value=value,
        confidence=confidence,
        source_page=page,
        source_assurance_document_id=document_id,
        source_locator=f"page:{page}",
        evidence_span=f"raw:{value}",
    )


def test_species_equivalence_collapses_epithet_and_binomial_with_unique_genus_context():
    genus_candidates = (_candidate(1, "Eucalyptus", confidence=0.99),)
    context = derive_taxonomic_comparison_context(genus_candidates)

    short = _candidate(10, "grandis", confidence=0.90, page=1, document_id=101)
    full = _candidate(
        11,
        "Eucalyptus grandis",
        confidence=0.75,
        page=2,
        document_id=102,
    )

    groups = group_candidate_evidence(
        "species",
        (short, full),
        comparison_context=context,
    )

    assert context == TaxonomicComparisonContext(genus="Eucalyptus")
    assert len(groups) == 1
    assert groups[0].canonical_value == "taxon:eucalyptus:grandis"
    assert groups[0].representative is short
    assert groups[0].evidence == (short, full)

    # Comparison identity must never rewrite source evidence.
    assert short.original_value == "grandis"
    assert short.normalized_value == "grandis"
    assert short.evidence_span == "raw:grandis"
    assert full.original_value == "Eucalyptus grandis"
    assert full.normalized_value == "Eucalyptus grandis"
    assert full.evidence_span == "raw:Eucalyptus grandis"


def test_species_equivalence_keeps_true_conflict_with_unique_genus_context():
    context = derive_taxonomic_comparison_context(
        (_candidate(1, "Pinus", confidence=0.99),)
    )
    taeda = _candidate(10, "taeda", confidence=0.95)
    grandis = _candidate(11, "grandis", confidence=0.95)

    groups = group_candidate_evidence(
        "species",
        (taeda, grandis),
        comparison_context=context,
    )

    assert context == TaxonomicComparisonContext(genus="Pinus")
    assert {group.canonical_value for group in groups} == {
        "taxon:pinus:taeda",
        "taxon:pinus:grandis",
    }
    assert len(groups) == 2


def test_species_equivalence_fails_closed_without_unique_genus_context():
    short = _candidate(10, "grandis", confidence=0.95)
    full = _candidate(11, "Eucalyptus grandis", confidence=0.95)

    no_context = derive_taxonomic_comparison_context(())
    ambiguous_context = derive_taxonomic_comparison_context(
        (
            _candidate(1, "Eucalyptus", confidence=0.99),
            _candidate(2, "Abies", confidence=0.99),
        )
    )

    groups_without_context = group_candidate_evidence(
        "species",
        (short, full),
        comparison_context=no_context,
    )
    groups_with_ambiguous_context = group_candidate_evidence(
        "species",
        (short, full),
        comparison_context=ambiguous_context,
    )

    assert no_context is None
    assert ambiguous_context is None
    assert len(groups_without_context) == 2
    assert len(groups_with_ambiguous_context) == 2
    assert {group.canonical_value for group in groups_without_context} == {
        "grandis",
        "eucalyptus grandis",
    }


def test_low_confidence_genus_does_not_create_taxonomic_context():
    context = derive_taxonomic_comparison_context(
        (_candidate(1, "Eucalyptus", confidence=0.89),)
    )

    assert context is None
