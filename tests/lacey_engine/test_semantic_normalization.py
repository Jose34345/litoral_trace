from __future__ import annotations

from litoral_trace.lacey_engine.multi_agent.semantic_normalization import semantic_value_key


def test_hts_formatting_collapses_without_changing_semantics() -> None:
    assert semantic_value_key("hts_code", "4407.11.0190") == "4407110190"
    assert semantic_value_key("hts_code", "4407110190") == "4407110190"


def test_money_formatting_collapses_to_one_numeric_identity() -> None:
    expected = "18300"
    assert semantic_value_key("entered_value", "$18,300.00") == expected
    assert semantic_value_key("entered_value", "USD 18,300.00") == expected
    assert semantic_value_key("entered_value", "18300") == expected


def test_metric_unit_variants_collapse_to_ppq_identity() -> None:
    assert semantic_value_key("metric_unit", "m³") == "m3"
    assert semantic_value_key("metric_unit", "M3") == "m3"
    assert semantic_value_key("metric_unit", "cubic meters") == "m3"


def test_brazil_multilingual_country_variants_are_equivalent() -> None:
    assert semantic_value_key("country_of_harvest", "Brazil") == "BR"
    assert semantic_value_key("country_of_harvest", "Brasil") == "BR"
    assert semantic_value_key("country_of_harvest", "BR") == "BR"


def test_species_binomial_collapses_to_epithet_only_with_known_genus_context() -> None:
    context = frozenset({"eucalyptus"})

    assert semantic_value_key("species", "grandis", genus_context=context) == "grandis"
    assert (
        semantic_value_key(
            "species",
            "Eucalyptus grandis",
            genus_context=context,
        )
        == "grandis"
    )


def test_species_binomial_is_not_collapsed_without_matching_genus_context() -> None:
    assert semantic_value_key("species", "grandis") == "grandis"
    assert semantic_value_key("species", "Eucalyptus grandis") == "eucalyptus grandis"
    assert (
        semantic_value_key(
            "species",
            "Eucalyptus grandis",
            genus_context=frozenset({"pinus"}),
        )
        == "eucalyptus grandis"
    )



def test_species_binomial_is_not_collapsed_when_genus_context_is_ambiguous() -> None:
    assert (
        semantic_value_key(
            "species",
            "Eucalyptus grandis",
            genus_context=frozenset({"eucalyptus", "pinus"}),
        )
        == "eucalyptus grandis"
    )
