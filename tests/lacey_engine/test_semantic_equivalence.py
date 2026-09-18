from __future__ import annotations

from uuid import NAMESPACE_URL, uuid4, uuid5

from litoral_trace.lacey_engine.ai_shadow import AICandidate
from litoral_trace.lacey_engine.domain import EvidenceClass
from litoral_trace.lacey_engine.multi_agent.contracts import (
    CandidateEnvelope,
    DocumentType,
    SpecialistRole,
)
from litoral_trace.lacey_engine.multi_agent.semantic_equivalence import (
    semantic_candidate_value,
    semantic_value_key,
)


def _candidate(
    field_key: str,
    value: str,
    *,
    seed: str,
    line_item_key: str | None = "SKU:WOOD-1",
    document_type: DocumentType = DocumentType.BOTANICAL_DECLARATION,
    specialist: SpecialistRole = SpecialistRole.BOTANICAL,
) -> CandidateEnvelope:
    return CandidateEnvelope(
        candidate=AICandidate(
            field_key=field_key,
            value=value,
            normalized_value=value,
            evidence_class=EvidenceClass.EXPLICIT,
            page=1,
            source_text=f"{field_key}: {value}",
            confidence=0.9,
            provider="fixture",
            model="fixture",
            evidence_verified=True,
        ),
        document_id=uuid5(NAMESPACE_URL, f"semantic-{seed}"),
        document_type=document_type,
        specialist=specialist,
        agent_run_id=uuid4(),
        line_item_key=line_item_key,
        source_span_id=None,
    )


def test_semantic_value_key_normalizes_hts_without_mutating_source_format() -> None:
    assert semantic_value_key("hts_code", "4407.11.0190") == "4407110190"
    assert semantic_value_key("hts_code", "4407 11 0190") == "4407110190"


def test_semantic_value_key_normalizes_metric_unit_variants() -> None:
    assert semantic_value_key("metric_unit", "m³") == "m3"
    assert semantic_value_key("metric_unit", "M3") == "m3"
    assert semantic_value_key("metric_unit", "cubic metres") == "m3"


def test_semantic_value_key_normalizes_country_aliases_to_iso_alpha2() -> None:
    assert semantic_value_key("country_of_harvest", "Brazil") == "BR"
    assert semantic_value_key("country_of_harvest", "Brasil") == "BR"
    assert semantic_value_key("country_of_harvest", "BR") == "BR"


def test_semantic_value_key_normalizes_money_representations_for_comparison() -> None:
    assert semantic_value_key("entered_value", "$18,300.00") == "18300"
    assert semantic_value_key("entered_value", "18300.0") == "18300"
    assert semantic_value_key("entered_value", "18,300") == "18300"


def test_species_epithet_matches_binomial_when_unique_genus_is_known() -> None:
    genus = _candidate("genus", "Eucalyptus", seed="genus")
    epithet = _candidate("species", "grandis", seed="epithet")
    binomial = _candidate("species", "Eucalyptus grandis", seed="binomial")
    packet = (genus, epithet, binomial)

    assert semantic_candidate_value(epithet, packet) == "eucalyptus grandis"
    assert semantic_candidate_value(binomial, packet) == "eucalyptus grandis"


def test_species_epithet_does_not_inherit_ambiguous_genus() -> None:
    eucalyptus = _candidate("genus", "Eucalyptus", seed="eucalyptus")
    pinus = _candidate("genus", "Pinus", seed="pinus")
    epithet = _candidate("species", "grandis", seed="epithet")
    packet = (eucalyptus, pinus, epithet)

    assert semantic_candidate_value(epithet, packet) == "grandis"


def test_full_binomial_keeps_its_own_genus_when_context_disagrees() -> None:
    pinus = _candidate("genus", "Pinus", seed="pinus")
    binomial = _candidate("species", "Eucalyptus grandis", seed="binomial")

    assert semantic_candidate_value(binomial, (pinus, binomial)) == "eucalyptus grandis"
