from __future__ import annotations

from uuid import NAMESPACE_URL, uuid4, uuid5

from litoral_trace.lacey_engine.ai_shadow import AICandidate
from litoral_trace.lacey_engine.domain import EvidenceClass
from litoral_trace.lacey_engine.multi_agent.contracts import (
    CandidateEnvelope,
    DocumentType,
    SpecialistRole,
)
from litoral_trace.us_lacey.specialized_taxonomy import (
    SPECIALIZED_TAXONOMY_VERSION,
    taxonomy_enrichment_for_candidates,
)


def _candidate(
    field_key: str,
    value: str,
    *,
    line_item_key: str = "SKU:RUBBER-1",
    seed: str,
) -> CandidateEnvelope:
    candidate = AICandidate(
        field_key=field_key,
        value=value,
        normalized_value=value,
        evidence_class=EvidenceClass.EXPLICIT,
        page=1,
        source_text=f"{line_item_key} {field_key} {value}",
        confidence=0.99,
        provider="fixture",
        model="fixture",
        evidence_verified=True,
    )
    return CandidateEnvelope(
        candidate=candidate,
        document_id=uuid5(NAMESPACE_URL, f"taxonomy-{seed}"),
        document_type=DocumentType.BOTANICAL_DECLARATION,
        specialist=SpecialistRole.BOTANICAL,
        agent_run_id=uuid4(),
        line_item_key=line_item_key,
        source_span_id=None,
    )


def test_specialized_taxonomy_enriches_genus_and_contextual_species_without_mutating_values() -> None:
    genus = _candidate("genus", "Hevea", seed="genus")
    species = _candidate("species", "brasiliensis", seed="species")

    enrichment = taxonomy_enrichment_for_candidates((genus, species))

    genus_payload = enrichment[genus]
    assert genus_payload["version"] == SPECIALIZED_TAXONOMY_VERSION
    assert genus_payload["query"] == "Hevea"
    assert genus_payload["query_source"] == "DIRECT"
    assert genus_payload["status"] == "REVIEW_REQUIRED"
    assert genus_payload["review_required"] is True
    assert genus_payload["candidates"][0]["scientific_name"] == "Hevea"
    assert genus_payload["candidates"][0]["rank"] == "GENUS"

    species_payload = enrichment[species]
    assert species_payload["query"] == "Hevea brasiliensis"
    assert species_payload["query_source"] == "LINE_GENUS_CONTEXT"
    assert species_payload["status"] == "RESOLVED"
    assert species_payload["review_required"] is False
    assert species_payload["candidates"][0]["scientific_name"] == "Hevea brasiliensis"
    assert species_payload["candidates"][0]["rank"] == "SPECIES"

    assert genus.candidate.value == "Hevea"
    assert species.candidate.value == "brasiliensis"


def test_specialized_taxonomy_species_without_genus_context_remains_review_required() -> None:
    species = _candidate("species", "brasiliensis", seed="species-alone")

    payload = taxonomy_enrichment_for_candidates((species,))[species]

    assert payload == {
        "version": SPECIALIZED_TAXONOMY_VERSION,
        "query": "brasiliensis",
        "query_source": "RAW_SPECIES_WITHOUT_UNIQUE_GENUS",
        "status": "REVIEW_REQUIRED",
        "reason": "MISSING_UNAMBIGUOUS_GENUS_CONTEXT",
        "review_required": True,
        "catalog_version": None,
        "candidates": [],
    }


def test_specialized_taxonomy_species_with_ambiguous_genus_context_fails_closed() -> None:
    line_key = "SKU:AMBIGUOUS-RUBBER"
    hevea = _candidate("genus", "Hevea", line_item_key=line_key, seed="hevea")
    quercus = _candidate("genus", "Quercus", line_item_key=line_key, seed="quercus")
    species = _candidate("species", "brasiliensis", line_item_key=line_key, seed="species-ambiguous")

    payload = taxonomy_enrichment_for_candidates((hevea, quercus, species))[species]

    assert payload["status"] == "REVIEW_REQUIRED"
    assert payload["reason"] == "MISSING_UNAMBIGUOUS_GENUS_CONTEXT"
    assert payload["query_source"] == "RAW_SPECIES_WITHOUT_UNIQUE_GENUS"
    assert payload["candidates"] == []


def test_specialized_taxonomy_ignores_non_botanical_candidates() -> None:
    hts = _candidate("hts_code", "4407110190", seed="hts")

    enrichment = taxonomy_enrichment_for_candidates((hts,))

    assert enrichment == {}
