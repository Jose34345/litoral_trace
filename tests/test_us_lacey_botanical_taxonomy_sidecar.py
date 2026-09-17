from __future__ import annotations

from uuid import NAMESPACE_URL, uuid4, uuid5

from litoral_trace.lacey_engine.ai_shadow import AICandidate
from litoral_trace.lacey_engine.domain import EvidenceClass
from litoral_trace.lacey_engine.multi_agent.contracts import (
    CandidateEnvelope,
    DocumentType,
    SpecialistRole,
)
from litoral_trace.us_lacey.botanical_taxonomy import (
    BOTANICAL_TAXONOMY_SIDECAR_VERSION,
    build_botanical_taxonomy_sidecars,
)


def _candidate(
    field_key: str,
    value: str,
    *,
    line_item_key: str | None,
    seed: str,
    document_type: DocumentType = DocumentType.BOTANICAL_DECLARATION,
) -> CandidateEnvelope:
    return CandidateEnvelope(
        candidate=AICandidate(
            field_key=field_key,
            value=value,
            normalized_value=value,
            evidence_class=EvidenceClass.EXPLICIT,
            page=1,
            source_text=f"{field_key}: {value}",
            confidence=0.98,
            provider="fixture",
            model="fixture",
            evidence_verified=True,
        ),
        document_id=uuid5(NAMESPACE_URL, f"taxonomy-sidecar-{seed}"),
        document_type=document_type,
        specialist=(
            SpecialistRole.BOTANICAL
            if field_key in {"genus", "species"}
            else SpecialistRole.COMMERCIAL_LINES
        ),
        agent_run_id=uuid4(),
        line_item_key=line_item_key,
        source_span_id=None,
    )


def test_species_epithet_uses_unique_explicit_line_genus_for_taxonomy_lookup() -> None:
    line = "SKU:PT-38"
    genus = _candidate("genus", "Pinus", line_item_key=line, seed="genus")
    species = _candidate("species", "taeda", line_item_key=line, seed="species")

    sidecars = build_botanical_taxonomy_sidecars((genus, species))

    genus_payload = sidecars[genus]
    species_payload = sidecars[species]
    assert genus_payload["version"] == BOTANICAL_TAXONOMY_SIDECAR_VERSION
    assert genus_payload["query"] == "Pinus"
    assert genus_payload["query_basis"] == "DIRECT_EVIDENCE"
    assert genus_payload["status"] == "REVIEW_REQUIRED"

    assert species_payload["query"] == "Pinus taeda"
    assert species_payload["query_basis"] == "LINE_GENUS_PLUS_SPECIES"
    assert species_payload["status"] == "RESOLVED"
    assert species_payload["review_required"] is False
    assert species_payload["candidates"][0]["scientific_name"] == "Pinus taeda"
    assert species.candidate.value == "taeda"


def test_full_species_name_is_resolved_from_direct_evidence_without_rewrite() -> None:
    species = _candidate(
        "species",
        "Pinus taeda",
        line_item_key="SKU:PT-FULL",
        seed="full-species",
    )

    sidecar = build_botanical_taxonomy_sidecars((species,))[species]

    assert sidecar["query"] == "Pinus taeda"
    assert sidecar["query_basis"] == "DIRECT_EVIDENCE"
    assert sidecar["status"] == "RESOLVED"
    assert species.candidate.value == "Pinus taeda"


def test_ambiguous_line_genus_fails_closed_and_does_not_synthesize_species_query() -> None:
    line = "SKU:MIXED"
    eucalyptus = _candidate("genus", "Eucalyptus", line_item_key=line, seed="eucalyptus")
    pinus = _candidate(
        "genus",
        "Pinus",
        line_item_key=line,
        seed="pinus",
        document_type=DocumentType.SUPPLIER_ORIGIN,
    )
    species = _candidate(
        "species",
        "grandis",
        line_item_key=line,
        seed="grandis",
        document_type=DocumentType.SUPPLIER_ORIGIN,
    )

    sidecar = build_botanical_taxonomy_sidecars((eucalyptus, pinus, species))[species]

    assert sidecar["query"] == "grandis"
    assert sidecar["query_basis"] == "DIRECT_EVIDENCE"
    assert sidecar["status"] == "NO_MATCH"
    assert sidecar["review_required"] is True


def test_unbound_epithet_does_not_borrow_genus_from_another_line() -> None:
    genus = _candidate("genus", "Pinus", line_item_key="SKU:A", seed="genus-a")
    species = _candidate("species", "taeda", line_item_key=None, seed="species-unbound")

    sidecar = build_botanical_taxonomy_sidecars((genus, species))[species]

    assert sidecar["query"] == "taeda"
    assert sidecar["status"] == "NO_MATCH"


def test_non_botanical_candidates_receive_no_taxonomy_sidecar() -> None:
    hts = _candidate(
        "hts_code",
        "4407110190",
        line_item_key="SKU:PT-38",
        seed="hts",
        document_type=DocumentType.COMMERCIAL_INVOICE,
    )

    assert build_botanical_taxonomy_sidecars((hts,)) == {}
