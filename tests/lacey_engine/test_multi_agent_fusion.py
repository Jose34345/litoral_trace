from __future__ import annotations

from dataclasses import replace
from uuid import NAMESPACE_URL, uuid4, uuid5

from litoral_trace.lacey_engine.ai_shadow import AICandidate
from litoral_trace.lacey_engine.domain import EvidenceClass
from litoral_trace.lacey_engine.multi_agent.authority import (
    authority_tuple,
    document_authority,
)
from litoral_trace.lacey_engine.multi_agent.contracts import (
    CandidateEnvelope,
    DocumentType,
    SpecialistRole,
)
from litoral_trace.lacey_engine.multi_agent.fusion import (
    FusionKey,
    cross_document_fusion_accuracy,
    fuse_candidates,
)


def _envelope(
    field_key: str,
    value: str,
    *,
    document_type: DocumentType,
    specialist: SpecialistRole,
    verified: bool = True,
    confidence: float = 0.5,
    line_item_key: str | None = None,
    document_seed: str | None = None,
) -> CandidateEnvelope:
    candidate = AICandidate(
        field_key=field_key,
        value=value,
        normalized_value=value,
        evidence_class=EvidenceClass.EXPLICIT,
        page=1,
        source_text=f"ROW {line_item_key or '-'} {field_key} {value} supporting evidence",
        confidence=confidence,
        provider="fixture",
        model="fixture",
        evidence_verified=verified,
    )
    return CandidateEnvelope(
        candidate=candidate,
        document_id=uuid5(NAMESPACE_URL, document_seed or f"{document_type.value}:{value}"),
        document_type=document_type,
        specialist=specialist,
        agent_run_id=uuid4(),
        line_item_key=line_item_key,
        source_span_id=None,
    )


def test_authority_table_encodes_required_precedence():
    assert document_authority("hts_code", DocumentType.ENTRY_WORKSHEET) > document_authority(
        "hts_code", DocumentType.COMMERCIAL_INVOICE
    )
    assert document_authority("entered_value", DocumentType.ENTRY_WORKSHEET) == document_authority(
        "entered_value", DocumentType.COMMERCIAL_INVOICE
    )
    assert document_authority("genus", DocumentType.BOTANICAL_DECLARATION) > document_authority(
        "genus", DocumentType.SUPPLIER_ORIGIN
    )
    assert document_authority("country_of_harvest", DocumentType.BOTANICAL_DECLARATION) == document_authority(
        "country_of_harvest", DocumentType.SUPPLIER_ORIGIN
    )
    assert document_authority("genus", DocumentType.PACKING_LIST) == 0


def test_verified_evidence_beats_higher_document_authority():
    verified_invoice = _envelope(
        "hts_code",
        "4419.90.9000",
        document_type=DocumentType.COMMERCIAL_INVOICE,
        specialist=SpecialistRole.COMMERCIAL_LINES,
        verified=True,
        line_item_key="SKU:ACT-TRAY-18",
    )
    unverified_entry = _envelope(
        "hts_code",
        "4421.99.9880",
        document_type=DocumentType.ENTRY_WORKSHEET,
        specialist=SpecialistRole.COMMERCIAL_LINES,
        verified=False,
        confidence=0.99,
        line_item_key="SKU:ACT-TRAY-18",
    )

    result = fuse_candidates((unverified_entry, verified_invoice))
    assert result.fused_candidates == (verified_invoice,)


def test_document_authority_beats_model_confidence_when_verification_is_equal():
    invoice = _envelope(
        "hts_code",
        "4421.99.9880",
        document_type=DocumentType.COMMERCIAL_INVOICE,
        specialist=SpecialistRole.COMMERCIAL_LINES,
        confidence=0.99,
        line_item_key="SKU:ACT-TRAY-18",
    )
    entry = _envelope(
        "hts_code",
        "4419.90.9000",
        document_type=DocumentType.ENTRY_WORKSHEET,
        specialist=SpecialistRole.COMMERCIAL_LINES,
        confidence=0.51,
        line_item_key="SKU:ACT-TRAY-18",
    )

    result = fuse_candidates((invoice, entry))
    assert result.fused_candidates == (entry,)
    assert result.conflicts[0].requires_ai_resolution is False


def test_correct_specialist_precedes_corroboration_and_confidence():
    correct = _envelope(
        "entered_value",
        "18900.00",
        document_type=DocumentType.COMMERCIAL_INVOICE,
        specialist=SpecialistRole.COMMERCIAL_LINES,
        confidence=0.5,
        line_item_key="SKU:ACT-TRAY-18",
        document_seed="correct",
    )
    wrong = _envelope(
        "entered_value",
        "99999.00",
        document_type=DocumentType.COMMERCIAL_INVOICE,
        specialist=SpecialistRole.BOTANICAL,
        confidence=0.99,
        line_item_key="SKU:ACT-TRAY-18",
        document_seed="wrong",
    )

    result = fuse_candidates((wrong, correct))
    assert result.fused_candidates == (correct,)


def test_cross_document_corroboration_precedes_confidence():
    a1 = _envelope(
        "country_of_harvest",
        "Vietnam",
        document_type=DocumentType.BOTANICAL_DECLARATION,
        specialist=SpecialistRole.BOTANICAL,
        confidence=0.6,
        line_item_key="SKU:ACT-TRAY-18",
        document_seed="a1",
    )
    a2 = _envelope(
        "country_of_harvest",
        "Vietnam",
        document_type=DocumentType.SUPPLIER_ORIGIN,
        specialist=SpecialistRole.BOTANICAL,
        confidence=0.6,
        line_item_key="SKU:ACT-TRAY-18",
        document_seed="a2",
    )
    b = _envelope(
        "country_of_harvest",
        "Indonesia",
        document_type=DocumentType.SUPPLIER_ORIGIN,
        specialist=SpecialistRole.BOTANICAL,
        confidence=0.99,
        line_item_key="SKU:ACT-TRAY-18",
        document_seed="b",
    )

    result = fuse_candidates((b, a1, a2))
    assert result.fused_candidates[0].candidate.value == "Vietnam"


def test_model_confidence_is_only_final_authority_dimension():
    low = _envelope(
        "entered_value",
        "18900.00",
        document_type=DocumentType.COMMERCIAL_INVOICE,
        specialist=SpecialistRole.COMMERCIAL_LINES,
        confidence=0.6,
        line_item_key="SKU:ACT-TRAY-18",
        document_seed="low",
    )
    high = _envelope(
        "entered_value",
        "17760.00",
        document_type=DocumentType.COMMERCIAL_INVOICE,
        specialist=SpecialistRole.COMMERCIAL_LINES,
        confidence=0.9,
        line_item_key="SKU:ACT-TRAY-18",
        document_seed="high",
    )

    assert authority_tuple(low, corroborating_documents=1)[:-1] == authority_tuple(
        high, corroborating_documents=1
    )[:-1]
    result = fuse_candidates((low, high))
    assert result.fused_candidates == (high,)
    assert result.conflicts[0].requires_ai_resolution is True


def test_equal_authority_real_conflict_is_flagged_for_phase_five():
    invoice = _envelope(
        "entered_value",
        "18900.00",
        document_type=DocumentType.COMMERCIAL_INVOICE,
        specialist=SpecialistRole.COMMERCIAL_LINES,
        confidence=0.8,
        line_item_key="SKU:ACT-TRAY-18",
    )
    entry = _envelope(
        "entered_value",
        "19000.00",
        document_type=DocumentType.ENTRY_WORKSHEET,
        specialist=SpecialistRole.COMMERCIAL_LINES,
        confidence=0.7,
        line_item_key="SKU:ACT-TRAY-18",
    )

    result = fuse_candidates((invoice, entry))
    assert len(result.conflicts) == 1
    assert result.conflicts[0].requires_ai_resolution is True


def test_unbound_line_candidates_are_never_collapsed_into_one_line():
    first = _envelope(
        "hts_code",
        "4419.90.9000",
        document_type=DocumentType.COMMERCIAL_INVOICE,
        specialist=SpecialistRole.COMMERCIAL_LINES,
        line_item_key=None,
        document_seed="row-one",
    )
    second = _envelope(
        "hts_code",
        "4421.99.9880",
        document_type=DocumentType.COMMERCIAL_INVOICE,
        specialist=SpecialistRole.COMMERCIAL_LINES,
        line_item_key=None,
        document_seed="row-two",
    )

    result = fuse_candidates((first, second))
    assert len(result.fused_candidates) == 2
    assert result.conflicts == ()


def test_cross_document_fusion_accuracy_uses_normalized_winner_value():
    candidate = _envelope(
        "hts_code",
        "4419.90.9000",
        document_type=DocumentType.ENTRY_WORKSHEET,
        specialist=SpecialistRole.COMMERCIAL_LINES,
        line_item_key="SKU:ACT-TRAY-18",
    )
    result = fuse_candidates((candidate,))
    key = FusionKey("hts_code", "SKU:ACT-TRAY-18")

    assert cross_document_fusion_accuracy({key: "4419.90.9000"}, result) == 1.0



def test_semantically_equivalent_hts_formatting_does_not_create_conflict() -> None:
    invoice = _envelope(
        "hts_code",
        "4407.11.0190",
        document_type=DocumentType.COMMERCIAL_INVOICE,
        specialist=SpecialistRole.COMMERCIAL_LINES,
        line_item_key="SKU:PT-38",
        document_seed="semantic-hts-invoice",
    )
    entry = _envelope(
        "hts_code",
        "4407110190",
        document_type=DocumentType.ENTRY_WORKSHEET,
        specialist=SpecialistRole.COMMERCIAL_LINES,
        line_item_key="SKU:PT-38",
        document_seed="semantic-hts-entry",
    )

    result = fuse_candidates((invoice, entry))

    assert result.fused_candidates == (entry,)
    assert result.fused_candidates[0].candidate.value == "4407110190"
    assert result.conflicts == ()


def test_semantically_equivalent_money_unit_and_country_do_not_create_conflicts() -> None:
    line_key = "SKU:EG-24"
    candidates = (
        _envelope(
            "entered_value",
            "$18,300.00",
            document_type=DocumentType.COMMERCIAL_INVOICE,
            specialist=SpecialistRole.COMMERCIAL_LINES,
            line_item_key=line_key,
            document_seed="money-invoice",
        ),
        _envelope(
            "entered_value",
            "18300",
            document_type=DocumentType.ENTRY_WORKSHEET,
            specialist=SpecialistRole.COMMERCIAL_LINES,
            line_item_key=line_key,
            document_seed="money-entry",
        ),
        _envelope(
            "metric_unit",
            "m³",
            document_type=DocumentType.BOTANICAL_DECLARATION,
            specialist=SpecialistRole.BOTANICAL,
            line_item_key=line_key,
            document_seed="unit-botanical",
        ),
        _envelope(
            "metric_unit",
            "M3",
            document_type=DocumentType.SUPPLIER_ORIGIN,
            specialist=SpecialistRole.BOTANICAL,
            line_item_key=line_key,
            document_seed="unit-origin",
        ),
        _envelope(
            "country_of_harvest",
            "Brazil",
            document_type=DocumentType.BOTANICAL_DECLARATION,
            specialist=SpecialistRole.BOTANICAL,
            line_item_key=line_key,
            document_seed="country-botanical",
        ),
        _envelope(
            "country_of_harvest",
            "Brasil",
            document_type=DocumentType.SUPPLIER_ORIGIN,
            specialist=SpecialistRole.BOTANICAL,
            line_item_key=line_key,
            document_seed="country-origin",
        ),
    )

    result = fuse_candidates(candidates)

    assert result.conflicts == ()
    assert len(result.fused_candidates) == 3
    assert {item.candidate.field_key for item in result.fused_candidates} == {
        "entered_value",
        "metric_unit",
        "country_of_harvest",
    }


def test_species_epithet_and_binomial_are_equivalent_when_line_genus_is_known() -> None:
    line_key = "SKU:EG-24"
    genus = _envelope(
        "genus",
        "Eucalyptus",
        document_type=DocumentType.BOTANICAL_DECLARATION,
        specialist=SpecialistRole.BOTANICAL,
        line_item_key=line_key,
        document_seed="genus",
    )
    full_species = _envelope(
        "species",
        "Eucalyptus grandis",
        document_type=DocumentType.BOTANICAL_DECLARATION,
        specialist=SpecialistRole.BOTANICAL,
        line_item_key=line_key,
        document_seed="species-full",
    )
    epithet = _envelope(
        "species",
        "grandis",
        document_type=DocumentType.SUPPLIER_ORIGIN,
        specialist=SpecialistRole.BOTANICAL,
        line_item_key=line_key,
        document_seed="species-epithet",
    )

    result = fuse_candidates((genus, full_species, epithet))

    assert result.conflicts == ()
    species = [item for item in result.fused_candidates if item.candidate.field_key == "species"]
    assert species == [full_species]
    assert species[0].candidate.value == "Eucalyptus grandis"


def test_species_binomial_and_epithet_remain_distinct_without_matching_genus_context() -> None:
    line_key = "SKU:EG-24"
    wrong_genus = _envelope(
        "genus",
        "Pinus",
        document_type=DocumentType.BOTANICAL_DECLARATION,
        specialist=SpecialistRole.BOTANICAL,
        line_item_key=line_key,
        document_seed="wrong-genus",
    )
    full_species = _envelope(
        "species",
        "Eucalyptus grandis",
        document_type=DocumentType.BOTANICAL_DECLARATION,
        specialist=SpecialistRole.BOTANICAL,
        line_item_key=line_key,
        document_seed="species-full-no-context",
    )
    epithet = _envelope(
        "species",
        "grandis",
        document_type=DocumentType.SUPPLIER_ORIGIN,
        specialist=SpecialistRole.BOTANICAL,
        line_item_key=line_key,
        document_seed="species-epithet-no-context",
    )

    result = fuse_candidates((wrong_genus, full_species, epithet))

    assert len(result.conflicts) == 1
    assert result.conflicts[0].key.field_key == "species"



def test_species_equivalence_fails_closed_when_line_has_multiple_genera() -> None:
    line_key = "SKU:AMBIGUOUS-TAXON"
    candidates = (
        _envelope(
            "genus",
            "Eucalyptus",
            document_type=DocumentType.BOTANICAL_DECLARATION,
            specialist=SpecialistRole.BOTANICAL,
            line_item_key=line_key,
            document_seed="ambiguous-genus-eucalyptus",
        ),
        _envelope(
            "genus",
            "Pinus",
            document_type=DocumentType.SUPPLIER_ORIGIN,
            specialist=SpecialistRole.BOTANICAL,
            line_item_key=line_key,
            document_seed="ambiguous-genus-pinus",
        ),
        _envelope(
            "species",
            "Eucalyptus grandis",
            document_type=DocumentType.BOTANICAL_DECLARATION,
            specialist=SpecialistRole.BOTANICAL,
            line_item_key=line_key,
            document_seed="ambiguous-species-full",
        ),
        _envelope(
            "species",
            "grandis",
            document_type=DocumentType.SUPPLIER_ORIGIN,
            specialist=SpecialistRole.BOTANICAL,
            line_item_key=line_key,
            document_seed="ambiguous-species-epithet",
        ),
    )

    result = fuse_candidates(candidates)

    conflict_fields = {item.key.field_key for item in result.conflicts}
    assert conflict_fields == {"genus", "species"}



def test_same_local_line_number_from_different_documents_never_fuses_without_proven_identity() -> None:
    first = _envelope(
        "hts_code",
        "4407.11.0190",
        document_type=DocumentType.ENTRY_WORKSHEET,
        specialist=SpecialistRole.COMMERCIAL_LINES,
        line_item_key="LINE:1",
        document_seed="entry-line-1",
    )
    second = _envelope(
        "hts_code",
        "4418.99.9090",
        document_type=DocumentType.COMMERCIAL_INVOICE,
        specialist=SpecialistRole.COMMERCIAL_LINES,
        line_item_key="LINE:1",
        document_seed="invoice-line-1",
    )

    result = fuse_candidates((first, second))

    assert len(result.fused_candidates) == 2
    assert {item.candidate.value for item in result.fused_candidates} == {
        "4407.11.0190",
        "4418.99.9090",
    }
    assert result.conflicts == ()
