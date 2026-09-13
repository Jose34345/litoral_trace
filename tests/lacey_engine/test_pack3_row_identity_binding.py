from __future__ import annotations

from uuid import NAMESPACE_URL, uuid4, uuid5

from litoral_trace.lacey_engine.ai_shadow import AICandidate, candidate_from_payload
from litoral_trace.lacey_engine.domain import EvidenceClass
from litoral_trace.lacey_engine.multi_agent.contracts import (
    CandidateEnvelope,
    DocumentType,
    SpecialistRole,
)
from litoral_trace.lacey_engine.multi_agent.fusion import fuse_candidates, fusion_key
from litoral_trace.lacey_engine.multi_agent.line_binding import bind_line_items, derive_line_item_key


def _candidate(
    field_key: str,
    value: str,
    *,
    line_key: str | None,
    table_id: str | None,
    row_index: int | None,
    page: int = 1,
) -> AICandidate:
    return AICandidate(
        field_key=field_key,
        value=value,
        normalized_value=value,
        evidence_class=EvidenceClass.EXPLICIT,
        page=page,
        source_text=value,
        confidence=0.97,
        provider="fixture",
        model="fixture",
        source_line_key=line_key,
        source_table_id=table_id,
        source_row_index=row_index,
    )


def _envelope(
    field_key: str,
    value: str,
    *,
    line_key: str | None,
    table_id: str | None,
    row_index: int | None,
    document_type: DocumentType,
) -> CandidateEnvelope:
    return CandidateEnvelope(
        candidate=_candidate(
            field_key,
            value,
            line_key=line_key,
            table_id=table_id,
            row_index=row_index,
        ),
        document_id=uuid5(NAMESPACE_URL, document_type.value),
        document_type=document_type,
        specialist=(
            SpecialistRole.BOTANICAL
            if document_type in {DocumentType.BOTANICAL_DECLARATION, DocumentType.SUPPLIER_ORIGIN}
            else SpecialistRole.COMMERCIAL_LINES
        ),
        agent_run_id=uuid4(),
        line_item_key=None,
        source_span_id=None,
    )


def test_specialist_payload_preserves_explicit_row_identity_metadata():
    candidate = candidate_from_payload(
        payload={
            "field_key": "hts_code",
            "value": "4407110190",
            "evidence_class": "EXPLICIT",
            "page": 1,
            "source_text": "4407110190",
            "confidence": 0.98,
            "bbox": None,
            "reason": "commercial row",
            "source_line_key": "PT-38",
            "source_table_id": "commercial-lines",
            "source_row_index": 0,
        },
        provider="fixture",
        model="fixture",
    )

    assert candidate.source_line_key == "PT-38"
    assert candidate.source_table_id == "commercial-lines"
    assert candidate.source_row_index == 0


def test_explicit_source_line_key_binds_sparse_candidates_before_text_fingerprint():
    candidate = _envelope(
        "species",
        "Pinus taeda",
        line_key="PT-38",
        table_id="botanical-lines",
        row_index=0,
        document_type=DocumentType.BOTANICAL_DECLARATION,
    )

    assert derive_line_item_key(candidate) == "SKU:PT-38"


def test_table_and_row_metadata_are_preserved_as_local_fallback_identity():
    candidate = _envelope(
        "description",
        "KD sawn boards",
        line_key=None,
        table_id="commercial-lines",
        row_index=1,
        document_type=DocumentType.COMMERCIAL_INVOICE,
    )

    key = derive_line_item_key(candidate)
    assert key is not None
    assert key.startswith("ROW:")
    assert key.endswith(":P1:TCOMMERCIAL-LINES:R1")


def test_pack3_two_rows_keep_hts_species_description_quantity_unit_and_value_separate():
    commercial = DocumentType.COMMERCIAL_INVOICE
    botanical = DocumentType.BOTANICAL_DECLARATION
    candidates = (
        # PT-38 commercial row
        _envelope("description", "Pinus taeda KD sawn boards", line_key="PT-38", table_id="invoice-lines", row_index=0, document_type=commercial),
        _envelope("hts_code", "4407110190", line_key="PT-38", table_id="invoice-lines", row_index=0, document_type=commercial),
        _envelope("entered_value", "18300", line_key="PT-38", table_id="invoice-lines", row_index=0, document_type=commercial),
        # PT-38 botanical row
        _envelope("genus", "Pinus", line_key="PT-38", table_id="botanical-lines", row_index=0, document_type=botanical),
        _envelope("species", "Pinus taeda", line_key="PT-38", table_id="botanical-lines", row_index=0, document_type=botanical),
        _envelope("country_of_harvest", "Brazil", line_key="PT-38", table_id="botanical-lines", row_index=0, document_type=botanical),
        _envelope("plant_quantity", "30", line_key="PT-38", table_id="botanical-lines", row_index=0, document_type=botanical),
        _envelope("metric_unit", "m3", line_key="PT-38", table_id="botanical-lines", row_index=0, document_type=botanical),
        # EG-22 commercial row
        _envelope("description", "Eucalyptus grandis KD sawn boards", line_key="EG-22", table_id="invoice-lines", row_index=1, document_type=commercial),
        _envelope("hts_code", "4407990190", line_key="EG-22", table_id="invoice-lines", row_index=1, document_type=commercial),
        _envelope("entered_value", "12640", line_key="EG-22", table_id="invoice-lines", row_index=1, document_type=commercial),
        # EG-22 botanical row
        _envelope("genus", "Eucalyptus", line_key="EG-22", table_id="botanical-lines", row_index=1, document_type=botanical),
        _envelope("species", "Eucalyptus grandis", line_key="EG-22", table_id="botanical-lines", row_index=1, document_type=botanical),
        _envelope("country_of_harvest", "Brazil", line_key="EG-22", table_id="botanical-lines", row_index=1, document_type=botanical),
        _envelope("plant_quantity", "16", line_key="EG-22", table_id="botanical-lines", row_index=1, document_type=botanical),
        _envelope("metric_unit", "m3", line_key="EG-22", table_id="botanical-lines", row_index=1, document_type=botanical),
    )

    bound = bind_line_items(candidates)
    assert {item.line_item_key for item in bound} == {"SKU:PT-38", "SKU:EG-22"}

    fused = fuse_candidates(bound)
    assert fused.conflicts == ()
    assert len(fused.fused_candidates) == 16

    actual = {
        (fusion_key(item).line_item_key, item.candidate.field_key): item.candidate.value
        for item in fused.fused_candidates
    }
    assert actual[("SKU:PT-38", "hts_code")] == "4407110190"
    assert actual[("SKU:PT-38", "genus")] == "Pinus"
    assert actual[("SKU:PT-38", "species")] == "Pinus taeda"
    assert actual[("SKU:PT-38", "description")] == "Pinus taeda KD sawn boards"
    assert actual[("SKU:PT-38", "plant_quantity")] == "30"
    assert actual[("SKU:PT-38", "metric_unit")] == "m3"
    assert actual[("SKU:PT-38", "entered_value")] == "18300"

    assert actual[("SKU:EG-22", "hts_code")] == "4407990190"
    assert actual[("SKU:EG-22", "genus")] == "Eucalyptus"
    assert actual[("SKU:EG-22", "species")] == "Eucalyptus grandis"
    assert actual[("SKU:EG-22", "description")] == "Eucalyptus grandis KD sawn boards"
    assert actual[("SKU:EG-22", "plant_quantity")] == "16"
    assert actual[("SKU:EG-22", "metric_unit")] == "m3"
    assert actual[("SKU:EG-22", "entered_value")] == "12640"
