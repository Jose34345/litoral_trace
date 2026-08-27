from __future__ import annotations

from litoral_trace.assurance.extraction import ExtractedCandidate
from litoral_trace.assurance.matching import (
    EntityRecord,
    FieldDecisionStatus,
    decide_field_acceptance,
    match_candidate_entities,
    match_entity,
)


def _candidate(field_name: str, value: str, confidence: float) -> ExtractedCandidate:
    return ExtractedCandidate(
        field_name=field_name,
        original_value=value,
        normalized_value=value,
        value_type="text",
        confidence=confidence,
        source_page=None,
        source_locator=f"test:{field_name}",
    )


def test_high_confidence_without_conflict_is_auto_accepted():
    decisions = decide_field_acceptance((_candidate("lot_id", "LOT-001", 0.98),))
    assert decisions[0].status == FieldDecisionStatus.AUTO_ACCEPTED


def test_medium_confidence_is_never_auto_written():
    decisions = decide_field_acceptance((_candidate("lot_id", "LOT-001", 0.75),))
    assert decisions[0].status == FieldDecisionStatus.NEEDS_REVIEW


def test_low_confidence_is_never_auto_written():
    decisions = decide_field_acceptance((_candidate("lot_id", "LOT-001", 0.40),))
    assert decisions[0].status == FieldDecisionStatus.LOW_CONFIDENCE


def test_conflicting_high_confidence_values_are_not_auto_accepted():
    decisions = decide_field_acceptance(
        (
            _candidate("quantity", "1000", 0.98),
            _candidate("quantity", "1200", 0.98),
        )
    )
    assert {decision.status for decision in decisions} == {FieldDecisionStatus.CONFLICT}


def test_entity_match_accepts_exact_and_normalized_identifiers_only():
    records = (
        EntityRecord(
            entity_type="SUPPLIER",
            entity_reference="30708323108",
            identifiers=("30-70832310-8", "Forestal Norte SA"),
        ),
    )
    exact = match_entity(
        "30-70832310-8",
        records,
        entity_type="SUPPLIER",
    )
    normalized = match_entity(
        "30708323108",
        records,
        entity_type="SUPPLIER",
    )
    assert exact is not None and exact.confidence == 1.0
    assert normalized is not None and normalized.confidence == 0.95
    assert exact.ambiguous is False
    assert normalized.ambiguous is False


def test_ambiguous_entity_identifier_never_selects_a_winner():
    records = (
        EntityRecord("LOT", "1", ("L-001",)),
        EntityRecord("LOT", "2", ("L001",)),
    )
    result = match_entity("L 001", records, entity_type="LOT")
    assert result is not None
    assert result.ambiguous is True
    assert result.entity_reference == ""
    assert result.confidence == 0.0


def test_supported_candidate_types_link_supplier_lot_shipment_and_order():
    candidates = (
        _candidate("issuer_cuit", "30708323108", 0.98),
        _candidate("lot_id", "LOT-001", 0.98),
        _candidate("shipment_code", "SHIP-9", 0.98),
        _candidate("sale_reference", "PO-77", 0.98),
    )
    records = (
        EntityRecord("SUPPLIER", "supplier:30708323108", ("30708323108",)),
        EntityRecord("LOT", "lot:1", ("LOT-001",)),
        EntityRecord("SHIPMENT", "shipment:9", ("SHIP-9",)),
        EntityRecord("ORDER", "order:77", ("PO-77",)),
    )
    matches = match_candidate_entities(candidates, records)
    assert {match.entity_type for match in matches} == {
        "SUPPLIER",
        "LOT",
        "SHIPMENT",
        "ORDER",
    }
