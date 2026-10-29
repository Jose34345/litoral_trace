from __future__ import annotations

import pytest

from litoral_trace.lacey_engine.domain import FieldStatus, LayoutBlock, ParsedLayout
from litoral_trace.lacey_engine.pipeline import _extract, process_bundle, process_document
from litoral_trace.lacey_engine.shipment import ReconciliationState, ShipmentDocumentInput, process_shipment


@pytest.mark.parametrize(("field", "label"), (
    ("consignee_name", "Consignee Name"),
    ("shipper_name", "Shipper Name"),
    ("importer_name", "Importer Name"),
))
@pytest.mark.parametrize("delimiter", (" ", ": ", " : "))
@pytest.mark.parametrize("structured", (False, True))
def test_complete_party_label_yields_one_value_per_source_block(field, label, delimiter, structured):
    block = LayoutBlock(
        "p1-l1", 1, None, f"{label}{delimiter}ACME Imports LLC", "TEXT_LINE",
        key_text=label if structured else None,
        value_text="ACME Imports LLC" if structured else None,
    )
    candidates = _extract(ParsedLayout((block,), 1))[field]
    assert [c.normalized_value for c in candidates] == ["ACME Imports LLC"]
    assert candidates[0].source_block is block


@pytest.mark.parametrize(("field", "label"), (
    ("consignee_name", "Consignee"), ("shipper_name", "Shipper"), ("importer_name", "Importer"),
))
def test_generic_explicit_label_preserves_company_name_starting_with_name(field, label):
    block = LayoutBlock("p1-l1", 1, None, f"{label}: Name Imports LLC", "TEXT_LINE")
    assert [c.normalized_value for c in _extract(ParsedLayout((block,), 1))[field]] == ["Name Imports LLC"]


@pytest.mark.parametrize(("field", "label"), (
    ("consignee_name", "Consignee Name"), ("consignee_name", "Consignee Name:"),
    ("shipper_name", "Shipper Name"), ("importer_name", "Importer Name"),
))
def test_party_heading_without_value_remains_missing(field, label):
    block = LayoutBlock("p1-l1", 1, None, label, "TEXT_LINE")
    assert _extract(ParsedLayout((block,), 1))[field] == []


@pytest.mark.parametrize("party", ("Consignee", "Importer"))
def test_address_label_does_not_create_party_name(party):
    block = LayoutBlock("p1-l1", 1, None, f"{party} Address: 42 Main Street", "TEXT_LINE")
    found = _extract(ParsedLayout((block,), 1))
    assert found[f"{party.lower()}_name"] == []
    assert [c.normalized_value for c in found[f"{party.lower()}_address"]] == ["42 Main Street"]


def test_structured_generic_label_does_not_reinterpret_name_as_label():
    block = LayoutBlock("p1-l1", 1, None, "Consignee Name Imports LLC", "TEXT_LINE",
                        key_text="Consignee", value_text="Name Imports LLC")
    assert [c.normalized_value for c in _extract(ParsedLayout((block,), 1))["consignee_name"]] == ["Name Imports LLC"]


def test_structured_and_colon_routes_preserve_genuine_disagreement():
    block = LayoutBlock("p1-l1", 1, None, "Consignee Name: OTHER Imports LLC", "TEXT_LINE",
                        key_text="Consignee Name", value_text="ACME Imports LLC")
    assert {c.normalized_value for c in _extract(ParsedLayout((block,), 1))["consignee_name"]} == {
        "ACME Imports LLC", "OTHER Imports LLC",
    }


def test_distinct_names_remain_conflict_with_original_evidence(monkeypatch):
    blocks = (
        LayoutBlock("p1-l1", 1, None, "Consignee Name: ACME Imports LLC", "TEXT_LINE"),
        LayoutBlock("p1-l2", 1, None, "Consignee Name: OTHER Imports LLC", "TEXT_LINE"),
    )
    monkeypatch.setattr("litoral_trace.lacey_engine.pipeline.parse_layout", lambda *_: ParsedLayout(blocks, 1))
    field = process_document(filename="conflict.pdf", content=b"synthetic").field("consignee_name")
    assert field.status is FieldStatus.CONFLICT
    assert field.effective_value is None
    assert {c.raw.normalized_value for c in field.candidates} == {"ACME Imports LLC", "OTHER Imports LLC"}
    assert {c.provenance.block_id for c in field.candidates} == {"p1-l1", "p1-l2"}
    assert all(c.provenance.filename == "conflict.pdf" and c.provenance.page == 1 for c in field.candidates)
    assert all(c.provenance.source_text == c.raw.source_block.text for c in field.candidates)


def test_logical_documents_preserve_distinct_consignee_identity(monkeypatch):
    blocks = (
        LayoutBlock("p1-title", 1, None, "OCEAN BILL OF LADING", "TEXT_LINE"),
        LayoutBlock("p1-party", 1, None, "Consignee Name: ACME Imports LLC", "TEXT_LINE"),
        LayoutBlock("p2-title", 2, None, "OCEAN BILL OF LADING", "TEXT_LINE"),
        LayoutBlock("p2-party", 2, None, "Consignee Name: OTHER Imports LLC", "TEXT_LINE"),
    )
    monkeypatch.setattr("litoral_trace.lacey_engine.pipeline.parse_layout", lambda *_: ParsedLayout(blocks, 2))
    bundle = process_bundle(filename="parties.pdf", content=b"synthetic")
    assert len(bundle.documents) == 2
    assert len({doc.logical_document_id for doc in bundle.documents}) == 2
    for doc, page, expected in zip(bundle.documents, (1, 2), ("ACME Imports LLC", "OTHER Imports LLC")):
        field = doc.resolution.field("consignee_name")
        assert field.status is FieldStatus.MATCHED
        assert field.effective_value == expected
        assert doc.page_start == doc.page_end == page
        assert field.winning_candidate.provenance.page == page
        assert field.winning_candidate.provenance.block_id == f"p{page}-party"
    shipment = process_shipment(documents=[
        ShipmentDocumentInput(doc.logical_document_id, doc.parent_filename, resolution=doc.resolution)
        for doc in bundle.documents
    ])
    assert shipment.canonical_fields["consignee_name"].state is ReconciliationState.CONFLICT


def test_structured_and_inline_routes_preserve_genuine_disagreement_without_colon():
    block = LayoutBlock("p1-l1", 1, None, "Consignee Name OTHER Imports LLC", "TEXT_LINE",
                        key_text="Consignee Name", value_text="ACME Imports LLC")
    assert {c.normalized_value for c in _extract(ParsedLayout((block,), 1))["consignee_name"]} == {
        "ACME Imports LLC", "OTHER Imports LLC",
    }


def test_table_label_and_colon_routes_do_not_truncate_mixed_case_name():
    block = LayoutBlock("p1-t1-r1", 1, None, "Consignee Name: ACME Imports LLC", "TABLE_ROW",
                        key_text="Consignee Name", value_text="ACME Imports LLC")
    assert [c.normalized_value for c in _extract(ParsedLayout((block,), 1))["consignee_name"]] == ["ACME Imports LLC"]


@pytest.mark.parametrize(("field", "structured_label", "text_label"), (
    ("consignee_name", "Consignee Name", "Consignee"),
    ("shipper_name", "Shipper Name", "Shipper"),
    ("shipper_name", "Shipper Name", "Embarcador"),
))
def test_valid_alternate_text_label_preserves_structured_disagreement(field, structured_label, text_label):
    block = LayoutBlock("p1-l1", 1, None, f"{text_label} OTHER IMPORTS LLC", "TEXT_LINE",
                        key_text=structured_label, value_text="ACME IMPORTS LLC")
    assert {c.normalized_value for c in _extract(ParsedLayout((block,), 1))[field]} == {
        "ACME IMPORTS LLC", "OTHER IMPORTS LLC",
    }


@pytest.mark.parametrize("label", ("Shipper / Embarcador", "Shipper/Embarcador", "Shipper /Embarcador", "Shipper/ Embarcador"))
def test_bilingual_shipper_label_keeps_only_the_company_value(label):
    block = LayoutBlock("p1-l1", 1, None, f"{label} ACME EXPORTS LLC", "TEXT_LINE")
    assert [c.normalized_value for c in _extract(ParsedLayout((block,), 1))["shipper_name"]] == ["ACME EXPORTS LLC"]


def test_multiple_consignee_labels_in_one_text_block_remain_conflict(monkeypatch):
    """Fallback must preserve the second distinct source label, not hide it."""
    block = LayoutBlock(
        "p1-l1", 1, None,
        "Consignee: ACME LLC 42 Main Street; Consignee: OTHER LLC 10 Second Street",
        "TEXT_LINE",
    )
    found = _extract(ParsedLayout((block,), 1))["consignee_name"]
    assert {c.normalized_value for c in found} == {"ACME LLC", "OTHER LLC"}
    assert all(c.source_block is block for c in found)

    monkeypatch.setattr(
        "litoral_trace.lacey_engine.pipeline.parse_layout",
        lambda *_: ParsedLayout((block,), 1),
    )
    resolved = process_document(filename="two-consignees.pdf", content=b"synthetic")
    field = resolved.field("consignee_name")
    assert field.status is FieldStatus.CONFLICT
    assert field.effective_value is None
    assert {c.raw.normalized_value for c in field.candidates} == {
        "ACME LLC", "OTHER LLC",
    }
    assert {c.provenance.block_id for c in field.candidates} == {"p1-l1"}
