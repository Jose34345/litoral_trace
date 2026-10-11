"""Literal source spans for declared value, plant mass and metric units.

A parser label is source support, not canonical, human or filing authority.
"""
from __future__ import annotations

from hashlib import sha256
from pathlib import Path

from litoral_trace.lacey_engine.domain import LayoutBlock, ParsedLayout
from litoral_trace.lacey_engine.labeled_pdf_fields import extract_explicit_field_spans
from litoral_trace.lacey_engine.pipeline import process_bundle
from litoral_trace.us_lacey.assurance_v2_claim_ingestion import collect_source_claims

PACK = Path(__file__).parent / "fixtures" / "assurance_v2_pdf_e2e" / "pack2"


def _layout(*lines: str) -> ParsedLayout:
    return ParsedLayout(
        blocks=tuple(LayoutBlock(
            block_id=f"p1-l{i}", page=1, bbox=None,
            text=line, block_type="TEXT_LINE",
        ) for i, line in enumerate(lines, 1)),
        page_count=1,
    )


def test_source_label_parser_preserves_raw_subspans_not_shipping_totals():
    layout = _layout(
        "Entered value USD 11,250.00",
        "Quantity of plant material 288.0 KG",
        "Shipment gross weight 585.0 KG",
        "Packing quantity 75 CARTONS",
        "Total cubic volume 4.125 CBM",
    )
    spans = extract_explicit_field_spans(layout)
    assert [(s.field_name, s.value) for s in spans] == [
        ("entered_value", "USD 11,250.00"),
        ("plant_quantity", "288.0"),
        ("metric_unit", "KG"),
    ]
    for claim in spans:
        assert claim.block.text[claim.start:claim.end] == claim.value


def test_ambiguous_decimal_form_missing_label_and_units_abstain():
    layout = _layout(
        "Declared shipment gross weight 288.0 KG",
        "Entered value USD 11,250.00 additional 13",
        "Quantity of plant material approximately 288 KG",
        "Quantity of plant material 288.0 CARTONS",
        "Quantity of plant material 288.0 KG gross 585 KG",
        "Entered value 11250",
        "Container TGHU6042314",
    )
    assert not extract_explicit_field_spans(layout)


def test_original_worksheet_generates_three_source_supported_unapproved_claims():
    pdf = PACK / "05_Entry_Preparation_Worksheet_Shipment2.pdf"
    data = pdf.read_bytes()
    bundle = process_bundle(filename=pdf.name, content=data)
    before = sha256(data).hexdigest()
    result = collect_source_claims(
        organization_id=11, operation_id=12, source_set_revision_id=13,
        source_set_fingerprint="f" * 64,
        assurance_document_id=14, operation_document_id=15,
        document_version_number=1, original_sha256=before,
        original_bytes=data, bundle=bundle,
    )
    candidates = [
        c for c in result["claims"]
        if c["extractor_name"] == "explicit_pdf_label"
    ]
    assert {(c["field_name"], c["original_value"]) for c in candidates} == {
        ("entered_value", "USD 11,250.00"),
        ("plant_quantity", "288.0"), ("metric_unit", "KG"),
    }
    assert all(c["support_status"] == "SUPPORTED" for c in candidates)
    assert all(c["candidate_state"] == "CANDIDATE" for c in candidates)
    assert all(c["association_status"] == "UNBOUND" for c in candidates)
    assert all(c["original_sha256"] == before for c in candidates)
    assert all(c["source_locator"]["kind"] == "TEXT_SPAN" for c in candidates)
    assert sha256(pdf.read_bytes()).hexdigest() == before
