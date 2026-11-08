"""Literal source spans for declared value, plant mass and metric units.

A parser label is source support, not canonical, human or filing authority.
"""
from __future__ import annotations

from hashlib import sha256
from pathlib import Path

from litoral_trace.lacey_engine.domain import DocumentType, LayoutBlock, ParsedLayout
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


def test_invoice_shipment_reference_never_masquerades_as_cbp_entry_number():
    layout = _layout(
        "Shipment reference HHI-IMP-2026-2083",
        "Entry type (exercise) 01 - Formal commercial entry",
        "Entry preparation worksheet REF HHI-IMP-2026-2083",
        "Invoice number LSBH-CI-261102",
        "B/L HBL-XMN-LAX-261120-083",
    )
    assert not extract_explicit_field_spans(layout)


def test_exact_entry_number_is_only_candidate_with_source_span():
    layout = _layout(
        "SYNTHETIC TEST DATA - NOT VALID FOR CUSTOMS OR FILING",
        "Entry number 999-9999999-9",
    )
    spans = extract_explicit_field_spans(layout)
    assert [(s.field_name, s.value) for s in spans] == [
        ("filing_entry_reference", "999-9999999-9"),
    ]
    assert layout.blocks[1].text[spans[0].start:spans[0].end] == spans[0].value


def test_goods_description_requires_explicit_label_and_packing_list_context():
    layout = _layout("Goods Retail boxed 4-piece solid bamboo coaster sets",
                     "Gross weight 585.0 KG",
                     "Description of merchandise Bamboo coaster sets")
    packing = extract_explicit_field_spans(layout, document_type=DocumentType.PACKING_LIST)
    assert [(s.field_name, s.value) for s in packing] == [
        ("merchandise_description", "Retail boxed 4-piece solid bamboo coaster sets"),
        ("merchandise_description", "Bamboo coaster sets"),
    ]
    invoice = extract_explicit_field_spans(layout, document_type=DocumentType.COMMERCIAL_INVOICE)
    assert [(s.field_name,s.value) for s in invoice] == [
        ("merchandise_description", "Bamboo coaster sets"),
    ]


def test_existing_pack2_source_goods_description_is_verbatim_not_derived():
    document = PACK / "02_Packing_List_Shipment2.pdf"
    bundle = process_bundle(filename=document.name, content=document.read_bytes())
    layout = bundle.documents[0].resolution.layout
    spans = extract_explicit_field_spans(
        layout, document_type=bundle.documents[0].resolution.document_type,
    )
    assert [(s.field_name,s.value,s.block.block_id) for s in spans] == [
        ("merchandise_description", "Retail boxed 4-piece solid bamboo coaster sets", "p1-l20"),
    ]


def test_alphanumeric_entry_reference_matches_ppq_contract():
    from litoral_trace.us_lacey.ppq505 import validate_ppq_value
    span = extract_explicit_field_spans(
        _layout("Entry Number ABC-1234567-Z")
    )
    assert [(s.field_name, s.value) for s in span] == [
        ("filing_entry_reference", "ABC-1234567-Z"),
    ]
    validation = validate_ppq_value("filing_entry_reference", span[0].value)
    assert validation.normalized_value == "ABC-1234567-Z"
