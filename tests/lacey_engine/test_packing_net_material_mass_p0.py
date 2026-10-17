"""Exact single-SKU source-linked bamboo mass; never guess gross/shipping totals."""
from dataclasses import replace
from pathlib import Path

from litoral_trace.lacey_engine.domain import DocumentType, EvidenceClass, ParsedLayout
from litoral_trace.lacey_engine.layout_parser import parse_layout
from litoral_trace.lacey_engine.packing_net_material_mass import extract_packing_plant_net_mass
from litoral_trace.lacey_engine.pipeline import process_document

ROOT = Path(__file__).resolve().parents[2]
PACKING = ROOT / "tests/fixtures/assurance_v2_pdf_e2e/pack2/02_Packing_List_Shipment2.pdf"


def _layout() -> ParsedLayout:
    return parse_layout(PACKING.name, PACKING.read_bytes())


def _swap(layout: ParsedLayout, fragment: str, new: str) -> ParsedLayout:
    blocks = [
        replace(block, text=block.text.replace(fragment, new))
        if fragment in block.text and block.block_type == "TEXT_LINE" else block
        for block in layout.blocks
    ]
    return replace(layout, blocks=tuple(blocks))


def test_pack2_original_packing_list_exposes_exact_net_bamboo_quantity_and_unit():
    result = process_document(filename=PACKING.name, content=PACKING.read_bytes())
    assert result.document_type is DocumentType.PACKING_LIST
    quantity = result.field("plant_quantity")
    unit = result.field("metric_unit")
    assert quantity.effective_value == "288.00"
    assert unit.effective_value == "KG"
    for field in (quantity, unit):
        assert field.winning_candidate.raw.evidence_class is EvidenceClass.EXPLICIT
        assert field.winning_candidate.provenance.page == 1
        assert field.winning_candidate.provenance.source_text == "Total bamboo net mass 288.00 KG"
        assert "BAM-COAST-04" in field.winning_candidate.raw.label


def test_filing_authority_not_inferred_from_gross_weight_or_derived_arithmetic():
    layout = _layout()
    assert extract_packing_plant_net_mass(layout, document_type=DocumentType.PACKING_LIST) is not None
    for original, modified in (
        ("Total bamboo net mass 288.00 KG", "Total gross mass 288.00 KG"),
        ("Total bamboo net mass 288.00 KG", "Total bamboo net mass 289.00 KG"),
        ("Total bamboo net mass 288.00 KG", "Total bamboo net mass 288.00 LB"),
        ("Carton weights Net bamboo 3.84 KG per carton; gross 7.80 KG per carton", "Carton weights Net bamboo 3.84 KG per carton; gross 2.80 KG per carton"),
        ("Product material Solid bamboo; natural finish", "Product material Bamboo / metal blend"),
    ):
        assert extract_packing_plant_net_mass(
            _swap(layout, original, modified), document_type=DocumentType.PACKING_LIST
        ) is None
    assert extract_packing_plant_net_mass(
        layout, document_type=DocumentType.BILL_OF_LADING
    ) is None


def test_multiple_sku_rows_refuse_shipment_net_mass_as_per_product_quantity():
    layout = _layout()
    blocks = list(layout.blocks)
    idx = next(i for i, b in enumerate(blocks) if b.block_type == "TEXT_LINE" and b.text == "001-075")
    blocks.insert(idx, replace(blocks[idx], block_id="extra-sku", text="OTHER-BATCH / OTHER-SKU-10 75 12 900"))
    assert extract_packing_plant_net_mass(
        replace(layout, blocks=tuple(blocks)), document_type=DocumentType.PACKING_LIST
    ) is None


def test_duplicate_total_mass_labels_fail_closed():
    layout = _layout()
    source = next(b for b in layout.blocks if b.block_type == "TEXT_LINE" and b.text.startswith("Total bamboo net mass"))
    blocks = tuple(layout.blocks) + (replace(source, block_id="second-total"),)
    assert extract_packing_plant_net_mass(
        replace(layout, blocks=blocks), document_type=DocumentType.PACKING_LIST
    ) is None


def test_plant_mass_decimal_presentation_does_not_create_false_conflict():
    from litoral_trace.lacey_engine.semantic_graph import semantic_equal, semantic_normalize
    assert semantic_equal("plant_quantity", "288.00", "288.0")
    assert semantic_normalize("plant_quantity", "288.00") == "288"
    assert not semantic_equal("plant_quantity", "288.00", "585.00")


def test_pack2_six_document_reconciliation_has_no_false_mass_precision_conflict():
    from litoral_trace.lacey_engine.shipment import process_shipment, ShipmentDocumentInput
    from litoral_trace.lacey_engine.shipment import ReconciliationState
    directory = PACKING.parent
    documents = [
        ShipmentDocumentInput(
            document_id=f"test-{index}",
            filename=path.name,
            content=path.read_bytes(),
        )
        for index, path in enumerate(sorted(directory.glob("*.pdf")), start=1)
    ]
    result = process_shipment(documents=documents)
    quantity = result.canonical_fields["plant_quantity"]
    unit = result.canonical_fields["metric_unit"]
    assert quantity.state is ReconciliationState.SUPPORTED_MULTIPLE
    assert [value.value for value in quantity.values] == ["288"]
    assert [value.value for value in unit.values] == ["KG"]
    assert any(
        entry.candidate.provenance.source_text == "Total bamboo net mass 288.00 KG"
        and entry.document_id == "test-2"
        and entry.component_key == "bam-coast-04"
        for entry in quantity.supporting_evidence
    )
    assert not any("gross mass" in entry.candidate.provenance.source_text.casefold()
                   for entry in quantity.supporting_evidence)
    assert not any(issue.field_key == "plant_quantity" for issue in result.issues)
    # Other genuine blockers remain; quantity does not unlock filing.
    assert result.readiness.value != "READY"
