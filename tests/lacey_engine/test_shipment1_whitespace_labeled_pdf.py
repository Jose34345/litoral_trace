"""Regression for synthetically generated US Lacey PDF text lines with no ':'.

The seven-document shipment rendered a full 1-page digital-text layout per PDF
but yielded 0 commercial lines and 0 suggestions in production on 2026-10-08.
No customer documents are embedded in this test; all values are synthetic.
"""
from __future__ import annotations

from litoral_trace.lacey_engine.classifier import classify
from litoral_trace.lacey_engine.domain import (
    DocumentSection, DocumentType, FieldStatus, LayoutBlock, ParsedLayout,
)
from litoral_trace.lacey_engine.pipeline import _resolve_logical_document, _extract
from litoral_trace.lacey_engine.shipment import (
    ReconciliationState, ShipmentDocumentInput, process_shipment,
)


def _layout(*lines: str) -> ParsedLayout:
    return ParsedLayout(
        tuple(
            LayoutBlock(f"p1-l{i}", 1, None, text, "TEXT_LINE")
            for i, text in enumerate(lines, 1)
        ),
        1,
    )


def _document(name: str, *lines: str):
    layout = _layout(*lines)
    kind, confidence = classify(layout)
    section = DocumentSection("page-1", 1, 1, kind, confidence, tuple(block.block_id for block in layout.blocks))
    return _resolve_logical_document(filename=name, layout=layout, section=section)


def test_unseparated_invoice_and_entry_worksheet_extract_explicit_evidence():
    invoice = _document(
        "01_Commercial_Invoice_Shipment1.pdf",
        "COMMERCIAL INVOICE",
        "Supplier / manufacturer LONGSHENG BAMBOO HOUSEWARES CO., LTD.",
        "Supplier address 88 Jinghua Industrial Road, Fuzhou (fictional address)",
        "Importer of record / consignee HARBORLINE HOME IMPORTS LLC",
        "Importer address 1710 NW Commerce Boulevard, Portland, OR 97209, USA",
        "Currency USD",
        "Plant-material quantity 384.0 KG solid bamboo",
        "Bill of lading HBL-XMN-LAX-261012-047",
        "Product / SKU Bamboo coaster set (4 pieces) / BAM-COAST-04",
    )
    assert invoice.field("supplier_name").effective_value == "LONGSHENG BAMBOO HOUSEWARES CO., LTD."
    assert invoice.field("manufacturer_name").status == FieldStatus.MATCHED
    assert invoice.field("importer_name").effective_value == "HARBORLINE HOME IMPORTS LLC"
    assert invoice.field("consignee_name").effective_value == "HARBORLINE HOME IMPORTS LLC"
    assert invoice.field("importer_address").status == FieldStatus.MATCHED
    assert invoice.field("plant_quantity").effective_value == "384.0"
    assert invoice.field("metric_unit").effective_value == "KG"
    assert invoice.field("currency").effective_value == "USD"
    assert invoice.field("bill_of_lading").status == FieldStatus.MATCHED
    assert "address" not in str(invoice.field("supplier_name").effective_value).lower()

    entry = _document(
        "05_Entry_Preparation_Worksheet_Shipment1.pdf",
        "U.S. LACEY DATA - ENTRY PREPARATION WORKSHEET",
        "HTS - proposed classification 4419.19.9010",
        "Entered value USD 15,000.00",
        "Article / component Solid bamboo coasters",
        "Scientific genus Phyllostachys",
        "Scientific species edulis",
        "Country of harvest CHINA (CN); planted/harvested Zhejiang Province, China",
        "Quantity of plant material 384.0 KG",
        "Percent recycled 0%",
        "Proposed manufacturer ID CNFUZLON88JIN",
        "Expected U.S. arrival 2026-11-01",
        "Container number MSCU7234183",
    )
    expected = {
        "hts_code": "4419.19.9010",
        "entered_value": "15000.00",
        "article_component": "Solid bamboo coasters",
        "genus": "Phyllostachys",
        "species": "edulis",
        "country_of_harvest": "CHINA",
        "plant_quantity": "384.0",
        "metric_unit": "KG",
        "percent_recycled": "0",
        "manufacturer_id": "CNFUZLON88JIN",
        "estimated_arrival_date": "2026-11-01",
        "container_number": "MSCU7234183",
    }
    for field, value in expected.items():
        assert entry.field(field).status is FieldStatus.MATCHED, field
        assert entry.field(field).effective_value == value, field


def test_botanical_declaration_is_typed_supplier_evidence_not_unknown():
    botanical = _document(
        "06_Supplier_Botanical_Declaration_Shipment1.pdf",
        "SUPPLIER BOTANICAL AND HARVEST DECLARATION",
        "Supplier legal name LONGSHENG BAMBOO HOUSEWARES CO., LTD.",
        "Manufacturer SKU BAM-COAST-04",
        "Country where plant was harvested CHINA (CN)",
        "Scientific genus Phyllostachys",
        "Scientific species edulis",
        "Component Solid bamboo coasters",
        "Source harvest lot BAM-ZJ-26-0815",
    )
    assert botanical.document_type == DocumentType.SUPPLIER_DECLARATION
    assert botanical.field("supplier_name").status is FieldStatus.MATCHED
    assert botanical.field("country_of_harvest").effective_value == "CHINA"
    assert botanical.field("genus").effective_value == "Phyllostachys"
    assert botanical.field("species").effective_value == "edulis"
    assert botanical.field("country_of_harvest").winning_candidate.provenance.source_text.startswith(
        "Country where plant was harvested"
    )

    spec = _document(
        "07_Supplier_Product_Specification_Shipment1.pdf",
        "MANUFACTURER PRODUCT SPECIFICATION",
        "Manufacturer / supplier LONGSHENG BAMBOO HOUSEWARES CO., LTD.",
        "Manufacturer SKU BAM-COAST-04",
        "Product name 4-piece solid bamboo table coaster set",
        "Species (raw material) Phyllostachys edulis",
        "Country of harvest China (Zhejiang Province)",
        "Bamboo mass per 4-piece set 0.320 KG",
        "No other plant species Yes - one botanical component in coasters",
    )
    assert spec.field("genus").effective_value == "Phyllostachys"
    assert spec.field("species").effective_value == "edulis"
    assert spec.field("country_of_harvest").effective_value == "China"
    assert spec.field("plant_quantity").status is FieldStatus.MISSING  # per-set != shipment mass


def test_seven_documents_project_real_fields_but_keep_regulatory_review_boundary():
    contents = [
        ("01_Commercial_Invoice_Shipment1.pdf", ("COMMERCIAL INVOICE", "Supplier / manufacturer LONGSHENG BAMBOO HOUSEWARES CO., LTD.", "Importer of record / consignee HARBORLINE HOME IMPORTS LLC", "Plant-material quantity 384.0 KG solid bamboo", "Bill of lading HBL-XMN-LAX-261012-047")),
        ("02_Packing_List_Shipment1.pdf", ("PACKING LIST", "Container number MSCU7234183")),
        ("03_House_Bill_of_Lading_Shipment1.pdf", ("HOUSE BILL OF LADING - NON-NEGOTIABLE", "Shipper LONGSHENG BAMBOO HOUSEWARES CO., LTD.", "Consignee HARBORLINE HOME IMPORTS LLC")),
        ("04_Supplier_Order_Confirmation_Shipment1.pdf", ("SUPPLIER ORDER CONFIRMATION", "Manufacturer LONGSHENG BAMBOO HOUSEWARES CO., LTD.")),
        ("05_Entry_Preparation_Worksheet_Shipment1.pdf", ("U.S. LACEY DATA - ENTRY PREPARATION WORKSHEET", "HTS - proposed classification 4419.19.9010", "Entered value USD 15,000.00", "Article / component Solid bamboo coasters", "Scientific genus Phyllostachys", "Scientific species edulis", "Country of harvest CHINA (CN)", "Quantity of plant material 384.0 KG", "Percent recycled 0%")),
        ("06_Supplier_Botanical_Declaration_Shipment1.pdf", ("SUPPLIER BOTANICAL AND HARVEST DECLARATION", "Supplier legal name LONGSHENG BAMBOO HOUSEWARES CO., LTD.", "Country where plant was harvested CHINA (CN)", "Scientific genus Phyllostachys", "Scientific species edulis")),
        ("07_Supplier_Product_Specification_Shipment1.pdf", ("MANUFACTURER PRODUCT SPECIFICATION", "Species (raw material) Phyllostachys edulis", "Country of harvest China (Zhejiang Province)")),
    ]
    documents = [
        ShipmentDocumentInput(str(i), name, resolution=_document(name, *lines))
        for i, (name, lines) in enumerate(contents, 1)
    ]
    result = process_shipment(documents=documents)
    assert result.metrics["fields_supported"] >= 5
    for field in ("genus", "species", "country_of_harvest", "bill_of_lading"):
        assert result.canonical_fields[field].state != ReconciliationState.MISSING, field
    # No automatic filing, no government approval and no invented entry number.
    assert result.canonical_fields["filing_entry_reference"].state is ReconciliationState.MISSING


def test_exact_production_shipment1_text_lines_are_extractable():
    # This fixture contains only synthetic text-line strings emitted by the
    # Engine 2 PDF parser for the actual seven-document 2026-10-08 golden run.
    # It is intentionally not a document byte dump or customer private data.
    import json
    from pathlib import Path
    fixture = (
        Path(__file__).resolve().parents[1]
        / "fixtures/us_lacey_shipment1_inline_20261008.json"
    )
    source_docs = json.loads(fixture.read_text(encoding="utf-8"))
    assert len(source_docs) == 7
    parsed = [
        ShipmentDocumentInput(str(i), doc["filename"], resolution=_document(doc["filename"], *doc["lines"]))
        for i, doc in enumerate(source_docs, 1)
    ]
    resolutions = {doc.filename: doc.resolution for doc in parsed}
    invoice = resolutions["01_Commercial_Invoice_Shipment1.pdf"]
    worksheet = resolutions["05_Entry_Preparation_Worksheet_Shipment1.pdf"]
    botanical = resolutions["06_Supplier_Botanical_Declaration_Shipment1.pdf"]
    assert botanical.document_type is DocumentType.SUPPLIER_DECLARATION
    assert resolutions["03_House_Bill_of_Lading_Shipment1.pdf"].document_type is DocumentType.BILL_OF_LADING
    assert resolutions["03_House_Bill_of_Lading_Shipment1.pdf"].field("bill_of_lading").status is FieldStatus.MATCHED
    for field in ("supplier_name", "importer_name", "consignee_name", "plant_quantity"):
        assert invoice.field(field).status is FieldStatus.MATCHED, field
    for field in ("genus", "species", "country_of_harvest", "hts_code", "entered_value"):
        assert worksheet.field(field).status is FieldStatus.MATCHED, field
    for field in ("genus", "species", "country_of_harvest"):
        assert botanical.field(field).status is FieldStatus.MATCHED, field
    shipment = process_shipment(documents=parsed)
    assert shipment.metrics["fields_supported"] >= 6, shipment.metrics
    assert shipment.canonical_fields["genus"].state is not ReconciliationState.MISSING
    assert shipment.canonical_fields["species"].state is not ReconciliationState.MISSING
    assert shipment.canonical_fields["country_of_harvest"].state is not ReconciliationState.MISSING
    assert shipment.canonical_fields["bill_of_lading"].state is not ReconciliationState.MISSING
    assert shipment.canonical_fields["filing_entry_reference"].state is ReconciliationState.MISSING

    # The reconciled payload must feed the existing (non-authoritative)
    # suggestion layer, not merely populate intermediate PDF fields.
    from litoral_trace.lacey_engine.serialization import serialize_shipment_resolution
    from litoral_trace.us_lacey.engine2_suggestions import supported_engine2_suggestions
    suggestions = supported_engine2_suggestions(serialize_shipment_resolution(shipment))
    assert len(suggestions) >= 3, [(item.field_name, item.value) for item in suggestions]
    assert "container_number" in {item.field_name for item in suggestions}, {
        "suggestions": [item.field_name for item in suggestions],
        "bol": shipment.canonical_fields["bill_of_lading"].state.value,
        "values": [item.value for item in shipment.canonical_fields["bill_of_lading"].values],
    }


def test_unlabeled_or_non_regulatory_prose_is_not_promoted():
    layout = _layout(
        "Original order mentions importer address 123 Main Road",
        "Importer",
        "Shipper address 34 Example Street",
        "Supplier order acceptance ACK-123456",
        "Manufacturer SKU BAM-COAST-04",
        "Bamboo mass per 4-piece set 0.320 KG",
        "Expected goods-ready date 2026-10-10",
        "Plant materials are harvested in many countries around the world.",
    )
    extracted = _extract(layout)
    for field in ("importer_name", "shipper_name", "supplier_name", "manufacturer_name", "plant_quantity", "estimated_arrival_date", "country_of_harvest"):
        assert not extracted[field], field
