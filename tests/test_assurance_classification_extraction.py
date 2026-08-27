from __future__ import annotations

from litoral_trace.assurance.domain import AssuranceDocumentType
from litoral_trace.assurance.extraction import (
    DOCUMENT_SCHEMAS,
    classify_document,
    extract_structured_fields,
    missing_required_fields,
)
from litoral_trace.assurance.parsers import ParsedDocument, ParsedTable, SourceLocation


def test_classifier_detects_invoice_from_document_evidence():
    parsed = ParsedDocument(
        file_kind="PDF",
        text=(
            "FACTURA E\n"
            "Numero: 0001-00001234\n"
            "Fecha de emision: 27/08/2026\n"
            "CUIT emisor: 30-70832310-8\n"
        ),
        metadata={"page_count": 1},
    )
    result = classify_document("factura_exportacion.pdf", parsed)
    assert result.document_type == AssuranceDocumentType.INVOICE
    assert result.confidence >= 0.80
    assert "factura" in {item.lower() for item in result.evidence}


def test_classifier_detects_forest_guide_and_phytosanitary_certificate():
    forest = classify_document(
        "guia_123.pdf",
        ParsedDocument(file_kind="PDF", text="Guia Forestal de productos forestales"),
    )
    phyto = classify_document(
        "cert_pov.pdf",
        ParsedDocument(file_kind="PDF", text="SENASA Certificado Fitosanitario ePhyto"),
    )
    assert forest.document_type == AssuranceDocumentType.FOREST_GUIDE
    assert phyto.document_type == AssuranceDocumentType.PHYTOSANITARY_CERTIFICATE


def test_spreadsheet_is_safe_fallback_when_no_semantic_keywords_exist():
    parsed = ParsedDocument(file_kind="XLSX")
    result = classify_document("datos_operacion.xlsx", parsed)
    assert result.document_type == AssuranceDocumentType.SPREADSHEET
    assert result.confidence == 0.99


def test_each_priority_document_type_has_explicit_schema():
    for document_type in (
        AssuranceDocumentType.INVOICE,
        AssuranceDocumentType.DELIVERY_NOTE,
        AssuranceDocumentType.FOREST_GUIDE,
        AssuranceDocumentType.PHYTOSANITARY_CERTIFICATE,
        AssuranceDocumentType.CUSTOMS_DOCUMENT,
        AssuranceDocumentType.SPREADSHEET,
    ):
        assert document_type in DOCUMENT_SCHEMAS


def test_table_extraction_keeps_original_normalized_confidence_and_locator():
    parsed = ParsedDocument(
        file_kind="XLSX",
        tables=(
            ParsedTable(
                name="Operacion",
                headers=("CUIT proveedor", "Fecha", "Cantidad", "Unidad", "Lote"),
                rows=(
                    {
                        "CUIT proveedor": "30-70832310-8",
                        "Fecha": "27/08/2026",
                        "Cantidad": "1.250,50",
                        "Unidad": "kg",
                        "Lote": "lt-001",
                    },
                ),
                source=SourceLocation(
                    sheet="Operacion",
                    row=1,
                    locator="sheet:Operacion;header_row:1",
                ),
            ),
        ),
    )
    candidates = extract_structured_fields(parsed)
    by_field = {}
    for candidate in candidates:
        by_field.setdefault(candidate.field_name, candidate)

    assert by_field["issuer_cuit"].original_value == "30-70832310-8"
    assert by_field["issuer_cuit"].normalized_value == "30708323108"
    assert by_field["document_date"].normalized_value == "2026-08-27"
    assert by_field["quantity"].normalized_value == "1250.50"
    assert by_field["lot_id"].normalized_value == "LT-001"
    assert by_field["quantity"].confidence == 0.98
    assert "sheet:Operacion" in by_field["quantity"].source_locator


def test_pdf_text_extraction_is_structured_and_missing_required_fields_are_explicit():
    parsed = ParsedDocument(
        file_kind="PDF",
        text=(
            "FACTURA E\n"
            "Numero: 0001-00001234\n"
            "Fecha de emision: 27/08/2026\n"
            "CUIT emisor: 30-70832310-8\n"
            "Destino: España\n"
        ),
    )
    classification = classify_document("factura.pdf", parsed)
    candidates = extract_structured_fields(parsed)
    names = {candidate.field_name for candidate in candidates}
    assert {"document_number", "document_date", "issuer_cuit", "destination"}.issubset(names)
    assert missing_required_fields(classification.document_type, candidates) == ()
