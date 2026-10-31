from __future__ import annotations

import pytest

from litoral_trace.lacey_engine.multi_agent.contracts import DocumentType
from litoral_trace.lacey_engine.multi_agent.router import classify_page


_GOLDEN_CASES = (
    # Commercial Invoice
    (
        "en",
        DocumentType.COMMERCIAL_INVOICE,
        "COMMERCIAL INVOICE\nInvoice No. INV-100\nCommercial Line Items\nEntered Value USD 1200",
    ),
    (
        "es",
        DocumentType.COMMERCIAL_INVOICE,
        "FACTURA COMERCIAL\nNúmero de factura INV-100\nPartidas comerciales\nValor declarado USD 1200",
    ),
    (
        "pt",
        DocumentType.COMMERCIAL_INVOICE,
        "FATURA COMERCIAL\nNúmero da fatura INV-100\nItens comerciais\nValor aduaneiro USD 1200",
    ),
    # Entry Worksheet
    (
        "en",
        DocumentType.ENTRY_WORKSHEET,
        "U.S. ENTRY WORKSHEET\nEntry / Filing Reference 123-4567890-1\nEntry Summary Lines\nImporter Number 9988",
    ),
    (
        "es",
        DocumentType.ENTRY_WORKSHEET,
        "HOJA DE TRABAJO DE ENTRADA\nReferencia de entrada / presentación 123-4567890-1\nLíneas de resumen de entrada\nNúmero de importador 9988",
    ),
    (
        "pt",
        DocumentType.ENTRY_WORKSHEET,
        "PLANILHA DE ENTRADA\nReferência de entrada / registro 123-4567890-1\nLinhas do resumo de entrada\nNúmero do importador 9988",
    ),
    # Bill of Lading
    (
        "en",
        DocumentType.BILL_OF_LADING,
        "OCEAN BILL OF LADING\nPort of Loading Auckland\nPort of Discharge Los Angeles\nShipper ACME Consignee WOOD LLC",
    ),
    (
        "es",
        DocumentType.BILL_OF_LADING,
        "CONOCIMIENTO DE EMBARQUE\nPuerto de carga Auckland\nPuerto de descarga Los Angeles\nCargador ACME Consignatario WOOD LLC",
    ),
    (
        "pt",
        DocumentType.BILL_OF_LADING,
        "CONHECIMENTO DE EMBARQUE\nPorto de embarque Auckland\nPorto de descarga Los Angeles\nEmbarcador ACME Consignatário WOOD LLC",
    ),
    # Packing List
    (
        "en",
        DocumentType.PACKING_LIST,
        "PACKING LIST\nPackage Detail\nCartons Pieces\nNet Wt. Gross Wt.",
    ),
    (
        "es",
        DocumentType.PACKING_LIST,
        "LISTA DE EMPAQUE\nDetalle de bultos\nCajas Piezas\nPeso neto Peso bruto",
    ),
    (
        "pt",
        DocumentType.PACKING_LIST,
        "LISTA DE EMBALAGEM\nDetalhe dos volumes\nCaixas Peças\nPeso líquido Peso bruto",
    ),
    # Botanical Declaration
    (
        "en",
        DocumentType.BOTANICAL_DECLARATION,
        "BOTANICAL / LACEY SUPPORTING DECLARATION\nGenus Species\nCountry of Harvest\nPlant Quantity",
    ),
    (
        "es",
        DocumentType.BOTANICAL_DECLARATION,
        "DECLARACIÓN BOTÁNICA / SOPORTE LACEY\nGénero Especie\nPaís de cosecha\nCantidad de material vegetal",
    ),
    (
        "pt",
        DocumentType.BOTANICAL_DECLARATION,
        "DECLARAÇÃO BOTÂNICA / SUPORTE LACEY\nGênero Espécie\nPaís de colheita\nQuantidade de material vegetal",
    ),
    # Supplier Origin Declaration
    (
        "en",
        DocumentType.SUPPLIER_ORIGIN,
        "SUPPLIER MATERIAL ORIGIN STATEMENT\nStatement of Material Origin\nScientific Name Pinus radiata\nHarvest Country NZ",
    ),
    (
        "es",
        DocumentType.SUPPLIER_ORIGIN,
        "DECLARACIÓN DE ORIGEN DEL PROVEEDOR\nDeclaración de origen del material\nNombre científico Pinus radiata\nPaís de cosecha NZ",
    ),
    (
        "pt",
        DocumentType.SUPPLIER_ORIGIN,
        "DECLARAÇÃO DE ORIGEM DO FORNECEDOR\nDeclaração de origem do material\nNome científico Pinus radiata\nPaís de colheita NZ",
    ),
    # Arrival Notice
    (
        "en",
        DocumentType.ARRIVAL_NOTICE,
        "ARRIVAL NOTICE\nCarrier Reference CR-1\nETA tomorrow\nAvailability subject to customs release",
    ),
    (
        "es",
        DocumentType.ARRIVAL_NOTICE,
        "AVISO DE LLEGADA\nReferencia del transportista CR-1\nETA mañana\nDisponibilidad sujeta a liberación aduanera",
    ),
    (
        "pt",
        DocumentType.ARRIVAL_NOTICE,
        "AVISO DE CHEGADA\nReferência do transportador CR-1\nETA amanhã\nDisponibilidade sujeita à liberação aduaneira",
    ),
)


@pytest.mark.parametrize(("language", "expected", "text"), _GOLDEN_CASES)
def test_golden_document_classification_en_es_pt(language, expected, text):
    result = classify_page(text, filename="shipment_document.pdf")

    assert result.document_type is expected, language
    assert result.confidence >= 0.88, language
    assert result.signals, language
    assert "filename_hint" not in result.signals


def test_filename_is_supporting_evidence_not_sole_classification_evidence():
    result = classify_page("Reference 12345", filename="commercial_invoice.pdf")

    assert result.document_type is DocumentType.UNKNOWN
    assert result.confidence == 0.0
