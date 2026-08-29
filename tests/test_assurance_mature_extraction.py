from __future__ import annotations

from litoral_trace.assurance.domain import AssuranceDocumentType
from litoral_trace.assurance.extraction import (
    classify_document,
    extract_structured_fields,
)
from litoral_trace.assurance.parsers import ParsedDocument, ParsedTable, SourceLocation


def _by_field(parsed: ParsedDocument):
    result = {}
    for candidate in extract_structured_fields(parsed):
        result.setdefault(candidate.field_name, candidate)
    return result


def test_real_world_english_invoice_labels_are_extracted_deterministically():
    parsed = ParsedDocument(
        file_kind="PDF",
        text=(
            "Concepts Marketing Group\n"
            "INVOICE\n"
            "Date: February 14, 1991\n"
            "Invoice: 6875\n"
            "Description: Video Press Release\n"
            "Total $ 4,174.59\n"
        ),
    )

    classification = classify_document("historic_invoice.pdf", parsed)
    fields = _by_field(parsed)

    assert classification.document_type == AssuranceDocumentType.INVOICE
    assert fields["document_number"].normalized_value == "6875"
    assert fields["document_date"].normalized_value == "1991-02-14"
    assert fields["product"].normalized_value == "Video Press Release"
    assert fields["total_amount"].normalized_value == "4174.59"


def test_invoice_amount_and_currency_use_price_parser():
    parsed = ParsedDocument(
        file_kind="PDF",
        text=(
            "RENTAL INVOICE\n"
            "INVOICE NUMBER: 317302\n"
            "INVOICE DATE: October 18, 1988\n"
            "TOTAL AMOUNT DUE: $98.74\n"
        ),
    )
    fields = _by_field(parsed)

    assert fields["document_number"].normalized_value == "317302"
    assert fields["document_date"].normalized_value == "1988-10-18"
    assert fields["total_amount"].normalized_value == "98.74"
    currencies = {
        candidate.normalized_value
        for candidate in extract_structured_fields(parsed)
        if candidate.field_name == "currency"
    }
    assert "$" in currencies


def test_rapidfuzz_recovers_minor_ocr_noise_in_table_header_without_auto_accepting_it():
    parsed = ParsedDocument(
        file_kind="PDF",
        tables=(
            ParsedTable(
                name="ocr_table",
                headers=("Invoice Numbr", "Invoice Date", "Total Amount Due"),
                rows=((
                    {
                        "Invoice Numbr": "9900001-IN",
                        "Invoice Date": "01/08/1999",
                        "Total Amount Due": "$75,000.00",
                    }
                ),),
                source=SourceLocation(page=1, locator="page:1;table:1"),
            ),
        ),
    )

    fields = _by_field(parsed)
    assert fields["document_number"].normalized_value == "9900001-IN"
    assert fields["document_number"].confidence < 0.90
    assert fields["document_date"].normalized_value == "1999-08-01"
    assert fields["total_amount"].normalized_value == "75000.00"


def test_numeric_ambiguous_date_does_not_use_dateparser_guessing():
    parsed = ParsedDocument(
        file_kind="PDF",
        text="INVOICE\nInvoice Date: 03/04/05\nInvoice Number: 12345\n",
    )
    fields = _by_field(parsed)

    # Existing deterministic normalizer handles explicitly supported numeric
    # formats; dateparser is reserved for textual month names only.
    assert fields["document_date"].normalized_value == "2005-04-03"
