from __future__ import annotations

from fpdf import FPDF
import pytest

from litoral_trace.lacey_engine.errors import UnsupportedDocumentDomainError
from litoral_trace.lacey_engine.pipeline import process_bundle


PACK_2_DOMAIN_FIXTURES = (
    (
        "01_Commercial_Invoice.pdf",
        "COMMERCIAL_INVOICE",
        "COMMERCIAL INVOICE\nInvoice Number: LT-260924-01\n"
        "Description: Sawn cedar boards\nHTS: 4407.99.0190\n",
    ),
    (
        "02_Bill_of_Lading.pdf",
        "BILL_OF_LADING",
        "OCEAN BILL OF LADING\nB/L No: ACE-MIA-260913-77\n"
        "Container Number: TGHU5519023\n",
    ),
    (
        "03_Packing_List.pdf",
        "PACKING_LIST",
        "PACKING LIST\nPacking List No: PL-260924-01\n"
        "Container Number: TGHU5519023\n",
    ),
    (
        "04_Botanical_Declaration.pdf",
        "BOTANICAL_DECLARATION",
        "BOTANICAL DECLARATION\nGenus: Cedrela\nSpecies: odorata\n"
        "Country of Harvest: Peru\nPlant Quantity: 18.000 m3\n",
    ),
    (
        "05_Supplier_Origin_Declaration.pdf",
        "SUPPLIER_DECLARATION",
        "SUPPLIER ORIGIN DECLARATION\nGenus: Cedrela\nSpecies: odorata\n"
        "Country of Harvest: Brazil\n",
    ),
    (
        "06_Entry_Worksheet.pdf",
        "CUSTOMS_ENTRY",
        "ENTRY WORKSHEET\nEntry Number: 123-4567890-1\n"
        "HTS: 4407.99.0190\nEntered Value: USD 49050.00\n",
    ),
    (
        "07_Arrival_Notice.pdf",
        "ARRIVAL_NOTICE",
        "ARRIVAL NOTICE\nB/L No: ACE-MIA-260913-77\n"
        "Container Number: TGHU5519023\nETA: 2026-10-01\n",
    ),
)


def _pdf_bytes(text: str) -> bytes:
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Helvetica", size=11)
    for line in text.splitlines():
        pdf.cell(0, 8, text=line, new_x="LMARGIN", new_y="NEXT")
    output = pdf.output()
    return bytes(output) if not isinstance(output, str) else output.encode("latin-1")


@pytest.mark.parametrize("filename,role_hint,text", PACK_2_DOMAIN_FIXTURES)
def test_pack_2_support_documents_are_not_rejected_by_domain_gate(
    filename: str,
    role_hint: str,
    text: str,
) -> None:
    try:
        bundle = process_bundle(
            filename=filename,
            content=_pdf_bytes(text),
            role_hint=role_hint,
        )
    except UnsupportedDocumentDomainError as exc:  # pragma: no cover - explicit regression signal
        pytest.fail(
            f"{filename} was falsely rejected as {exc.domain}: {exc.safe_message}"
        )

    assert bundle.documents
    assert bundle.page_count == 1


def test_pack_2_all_seven_documents_complete_domain_preflight() -> None:
    processed = [
        process_bundle(
            filename=filename,
            content=_pdf_bytes(text),
            role_hint=role_hint,
        )
        for filename, role_hint, text in PACK_2_DOMAIN_FIXTURES
    ]

    assert len(processed) == 7
    assert all(bundle.documents for bundle in processed)
