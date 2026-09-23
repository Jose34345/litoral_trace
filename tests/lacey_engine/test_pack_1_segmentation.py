from __future__ import annotations

from fpdf import FPDF

from litoral_trace.lacey_engine.domain import DocumentType, FieldStatus, PageClassification if False else DocumentType
from litoral_trace.lacey_engine.pipeline import process_bundle
from litoral_trace.lacey_engine.segmentation import PageClassification, starts_new_document


def _pdf_bytes(pages: tuple[str, ...]) -> bytes:
    pdf = FPDF()
    for text in pages:
        pdf.add_page()
        pdf.set_font("Helvetica", size=10)
        for line in text.split("\n"):
            pdf.multi_cell(0, 5, text=line)
    output = pdf.output()
    return bytes(output) if not isinstance(output, str) else output.encode("latin-1")


def _pack_1_pdf() -> bytes:
    """Deterministic seven-page Pack 1 regression shape.

    The fixture uses identifiers from the real FMC Pack 1 while keeping the
    page-boundary oracle explicit and repository-local.
    """
    return _pdf_bytes(
        (
            (
                "COMMERCIAL INVOICE\n"
                "Invoice Number: 406499122214\n"
                "Page 1 of 2\n"
                "Cryogenic cylinders\n"
                "HTS 7613.00.0000"
            ),
            (
                "COMMERCIAL INVOICE\n"
                "Invoice Number: 406499122214\n"
                "Page 2 of 2\n"
                "Invoice Total: USD 31,110.21"
            ),
            (
                "PACKING LIST\n"
                "Packing List Number: 1166703\n"
                "4 PALLETS\n"
                "619 KG"
            ),
            (
                "OCEAN BILL OF LADING\n"
                "B/L No: 24359-07\n"
                "Container Number: MSKU0857658\n"
                "Page 1 of 4"
            ),
            (
                "BILL OF LADING\n"
                "B/L No: 24359-07\n"
                "Container Number: MSKU0857658\n"
                "Page 2 of 4"
            ),
            (
                "BILL OF LADING\n"
                "B/L No: 24359-07\n"
                "Container Number: MSKU0857658\n"
                "Page 3 of 4"
            ),
            (
                "BILL OF LADING\n"
                "B/L No: 24359-07\n"
                "Container Number: MSKU0857658\n"
                "Seal: 6548158\n"
                "Page 4 of 4"
            ),
        )
    )


def test_repeated_strong_title_does_not_break_shared_invoice_fingerprint():
    previous = PageClassification(
        page=1,
        document_type=DocumentType.COMMERCIAL_INVOICE,
        confidence=0.99,
        block_ids=("p1",),
        strong_anchor=DocumentType.COMMERCIAL_INVOICE,
        invoice_numbers=frozenset({"406499122214"}),
        pagination=(1, 2),
    )
    current = PageClassification(
        page=2,
        document_type=DocumentType.COMMERCIAL_INVOICE,
        confidence=0.99,
        block_ids=("p2",),
        strong_anchor=DocumentType.COMMERCIAL_INVOICE,
        invoice_numbers=frozenset({"406499122214"}),
        pagination=(2, 2),
    )

    assert starts_new_document(previous, current) is False


def test_high_confidence_type_transition_starts_new_document_without_continuity():
    previous = PageClassification(
        page=2,
        document_type=DocumentType.COMMERCIAL_INVOICE,
        confidence=0.95,
        block_ids=("p2",),
    )
    current = PageClassification(
        page=3,
        document_type=DocumentType.PACKING_LIST,
        confidence=0.90,
        block_ids=("p3",),
    )

    assert starts_new_document(previous, current) is True


def test_pack_1_bundle_segments_into_invoice_packing_and_bill_of_lading():
    bundle = process_bundle(
        filename="pack-1.pdf",
        content=_pack_1_pdf(),
    )

    assert [
        (
            item.document_type,
            item.page_start,
            item.page_end,
        )
        for item in bundle.documents
    ] == [
        (DocumentType.COMMERCIAL_INVOICE, 1, 2),
        (DocumentType.PACKING_LIST, 3, 3),
        (DocumentType.BILL_OF_LADING, 4, 7),
    ]


def test_pack_1_logical_document_extraction_does_not_cross_boundaries():
    bundle = process_bundle(
        filename="pack-1.pdf",
        content=_pack_1_pdf(),
    )

    invoice, packing, bill = bundle.documents

    assert invoice.resolution.field("container_number").status is FieldStatus.MISSING
    assert packing.resolution.field("container_number").status is FieldStatus.MISSING

    container = bill.resolution.field("container_number")
    assert container.status is FieldStatus.MATCHED
    assert container.effective_value == "MSKU0857658"
    assert container.winning_candidate is not None
    assert 4 <= container.winning_candidate.provenance.page <= 7

    invoice_block_pages = {
        block.page
        for block in invoice.resolution.layout.blocks
    }
    bill_block_pages = {
        block.page
        for block in bill.resolution.layout.blocks
    }
    assert invoice_block_pages <= {1, 2}
    assert bill_block_pages <= {4, 5, 6, 7}
