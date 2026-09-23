from __future__ import annotations

from fpdf import FPDF

from litoral_trace.lacey_engine.domain import DocumentType, FieldStatus
from litoral_trace.lacey_engine.pipeline import process_bundle
from litoral_trace.lacey_engine.segmentation import (
    PageClassification,
    starts_new_document,
)
from litoral_trace.us_lacey.regulatory.applicability.domain import (
    DeclarationScope,
    MerchandiseLineFacts,
    PlantMaterialEvidence,
)
from litoral_trace.us_lacey.regulatory.applicability.service import (
    DeclarationApplicabilityService,
)


def _pack_1_pdf() -> bytes:
    """Seven-page Golden Fixture preserving the real Pack 1 identities/boundaries."""

    pages = (
        (
            "COMMERCIAL INVOICE\n"
            "CHART INC.\n"
            "Invoice Number: 406499122214\n"
            "Invoice Date: 12/22/14\n"
            "Page 1 of 2\n"
            "Sold To: 3A-CRYOGENIC FZE\n"
            "HS Code: 7613.00.0000\n"
            "HS Code: 8424.89.0000\n"
            "HS Code: 8716.80.5070\n"
        ),
        (
            "COMMERCIAL INVOICE\n"
            "Invoice Number: 406499122214\n"
            "Page 2 of 2\n"
            "Invoice Total: USD 31,110.21\n"
            "3A-CRYOGENIC FZE\n"
        ),
        (
            "PACKING LIST\n"
            "Packing List No: 1166703\n"
            "CHART INC.\n"
            "4 pallets\n"
            "46 boxes\n"
        ),
        (
            "BILL OF LADING\n"
            "B/L No: 24359-07\n"
            "Container Number: MSKU0857658\n"
            "Seal: 6548158\n"
            "Shipper: CHART INC.\n"
        ),
        (
            "DELIVERY RECEIPT\n"
            "B/L No: 24359-07\n"
            "Container Number: MSKU0857658\n"
            "4 pallets received\n"
        ),
        (
            "WAREHOUSE / TRANSPORT RECEIPT\n"
            "Container Number: MSKU0857658\n"
            "B/L No: 24359-07\n"
            "AZ MIDWEST CFS\n"
        ),
        (
            "OCEAN BILL OF LADING\n"
            "B/L No: 24359-07\n"
            "Container Number: MSKU0857658\n"
            "Seal: 6548158\n"
            "Sharjah, UAE\n"
        ),
    )

    pdf = FPDF()
    for page_text in pages:
        pdf.add_page()
        pdf.set_font("Helvetica", size=11)
        for line in page_text.splitlines():
            pdf.cell(0, 8, text=line, new_x="LMARGIN", new_y="NEXT")
    output = pdf.output()
    return bytes(output) if not isinstance(output, str) else output.encode("latin-1")


def test_continuity_fingerprint_beats_repeated_strong_anchor():
    previous = PageClassification(
        page=1,
        document_type=DocumentType.COMMERCIAL_INVOICE,
        confidence=0.99,
        block_ids=("p1",),
        strong_anchor=DocumentType.COMMERCIAL_INVOICE,
        invoice_numbers=frozenset({"406499122214"}),
        page_number=1,
        page_total=2,
    )
    current = PageClassification(
        page=2,
        document_type=DocumentType.COMMERCIAL_INVOICE,
        confidence=0.99,
        block_ids=("p2",),
        strong_anchor=DocumentType.COMMERCIAL_INVOICE,
        invoice_numbers=frozenset({"406499122214"}),
        page_number=2,
        page_total=2,
    )

    assert starts_new_document(previous, current) is False


def test_strong_anchor_starts_new_document_without_continuity():
    previous = PageClassification(
        page=2,
        document_type=DocumentType.COMMERCIAL_INVOICE,
        confidence=0.99,
        block_ids=("p2",),
        invoice_numbers=frozenset({"406499122214"}),
    )
    current = PageClassification(
        page=3,
        document_type=DocumentType.PACKING_LIST,
        confidence=0.99,
        block_ids=("p3",),
        strong_anchor=DocumentType.PACKING_LIST,
    )

    assert starts_new_document(previous, current) is True


def test_high_confidence_semantic_transition_starts_new_document():
    previous = PageClassification(
        page=3,
        document_type=DocumentType.PACKING_LIST,
        confidence=0.95,
        block_ids=("p3",),
    )
    current = PageClassification(
        page=4,
        document_type=DocumentType.BILL_OF_LADING,
        confidence=0.90,
        block_ids=("p4",),
    )

    assert starts_new_document(previous, current) is True


def test_pack_1_golden_fixture_segments_three_logical_documents_without_leakage():
    bundle = process_bundle(
        filename="pack-1-fmc-pages-7-13.pdf",
        content=_pack_1_pdf(),
    )

    assert len(bundle.documents) == 3
    assert [
        (
            logical.document_type,
            logical.page_start,
            logical.page_end,
        )
        for logical in bundle.documents
    ] == [
        (DocumentType.COMMERCIAL_INVOICE, 1, 2),
        (DocumentType.PACKING_LIST, 3, 3),
        (DocumentType.BILL_OF_LADING, 4, 7),
    ]

    invoice, packing_list, bill_of_lading = bundle.documents

    assert invoice.resolution.field("container_number").status is FieldStatus.MISSING
    assert packing_list.resolution.field("container_number").status is FieldStatus.MISSING

    container = bill_of_lading.resolution.field("container_number")
    assert container.status is FieldStatus.MATCHED
    assert container.effective_value == "MSKU0857658"
    assert container.winning_candidate is not None
    assert 4 <= container.winning_candidate.provenance.page <= 7

    assert all(
        1 <= candidate.provenance.page <= 2
        for field in invoice.resolution.fields.values()
        for candidate in field.candidates
    )
    assert all(
        candidate.provenance.page == 3
        for field in packing_list.resolution.fields.values()
        for candidate in field.candidates
    )
    assert all(
        4 <= candidate.provenance.page <= 7
        for field in bill_of_lading.resolution.fields.values()
        for candidate in field.candidates
    )


def test_pack_1_hts_lines_do_not_request_botanical_fields():
    service = DeclarationApplicabilityService()
    decisions = tuple(
        service.evaluate(
            MerchandiseLineFacts(
                line_key=f"pack1-{index}",
                hts10=hts10,
                description=description,
                entered_value=None,
                plant_material=PlantMaterialEvidence.UNKNOWN,
            )
        )
        for index, (hts10, description) in enumerate(
            (
                ("7613000000", "Cylinder"),
                ("8424890000", "Mechanical equipment"),
                ("8716805070", "Industrial cart"),
            ),
            start=1,
        )
    )

    assert all(
        decision.scope in {
            DeclarationScope.NOT_REQUIRED,
            DeclarationScope.REVIEW_REQUIRED,
        }
        for decision in decisions
    )
    assert all(decision.requires_botanical_fields is False for decision in decisions)
    assert all(
        "HTS_ON_APHIS_SCHEDULE" not in decision.reason_codes
        for decision in decisions
    )
