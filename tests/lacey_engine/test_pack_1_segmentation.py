from __future__ import annotations

from fpdf import FPDF
import pytest

from litoral_trace.lacey_engine.domain import DocumentType, FieldStatus
from litoral_trace.lacey_engine.errors import UnsupportedDocumentDomainError
from litoral_trace.lacey_engine.pipeline import process_bundle
from litoral_trace.lacey_engine.segmentation import (
    DocumentDomain,
    PageClassification,
    classify_domain_text,
    starts_new_document,
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
            "Claim Amount: USD 11,668.25\n"
            "OCR Total Candidate: vtcNT...KG\n"
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
            "ROSE CONTAINERLINE C/O AZ MIDWEST CFS SCAC: CTII\n"
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

    seal = bill_of_lading.resolution.field("seal_number")
    assert seal.status is FieldStatus.MATCHED
    assert seal.effective_value == "6548158"
    assert seal.winning_candidate is not None
    assert 4 <= seal.winning_candidate.provenance.page <= 7

    invoice_total = invoice.resolution.field("invoice_total")
    assert invoice_total.status is FieldStatus.MATCHED
    assert invoice_total.effective_value == "31110.21"
    assert invoice_total.winning_candidate is not None
    assert 1 <= invoice_total.winning_candidate.provenance.page <= 2

    # Carrier SCAC context must not become a CBP Manufacturer Identification Code.
    assert bill_of_lading.resolution.field("manufacturer_id").status is FieldStatus.MISSING

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



def _single_page_pdf(text: str) -> bytes:
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Helvetica", size=11)
    for line in text.splitlines():
        pdf.cell(0, 8, text=line, new_x="LMARGIN", new_y="NEXT")
    output = pdf.output()
    return bytes(output) if not isinstance(output, str) else output.encode("latin-1")


def test_legal_pleading_anchors_override_incidental_trade_terms():
    classification = classify_domain_text(
        "BEFORE THE FEDERAL MARITIME COMMISSION\n"
        "DOCKET NO. 25-16\n"
        "IWG INTERNATIONAL WOOD GROUP, Complainants\n"
        "VERSUS DB SCHENKER USA, INC., Respondent\n"
        "The pleading discusses a Bill of Lading and commercial invoice."
    )

    assert classification.domain is DocumentDomain.COURT_PLEADING
    assert classification.rejected is True


def test_initial_decision_anchor_rejects_legal_decision_even_when_logs_are_shipped():
    classification = classify_domain_text(
        "FEDERAL MARITIME COMMISSION\n"
        "Office of Administrative Law Judges\n"
        "DOCKET NO. 25-16\n"
        "INITIAL DECISION\n"
        "The record discusses shipments of logs and bills of lading."
    )

    assert classification.domain is DocumentDomain.LEGAL_DECISION
    assert classification.rejected is True


def test_email_thread_requires_multiple_header_anchors():
    classification = classify_domain_text(
        "From: broker@example.com\n"
        "Sent: Monday, May 4, 2026 9:00 AM\n"
        "To: importer@example.com\n"
        "Subject: shipment question\n"
        "Please see below."
    )

    assert classification.domain is DocumentDomain.EMAIL_THREAD
    assert classification.rejected is True


def test_process_bundle_rejects_legal_document_before_layout_extraction(monkeypatch):
    content = _single_page_pdf(
        "FEDERAL MARITIME COMMISSION\n"
        "Office of Administrative Law Judges\n"
        "DOCKET NO. 25-16\n"
        "INITIAL DECISION\n"
        "Shipment of logs under a Bill of Lading"
    )
    monkeypatch.setattr(
        "litoral_trace.lacey_engine.pipeline.parse_layout",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("layout extraction must not run for manifest legal decisions")
        ),
    )

    with pytest.raises(UnsupportedDocumentDomainError) as excinfo:
        process_bundle(filename="initial-decision.pdf", content=content)

    assert excinfo.value.code == "UNSUPPORTED_DOMAIN"
    assert excinfo.value.domain == "LEGAL_DECISION"
