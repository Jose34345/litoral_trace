"""Field-level canonical source checks on original synthetic fixture bytes."""
from pathlib import Path

from fpdf import FPDF
import pytest

from litoral_trace.lacey_engine.serialization import serialize_shipment_resolution
from litoral_trace.lacey_engine.shipment import ShipmentDocumentInput, process_shipment
from litoral_trace.us_lacey.canonical_shipment_truth import (
    CanonicalTruthState, _primary_evidence, build_canonical_shipment_truth,
)


def _canonical(inputs):
    return build_canonical_shipment_truth(
        serialize_shipment_resolution(process_shipment(documents=inputs))
    )


@pytest.mark.parametrize("pack", ["pack1", "pack2"])
def test_synthetic_packs_prefer_original_carrier_anchor_without_promoting_references(pack):
    root = Path(__file__).resolve().parents[1] / "fixtures" / "assurance_v2_pdf_e2e" / pack
    inputs = [
        ShipmentDocumentInput(path.name, path.name, path.read_bytes())
        for path in sorted(root.glob("*.pdf"))
    ]
    fields = _canonical(inputs).shipment_fields

    bill = fields["bill_of_lading"]
    assert bill.state is CanonicalTruthState.REVIEW_REQUIRED
    assert len(bill.values) == 1
    primary = _primary_evidence(bill)
    assert primary.document_id.startswith("03_House_Bill_of_Lading_")
    assert primary.source_page == 1
    assert ":house_bill_of_lading:p1-l5:" in primary.candidate_id
    assert any(row.document_id.startswith("01_Commercial_Invoice_") for row in bill.evidence)
    assert any(row.document_id.startswith("05_Entry_Preparation_Worksheet_") for row in bill.evidence)

    for field_name, anchor in (("estimated_arrival_date", "p1-l14"), ("manufacturer_id", "p1-l12")):
        proposal = fields[field_name]
        assert proposal.state is CanonicalTruthState.REVIEW_REQUIRED
        assert len(proposal.values) == 1
        assert all(row.document_id.startswith("05_Entry_Preparation_Worksheet_") for row in proposal.evidence)
        assert any(f":{anchor}:" in row.candidate_id for row in proposal.evidence)
    assert fields["filing_entry_reference"].state is CanonicalTruthState.MISSING

    only_references = [item for item in inputs if item.filename.startswith(("01_", "05_"))]
    reference_bill = _canonical(only_references).shipment_fields["bill_of_lading"]
    assert reference_bill.state is CanonicalTruthState.REVIEW_REQUIRED
    assert all(row.document_id.startswith(("01_", "05_")) for row in reference_bill.evidence)


def test_an_original_entry_summary_is_distinct_from_a_named_preparation_worksheet():
    from litoral_trace.us_lacey.document_field_authority import is_preparation_source

    assert not is_preparation_source("CUSTOMS_ENTRY_SUMMARY", "cbp-form-7501.pdf")
    assert is_preparation_source("CUSTOMS_ENTRY_SUMMARY", "entry-preparation-worksheet.pdf")


def test_current_original_carrier_field_remains_supported_as_candidate():
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Helvetica", size=11)
    for text in ("BILL OF LADING", "B/L No: MAEU274342495"):
        pdf.cell(0, 8, text=text, new_x="LMARGIN", new_y="NEXT")
    content = bytes(pdf.output())
    fields = _canonical([ShipmentDocumentInput("carrier", "document.pdf", content)]).shipment_fields
    bill = fields["bill_of_lading"]
    assert bill.state is CanonicalTruthState.SUPPORTED
    assert bill.values == ("MAEU274342495",)
    assert _primary_evidence(bill).document_id == "carrier"
    assert _primary_evidence(bill).source_page == 1



@pytest.mark.parametrize(
    "title,filename,expected",
    [
        ("BOTANICAL SUPPLIER DECLARATION", "vendor_record.pdf", CanonicalTruthState.REVIEW_REQUIRED),
        ("ARRIVAL NOTICE", "botanical-declaration.pdf", CanonicalTruthState.REVIEW_REQUIRED),
        ("ARRIVAL NOTICE", "carrier_notice.pdf", CanonicalTruthState.SUPPORTED),
    ],
    ids=["supplier", "role-conflict", "carrier"],
)
def test_eta_source_role_controls_support_without_discarding_the_candidate(title, filename, expected):
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Helvetica", size=11)
    for text in (title, "Estimated Arrival Date: 2026-11-15"):
        pdf.cell(0, 8, text=text, new_x="LMARGIN", new_y="NEXT")
    fields = _canonical(
        [ShipmentDocumentInput("document", filename, bytes(pdf.output()))]
    ).shipment_fields
    eta = fields["estimated_arrival_date"]

    assert eta.state is expected
    assert eta.values == ("2026-11-15",)
    assert _primary_evidence(eta).source_page == 1
    assert _primary_evidence(eta).filename == filename


def test_renamed_worksheet_keeps_eta_and_mid_in_review():
    worksheet = (
        Path(__file__).resolve().parents[1] / "fixtures" / "assurance_v2_pdf_e2e"
        / "pack2" / "05_Entry_Preparation_Worksheet_Shipment2.pdf"
    )
    fields = _canonical([
        ShipmentDocumentInput("worksheet", "supporting_record.pdf", worksheet.read_bytes())
    ]).shipment_fields

    for name in ("estimated_arrival_date", "manufacturer_id"):
        field = fields[name]
        assert field.state is CanonicalTruthState.REVIEW_REQUIRED
        assert len(field.values) == 1
        assert _primary_evidence(field).document_type == "UNKNOWN"
        assert _primary_evidence(field).source_page == 1


def test_literal_mid_on_original_cbp_form_remains_supported():
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Helvetica", size=11)
    for text in ("CBP FORM 7501", "Manufacturer ID: CNFUZLON88JIN"):
        pdf.cell(0, 8, text=text, new_x="LMARGIN", new_y="NEXT")
    fields = _canonical([
        ShipmentDocumentInput("entry-summary", "entry-summary.pdf", bytes(pdf.output()))
    ]).shipment_fields

    mid = fields["manufacturer_id"]
    assert mid.state is CanonicalTruthState.SUPPORTED
    assert mid.values == ("CNFUZLON88JIN",)
    assert _primary_evidence(mid).document_type == "CUSTOMS_ENTRY_SUMMARY"
    assert _primary_evidence(mid).source_page == 1
