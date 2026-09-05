from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
import sys

import pytest

from litoral_trace.lacey_engine.admission import admit
from litoral_trace.lacey_engine.domain import (
    EvidenceClass, FieldStatus, LayoutBlock, ParsedLayout, RawCandidate, ResolvedField,
)
import litoral_trace.lacey_engine.layout_parser as layout_parser
from litoral_trace.lacey_engine.layout_parser import layout_from_key_value_rows
from litoral_trace.lacey_engine.pipeline import _extract, process_document
from litoral_trace.lacey_engine.classifier import classify
from litoral_trace.lacey_engine.segmentation import segment
from litoral_trace.lacey_engine.domain import DocumentType
from litoral_trace.lacey_engine.shipment import ReconciliationState, ShipmentDocumentInput, process_shipment


def _raw(field: str, value: str, label: str) -> RawCandidate:
    block = LayoutBlock("test-1", 1, None, f"{label}: {value}", "TABLE_ROW", key_text=label, value_text=value)
    return RawCandidate(field, value, value, block, EvidenceClass.EXPLICIT, "test", "1", label=label)


def test_vertical_key_value_table_keeps_each_row_value_with_its_own_label():
    layout = layout_from_key_value_rows([
        ("Container Number", "MSKU9228574"),
        ("Seal Number 1", "NZ681338"),
        ("Container Type", "45G1"),
    ])
    assert [(block.key_text, block.value_text) for block in layout.blocks] == [
        ("Container Number", "MSKU9228574"),
        ("Seal Number 1", "NZ681338"),
        ("Container Type", "45G1"),
    ]
    extracted = _extract(layout)
    assert [candidate.normalized_value for candidate in extracted["container_number"]] == ["MSKU9228574"]


def test_container_admission_rejects_labels_and_accepts_explicit_iso_shape():
    assert admit(_raw("container_number", "MSKU9228574", "Container Number"))
    assert not admit(_raw("container_number", "Seal Number 1", "Container Number"))
    assert not admit(_raw("container_number", "Container Height", "Container Number"))


@pytest.mark.parametrize("label", ("Commodity Description", "Cargo Description 1"))
def test_explicit_merchandise_description_is_extracted_and_admitted(monkeypatch, label):
    layout = layout_from_key_value_rows([(label, "SINGLE PACKS OF PINUS RADIATA TIMBER")])
    monkeypatch.setattr("litoral_trace.lacey_engine.pipeline.parse_layout", lambda *_: layout)
    resolution = process_document(filename="description.pdf", content=b"unused")
    field = resolution.field("description")
    assert field.status is FieldStatus.MATCHED and "PINUS RADIATA TIMBER" in field.effective_value
    assert field.winning_candidate.raw.evidence_class is EvidenceClass.EXPLICIT and field.winning_candidate.raw.label == label


@pytest.mark.parametrize("label", ("Equipment Description", "Description"))
def test_non_merchandise_description_labels_remain_missing(monkeypatch, label):
    layout = layout_from_key_value_rows([(label, "Opening(s) at one end or both ends.")])
    monkeypatch.setattr("litoral_trace.lacey_engine.pipeline.parse_layout", lambda *_: layout)
    assert process_document(filename="equipment.pdf", content=b"unused").field("description").status is FieldStatus.MISSING


def test_scientific_taxon_yields_explicit_species_and_derived_genus():
    layout = ParsedLayout((LayoutBlock("p1-l1", 1, None, "SINGLE PACKS OF PINUS RADIATA TIMBER", "TEXT_LINE"),), 1)
    extracted = _extract(layout)
    assert extracted["species"][0].normalized_value == "radiata"
    assert extracted["species"][0].evidence_class is EvidenceClass.EXPLICIT
    assert extracted["genus"][0].normalized_value == "Pinus"
    assert extracted["genus"][0].evidence_class is EvidenceClass.DERIVED
    assert extracted["genus"][0].derived_from_field_key == "species"


def test_split_taxon_is_missing_and_never_cross_block_crashes():
    layout = ParsedLayout((LayoutBlock("a", 1, None, "SINGLE PACKS OF PINUS", "TEXT_LINE"), LayoutBlock("b", 1, None, "RADIATA TIMBER", "TEXT_LINE")), 1)
    extracted = _extract(layout)
    assert extracted["species"] == []
    assert extracted["genus"] == []


@pytest.mark.parametrize(("text", "expected"), [
    ("COMMERCIAL INVOICE\nInvoice Number 12345\nMaster BOL MAEU123456789", DocumentType.COMMERCIAL_INVOICE),
    ("ARRIVAL NOTICE\nEstimated Arrival Date 2026-09-01\nMaster BOL MAEU123456789", DocumentType.ARRIVAL_NOTICE),
    ("OCEAN BILL OF LADING\nMaster BOL MAEU123456789", DocumentType.BILL_OF_LADING),
])
def test_title_scoring_beats_referenced_fields(text, expected):
    layout = ParsedLayout((LayoutBlock("p1", 1, None, text, "TEXT_LINE"),), 1)
    assert classify(layout)[0] is expected


def test_packet_segmentation_keeps_continuation_and_uses_section_type():
    layout = ParsedLayout((
        LayoutBlock("p1", 1, None, "COMMERCIAL INVOICE Invoice Number 1001 Master BOL MAEU111111111", "TEXT_LINE"),
        LayoutBlock("p2", 2, None, "invoice continuation", "TEXT_LINE"),
        LayoutBlock("p3", 3, None, "OCEAN BILL OF LADING Master BOL MAEU111111111", "TEXT_LINE"),
    ), 3)
    sections = segment(layout, DocumentType.COMMERCIAL_INVOICE)
    assert [(section.page_start, section.page_end, section.document_type) for section in sections] == [(1, 2, DocumentType.COMMERCIAL_INVOICE), (3, 3, DocumentType.BILL_OF_LADING)]


def test_page_count_is_not_derived_from_last_block_page():
    layout = ParsedLayout((LayoutBlock("p1", 1, None, "COMMERCIAL INVOICE", "TEXT_LINE"),), 5)
    assert segment(layout, DocumentType.COMMERCIAL_INVOICE)[-1].page_end == 5


def test_layout_ocr_decision_is_per_page_and_keeps_pdf_page_count(monkeypatch):
    class Page:
        def __init__(self, digital=False): self.digital = digital
        def extract_text_lines(self, **_kwargs): return [{"text": "Commercial Invoice", "x0": 0, "top": 0, "x1": 100, "bottom": 10}] if self.digital else []
        def extract_words(self, **_kwargs): return []
        def find_tables(self): return []
    class Pdf:
        pages = [Page(True), Page()]
        def __enter__(self): return self
        def __exit__(self, *_args): return None
    calls = []
    monkeypatch.setitem(sys.modules, "pdfplumber", SimpleNamespace(open=lambda _source: Pdf()))
    monkeypatch.setattr(layout_parser, "_ocr_blocks", lambda _content, pages: calls.append(pages) or [LayoutBlock(f"ocr-p{next(iter(pages))}-l1", next(iter(pages)), None, "OCR", "OCR_LINE")])
    parsed = layout_parser._pdf_layout(b"%PDF-test")
    assert parsed.page_count == 2
    assert calls == [{2}]
    assert [block.block_id for block in parsed.blocks] == ["p1-l1", "ocr-p2-l1"]


def test_country_of_harvest_is_not_inferred_from_new_zealand_context():
    layout = layout_from_key_value_rows([
        ("Supplier", "Southwood NZ Limited"),
        ("Port", "Port Chalmers, New Zealand"),
        ("Country Code", "NZ"),
    ])
    assert _extract(layout)["country_of_harvest"] == []


def test_field_status_invariants_reject_impossible_states():
    with pytest.raises(ValueError, match="MATCHED"):
        ResolvedField("container_number", FieldStatus.MATCHED, None, None)
    with pytest.raises(ValueError, match="MISSING"):
        ResolvedField("container_number", FieldStatus.MISSING, None, object())
    with pytest.raises(ValueError, match="CONFLICT"):
        ResolvedField("container_number", FieldStatus.CONFLICT, None, None, ())


def test_real_import_info_gate1_regression_fixture():
    fixture = Path(__file__).parents[1] / "fixtures" / "import_info_wood_brokerage_real.pdf"
    if not fixture.exists():
        pytest.skip(f"REAL_FIXTURE_NOT_AVAILABLE: {fixture}")
    resolution = process_document(filename=fixture.name, content=fixture.read_bytes(), role_hint="SUPPLIER_SHEET")
    expected = {
        "estimated_arrival_date": "2026-09-01",
        "bill_of_lading": "MAEU274342495",
        "container_number": "MSKU9228574",
        "consignee_name": "WOOD BROKERAGE INTERNATIONAL",
        "species": "radiata",
        "genus": "Pinus",
    }
    for key, value in expected.items():
        assert resolution.field(key).status is FieldStatus.MATCHED
        assert resolution.field(key).effective_value == value
        winner = resolution.field(key).winning_candidate
        assert winner is not None
        assert winner.provenance.page >= 1
        assert winner.provenance.bbox is not None
        assert winner.provenance.block_id
        assert winner.provenance.source_text
        assert winner.provenance.extractor_name
        assert winner.provenance.extractor_version
    assert resolution.field("consignee_address").status is FieldStatus.MATCHED
    assert resolution.field("consignee_address").effective_value == "SUITE 130 5285 MEADOWS RD LAKE OSWE; LAKE OSWEGO, OR 97035"
    for key in ("filing_entry_reference", "manufacturer_id", "hts_code", "country_of_harvest"):
        assert resolution.field(key).status is FieldStatus.MISSING
    description = resolution.field("description")
    assert description.status is FieldStatus.MATCHED
    assert "PINUS RADIATA TIMBER" in description.effective_value
    assert "Opening(s) at one end or both ends." not in description.effective_value
    assert resolution.field("plant_quantity").status is FieldStatus.MISSING
    shipment = process_shipment(documents=[ShipmentDocumentInput("golden-001", fixture.name, resolution=resolution)])
    assert shipment.canonical_fields["description"].state in {ReconciliationState.SUPPORTED, ReconciliationState.SUPPORTED_MULTIPLE}
    assert any("PINUS RADIATA TIMBER" in value.value for value in shipment.canonical_fields["description"].values)
    assert shipment.canonical_fields["country_of_harvest"].state is ReconciliationState.MISSING
    assert shipment.canonical_fields["plant_quantity"].state is ReconciliationState.MISSING
    assert shipment.metrics["fields_conflicting"] == 0
