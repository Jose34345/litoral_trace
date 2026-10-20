from __future__ import annotations

from types import SimpleNamespace

from litoral_trace.lacey_engine.domain import (
    AdmittedCandidate,
    DocumentResolution,
    DocumentType,
    LayoutBlock,
    LayoutStructureType,
    ParsedLayout,
    Provenance,
)
from litoral_trace.lacey_engine.layout_parser import _table_blocks
from litoral_trace.lacey_engine.pipeline import ENGINE_VERSION, _extract
from litoral_trace.lacey_engine.ranking import resolve
from litoral_trace.lacey_engine.shipment import (
    ReconciliationState,
    ShipmentDocumentInput,
    process_shipment,
)
from litoral_trace.lacey_engine.source_authority import authority
from litoral_trace.us_lacey.ppq505 import PpqValidationStatus, normalize_entered_value
from litoral_trace.web.us_lacey_operational_views import processing_view


def _entry_line_rows() -> list[list[str]]:
    return [
        ["Line", "HTS", "Description", "Qty", "Entered Value"],
        ["1", "4407.11.0190", "Pinus taeda KD sawn boards", "30.000 m3", "USD 18,300.00"],
        ["2", "4407.99.0190", "Eucalyptus grandis KD sawn boards", "16.000 m3", "USD 12,640.00"],
    ]


def _origin_blocks() -> tuple[LayoutBlock, ...]:
    return (
        LayoutBlock(
            "ocr-p1-l3",
            1,
            None,
            "Lote PT-SV-2608: Pinus taeda - pais de colheita Brasil - data de colheita 2026-08-03 -",
            "OCR_LINE",
            LayoutStructureType.FREE_TEXT,
        ),
        LayoutBlock(
            "ocr-p1-l5",
            1,
            None,
            "Lote EG-SV-2608: Eucalyptus grandis - pais de colheita Brasil - data de colheita 2026-08-05 -",
            "OCR_LINE",
            LayoutStructureType.FREE_TEXT,
        ),
    )


def _resolution(filename: str, document_type: DocumentType, blocks: tuple[LayoutBlock, ...]) -> DocumentResolution:
    layout = ParsedLayout(blocks, 1)
    extracted = _extract(layout)

    def make(raw, source_score):
        provenance = Provenance(
            filename,
            raw.source_block.page,
            raw.source_block.bbox,
            raw.source_block.block_id,
            raw.source_block.text,
            raw.extractor_name,
            raw.extractor_version,
            raw.evidence_class,
        )
        return AdmittedCandidate(
            raw,
            provenance,
            60 + source_score + (10 if raw.label else 0),
            document_type,
        )

    make.document_type_for = lambda raw: document_type
    fields = {
        key: resolve(key, candidates, make)
        for key, candidates in extracted.items()
    }
    return DocumentResolution(
        filename,
        ENGINE_VERSION,
        document_type,
        1.0,
        layout,
        (),
        fields,
    )


def test_entry_rows_extract_plant_quantity_unit_and_currency_by_line():
    blocks = tuple(_table_blocks(page_number=1, table_number=2, rows=_entry_line_rows()))
    found = _extract(ParsedLayout(blocks, 1))

    quantities = {
        candidate.source_block.row_index: candidate.normalized_value
        for candidate in found["plant_quantity"]
    }
    units = {
        candidate.source_block.row_index: candidate.normalized_value
        for candidate in found["metric_unit"]
    }
    currencies = {
        candidate.source_block.row_index: candidate.normalized_value
        for candidate in found["currency"]
    }

    assert quantities == {1: "30.000", 2: "16.000"}
    assert units == {1: "m3", 2: "m3"}
    assert currencies == {1: "USD", 2: "USD"}


def test_supplier_origin_free_text_extracts_explicit_harvest_country_for_each_taxon():
    found = _extract(ParsedLayout(_origin_blocks(), 1))

    harvest = [candidate.normalized_value for candidate in found["country_of_harvest"]]
    assert harvest == ["Brasil", "Brasil"]


def test_shipment_reconciles_explicit_taxon_rows_with_supplier_harvest_evidence():
    entry = _resolution(
        "06_Entry_Worksheet_EN.pdf",
        DocumentType.CUSTOMS_ENTRY_SUMMARY,
        tuple(_table_blocks(page_number=1, table_number=2, rows=_entry_line_rows())),
    )
    origin = _resolution(
        "05_Declaracao_Origem_Fornecedor_SCAN_PT.pdf",
        DocumentType.SUPPLIER_DECLARATION,
        _origin_blocks(),
    )

    shipment = process_shipment(
        documents=[
            ShipmentDocumentInput("entry", entry.filename, resolution=entry),
            ShipmentDocumentInput("origin", origin.filename, resolution=origin),
        ]
    )

    for field_name in (
        "genus",
        "species",
        "country_of_harvest",
        "plant_quantity",
        "metric_unit",
        "hts_code",
        "description",
        "entered_value",
    ):
        assert shipment.canonical_fields[field_name].state is ReconciliationState.SUPPORTED_MULTIPLE

    assert shipment.canonical_fields["currency"].state in {
        ReconciliationState.SUPPORTED,
        ReconciliationState.SUPPORTED_MULTIPLE,
    }
    assert {value.value for value in shipment.canonical_fields["country_of_harvest"].values} == {"BRAZIL"}
    assert {value.value for value in shipment.canonical_fields["plant_quantity"].values} == {"30.000", "16.000"}
    assert {value.value for value in shipment.canonical_fields["metric_unit"].values} == {"M3"}
    assert {value.value for value in shipment.canonical_fields["hts_code"].values} == {"4407110190", "4407990190"}
    assert {value.value for value in shipment.canonical_fields["entered_value"].values} == {"18300", "12640"}
    assert not {
        issue.field_key
        for issue in shipment.issues
        if issue.issue_type == "AMBIGUOUS_ASSOCIATION"
    } & {
        "genus",
        "species",
        "country_of_harvest",
        "plant_quantity",
        "metric_unit",
        "hts_code",
        "description",
        "entered_value",
    }


def test_duplicate_same_taxon_rows_preserve_distinct_component_identity():
    rows = [
        ["Line", "HTS", "Description", "Qty", "Entered Value"],
        ["1", "4407.11.0190", "Pinus taeda KD boards grade A", "30.000 m3", "USD 18,300.00"],
        ["2", "4407.11.0191", "Pinus taeda KD boards grade B", "12.000 m3", "USD 7,320.00"],
    ]
    entry = _resolution(
        "duplicate-pinus-entry.pdf",
        DocumentType.CUSTOMS_ENTRY_SUMMARY,
        tuple(_table_blocks(page_number=1, table_number=4, rows=rows)),
    )

    shipment = process_shipment(
        documents=[ShipmentDocumentInput("entry", entry.filename, resolution=entry)]
    )

    quantities = shipment.canonical_fields["plant_quantity"]
    assert quantities.state is ReconciliationState.SUPPORTED_MULTIPLE
    assert {value.value for value in quantities.values} == {"30.000", "12.000"}
    assert len({item.component_key for item in quantities.supporting_evidence}) == 2


def test_ppq_entered_value_accepts_an_explicit_iso_currency_prefix_without_storing_it():
    result = normalize_entered_value("USD 18,300.00")
    assert result.status is PpqValidationStatus.VALID
    assert result.normalized_value == "18300"


def test_final_running_job_reports_reconciliation_progress_instead_of_static_sixty_percent():
    documents = tuple(
        SimpleNamespace(job_status="COMPLETED", processing_status="EXTRACTED")
        for _ in range(6)
    ) + (
        SimpleNamespace(job_status="RUNNING", processing_status="EXTRACTED"),
    )
    detail = SimpleNamespace(documents=documents, status="PROCESSING")

    view = processing_view(detail)

    assert view.terminal is False
    assert view.percent >= 85
    assert "reconcil" in view.message.casefold() or "prepar" in view.message.casefold()
