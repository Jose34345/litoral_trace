from __future__ import annotations

from types import SimpleNamespace
from uuid import NAMESPACE_URL, uuid4, uuid5

from litoral_trace.lacey_engine.ai_shadow import AICandidate
from litoral_trace.lacey_engine.domain import EvidenceClass, ParsedLayout
from litoral_trace.lacey_engine.layout_parser import _table_blocks
from litoral_trace.lacey_engine.multi_agent.contracts import (
    CandidateEnvelope,
    DocumentType,
    SpecialistRole,
)
from litoral_trace.lacey_engine.multi_agent.line_binding import bind_line_items
from litoral_trace.lacey_engine.pipeline import _extract
from litoral_trace.us_lacey.projection import (
    _explicit_plant_data_rows,
    _has_explicit_line_entered_value,
    _is_line_allocation_table,
    _target_field,
)


def _entry_summary_rows() -> list[list[str]]:
    return [
        ["Entry Ref", "GWS-ENTRY-C-042", "Importer", "Gulf Wood Supply Inc."],
        ["BOL", "RPT-HOU-260913-42", "Container", "CMAU8842110"],
        ["Total Entered Value", "USD 30,940.00", "Total Volume", "46.000 m3"],
        ["Country of Origin", "Brazil", "Lines", "2"],
    ]


def _entry_line_rows() -> list[list[str]]:
    return [
        ["Line", "HTS", "Description", "Qty", "Entered Value"],
        ["1", "4407.11.0190", "Pinus taeda KD sawn boards", "30.000 m3", "USD 18,300.00"],
        ["2", "4407.99.0190", "Eucalyptus grandis KD sawn boards", "16.000 m3", "USD 12,640.00"],
    ]


def test_entry_summary_key_value_matrix_extracts_only_the_real_importer_name():
    blocks = _table_blocks(page_number=1, table_number=1, rows=_entry_summary_rows())
    found = _extract(ParsedLayout(tuple(blocks), 1))

    assert [candidate.normalized_value for candidate in found["importer_name"]] == [
        "Gulf Wood Supply Inc."
    ]


def test_entry_line_matrix_preserves_description_hts_and_entered_value_by_row():
    blocks = _table_blocks(page_number=1, table_number=2, rows=_entry_line_rows())
    found = _extract(ParsedLayout(tuple(blocks), 1))

    descriptions = {
        candidate.source_block.row_index: candidate.normalized_value
        for candidate in found["description"]
    }
    hts_codes = {
        candidate.source_block.row_index: candidate.normalized_value
        for candidate in found["hts_code"]
    }
    entered_values = {
        candidate.source_block.row_index: candidate.normalized_value
        for candidate in found["entered_value"]
    }

    assert descriptions == {
        1: "Pinus taeda KD sawn boards",
        2: "Eucalyptus grandis KD sawn boards",
    }
    assert hts_codes == {1: "4407.11.0190", 2: "4407.99.0190"}
    assert entered_values == {1: "18300.00", 2: "12640.00"}


def test_customs_line_table_admits_entered_value_as_explicit_line_allocation():
    source = SimpleNamespace(
        field_name="raw.table.2.Entered Value",
        original_value="USD 18,300.00",
        normalized_value="USD 18,300.00",
        source_locator="pdf:page:1;table:2;header_row:1;data_row:1;column:5",
    )
    table_headers = {
        2: frozenset({"line", "hts", "description", "qty", "entered value"})
    }

    assert _target_field(source, table_headers=table_headers) == ("entered_value", 3)
    assert _has_explicit_line_entered_value(
        [source], table_headers=table_headers
    ) is True


def test_commercial_pricing_table_is_not_a_customs_line_allocation():
    headers = frozenset(
        {"line", "sku", "description", "hts", "qty", "unit price", "entered value"}
    )

    assert _is_line_allocation_table(headers) is False


def test_customs_entry_rows_can_materialize_lines_before_botanical_documents_arrive():
    table_headers = {
        2: frozenset({"line", "hts", "description", "qty", "entered value"})
    }
    extracted = [
        SimpleNamespace(
            field_name="raw.table.2.HTS",
            original_value="4407.11.0190",
            normalized_value="4407.11.0190",
            source_locator="pdf:page:1;table:2;header_row:1;data_row:1;column:2",
        ),
        SimpleNamespace(
            field_name="raw.table.2.Entered Value",
            original_value="USD 18,300.00",
            normalized_value="USD 18,300.00",
            source_locator="pdf:page:1;table:2;header_row:1;data_row:1;column:5",
        ),
        SimpleNamespace(
            field_name="raw.table.2.HTS",
            original_value="4407.99.0190",
            normalized_value="4407.99.0190",
            source_locator="pdf:page:1;table:2;header_row:1;data_row:2;column:2",
        ),
        SimpleNamespace(
            field_name="raw.table.2.Entered Value",
            original_value="USD 12,640.00",
            normalized_value="USD 12,640.00",
            source_locator="pdf:page:1;table:2;header_row:1;data_row:2;column:5",
        ),
    ]

    assert _explicit_plant_data_rows(
        extracted,
        table_headers=table_headers,
    ) == (1, 2)


def _candidate(field_key: str, value: str) -> AICandidate:
    return AICandidate(
        field_key=field_key,
        value=value,
        normalized_value=value,
        evidence_class=EvidenceClass.EXPLICIT,
        page=1,
        source_text=value,
        confidence=0.97,
        provider="fixture",
        model="fixture",
    )


def _envelope(
    field_key: str,
    value: str,
    *,
    line_key: str,
    document_type: DocumentType,
) -> CandidateEnvelope:
    return CandidateEnvelope(
        candidate=_candidate(field_key, value),
        document_id=uuid5(NAMESPACE_URL, f"{document_type.value}:{line_key}"),
        document_type=document_type,
        specialist=(
            SpecialistRole.BOTANICAL
            if document_type is DocumentType.BOTANICAL_DECLARATION
            else SpecialistRole.COMMERCIAL_LINES
        ),
        agent_run_id=uuid4(),
        line_item_key=None,
        source_span_id=None,
        source_line_key=line_key,
        source_table_id="lines",
        source_row_index=None,
    )


def test_cross_document_binding_reconciles_line_numbers_to_unique_botanical_skus():
    candidates = (
        _envelope(
            "description",
            "Pinus taeda KD sawn boards",
            line_key="1",
            document_type=DocumentType.ENTRY_WORKSHEET,
        ),
        _envelope(
            "hts_code",
            "4407110190",
            line_key="1",
            document_type=DocumentType.ENTRY_WORKSHEET,
        ),
        _envelope(
            "entered_value",
            "18300",
            line_key="1",
            document_type=DocumentType.ENTRY_WORKSHEET,
        ),
        _envelope(
            "description",
            "Eucalyptus grandis KD sawn boards",
            line_key="2",
            document_type=DocumentType.ENTRY_WORKSHEET,
        ),
        _envelope(
            "hts_code",
            "4407990190",
            line_key="2",
            document_type=DocumentType.ENTRY_WORKSHEET,
        ),
        _envelope(
            "entered_value",
            "12640",
            line_key="2",
            document_type=DocumentType.ENTRY_WORKSHEET,
        ),
        _envelope(
            "genus",
            "Pinus",
            line_key="PT-38",
            document_type=DocumentType.BOTANICAL_DECLARATION,
        ),
        _envelope(
            "species",
            "Pinus taeda",
            line_key="PT-38",
            document_type=DocumentType.BOTANICAL_DECLARATION,
        ),
        _envelope(
            "genus",
            "Eucalyptus",
            line_key="EG-22",
            document_type=DocumentType.BOTANICAL_DECLARATION,
        ),
        _envelope(
            "species",
            "Eucalyptus grandis",
            line_key="EG-22",
            document_type=DocumentType.BOTANICAL_DECLARATION,
        ),
    )

    bound = bind_line_items(candidates)
    actual = {
        (candidate.candidate.field_key, candidate.candidate.value): candidate.line_item_key
        for candidate in bound
    }

    assert actual[("description", "Pinus taeda KD sawn boards")] == "SKU:PT-38"
    assert actual[("hts_code", "4407110190")] == "SKU:PT-38"
    assert actual[("entered_value", "18300")] == "SKU:PT-38"
    assert actual[("genus", "Pinus")] == "SKU:PT-38"
    assert actual[("species", "Pinus taeda")] == "SKU:PT-38"

    assert actual[("description", "Eucalyptus grandis KD sawn boards")] == "SKU:EG-22"
    assert actual[("hts_code", "4407990190")] == "SKU:EG-22"
    assert actual[("entered_value", "12640")] == "SKU:EG-22"
    assert actual[("genus", "Eucalyptus")] == "SKU:EG-22"
    assert actual[("species", "Eucalyptus grandis")] == "SKU:EG-22"
