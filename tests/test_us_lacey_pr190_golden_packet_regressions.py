from __future__ import annotations

import base64
import hashlib
from pathlib import Path
import re
from types import SimpleNamespace
import zlib

from litoral_trace.assurance.extraction import extract_structured_fields
from litoral_trace.assurance.parsers import parse_document
from litoral_trace.us_lacey.projection import _fold, _target_field


_FIXTURE = Path(__file__).parent / "fixtures" / "us_lacey_golden_packet_01.pdf.zlib.b64"
_GOLDEN_SHA256 = "6061e66f3b80b94ed69a87c3295f30ead300898908f2baea0657875388da4c50"
_RAW_TABLE = re.compile(r"^raw\.table\.(?P<table>\d+)\.(?P<header>.+)$")
_DATA_ROW = re.compile(r"(?:^|;)data_row:(?P<row>\d+)(?:;|$)")


def _golden_packet_bytes() -> bytes:
    compressed = base64.b64decode("".join(_FIXTURE.read_text(encoding="ascii").split()))
    payload = zlib.decompress(compressed)
    assert hashlib.sha256(payload).hexdigest() == _GOLDEN_SHA256
    return payload


def _raw_cells(parsed):
    """Mirror AssuranceProcessingService._persist_raw_parsed_fields exactly enough for projection."""
    rows = []
    for table_index, table in enumerate(parsed.tables, start=1):
        for row_index, record in enumerate(table.rows, start=1):
            for column_index, header in enumerate(table.headers, start=1):
                value = record.get(header)
                if value is None:
                    continue
                rows.append(
                    SimpleNamespace(
                        field_name=f"raw.table.{table_index}.{header}",
                        original_value=str(value),
                        normalized_value=str(value),
                        source_page=table.source.page,
                        source_locator=(
                            f"{table.source.locator or table.name};"
                            f"data_row:{row_index};column:{column_index}"
                        ),
                    )
                )
    return rows


def _headers_by_table(rows):
    result: dict[int, set[str]] = {}
    for row in rows:
        match = _RAW_TABLE.match(row.field_name)
        if match:
            result.setdefault(int(match.group("table")), set()).add(_fold(match.group("header")))
    return {key: frozenset(value) for key, value in result.items()}


def _table_id(row) -> int:
    match = _RAW_TABLE.match(row.field_name)
    assert match is not None
    return int(match.group("table"))


def _data_row(row) -> int:
    match = _DATA_ROW.search(row.source_locator)
    assert match is not None
    return int(match.group("row"))


def _find_raw(rows, *, header: str, value: str):
    folded_header = _fold(header)
    for row in rows:
        match = _RAW_TABLE.match(row.field_name)
        if match and _fold(match.group("header")) == folded_header and row.original_value == value:
            return row
    raise AssertionError(f"Golden Packet cell not found: {header}={value}")


def _projection_sources(parsed):
    # Production projection sees both raw persisted cells and deterministic
    # structured fields. Keep both paths in this regression so a semantic filter
    # cannot be bypassed by a different extractor representation.
    raw = _raw_cells(parsed)
    structured = [
        SimpleNamespace(
            field_name=item.field_name,
            original_value=item.original_value,
            normalized_value=item.normalized_value,
            source_page=item.source_page,
            source_locator=item.source_locator,
        )
        for item in extract_structured_fields(parsed)
    ]
    return raw + structured


def test_exact_golden_packet_fixture_and_real_plant_rows_are_stable():
    payload = _golden_packet_bytes()
    parsed = parse_document("LitoralTrace_Lacey_Golden_Test_Packet_01.pdf", payload)
    rows = _raw_cells(parsed)

    quantity_1440 = _find_raw(rows, header="Plant quantity", value="1,440")
    quantity_360 = _find_raw(rows, header="Plant quantity", value="360")
    unit_1440 = _find_raw(rows, header="Unit", value="KG")
    same_table_units = [
        row
        for row in rows
        if _table_id(row) == _table_id(quantity_1440)
        and _fold(_RAW_TABLE.match(row.field_name).group("header")) == "unit"
    ]

    assert len(payload) == 18440
    assert _table_id(quantity_1440) == _table_id(quantity_360)
    assert {_data_row(row) for row in same_table_units} == {1, 2}
    assert {_data_row(quantity_1440), _data_row(quantity_360)} == {1, 2}
    assert unit_1440.original_value == "KG"


def test_exact_golden_packet_unit_is_projected_only_inside_plant_declaration_table():
    parsed = parse_document(
        "LitoralTrace_Lacey_Golden_Test_Packet_01.pdf", _golden_packet_bytes()
    )
    rows = _raw_cells(parsed)
    headers_by_table = _headers_by_table(rows)

    quantity_1440 = _find_raw(rows, header="Plant quantity", value="1,440")
    quantity_360 = _find_raw(rows, header="Plant quantity", value="360")
    plant_table = _table_id(quantity_1440)
    plant_units = [
        row
        for row in rows
        if _table_id(row) == plant_table
        and _fold(_RAW_TABLE.match(row.field_name).group("header")) == "unit"
    ]

    assert _target_field(quantity_1440, table_headers=headers_by_table) == ("plant_quantity", 3)
    assert _target_field(quantity_360, table_headers=headers_by_table) == ("plant_quantity", 3)
    assert {
        (_data_row(row), row.original_value, _target_field(row, table_headers=headers_by_table)[0])
        for row in plant_units
    } == {(1, "KG", "metric_unit"), (2, "KG", "metric_unit")}

    # The same document contains a commercial "Unit Price" column. It is not a
    # regulatory plant unit and must never enter the PPQ candidate pool.
    unit_price = _find_raw(rows, header="Unit Price", value="$15.50")
    assert _target_field(unit_price, table_headers=headers_by_table) == (None, 0)


def test_exact_golden_packet_shipment_total_never_competes_with_line_allocations():
    parsed = parse_document(
        "LitoralTrace_Lacey_Golden_Test_Packet_01.pdf", _golden_packet_bytes()
    )
    rows = _raw_cells(parsed)
    headers_by_table = _headers_by_table(rows)

    shipment_total = _find_raw(rows, header="Entered Value", value="$18,600.00")
    line_1 = _find_raw(rows, header="Entered value", value="$14,880.00")
    line_2 = _find_raw(rows, header="Entered value", value="$3,720.00")

    assert _table_id(shipment_total) != _table_id(line_1)
    assert _table_id(line_1) == _table_id(line_2)

    # Invoice total remains source evidence for reconciliation, but it is not a
    # plant-line field candidate. Only the explicit allocation table is line-scoped.
    assert _target_field(shipment_total, table_headers=headers_by_table) == (None, 0)
    assert _target_field(line_1, table_headers=headers_by_table) == ("entered_value", 3)
    assert _target_field(line_2, table_headers=headers_by_table) == ("entered_value", 3)


def test_exact_golden_packet_supplier_statement_cannot_be_merchandise_description():
    parsed = parse_document(
        "LitoralTrace_Lacey_Golden_Test_Packet_01.pdf", _golden_packet_bytes()
    )
    raw_rows = _raw_cells(parsed)
    headers_by_table = _headers_by_table(raw_rows)
    sources = _projection_sources(parsed)

    supplier_sources = [
        row
        for row in sources
        if "natural vietnamese-made wood coaster set" in str(row.original_value).casefold()
        or "supplier's production plant is in" in str(row.original_value).casefold()
    ]
    assert supplier_sources, "The regression must exercise the real supplier-statement evidence."
    assert all(
        _target_field(row, table_headers=headers_by_table)[0] != "merchandise_description"
        for row in supplier_sources
    )

    commercial_sources = [
        row
        for row in sources
        if "retail set: four solid rubberwood coasters with one mdf holder" in str(row.original_value).casefold()
    ]
    assert commercial_sources, "The real commercial description must remain present."
    assert any(
        _target_field(row, table_headers=headers_by_table)[0] == "merchandise_description"
        for row in commercial_sources
    )
