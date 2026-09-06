from __future__ import annotations

from io import BytesIO
from types import SimpleNamespace

import pytest

from litoral_trace.assurance.parsers import ParsedDocument, ParsedTable, SourceLocation
from litoral_trace.assurance.processing import _persist_raw_parsed_fields
from litoral_trace.lacey_benchmark.bulk_csv import iter_bulk_csv_batches
from litoral_trace.us_lacey.batch_hardening import (
    DATASET_TOO_COMPLEX,
    ShipmentBatchRejected,
    ShipmentSpreadsheetLimits,
    enforce_shipment_document_budget,
    inspect_csv_chunks_for_shipment,
)
from litoral_trace.us_lacey import workflow


def _limits(*, rows: int = 5, columns: int = 8, cells: int = 32, size: int = 1024 * 1024):
    return ShipmentSpreadsheetLimits(max_bytes=size, max_rows=rows, max_columns=columns, max_cells=cells)


def test_small_shipment_csv_passes_bounded_profile():
    content = b"BOL,Container,Quantity\nABC123,MSCU1234567,10\nABC123,MSCU1234567,12\n"
    profile = enforce_shipment_document_budget(filename="packing.csv", content=content, limits=_limits())
    assert profile is not None
    assert profile.file_kind == "CSV"
    assert profile.rows == 2
    assert profile.columns == 3


def test_bulk_csv_is_rejected_before_shipment_processing():
    content = b"BOL,Container\n" + b"ABC,MSCU1234567\n" * 8
    with pytest.raises(ShipmentBatchRejected) as captured:
        inspect_csv_chunks_for_shipment((content,), size_bytes=len(content), limits=_limits(rows=5, cells=40))
    assert captured.value.code == DATASET_TOO_COMPLEX
    assert "bulk" in captured.value.safe_message.lower()


def test_customer_workflow_rejects_bulk_before_ingestion_or_queue(monkeypatch):
    monkeypatch.setattr(workflow, "require_us_lacey_operational_access", lambda **kwargs: None)
    monkeypatch.setenv("US_LACEY_SHIPMENT_SPREADSHEET_MAX_ROWS", "100")
    monkeypatch.setenv("US_LACEY_SHIPMENT_SPREADSHEET_MAX_CELLS", "1000")
    content = b"a,b\n" + b"1,2\n" * 101

    class ShouldNotRun:
        def get_internal_id(self, **kwargs):
            raise AssertionError("operation lookup must not run for rejected bulk data")

    with pytest.raises(workflow.UsLaceyWorkflowError) as captured:
        workflow.upload_and_enqueue_us_lacey_document(
            organization_id=1,
            user_id=1,
            operation_public_id="00000000-0000-0000-0000-000000000001",
            filename="abril.csv",
            content_type="text/csv",
            content=content,
            operations=ShouldNotRun(),
        )
    assert "bulk" in str(captured.value).lower()


def test_bulk_importer_batches_without_shipment_semantics():
    content = b"country,weight\nCO,10\nBR,20\nCL,30\nAR,40\nUY,50\n"
    batches = list(iter_bulk_csv_batches(BytesIO(content), batch_size=2))
    assert [len(batch.rows) for batch in batches] == [2, 2, 1]
    assert batches[0].start_row == 1
    assert batches[-1].end_row == 5
    assert batches[0].headers == ("country", "weight")


def test_large_spreadsheet_raw_persistence_uses_schema_summary(monkeypatch):
    monkeypatch.setenv("LT_ASSURANCE_RAW_CELL_PERSIST_LIMIT", "100")
    table = ParsedTable(
        name="sheet",
        headers=("a", "b"),
        rows=tuple({"a": str(index), "b": str(index)} for index in range(60)),
        source=SourceLocation(sheet="sheet", row=1, locator="sheet:sheet;header_row:1"),
    )
    parsed = ParsedDocument(file_kind="CSV", tables=(table,), metadata={"row_count": 60})

    class FakeSession:
        def __init__(self):
            self.added = []
        def add(self, value):
            self.added.append(value)

    session = FakeSession()
    count = _persist_raw_parsed_fields(
        session,
        organization_id=1,
        assurance_document=SimpleNamespace(id=10),
        extraction_run=SimpleNamespace(id=20),
        parsed=parsed,
    )
    assert count == 1
    assert len(session.added) == 1
    assert session.added[0].field_name == "raw.table.1.schema"
    assert session.added[0].value_type == "table_schema"
