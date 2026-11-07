"""Focused request-body and XLSX preflight regression tests."""
from io import BytesIO
from zipfile import ZIP_DEFLATED, ZipFile

import pytest
from fastapi.testclient import TestClient
from openpyxl import Workbook

from litoral_trace.us_lacey.batch_hardening import (
    DATASET_TOO_COMPLEX,
    ShipmentBatchRejected,
    ShipmentSpreadsheetLimits,
    enforce_shipment_document_budget,
)
from litoral_trace.web.us_lacey_pilot_app import app


def _limits():
    return ShipmentSpreadsheetLimits(
        max_bytes=1024 * 1024, max_rows=100, max_columns=20, max_cells=1000
    )


def test_regular_xlsx_passes_archive_preflight():
    workbook = Workbook()
    workbook.active.append(["Product", "Quantity"])
    workbook.active.append(["Plywood", 25])
    stream = BytesIO()
    workbook.save(stream)
    result = enforce_shipment_document_budget(
        filename="packing.xlsx", content=stream.getvalue(), limits=_limits()
    )
    assert result.file_kind == "XLSX"
    assert result.rows == 2


def _zip_payload(*, extra_name, extra_data):
    content = BytesIO()
    with ZipFile(content, mode="w", compression=ZIP_DEFLATED) as archive:
        archive.writestr("[Content_Types].xml", b"types")
        archive.writestr("xl/workbook.xml", b"book")
        archive.writestr(extra_name, extra_data)
    return content.getvalue()


@pytest.mark.parametrize(
    "name,data",
    [
        ("xl/sharedStrings.xml", b"A" * (1024 * 1024 * 2)),
        ("../outside.xml", b"data"),
    ],
    ids=["compressed-archive", "traversal-entry"],
)
def test_reject_expanding_or_traversal_xlsx_before_parser(monkeypatch, name, data):
    from litoral_trace.us_lacey import batch_hardening

    def must_not_parse(*args, **kwargs):
        raise AssertionError("openpyxl was called before ZIP preflight")

    monkeypatch.setattr(batch_hardening, "load_workbook", must_not_parse)
    with pytest.raises(ShipmentBatchRejected) as exc:
        enforce_shipment_document_budget(
            filename="packing.xlsx", content=_zip_payload(extra_name=name, extra_data=data),
            limits=_limits(),
        )
    assert exc.value.code == DATASET_TOO_COMPLEX


def test_multipart_content_length_rejected_before_form_parsing(monkeypatch):
    monkeypatch.setenv("US_LACEY_MAX_UPLOAD_BYTES", "16")
    monkeypatch.setenv("US_LACEY_MAX_REQUEST_BYTES", "32")
    client = TestClient(app, follow_redirects=False)
    result = client.post(
        "/operations/new",
        data={"csrf_token": "sample"},
        files=[("documents", ("invoice.pdf", b"%PDF-example", "application/pdf"))],
    )
    assert result.status_code == 413
    assert "maximum total upload size" in result.text


@pytest.mark.anyio
async def test_multipart_stream_budget_rejects_false_content_length(monkeypatch):
    from litoral_trace.web.us_lacey_multipart_limits import LaceyMultipartBudgetMiddleware

    monkeypatch.setenv("US_LACEY_MAX_UPLOAD_BYTES", "8")
    monkeypatch.setenv("US_LACEY_MAX_REQUEST_BYTES", "16")
    consumed = []
    outbound = []
    incoming = iter([
        {"type": "http.request", "body": b"A" * 12, "more_body": True},
        {"type": "http.request", "body": b"B" * 10, "more_body": False},
    ])

    async def downstream(scope, receive, send):
        consumed.append(await receive())
        consumed.append(await receive())

    async def receive():
        return next(incoming)

    async def send(message):
        outbound.append(message)

    middleware = LaceyMultipartBudgetMiddleware(downstream)
    await middleware(
        {"type": "http", "method": "POST", "path": "/operations/new",
         "headers": [(b"content-length", b"8")]},
        receive, send,
    )
    assert len(consumed) == 1
    assert any(m.get("status") == 413 for m in outbound)


@pytest.mark.anyio
async def test_other_routes_not_affected_by_upload_middleware(monkeypatch):
    from litoral_trace.web.us_lacey_multipart_limits import LaceyMultipartBudgetMiddleware

    monkeypatch.setenv("US_LACEY_MAX_UPLOAD_BYTES", "8")
    monkeypatch.setenv("US_LACEY_MAX_REQUEST_BYTES", "16")
    recorded = []

    async def downstream(scope, receive, send):
        recorded.append(scope["path"])

    await LaceyMultipartBudgetMiddleware(downstream)(
        {"type": "http", "method": "POST", "path": "/login", "headers": []},
        lambda: None, lambda _: None,
    )
    assert recorded == ["/login"]
