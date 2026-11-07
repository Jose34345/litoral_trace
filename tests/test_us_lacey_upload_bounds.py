from types import SimpleNamespace

import pytest
from fastapi.responses import HTMLResponse
from fastapi.testclient import TestClient
from starlette.datastructures import UploadFile

from litoral_trace.web import us_lacey_pilot_app as pilot


@pytest.mark.parametrize("route", ["/operations/new", "/operations/OP-1/upload"])
@pytest.mark.parametrize("oversized", [False, True])
def test_operation_upload_respects_file_limit_before_materializing_or_creating(
    monkeypatch, route, oversized
):
    monkeypatch.setenv("US_LACEY_MAX_UPLOAD_BYTES", "16")
    identity = SimpleNamespace(organization_id=41, user_id=7)
    entitlement = SimpleNamespace(evaluation_status="ACTIVE")
    monkeypatch.setattr(pilot, "_operational_mutation_context", lambda _: (identity, entitlement))
    monkeypatch.setattr(pilot, "safe_record_outreach_event", lambda **_: None)
    monkeypatch.setattr(
        pilot,
        "_detail_page",
        lambda *, error, status_code, **_: HTMLResponse(error, status_code),
    )

    monkeypatch.setattr(pilot, "render_new_operation", lambda *, error, **_: error)

    writes = []

    def create_operation(**_):
        writes.append("create")
        return SimpleNamespace(public_id="OP-1")

    def enqueue_documents(**kwargs):
        writes.append(kwargs["documents"])

    monkeypatch.setattr(pilot, "create_us_lacey_customer_operation", create_operation)
    monkeypatch.setattr(pilot, "upload_and_enqueue_us_lacey_document_batch", enqueue_documents)

    read = UploadFile.read
    read_sizes = []

    async def observe_read(self, size=-1):
        read_sizes.append(size)
        return await read(self, size)

    monkeypatch.setattr(UploadFile, "read", observe_read)
    content = b"%PDF-test-data\n!"
    assert len(content) == 16
    second_content = content + b"x" if oversized else content
    session = "synthetic-portal-session"
    purpose = "operation:create" if route == "/operations/new" else "upload:OP-1"
    client = TestClient(pilot.app, follow_redirects=False)
    client.cookies.set(pilot.US_LACEY_SESSION_COOKIE, session)
    response = client.post(
        route,
        data={"csrf_token": pilot.us_lacey_csrf_token(session_token=session, purpose=purpose)},
        files=[
            ("documents", ("invoice.pdf", content, "application/pdf")),
            ("documents", ("packing.pdf", second_content, "application/pdf")),
        ],
    )

    if oversized:
        assert response.status_code == 400
        assert "maximum file size" in response.text
        assert read_sizes == []
        assert writes == []
    else:
        assert response.status_code == 303
        assert read_sizes == [17, 17]
        assert writes == (["create"] if route == "/operations/new" else []) + [
            (
                ("invoice.pdf", "application/pdf", content, "UNKNOWN"),
                ("packing.pdf", "application/pdf", content, "UNKNOWN"),
            )
        ]
