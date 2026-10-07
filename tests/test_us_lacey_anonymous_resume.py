from __future__ import annotations

from types import SimpleNamespace

from fastapi.testclient import TestClient

from litoral_trace.us_lacey.portal_auth import (
    US_LACEY_SESSION_COOKIE,
    UsLaceyPortalIdentity,
)
from litoral_trace.web.us_lacey_pilot_app import app


def _portal_env(monkeypatch) -> None:
    values = {
        "US_LACEY_ENVIRONMENT": "test",
        "US_LACEY_DATABASE_URL": "postgresql://us_user:secret@us-db.example.com/us_lacey",
        "US_LACEY_STORAGE_BUCKET": "litoral-trace-us-lacey-test",
        "US_LACEY_STORAGE_PREFIX": "us-lacey/test",
        "US_LACEY_APP_HOSTNAME": "app.lacey.litoraltrace.com",
        "US_LACEY_TERMS_VERSION": "terms-v1",
        "US_LACEY_PRIVACY_VERSION": "privacy-v1",
        "US_LACEY_BETA_TERMS_VERSION": "beta-v1",
        "US_LACEY_TERMS_URL": "https://litoraltrace.com/legal/us-terms",
        "US_LACEY_PRIVACY_URL": "https://litoraltrace.com/legal/privacy",
        "US_LACEY_BETA_TERMS_URL": "https://litoraltrace.com/legal/us-private-beta",
    }
    for key, value in values.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("DATABASE_URL", raising=False)


def _identity() -> UsLaceyPortalIdentity:
    return UsLaceyPortalIdentity(
        user_id=21,
        organization_id=122,
        email="sandbox@example.invalid",
        full_name="Evaluation User",
        legal_name="Litoral Trace Sandbox",
        business_type="IMPORTER",
        account_status="PILOT",
    )


def _anonymous_entitlement() -> SimpleNamespace:
    return SimpleNamespace(
        used_operations=0,
        monthly_operation_limit=1,
        remaining_operations=1,
        evaluation_status="ANONYMOUS",
        evaluation_successful_operations_used=0,
        evaluation_can_claim=False,
        evaluation_read_only=False,
    )


def _patch_context(monkeypatch) -> None:
    monkeypatch.setattr(
        "litoral_trace.web.us_lacey_pilot_app.resolve_us_lacey_session",
        lambda _token: _identity(),
    )
    monkeypatch.setattr(
        "litoral_trace.web.us_lacey_pilot_app.require_us_lacey_operational_access",
        lambda **_kwargs: _anonymous_entitlement(),
    )


class _ExistingOperations:
    def list_operations(self, **_kwargs):
        return [SimpleNamespace(public_id="OP-IN-PROGRESS", status="NEW")]


def test_get_new_shipment_resumes_existing_anonymous_incomplete_shipment(monkeypatch):
    _portal_env(monkeypatch)
    _patch_context(monkeypatch)
    monkeypatch.setattr(
        "litoral_trace.web.us_lacey_pilot_app.UsLaceyOperationService",
        _ExistingOperations,
    )

    client = TestClient(app, follow_redirects=False)
    client.cookies.set(US_LACEY_SESSION_COOKIE, "opaque-us-session-token")
    response = client.get("/operations/new")

    assert response.status_code == 303
    assert response.headers["location"] == "/operations/OP-IN-PROGRESS"


def test_stale_new_shipment_post_uploads_into_existing_anonymous_shipment(monkeypatch):
    _portal_env(monkeypatch)
    _patch_context(monkeypatch)
    monkeypatch.setattr(
        "litoral_trace.web.us_lacey_pilot_app.verify_us_lacey_csrf",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        "litoral_trace.web.us_lacey_pilot_app.UsLaceyOperationService",
        _ExistingOperations,
    )

    created: list[dict[str, object]] = []
    uploaded: dict[str, object] = {}

    def fake_create(**kwargs):
        created.append(kwargs)
        return SimpleNamespace(public_id="OP-UNEXPECTED-NEW")

    def fake_upload_batch(**kwargs):
        uploaded.update(kwargs)

    monkeypatch.setattr(
        "litoral_trace.web.us_lacey_pilot_app.create_us_lacey_customer_operation",
        fake_create,
    )
    monkeypatch.setattr(
        "litoral_trace.web.us_lacey_pilot_app.upload_and_enqueue_us_lacey_document_batch",
        fake_upload_batch,
    )
    monkeypatch.setattr(
        "litoral_trace.web.us_lacey_pilot_app.safe_record_outreach_event",
        lambda **_kwargs: None,
    )

    client = TestClient(app, follow_redirects=False)
    client.cookies.set(US_LACEY_SESSION_COOKIE, "opaque-us-session-token")
    response = client.post(
        "/operations/new",
        data={"client_reference": "", "csrf_token": "test-token"},
        files=[("documents", ("invoice.pdf", b"invoice-bytes", "application/pdf"))],
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/operations/OP-IN-PROGRESS?uploaded=1"
    assert created == []
    assert uploaded["operation_public_id"] == "OP-IN-PROGRESS"
    assert len(uploaded["documents"]) == 1
