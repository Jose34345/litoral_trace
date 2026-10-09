from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from fastapi.testclient import TestClient

from litoral_trace.us_lacey.portal_auth import (
    US_LACEY_SESSION_COOKIE,
    UsLaceyPortalAuthError,
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
        "US_LACEY_PRIVATE_BETA_PRICE_CENTS": "12500",
        "US_LACEY_MONTHLY_OPERATION_LIMIT": "25",
        "US_LACEY_PAYMENT_PROVIDER": "MANUAL_BANK_TRANSFER",
        "US_LACEY_BANK_TRANSFER_INSTRUCTIONS": "Send USD and include the exact reference.",
        "US_LACEY_TERMS_VERSION": "terms-v1",
        "US_LACEY_PRIVACY_VERSION": "privacy-v1",
        "US_LACEY_BETA_TERMS_VERSION": "beta-v1",
        "US_LACEY_TERMS_URL": "https://litoraltrace.com/legal/us-terms",
        "US_LACEY_PRIVACY_URL": "https://litoraltrace.com/legal/privacy",
        "US_LACEY_BETA_TERMS_URL": "https://litoraltrace.com/legal/us-private-beta",
        "US_LACEY_SMTP_HOST": "smtp.example.com",
        "US_LACEY_SMTP_PORT": "587",
        "US_LACEY_SMTP_USERNAME": "mailer",
        "US_LACEY_SMTP_PASSWORD": "test-password",
        "US_LACEY_EMAIL_FROM": "support@litoraltrace.com",
    }
    for key, value in values.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("STORAGE_BUCKET_NAME", raising=False)
    monkeypatch.delenv("STORAGE_KEY_PREFIX", raising=False)


def test_signup_requires_all_legal_acceptances(monkeypatch):
    _portal_env(monkeypatch)
    called = False

    def fake_register(**_kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr(
        "litoral_trace.web.us_lacey_pilot_app.register_us_lacey_company",
        fake_register,
    )
    client = TestClient(app)
    response = client.post(
        "/signup",
        data={
            "legal_name": "Example Imports LLC",
            "business_type": "IMPORTER",
            "admin_name": "Alex Importer",
            "admin_email": "alex@example.com",
            "password": "correct-horse-123",
            "accept_terms": "yes",
            "accept_privacy": "yes",
        },
    )
    assert response.status_code == 400
    assert "accept all three legal documents" in response.text
    assert called is False


def test_signup_delivers_verification_without_exposing_raw_token(monkeypatch):
    _portal_env(monkeypatch)
    delivered: dict[str, str] = {}

    def fake_send(*, recipient, company_name, verification_token, public_origin=None, **_kwargs):
        delivered.update(
            recipient=recipient,
            company_name=company_name,
            verification_token=verification_token,
            public_origin=public_origin,
        )

    def fake_register(**kwargs):
        kwargs["verification_delivery"](
            kwargs["admin_email"], kwargs["legal_name"], "raw-secret-verification-token"
        )
        return SimpleNamespace(account_status="PENDING_EMAIL")

    monkeypatch.setattr(
        "litoral_trace.web.us_lacey_pilot_app.send_us_lacey_verification_email",
        fake_send,
    )
    monkeypatch.setattr(
        "litoral_trace.web.us_lacey_pilot_app.register_us_lacey_company",
        fake_register,
    )
    client = TestClient(app)
    response = client.post(
        "/signup",
        data={
            "legal_name": "Example Imports LLC",
            "business_type": "IMPORTER",
            "admin_name": "Alex Importer",
            "admin_email": "alex@example.com",
            "password": "correct-horse-123",
            "accept_terms": "yes",
            "accept_privacy": "yes",
            "accept_beta": "yes",
        },
    )
    assert response.status_code == 201
    assert "Check your email" in response.text
    assert "raw-secret-verification-token" not in response.text
    assert delivered["recipient"] == "alex@example.com"
    assert delivered["public_origin"] == "https://app.lacey.litoraltrace.com"


def test_verify_email_transitions_browser_to_login(monkeypatch):
    _portal_env(monkeypatch)
    monkeypatch.setattr(
        "litoral_trace.web.us_lacey_pilot_app.verify_us_lacey_email",
        lambda _token: SimpleNamespace(account_status="PAYMENT_PENDING"),
    )
    client = TestClient(app, follow_redirects=False)
    response = client.get("/verify-email?token=valid-token")
    assert response.status_code == 303
    assert response.headers["location"] == "/login?verified=1"


def test_unverified_account_cannot_login(monkeypatch):
    _portal_env(monkeypatch)

    def fake_login(**_kwargs):
        raise UsLaceyPortalAuthError(
            "Verify your email before signing in.", code="email_unverified"
        )

    monkeypatch.setattr(
        "litoral_trace.web.us_lacey_pilot_app.login_us_lacey_user",
        fake_login,
    )
    client = TestClient(app)
    response = client.post(
        "/login",
        data={"email": "alex@example.com", "password": "correct-horse-123"},
    )
    assert response.status_code == 403
    assert "Verify your email before signing in" in response.text


def test_verified_login_sets_isolated_opaque_cookie(monkeypatch):
    _portal_env(monkeypatch)
    identity = UsLaceyPortalIdentity(
        user_id=7,
        organization_id=41,
        email="alex@example.com",
        full_name="Alex Importer",
        legal_name="Example Imports LLC",
        business_type="IMPORTER",
        account_status="PAYMENT_PENDING",
    )
    monkeypatch.setattr(
        "litoral_trace.web.us_lacey_pilot_app.login_us_lacey_user",
        lambda **_kwargs: SimpleNamespace(
            session_token="opaque-us-session-token",
            expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
            identity=identity,
        ),
    )
    client = TestClient(app, follow_redirects=False)
    response = client.post(
        "/login",
        data={"email": "alex@example.com", "password": "correct-horse-123"},
    )
    assert response.status_code == 303
    assert response.headers["location"] == "/billing"
    cookie = response.headers["set-cookie"]
    assert f"{US_LACEY_SESSION_COOKIE}=opaque-us-session-token" in cookie
    assert "HttpOnly" in cookie
    assert "SameSite=lax" in cookie
    assert "Secure" not in cookie
    assert "session_jwt" not in cookie


def test_payment_pending_account_can_view_billing_but_not_self_activate(monkeypatch):
    _portal_env(monkeypatch)
    identity = UsLaceyPortalIdentity(
        user_id=7,
        organization_id=41,
        email="alex@example.com",
        full_name="Alex Importer",
        legal_name="Example Imports LLC",
        business_type="IMPORTER",
        account_status="PAYMENT_PENDING",
    )
    billing = SimpleNamespace(
        price_cents=12500,
        currency="USD",
        used_operations=0,
        monthly_operation_limit=25,
        subscription_status="PENDING",
        payment_status="PENDING",
        payment_reference="LT-US-ABC123",
        payment_provider="MANUAL_BANK_TRANSFER",
    )
    monkeypatch.setattr(
        "litoral_trace.web.us_lacey_pilot_app.resolve_us_lacey_session",
        lambda _token: identity,
    )
    monkeypatch.setattr(
        "litoral_trace.web.us_lacey_pilot_app.get_us_lacey_billing_summary",
        lambda **_kwargs: billing,
    )
    client = TestClient(app)
    client.cookies.set(US_LACEY_SESSION_COOKIE, "opaque-us-session-token")
    response = client.get("/billing")
    assert response.status_code == 200
    assert "Payment pending" in response.text
    assert "LT-US-ABC123" in response.text
    assert "Send USD and include the exact reference" in response.text
    assert "activate" not in response.text.lower() or "cannot activate" in response.text.lower()
    assert "/verify-payment" not in response.text


def test_public_auth_pages_preserve_get_forms_and_verification_error(monkeypatch):
    _portal_env(monkeypatch)
    client = TestClient(app)

    signup = client.get("/signup")
    assert signup.status_code == 200
    for field in (
        "legal_name",
        "business_type",
        "admin_name",
        "admin_email",
        "password",
        "accept_terms",
        "accept_privacy",
        "accept_beta",
    ):
        assert f'name="{field}"' in signup.text
    assert 'method="post" action="/signup"' in signup.text

    login = client.get("/login")
    assert login.status_code == 200
    assert 'method="post" action="/login"' in login.text
    assert 'name="email"' in login.text
    assert 'name="password"' in login.text
    assert "max-w-md" in login.text
    assert "Forgot password?" in login.text
    assert 'id="password-toggle"' in login.text
    assert "fa-eye" in login.text
    assert "focus:ring-2 focus:ring-emerald-600" in login.text
    assert "w-full justify-center" in login.text
    assert "Don't have an account?" in login.text
    assert '>Sign up</a>' in login.text

    invalid_verify = client.get("/verify-email")
    assert invalid_verify.status_code == 400
    assert "Verification token is missing" in invalid_verify.text


def test_pilot_billing_and_operations_render_canonical_action_contracts(monkeypatch):
    _portal_env(monkeypatch)
    identity = UsLaceyPortalIdentity(
        user_id=7,
        organization_id=41,
        email="pilot@example.com",
        full_name="Pilot User",
        legal_name="Pilot Imports LLC",
        business_type="IMPORTER",
        account_status="PILOT",
    )
    entitlement = SimpleNamespace(
        used_operations=2,
        monthly_operation_limit=5,
        remaining_operations=3,
    )
    billing = SimpleNamespace(
        price_cents=12500,
        currency="USD",
        used_operations=2,
        monthly_operation_limit=5,
        subscription_status="PENDING",
        payment_status="PENDING",
        payment_reference="LT-US-PILOT",
        payment_provider="MANUAL_BANK_TRANSFER",
    )
    detail = SimpleNamespace(
        public_id="OP-DEMO",
        status="DRAFT",
        client_reference="Synthetic shipment",
        document_count=0,
        merchandise_line_count=1,
        importer_name="Pilot Imports LLC",
        supplier_name="Synthetic Supplier",
        documents=[],
        fields=[],
    )

    class FakeOperations:
        def list_operations(self, **_kwargs):
            return []

        def get_detail(self, **_kwargs):
            return detail

    monkeypatch.setattr(
        "litoral_trace.web.us_lacey_pilot_app.resolve_us_lacey_session",
        lambda _token: identity,
    )
    monkeypatch.setattr(
        "litoral_trace.web.us_lacey_pilot_app.require_us_lacey_operational_access",
        lambda **_kwargs: entitlement,
    )
    monkeypatch.setattr(
        "litoral_trace.web.us_lacey_pilot_app.get_us_lacey_billing_summary",
        lambda **_kwargs: billing,
    )
    monkeypatch.setattr(
        "litoral_trace.web.us_lacey_pilot_app.UsLaceyOperationService",
        FakeOperations,
    )

    client = TestClient(app)
    client.cookies.set(US_LACEY_SESSION_COOKIE, "opaque-us-session-token")

    billing_page = client.get("/billing")
    assert billing_page.status_code == 200
    assert "Workspace access active" in billing_page.text
    assert "Subscription billing is still pending confirmation." in billing_page.text
    assert "Billing is verified for this workspace." not in billing_page.text

    operations = client.get("/operations")
    assert operations.status_code == 200
    assert "Shipment queue" in operations.text
    assert "New shipment" in operations.text
    assert 'name="q"' in operations.text
    assert 'name="sort"' in operations.text
    assert "Needs review" in operations.text
    assert "3 / 5" in operations.text

    new_operation = client.get("/operations/new")
    assert new_operation.status_code == 200
    assert 'method="post" action="/operations/new"' in new_operation.text
    assert 'name="csrf_token"' in new_operation.text
    assert 'name="client_reference"' in new_operation.text
    assert "Founding Broker" in new_operation.text
    assert "Create shipment" in new_operation.text
    assert "Create shipment & process documents" in new_operation.text
    assert 'name="documents"' in new_operation.text
    assert 'data-dropzone' in new_operation.text
    assert "Select documents" in new_operation.text
    assert "Audit trail enabled" in new_operation.text
    regulatory = client.get("/regulatory")
    assert regulatory.status_code == 200
    assert "Regulatory Analysis" in regulatory.text
    assert "us-lacey-regulatory-rules-v4" in regulatory.text
    assert "aphis-phase-vii-2024" in regulatory.text
    assert "Audit trail enabled" in regulatory.text
    trust = client.get("/trust")
    assert trust.status_code == 200
    assert "Trust &amp; Controls" in trust.text
    assert "Workspace isolated" in trust.text
    assert "Audit trail enabled" in trust.text
    assert "us-lacey-regulatory-rules-v4" in trust.text
    assert "aphis-phase-vii-2024" in trust.text

    for removed_field in (
        "importer_name",
        "supplier_name",
        "consignee_name",
        "broker_name",
        "operation_date",
        "line_references",
    ):
        assert f'name="{removed_field}"' not in new_operation.text

    operation = client.get("/operations/OP-DEMO")
    assert operation.status_code == 200
    assert 'enctype="multipart/form-data"' in operation.text
    assert 'action="/operations/OP-DEMO/upload"' in operation.text
    assert 'name="documents"' in operation.text
    assert 'multiple required' in operation.text
    assert 'action="/operations/OP-DEMO/complete"' not in operation.text


def test_shipment_queue_filters_search_and_readiness_server_side(monkeypatch):
    _portal_env(monkeypatch)
    identity = UsLaceyPortalIdentity(
        user_id=7, organization_id=41, email="pilot@example.com",
        full_name="Pilot User", legal_name="Pilot Imports LLC",
        business_type="IMPORTER", account_status="PILOT",
    )
    entitlement = SimpleNamespace(
        used_operations=0, monthly_operation_limit=5, remaining_operations=5,
    )
    now = datetime.now(timezone.utc)
    def shipment(public_id, reference, supplier, status, exceptions):
        return SimpleNamespace(
            public_id=public_id, client_reference=reference,
            business_reference=reference, supplier_name=supplier,
            operation_date=None, created_at=now, updated_at=now,
            status=status, exception_count=exceptions,
            document_count=2, merchandise_line_count=1,
        )
    rows = (
        shipment("SHIP-1", "Oak-PO", "Oak Supplier", "REVIEW_REQUIRED", 2),
        shipment("SHIP-2", "Pine-PO", "Pine Supplier", "READY_FOR_REVIEW", 0),
        shipment("SHIP-3", "Other-PO", None, "PROCESSING", 0),
    )
    class FakeOperations:
        def list_operations(self, *, organization_id, limit):
            assert organization_id == identity.organization_id
            assert limit == 500
            return rows

    monkeypatch.setattr(
        "litoral_trace.web.us_lacey_pilot_app.resolve_us_lacey_session",
        lambda _token: identity,
    )
    monkeypatch.setattr(
        "litoral_trace.web.us_lacey_pilot_app.require_us_lacey_operational_access",
        lambda **_kwargs: entitlement,
    )
    monkeypatch.setattr(
        "litoral_trace.web.us_lacey_pilot_app.UsLaceyOperationService",
        FakeOperations,
    )
    monkeypatch.setattr(
        "litoral_trace.web.us_lacey_pilot_app._schedule_translation_backfill_once",
        lambda *args, **kwargs: None,
    )
    client = TestClient(app)
    client.cookies.set(US_LACEY_SESSION_COOKIE, "opaque-us-session-token")
    filtered = client.get("/operations?state=needs_review&q=oak&sort=exceptions_desc")
    assert filtered.status_code == 200
    assert 'href="/operations/SHIP-1"' in filtered.text
    assert 'href="/operations/SHIP-2"' not in filtered.text
    assert 'href="/operations/SHIP-3"' not in filtered.text

    ready = client.get("/operations?state=ready")
    assert ready.status_code == 200
    assert 'href="/operations/SHIP-2"' in ready.text
    assert 'href="/operations/SHIP-1"' not in ready.text

    processing = client.get("/operations?state=processing")
    assert processing.status_code == 200
    assert 'href="/operations/SHIP-3"' in processing.text

    no_match = client.get("/operations?q=no-such-shipment")
    assert no_match.status_code == 200
    assert "No shipments match these filters" in no_match.text


def test_anonymous_new_operation_page_resumes_existing_incomplete_shipment(monkeypatch):
    _portal_env(monkeypatch)
    identity = UsLaceyPortalIdentity(
        user_id=21,
        organization_id=122,
        email="sandbox@example.invalid",
        full_name="Evaluation User",
        legal_name="Litoral Trace Sandbox",
        business_type="IMPORTER",
        account_status="PILOT",
    )
    entitlement = SimpleNamespace(
        used_operations=0,
        monthly_operation_limit=1,
        remaining_operations=1,
        evaluation_status="ANONYMOUS",
        evaluation_successful_operations_used=0,
        evaluation_can_claim=False,
        evaluation_read_only=False,
    )

    class FakeOperations:
        def list_operations(self, **_kwargs):
            return [SimpleNamespace(public_id="OP-IN-PROGRESS", status="NEW")]

    monkeypatch.setattr(
        "litoral_trace.web.us_lacey_pilot_app.resolve_us_lacey_session",
        lambda _token: identity,
    )
    monkeypatch.setattr(
        "litoral_trace.web.us_lacey_pilot_app.require_us_lacey_operational_access",
        lambda **_kwargs: entitlement,
    )
    monkeypatch.setattr(
        "litoral_trace.web.us_lacey_pilot_app.UsLaceyOperationService",
        FakeOperations,
    )

    client = TestClient(app, follow_redirects=False)
    client.cookies.set(US_LACEY_SESSION_COOKIE, "opaque-us-session-token")
    response = client.get("/operations/new")

    assert response.status_code == 303
    assert response.headers["location"] == "/operations/OP-IN-PROGRESS"


def test_anonymous_stale_new_operation_post_uploads_into_existing_shipment(monkeypatch):
    _portal_env(monkeypatch)
    identity = UsLaceyPortalIdentity(
        user_id=21,
        organization_id=122,
        email="sandbox@example.invalid",
        full_name="Evaluation User",
        legal_name="Litoral Trace Sandbox",
        business_type="IMPORTER",
        account_status="PILOT",
    )
    entitlement = SimpleNamespace(
        used_operations=0,
        monthly_operation_limit=1,
        remaining_operations=1,
        evaluation_status="ANONYMOUS",
        evaluation_successful_operations_used=0,
        evaluation_can_claim=False,
        evaluation_read_only=False,
    )
    uploaded: dict[str, object] = {}
    created: list[dict[str, object]] = []

    class FakeOperations:
        def list_operations(self, **_kwargs):
            return [SimpleNamespace(public_id="OP-IN-PROGRESS", status="NEW")]

    monkeypatch.setattr(
        "litoral_trace.web.us_lacey_pilot_app.resolve_us_lacey_session",
        lambda _token: identity,
    )
    monkeypatch.setattr(
        "litoral_trace.web.us_lacey_pilot_app.require_us_lacey_operational_access",
        lambda **_kwargs: entitlement,
    )
    monkeypatch.setattr(
        "litoral_trace.web.us_lacey_pilot_app.verify_us_lacey_csrf",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        "litoral_trace.web.us_lacey_pilot_app.UsLaceyOperationService",
        FakeOperations,
    )

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


def test_new_operation_submit_uses_zero_data_entry_defaults(monkeypatch):
    _portal_env(monkeypatch)
    identity = UsLaceyPortalIdentity(
        user_id=7,
        organization_id=41,
        email="broker@example.com",
        full_name="Broker User",
        legal_name="Broker LLC",
        business_type="CUSTOMS_BROKER",
        account_status="PILOT",
    )
    entitlement = SimpleNamespace(
        used_operations=0,
        monthly_operation_limit=100,
        remaining_operations=100,
    )
    observed: dict[str, object] = {}

    monkeypatch.setattr(
        "litoral_trace.web.us_lacey_pilot_app.resolve_us_lacey_session",
        lambda _token: identity,
    )
    monkeypatch.setattr(
        "litoral_trace.web.us_lacey_pilot_app.require_us_lacey_operational_access",
        lambda **_kwargs: entitlement,
    )
    monkeypatch.setattr(
        "litoral_trace.web.us_lacey_pilot_app.verify_us_lacey_csrf",
        lambda **_kwargs: None,
    )

    def fake_create(**kwargs):
        observed.update(kwargs)
        return SimpleNamespace(public_id="OP-ZERO-DATA")

    monkeypatch.setattr(
        "litoral_trace.web.us_lacey_pilot_app.create_us_lacey_customer_operation",
        fake_create,
    )

    client = TestClient(app, follow_redirects=False)
    client.cookies.set(US_LACEY_SESSION_COOKIE, "opaque-us-session-token")
    response = client.post(
        "/operations/new",
        data={"client_reference": "", "csrf_token": "test-token"},
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/operations/OP-ZERO-DATA"
    assert observed["organization_id"] == 41
    assert observed["user_id"] == 7
    assert str(observed["client_reference"]).startswith("LACEY-")
    assert observed["line_references"] == ("1",)
    for legacy_field in (
        "importer_name",
        "supplier_name",
        "consignee_name",
        "broker_name",
        "operation_date",
    ):
        assert legacy_field not in observed


def test_new_shipment_submit_can_upload_documents_immediately(monkeypatch):
    _portal_env(monkeypatch)
    identity = UsLaceyPortalIdentity(
        user_id=9,
        organization_id=43,
        email="importer@example.com",
        full_name="Importer User",
        legal_name="Importer LLC",
        business_type="IMPORTER",
        account_status="PILOT",
    )
    entitlement = SimpleNamespace(
        used_operations=0,
        monthly_operation_limit=100,
        remaining_operations=100,
    )
    uploaded: dict[str, object] = {}

    monkeypatch.setattr(
        "litoral_trace.web.us_lacey_pilot_app.resolve_us_lacey_session",
        lambda _token: identity,
    )
    monkeypatch.setattr(
        "litoral_trace.web.us_lacey_pilot_app.require_us_lacey_operational_access",
        lambda **_kwargs: entitlement,
    )
    monkeypatch.setattr(
        "litoral_trace.web.us_lacey_pilot_app.verify_us_lacey_csrf",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        "litoral_trace.web.us_lacey_pilot_app.create_us_lacey_customer_operation",
        lambda **_kwargs: SimpleNamespace(public_id="OP-DIRECT-UPLOAD"),
    )

    def fake_upload_batch(**kwargs):
        uploaded.update(kwargs)

    monkeypatch.setattr(
        "litoral_trace.web.us_lacey_pilot_app.upload_and_enqueue_us_lacey_document_batch",
        fake_upload_batch,
    )

    client = TestClient(app, follow_redirects=False)
    client.cookies.set(US_LACEY_SESSION_COOKIE, "opaque-us-session-token")
    response = client.post(
        "/operations/new",
        data={"client_reference": "SHIP-43", "csrf_token": "test-token"},
        files=[
            ("documents", ("invoice.pdf", b"invoice-bytes", "application/pdf")),
            ("documents", ("packing.pdf", b"packing-bytes", "application/pdf")),
        ],
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/operations/OP-DIRECT-UPLOAD?uploaded=1"
    assert uploaded["organization_id"] == 43
    assert uploaded["operation_public_id"] == "OP-DIRECT-UPLOAD"
    assert len(uploaded["documents"]) == 2
    assert uploaded["documents"][0][0] == "invoice.pdf"
    assert uploaded["documents"][0][3] == "UNKNOWN"


def test_new_operation_submit_preserves_optional_customer_reference(monkeypatch):
    _portal_env(monkeypatch)
    identity = UsLaceyPortalIdentity(
        user_id=8,
        organization_id=42,
        email="broker2@example.com",
        full_name="Broker Two",
        legal_name="Broker Two LLC",
        business_type="CUSTOMS_BROKER",
        account_status="ACTIVE",
    )
    entitlement = SimpleNamespace(
        used_operations=1,
        monthly_operation_limit=100,
        remaining_operations=99,
    )
    observed: dict[str, object] = {}

    monkeypatch.setattr(
        "litoral_trace.web.us_lacey_pilot_app.resolve_us_lacey_session",
        lambda _token: identity,
    )
    monkeypatch.setattr(
        "litoral_trace.web.us_lacey_pilot_app.require_us_lacey_operational_access",
        lambda **_kwargs: entitlement,
    )
    monkeypatch.setattr(
        "litoral_trace.web.us_lacey_pilot_app.verify_us_lacey_csrf",
        lambda **_kwargs: None,
    )

    def fake_create(**kwargs):
        observed.update(kwargs)
        return SimpleNamespace(public_id="OP-CUSTOM-REF")

    monkeypatch.setattr(
        "litoral_trace.web.us_lacey_pilot_app.create_us_lacey_customer_operation",
        fake_create,
    )

    client = TestClient(app, follow_redirects=False)
    client.cookies.set(US_LACEY_SESSION_COOKIE, "opaque-us-session-token")
    response = client.post(
        "/operations/new",
        data={"client_reference": "  ENTRY-2026-0042  ", "csrf_token": "test-token"},
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/operations/OP-CUSTOM-REF"
    assert observed["client_reference"] == "ENTRY-2026-0042"
    assert observed["line_references"] == ("1",)


def test_logout_preserves_session_cookie_contract(monkeypatch):
    _portal_env(monkeypatch)
    observed: list[str] = []
    monkeypatch.setattr(
        "litoral_trace.web.us_lacey_pilot_app.logout_us_lacey_user",
        observed.append,
    )
    client = TestClient(app, follow_redirects=False)
    client.cookies.set(US_LACEY_SESSION_COOKIE, "opaque-us-session-token")

    response = client.post("/logout")

    assert response.status_code == 303
    assert response.headers["location"] == "/login"
    assert observed == ["opaque-us-session-token"]
    assert f"{US_LACEY_SESSION_COOKIE}=" in response.headers["set-cookie"]



def test_retry_processing_htmx_requeues_and_redirects(monkeypatch):
    _portal_env(monkeypatch)
    identity = UsLaceyPortalIdentity(
        user_id=17,
        organization_id=41,
        email="broker@example.com",
        full_name="Broker User",
        legal_name="Broker LLC",
        business_type="CUSTOMS_BROKER",
        account_status="ACTIVE",
    )
    entitlement = SimpleNamespace(
        used_operations=1,
        monthly_operation_limit=100,
        remaining_operations=99,
    )
    observed: dict[str, object] = {}

    monkeypatch.setattr(
        "litoral_trace.web.us_lacey_pilot_app._operational_context",
        lambda _session: (identity, entitlement),
    )

    def fake_verify(**kwargs):
        observed["csrf"] = kwargs

    def fake_retry(**kwargs):
        observed["retry"] = kwargs
        return 1

    def fake_wake():
        observed["woke"] = True

    monkeypatch.setattr(
        "litoral_trace.web.us_lacey_pilot_app.verify_us_lacey_csrf",
        fake_verify,
    )
    monkeypatch.setattr(
        "litoral_trace.web.us_lacey_pilot_app.retry_failed_us_lacey_operation",
        fake_retry,
    )
    monkeypatch.setattr(
        "litoral_trace.web.us_lacey_pilot_app.wake_us_lacey_worker",
        fake_wake,
    )

    operation_id = "11111111-2222-3333-4444-555555555555"
    client = TestClient(app, follow_redirects=False)
    client.cookies.set(US_LACEY_SESSION_COOKIE, "opaque-us-session-token")
    response = client.post(
        f"/operations/{operation_id}/actions/retry",
        data={"csrf_token": "retry-token"},
        headers={"HX-Request": "true"},
    )

    assert response.status_code == 200
    assert response.headers["HX-Redirect"] == f"/operations/{operation_id}"
    assert observed["retry"] == {
        "organization_id": 41,
        "operation_public_id": operation_id,
    }
    assert observed["csrf"]["purpose"] == f"retry:{operation_id}"
    assert observed["csrf"]["submitted_token"] == "retry-token"
    assert observed["woke"] is True
