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
        "US_LACEY_PAYMENT_PROVIDER": "WISE",
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
        payment_provider="WISE",
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
