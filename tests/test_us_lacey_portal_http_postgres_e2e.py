from __future__ import annotations

import os
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from litoral_trace.us_lacey.db import reset_us_lacey_engine_state
from litoral_trace.web import us_lacey_pilot_app as portal_module


pytestmark = pytest.mark.skipif(
    os.environ.get("ENABLE_POSTGRES_TESTS") != "1"
    or not os.environ.get("US_LACEY_DATABASE_URL"),
    reason="requires the isolated U.S. PostgreSQL integration database",
)


def _configure_customer_portal(monkeypatch: pytest.MonkeyPatch) -> None:
    values = {
        "US_LACEY_ENVIRONMENT": "test",
        "US_LACEY_STORAGE_BUCKET": "us-lacey-ci-private",
        "US_LACEY_STORAGE_PREFIX": "us-lacey/ci-http-e2e",
        "US_LACEY_APP_HOSTNAME": "app.lacey.litoraltrace.com",
        "US_LACEY_SESSION_TTL_HOURS": "1",
        "US_LACEY_PRIVATE_BETA_PRICE_CENTS": "12500",
        "US_LACEY_MONTHLY_OPERATION_LIMIT": "25",
        "US_LACEY_PAYMENT_PROVIDER": "WISE",
        "US_LACEY_BANK_TRANSFER_INSTRUCTIONS": "CI-only Wise USD transfer instructions",
        "US_LACEY_TERMS_VERSION": "terms-http-e2e-v1",
        "US_LACEY_PRIVACY_VERSION": "privacy-http-e2e-v1",
        "US_LACEY_BETA_TERMS_VERSION": "beta-http-e2e-v1",
        "US_LACEY_SUPPORT_EMAIL": "support@litoraltrace.com",
        "US_LACEY_TERMS_URL": "https://lacey.litoraltrace.com/terms",
        "US_LACEY_PRIVACY_URL": "https://lacey.litoraltrace.com/privacy",
        "US_LACEY_BETA_TERMS_URL": "https://lacey.litoraltrace.com/private-beta-terms",
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)
    # The U.S. runtime must never compare equal to a generic Argentina DB URL.
    monkeypatch.delenv("DATABASE_URL", raising=False)


def test_signup_verify_login_billing_logout_real_http_and_postgres(
    monkeypatch: pytest.MonkeyPatch,
):
    """Certify the browser journey against real U.S. PostgreSQL state.

    SMTP transport is the only mocked boundary: registration, email transition,
    login, opaque session persistence, billing lookup and logout are real.
    """
    _configure_customer_portal(monkeypatch)
    reset_us_lacey_engine_state()

    delivered: dict[str, str] = {}

    def capture_verification_email(**kwargs) -> None:
        delivered.update({key: str(value) for key, value in kwargs.items()})

    monkeypatch.setattr(
        portal_module,
        "send_us_lacey_verification_email",
        capture_verification_email,
    )

    suffix = uuid4().hex[:12]
    email = f"http-e2e-{suffix}@example.com"
    password = "correct-horse-http-e2e-123"
    legal_name = f"HTTP E2E Imports {suffix} LLC"

    with TestClient(portal_module.app, follow_redirects=False) as client:
        unauthenticated = client.get("/billing")
        assert unauthenticated.status_code == 303
        assert unauthenticated.headers["location"] == "/login"

        signup = client.post(
            "/signup",
            data={
                "legal_name": legal_name,
                "business_type": "IMPORTER",
                "admin_name": "HTTP E2E Admin",
                "admin_email": email,
                "password": password,
                "accept_terms": "yes",
                "accept_privacy": "yes",
                "accept_beta": "yes",
            },
        )
        assert signup.status_code == 201
        # Keep this E2E assertion semantic rather than punctuation-sensitive so
        # canonical UI copy refinements do not invalidate a working signup flow.
        assert "Check your email" in signup.text
        assert email in signup.text
        assert delivered["recipient"] == email
        assert delivered["company_name"] == legal_name
        verification_token = delivered["verification_token"]
        assert len(verification_token) >= 32
        assert verification_token not in signup.text
        assert delivered["public_origin"] == "https://app.lacey.litoraltrace.com"

        before_verification = client.post(
            "/login", data={"email": email, "password": password}
        )
        assert before_verification.status_code == 403
        assert "Verify your email before signing in" in before_verification.text
        assert "us_lacey_session=" not in before_verification.headers.get("set-cookie", "")

        verified = client.get(f"/verify-email?token={verification_token}")
        assert verified.status_code == 303
        assert verified.headers["location"] == "/login?verified=1"

        login = client.post("/login", data={"email": email, "password": password})
        assert login.status_code == 303
        assert login.headers["location"] == "/billing"
        cookie_header = login.headers.get("set-cookie", "")
        lowered_cookie = cookie_header.lower()
        assert "us_lacey_session=" in lowered_cookie
        assert "httponly" in lowered_cookie
        assert "samesite=lax" in lowered_cookie
        assert "access_token" not in lowered_cookie
        assert "refresh_token" not in lowered_cookie

        billing = client.get("/billing")
        assert billing.status_code == 200
        assert legal_name in billing.text
        assert "Payment pending" in billing.text
        assert "USD 125.00" in billing.text
        assert "0 / 25" in billing.text
        assert "LT-US-" in billing.text
        assert "How to complete the payment" in billing.text
        assert "Payment matching code" in billing.text
        assert "bank account, payment link or destination address" in billing.text
        assert "CI-only Wise USD transfer instructions" in billing.text
        assert "Document processing unlocks only after Litoral Trace verifies the payment server-side" in billing.text
        assert "verify-payment" not in billing.text.lower()

        logout = client.post("/logout")
        assert logout.status_code == 303
        assert logout.headers["location"] == "/login"

        after_logout = client.get("/billing")
        assert after_logout.status_code == 303
        assert after_logout.headers["location"] == "/login"
