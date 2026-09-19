from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from litoral_trace.us_lacey.access import UsLaceyOperationalEntitlement
from litoral_trace.us_lacey.portal_auth import (
    UsLaceyPortalIdentity,
    UsLaceyPortalLoginResult,
)
from litoral_trace.web import us_lacey_sandbox as sandbox_module


def _identity(*, organization_id: int = 41) -> UsLaceyPortalIdentity:
    return UsLaceyPortalIdentity(
        user_id=7,
        organization_id=organization_id,
        email="sandbox-test@sandbox.invalid",
        full_name="Sandbox visitor",
        legal_name="Anonymous Customs Broker Sandbox",
        business_type="CUSTOMS_BROKER",
        account_status="PILOT",
    )


def _entitlement(*, sandbox: bool, used: int = 0) -> UsLaceyOperationalEntitlement:
    return UsLaceyOperationalEntitlement(
        organization_id=41,
        account_status="PILOT",
        subscription_status="ACTIVE",
        monthly_operation_limit=1,
        used_operations=used,
        is_sandbox=sandbox,
        expires_at=datetime.now(timezone.utc) + timedelta(hours=4)
        if sandbox
        else None,
    )


def _app() -> FastAPI:
    app = FastAPI()
    app.include_router(sandbox_module.router)
    return app


def test_public_sandbox_start_sets_opaque_secure_cookie_and_redirects(monkeypatch):
    expires_at = datetime.now(timezone.utc) + timedelta(hours=4)
    started = UsLaceyPortalLoginResult(
        session_token="opaque-sandbox-token",
        expires_at=expires_at,
        identity=_identity(),
    )
    monkeypatch.setattr(
        sandbox_module,
        "start_us_lacey_sandbox_session",
        lambda **_kwargs: started,
    )
    monkeypatch.setattr(
        sandbox_module,
        "load_us_lacey_portal_config",
        lambda: SimpleNamespace(session_cookie_secure=True),
    )

    with TestClient(_app(), follow_redirects=False) as client:
        response = client.get("/sandbox/start")

    assert response.status_code == 303
    assert response.headers["location"] == "/operations/new"
    cookie = response.headers["set-cookie"].lower()
    assert "us_lacey_session=opaque-sandbox-token" in cookie
    assert "httponly" in cookie
    assert "secure" in cookie
    assert "samesite=lax" in cookie
    assert "organization_id" not in cookie
    assert response.headers["x-robots-tag"] == "noindex, nofollow"


def test_sandbox_start_is_idempotent_for_existing_live_sandbox(monkeypatch):
    monkeypatch.setattr(
        sandbox_module,
        "resolve_us_lacey_session",
        lambda _token: _identity(),
    )
    monkeypatch.setattr(
        sandbox_module,
        "require_us_lacey_operational_access",
        lambda **_kwargs: _entitlement(sandbox=True, used=0),
    )

    def unexpected_provision(**_kwargs):
        raise AssertionError("existing sandbox must not provision another tenant")

    monkeypatch.setattr(
        sandbox_module,
        "start_us_lacey_sandbox_session",
        unexpected_provision,
    )

    with TestClient(_app(), follow_redirects=False) as client:
        response = client.get(
            "/sandbox/start",
            cookies={"us_lacey_session": "existing"},
        )

    assert response.status_code == 303
    assert response.headers["location"] == "/operations/new"
    assert "set-cookie" not in response.headers


def test_sandbox_start_never_overwrites_existing_paid_session(monkeypatch):
    paid = UsLaceyPortalIdentity(
        user_id=9,
        organization_id=99,
        email="broker@example.com",
        full_name="Broker",
        legal_name="Broker LLC",
        business_type="CUSTOMS_BROKER",
        account_status="ACTIVE",
    )
    monkeypatch.setattr(
        sandbox_module,
        "resolve_us_lacey_session",
        lambda _token: paid,
    )
    monkeypatch.setattr(
        sandbox_module,
        "require_us_lacey_operational_access",
        lambda **_kwargs: UsLaceyOperationalEntitlement(
            organization_id=99,
            account_status="ACTIVE",
            subscription_status="ACTIVE",
            monthly_operation_limit=25,
            used_operations=2,
            is_sandbox=False,
        ),
    )

    def unexpected_provision(**_kwargs):
        raise AssertionError("paid session must never be replaced")

    monkeypatch.setattr(
        sandbox_module,
        "start_us_lacey_sandbox_session",
        unexpected_provision,
    )

    with TestClient(_app(), follow_redirects=False) as client:
        response = client.get(
            "/sandbox/start",
            cookies={"us_lacey_session": "paid"},
        )

    assert response.status_code == 303
    assert response.headers["location"] == "/operations"
    assert "set-cookie" not in response.headers
