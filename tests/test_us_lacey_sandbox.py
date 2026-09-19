from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

import litoral_trace.us_lacey.sandbox as sandbox_service
import litoral_trace.web.us_lacey_sandbox as sandbox_web
from litoral_trace.us_lacey.portal_auth import US_LACEY_SESSION_COOKIE
from litoral_trace.us_lacey.sandbox import (
    SANDBOX_MAX_DOCUMENTS_PER_OPERATION,
    UsLaceySandboxError,
    UsLaceySandboxSession,
    enforce_sandbox_document_capacity,
    provision_us_lacey_sandbox,
)
from litoral_trace.web.us_lacey_unified_app import app


class _Mappings:
    def __init__(self, row):
        self.row = row

    def one(self):
        return self.row


class _Result:
    def __init__(self, row):
        self.row = row

    def mappings(self):
        return _Mappings(self.row)


class _ProvisionSession:
    def __init__(self, row):
        self.row = row
        self.params = None
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def execute(self, _statement, params=None):
        self.params = params
        return _Result(self.row)

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        self.closed = True


def test_provision_sandbox_persists_only_token_hash_and_returns_raw_token(monkeypatch):
    expires_at = datetime.now(timezone.utc) + timedelta(hours=4)
    fake = _ProvisionSession(
        {
            "organization_id": 101,
            "user_id": 202,
            "session_id": 303,
            "expires_at": expires_at,
        }
    )
    monkeypatch.setattr(sandbox_service, "get_us_lacey_db_session", lambda: fake)
    monkeypatch.setattr(
        sandbox_service.secrets,
        "token_urlsafe",
        lambda size: "browser-opaque-token" if size == 48 else "unrecoverable-password",
    )
    monkeypatch.setattr(
        sandbox_service,
        "hash_password",
        lambda _value: "$2b$12$" + ("x" * 53),
    )

    result = provision_us_lacey_sandbox(
        client_ip="203.0.113.10",
        user_agent="pytest",
    )

    assert result.organization_id == 101
    assert result.session_token == "browser-opaque-token"
    assert fake.params["token_hash"] != result.session_token
    assert len(fake.params["token_hash"]) == 64
    assert fake.params["client_ip"] == "203.0.113.10"
    assert fake.committed is True
    assert fake.closed is True


class _ScalarSession:
    def __init__(self, values):
        self.values = list(values)
        self.closed = False

    def scalar(self, _statement):
        return self.values.pop(0)

    def close(self):
        self.closed = True


def test_sandbox_document_cap_allows_third_document_and_rejects_fourth(monkeypatch):
    expires_at = datetime.now(timezone.utc) + timedelta(hours=1)

    allowed = _ScalarSession(
        [
            SimpleNamespace(is_sandbox=True, sandbox_expires_at=expires_at),
            SimpleNamespace(document_count=2),
        ]
    )
    monkeypatch.setattr(sandbox_service, "get_us_lacey_db_session", lambda: allowed)
    monkeypatch.setattr(sandbox_service, "set_tenant_db_context", lambda *_args: None)

    enforce_sandbox_document_capacity(
        organization_id=10,
        operation_id=20,
        incoming_document_count=1,
    )
    assert allowed.closed is True

    blocked = _ScalarSession(
        [
            SimpleNamespace(is_sandbox=True, sandbox_expires_at=expires_at),
            SimpleNamespace(document_count=2),
        ]
    )
    monkeypatch.setattr(sandbox_service, "get_us_lacey_db_session", lambda: blocked)

    with pytest.raises(UsLaceySandboxError, match="up to 3 documents"):
        enforce_sandbox_document_capacity(
            organization_id=10,
            operation_id=20,
            incoming_document_count=2,
        )

    assert SANDBOX_MAX_DOCUMENTS_PER_OPERATION == 3


def test_paid_tenant_is_not_subject_to_sandbox_document_cap(monkeypatch):
    fake = _ScalarSession(
        [SimpleNamespace(is_sandbox=False, sandbox_expires_at=None)]
    )
    monkeypatch.setattr(sandbox_service, "get_us_lacey_db_session", lambda: fake)
    monkeypatch.setattr(sandbox_service, "set_tenant_db_context", lambda *_args: None)

    enforce_sandbox_document_capacity(
        organization_id=10,
        operation_id=20,
        incoming_document_count=100,
    )


client = TestClient(app)


def _sandbox_session() -> UsLaceySandboxSession:
    return UsLaceySandboxSession(
        organization_id=501,
        user_id=502,
        session_id=503,
        session_token="new-sandbox-token",
        expires_at=datetime.now(timezone.utc) + timedelta(hours=4),
    )


def test_public_sandbox_start_sets_opaque_cookie_and_redirects_to_new_operation(monkeypatch):
    client.cookies.clear()
    monkeypatch.setattr(
        sandbox_web,
        "load_us_lacey_portal_config",
        lambda: SimpleNamespace(session_cookie_secure=False),
    )
    monkeypatch.setattr(
        sandbox_web,
        "provision_us_lacey_sandbox",
        lambda **_kwargs: _sandbox_session(),
    )

    response = client.get("/sandbox/start", follow_redirects=False)

    assert response.status_code == 303
    assert response.headers["location"] == "/operations/new"
    cookie = response.headers["set-cookie"]
    assert f"{US_LACEY_SESSION_COOKIE}=new-sandbox-token" in cookie
    assert "HttpOnly" in cookie
    assert "SameSite=lax" in cookie
    assert response.headers["cache-control"] == "no-store, max-age=0"
    assert response.headers["x-robots-tag"] == "noindex, nofollow, noarchive"


def test_sandbox_start_preserves_existing_valid_tenant_session(monkeypatch):
    client.cookies.clear()
    monkeypatch.setattr(
        sandbox_web,
        "resolve_us_lacey_session",
        lambda _token: SimpleNamespace(organization_id=77),
    )

    def should_not_provision(**_kwargs):
        raise AssertionError("existing tenant must not be replaced")

    monkeypatch.setattr(
        sandbox_web,
        "provision_us_lacey_sandbox",
        should_not_provision,
    )

    client.cookies.set(US_LACEY_SESSION_COOKIE, "existing-session")
    try:
        response = client.get("/sandbox/start", follow_redirects=False)
    finally:
        client.cookies.clear()

    assert response.status_code == 303
    assert response.headers["location"] == "/operations/new"
    assert "set-cookie" not in response.headers


def test_sandbox_start_returns_429_when_database_rate_limit_fires(monkeypatch):
    client.cookies.clear()
    monkeypatch.setattr(
        sandbox_web,
        "load_us_lacey_portal_config",
        lambda: SimpleNamespace(session_cookie_secure=False),
    )

    def rate_limited(**_kwargs):
        raise UsLaceySandboxError(
            "Too many sandbox sessions were started from this network. Try again later.",
            code="rate_limited",
        )

    monkeypatch.setattr(
        sandbox_web,
        "provision_us_lacey_sandbox",
        rate_limited,
    )

    response = client.get("/sandbox/start", follow_redirects=False)

    assert response.status_code == 429
    assert response.headers["retry-after"] == "3600"
    assert "Too many sandbox sessions" in response.text
