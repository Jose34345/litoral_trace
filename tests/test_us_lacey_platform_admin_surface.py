from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from fastapi import HTTPException
from fastapi.testclient import TestClient

from litoral_trace.us_lacey.csrf import us_lacey_csrf_token
from litoral_trace.us_lacey.portal_auth import US_LACEY_SESSION_COOKIE
import litoral_trace.web.us_lacey_platform_admin as admin_surface
from litoral_trace.web.us_lacey_unified_app import app


client = TestClient(app)
SESSION = "opaque-us-lacey-superadmin-session"


def _identity():
    return SimpleNamespace(user_id=12, organization_id=14, account_status="PILOT")


def _account():
    return {
        "organization_id": 14,
        "legal_name": "Founder Test Organization",
        "business_type": "IMPORTER",
        "admin_contact_email": "owner@example.test",
        "account_status": "PILOT",
        "payment_provider": "LEMON_SQUEEZY",
        "payment_status": "PENDING",
        "subscription_status": "PENDING",
        "monthly_operation_limit": 25,
        "used_operations": 0,
        "queued_jobs": 0,
        "running_jobs": 0,
        "retry_jobs": 0,
        "failed_jobs": 0,
        "last_payment_event_at": None,
    }


def _user():
    return {
        "user_id": 12,
        "organization_id": 14,
        "organization_name": "Founder Test Organization",
        "full_name": "Founder",
        "email": "owner@example.test",
        "role": "superadmin",
        "is_active": True,
        "created_at": None,
        "last_login_at": None,
    }


def _patch_admin_reads(monkeypatch, seen_tokens: list[str]):
    monkeypatch.setattr(admin_surface, "resolve_us_lacey_session", lambda token: _identity())

    def verified_token(token: str) -> str:
        seen_tokens.append(token)
        return token

    monkeypatch.setattr(admin_surface, "_platform_admin_refresh_token", verified_token)
    monkeypatch.setattr(
        admin_surface,
        "list_us_lacey_accounts_superadmin",
        lambda *, refresh_token: [_account()],
    )
    monkeypatch.setattr(
        admin_surface,
        "list_platform_users_superadmin",
        lambda *, refresh_token: [_user()],
    )
    monkeypatch.setattr(
        admin_surface,
        "list_failed_jobs_superadmin",
        lambda *, refresh_token: [],
    )


def test_admin_without_us_session_redirects_to_portal_login():
    client.cookies.clear()
    response = client.get("/admin", follow_redirects=False)
    assert response.status_code == 303
    assert response.headers["location"] == "/login"


def test_admin_non_superadmin_is_forbidden(monkeypatch):
    monkeypatch.setattr(admin_surface, "resolve_us_lacey_session", lambda token: _identity())

    def deny(_token: str) -> str:
        raise HTTPException(status_code=403, detail="not platform admin")

    monkeypatch.setattr(admin_surface, "_platform_admin_refresh_token", deny)
    client.cookies.set(US_LACEY_SESSION_COOKIE, SESSION)
    try:
        response = client.get("/admin", follow_redirects=False)
    finally:
        client.cookies.delete(US_LACEY_SESSION_COOKIE)
    assert response.status_code == 403
    assert "Access denied" in response.text


def test_superadmin_page_reuses_same_us_session_for_control_plane(monkeypatch):
    seen_tokens: list[str] = []
    _patch_admin_reads(monkeypatch, seen_tokens)
    client.cookies.set(US_LACEY_SESSION_COOKIE, SESSION)
    try:
        response = client.get("/admin")
    finally:
        client.cookies.delete(US_LACEY_SESSION_COOKIE)

    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store, max-age=0"
    assert "Global Platform Admin" in response.text
    assert "Founder Test Organization" in response.text
    assert "Processing / errors" in response.text
    assert seen_tokens == [SESSION]


def test_admin_status_mutation_requires_valid_session_bound_csrf(monkeypatch):
    monkeypatch.setattr(admin_surface, "resolve_us_lacey_session", lambda token: _identity())
    monkeypatch.setattr(admin_surface, "_platform_admin_refresh_token", lambda token: token)
    calls: list[tuple[str, int, str]] = []

    def set_status(*, refresh_token: str, organization_id: int, account_status: str):
        calls.append((refresh_token, organization_id, account_status))
        return {"organization_id": organization_id, "account_status": account_status}

    monkeypatch.setattr(admin_surface, "set_us_lacey_account_status_superadmin", set_status)
    client.cookies.set(US_LACEY_SESSION_COOKIE, SESSION)
    try:
        invalid = client.post(
            "/admin/us-lacey/accounts/14/status",
            data={"account_status": "PILOT", "csrf_token": "invalid"},
            follow_redirects=False,
        )
        valid = client.post(
            "/admin/us-lacey/accounts/14/status",
            data={
                "account_status": "PILOT",
                "csrf_token": us_lacey_csrf_token(
                    session_token=SESSION,
                    purpose="platform-admin-status:14",
                ),
            },
            follow_redirects=False,
        )
    finally:
        client.cookies.delete(US_LACEY_SESSION_COOKIE)

    assert invalid.status_code == 403
    assert calls == [(SESSION, 14, "PILOT")]
    assert valid.status_code == 303
    assert valid.headers["location"].startswith("/admin?notice=")


def test_admin_reset_uses_reviewed_control_plane_with_same_session(monkeypatch):
    monkeypatch.setattr(admin_surface, "resolve_us_lacey_session", lambda token: _identity())
    monkeypatch.setattr(admin_surface, "_platform_admin_refresh_token", lambda token: token)
    calls: list[tuple[str, int]] = []

    def reset(*, refresh_token: str, organization_id: int):
        calls.append((refresh_token, organization_id))
        return {"operations_deleted": 1, "jobs_deleted": 1}

    monkeypatch.setattr(admin_surface, "reset_pilot_account_superadmin", reset)
    client.cookies.set(US_LACEY_SESSION_COOKIE, SESSION)
    try:
        response = client.post(
            "/admin/us-lacey/accounts/14/reset-pilot",
            data={
                "csrf_token": us_lacey_csrf_token(
                    session_token=SESSION,
                    purpose="platform-admin-reset:14",
                )
            },
            follow_redirects=False,
        )
    finally:
        client.cookies.delete(US_LACEY_SESSION_COOKIE)

    assert response.status_code == 303
    assert calls == [(SESSION, 14)]


def test_admin_surface_does_not_create_synthetic_generic_sessions():
    source = Path("src/litoral_trace/web/us_lacey_platform_admin.py").read_text(
        encoding="utf-8"
    )
    assert "create_user_session" not in source
    assert "revoke_session" not in source
    assert "session_jwt" not in source


def test_unified_entrypoint_aliases_existing_us_runtime_database_without_literal_secret():
    source = Path("src/litoral_trace/web/us_lacey_unified_app.py").read_text(
        encoding="utf-8"
    )
    assert 'os.environ["DATABASE_URL"] = os.environ["US_LACEY_DATABASE_URL"]' in source
    assert 'os.environ["ENVIRONMENT"] = os.environ["US_LACEY_ENVIRONMENT"]' in source
    assert "postgresql://" not in source
    assert "MIGRATION_DATABASE_URL" not in source
