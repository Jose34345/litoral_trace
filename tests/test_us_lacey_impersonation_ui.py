from __future__ import annotations

from datetime import date, datetime, timezone
from types import SimpleNamespace
from uuid import UUID

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from litoral_trace.us_lacey.csrf import us_lacey_csrf_token
from litoral_trace.us_lacey.impersonation_db import (
    IMPERSONATION_COOKIE,
    ReadOnlyImpersonationContext,
    readonly_impersonation_db,
    resolve_readonly_impersonation_context,
)
from litoral_trace.us_lacey.portal_auth import US_LACEY_SESSION_COOKIE
import litoral_trace.web.us_lacey_platform_admin as admin_surface
from litoral_trace.web.us_lacey_unified_app import app


client = TestClient(app)
SESSION = "opaque-us-lacey-superadmin-session"
TARGET_ORG = 42
OPERATION_ID = UUID("11111111-1111-4111-8111-111111111111")
FOREIGN_OPERATION_ID = UUID("22222222-2222-4222-8222-222222222222")


class _MappingResult:
    def __init__(self, rows):
        self._rows = list(rows)

    def mappings(self):
        return self

    def all(self):
        return list(self._rows)

    def one_or_none(self):
        if not self._rows:
            return None
        if len(self._rows) != 1:
            raise AssertionError("expected at most one row")
        return self._rows[0]


class _SequencedConnection:
    def __init__(self, result_sets):
        self._result_sets = list(result_sets)
        self.statements: list[str] = []

    def execute(self, statement):
        self.statements.append(str(statement))
        if not self._result_sets:
            raise AssertionError("unexpected query")
        return _MappingResult(self._result_sets.pop(0))


def _context() -> ReadOnlyImpersonationContext:
    return ReadOnlyImpersonationContext(
        session_id=9001,
        admin_user_id=7,
        admin_organization_id=8,
        target_organization_id=TARGET_ORG,
    )


def _operation_row(public_id: UUID = OPERATION_ID) -> dict:
    now = datetime(2026, 9, 20, 1, 0, tzinfo=timezone.utc)
    return {
        "id": 101,
        "public_id": public_id,
        "client_reference": "SUPPORT-READ-ONLY-001",
        "importer_name": "Example Importer",
        "consignee_name": "Example Consignee",
        "broker_name": "Example Broker",
        "supplier_name": "Example Supplier",
        "operation_date": date(2026, 9, 19),
        "status": "READY_FOR_REVIEW",
        "document_count": 1,
        "merchandise_line_count": 1,
        "review_result": "REVIEW_REQUIRED",
        "created_at": now,
        "updated_at": now,
    }


def _list_row() -> dict:
    row = _operation_row()
    return {
        key: row[key]
        for key in (
            "public_id",
            "client_reference",
            "status",
            "document_count",
            "merchandise_line_count",
            "review_result",
            "operation_date",
            "created_at",
            "updated_at",
        )
    }


def _fields():
    return [
        {
            "id": 1,
            "merchandise_line_reference": "1",
            "field_name": "genus",
            "field_scope": "PLANT_LINE",
            "original_value": "Eucalyptus",
            "normalized_value": "Eucalyptus",
            "human_value": None,
            "field_status": "FOUND",
            "validation_status": "VALID",
            "validation_error": None,
            "confidence": 0.99,
            "source_page": 1,
            "source_locator": "page:1",
            "reviewed_at": None,
        }
    ]


def _declarations():
    return [
        {
            "line_reference": "1",
            "line_ordinal": 1,
            "ordinal": 1,
            "genus": "Eucalyptus",
            "species": "grandis",
            "country_of_harvest": "BR",
            "quantity": "10",
            "unit": "KG",
            "confidence": 0.97,
            "source_page": 1,
            "source_locator": "page:1",
        }
    ]


def _documents():
    return [
        {
            "document_role": "COMMERCIAL_INVOICE",
            "version_number": 1,
            "linked_at": datetime(2026, 9, 20, tzinfo=timezone.utc),
            "processing_status": "PROCESSED",
            "last_error_code": None,
            "vault_public_id": UUID("33333333-3333-4333-8333-333333333333"),
            "original_filename": "invoice.pdf",
            "content_type": "application/pdf",
            "size_bytes": 2048,
            "sha256": "a" * 64,
            "document_type": "OTHER_EVIDENCE",
            "vault_status": "available",
            "created_at": datetime(2026, 9, 20, tzinfo=timezone.utc),
        }
    ]


@pytest.fixture(autouse=True)
def _clean_client_state():
    client.cookies.clear()
    app.dependency_overrides.clear()
    yield
    client.cookies.clear()
    app.dependency_overrides.clear()


def _install_valid_impersonation(connection: _SequencedConnection) -> None:
    app.dependency_overrides[resolve_readonly_impersonation_context] = _context
    app.dependency_overrides[readonly_impersonation_db] = lambda: connection
    client.cookies.set(US_LACEY_SESSION_COOKIE, SESSION)
    client.cookies.set(IMPERSONATION_COOKIE, "test-impersonation-token")


def _assert_readonly_chrome(html: str) -> None:
    assert "READ-ONLY SUPPORT VIEW" in html
    assert "Modifying data is disabled by database policies." in html
    assert "End Session" in html
    lowered = html.lower()
    assert "new operation" not in lowered
    assert "upload" not in lowered
    assert "delete" not in lowered
    assert lowered.count("<form") == 1
    assert 'action="/admin/impersonation/end"' in html


def test_impersonation_operations_returns_readonly_ui() -> None:
    connection = _SequencedConnection([[_list_row()]])
    _install_valid_impersonation(connection)

    response = client.get("/admin/impersonation/operations")

    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store, max-age=0"
    assert "SUPPORT-READ-ONLY-001" in response.text
    _assert_readonly_chrome(response.text)
    assert len(connection.statements) == 1
    assert "us_lacey_operations.organization_id" in connection.statements[0]


def test_impersonation_operation_detail_returns_complete_readonly_ui() -> None:
    connection = _SequencedConnection(
        [
            [_operation_row()],
            _fields(),
            _declarations(),
            _documents(),
        ]
    )
    _install_valid_impersonation(connection)

    response = client.get(
        f"/admin/impersonation/operations/{OPERATION_ID}"
    )

    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store, max-age=0"
    assert "Example Importer" in response.text
    assert "Eucalyptus" in response.text
    assert "grandis" in response.text
    assert "invoice.pdf" in response.text
    _assert_readonly_chrome(response.text)
    assert len(connection.statements) == 4
    assert "vault_documents.object_key" not in connection.statements[-1]
    assert "vault_documents.storage_bucket" not in connection.statements[-1]
    assert "vault_documents.storage_backend" not in connection.statements[-1]


def test_cross_tenant_operation_is_indistinguishable_from_missing() -> None:
    connection = _SequencedConnection([[]])
    _install_valid_impersonation(connection)

    response = client.get(
        f"/admin/impersonation/operations/{FOREIGN_OPERATION_ID}"
    )

    assert response.status_code == 404
    assert response.json()["detail"] == "Operation not found."
    assert len(connection.statements) == 1
    assert "us_lacey_operations.organization_id" in connection.statements[0]


@pytest.mark.parametrize("state", ["expired", "revoked"])
def test_expired_or_revoked_impersonation_rejects_navigation(state: str) -> None:
    def reject_context():
        raise HTTPException(
            status_code=403,
            detail=f"impersonation session is {state}",
        )

    app.dependency_overrides[resolve_readonly_impersonation_context] = reject_context
    app.dependency_overrides[readonly_impersonation_db] = lambda: (_ for _ in ()).throw(
        AssertionError("database boundary must not execute after rejected context")
    )
    client.cookies.set(US_LACEY_SESSION_COOKIE, SESSION)
    client.cookies.set(IMPERSONATION_COOKIE, "invalidated-token")

    response = client.get("/admin/impersonation/operations")

    assert response.status_code == 403
    assert state in response.json()["detail"]


def test_admin_renders_view_data_form_with_purpose_bound_csrf(monkeypatch) -> None:
    identity = SimpleNamespace(user_id=7, organization_id=8, account_status="PILOT")
    account = {
        "organization_id": TARGET_ORG,
        "legal_name": "Support Tenant",
        "business_type": "IMPORTER",
        "admin_contact_email": "support@example.test",
        "account_status": "PILOT",
        "payment_provider": "LEMON_SQUEEZY",
        "payment_status": "PENDING",
        "subscription_status": "PENDING",
        "monthly_operation_limit": 25,
        "used_operations": 1,
        "queued_jobs": 0,
        "running_jobs": 0,
        "retry_jobs": 0,
        "failed_jobs": 0,
        "last_payment_event_at": None,
    }
    monkeypatch.setattr(
        admin_surface,
        "resolve_us_lacey_session",
        lambda token: identity,
    )
    monkeypatch.setattr(
        admin_surface,
        "_platform_admin_refresh_token",
        lambda token: token,
    )
    monkeypatch.setattr(
        admin_surface,
        "list_us_lacey_accounts_superadmin",
        lambda *, refresh_token: [account],
    )
    monkeypatch.setattr(
        admin_surface,
        "list_platform_users_superadmin",
        lambda *, refresh_token: [],
    )
    monkeypatch.setattr(
        admin_surface,
        "list_failed_jobs_superadmin",
        lambda *, refresh_token: [],
    )
    client.cookies.set(US_LACEY_SESSION_COOKIE, SESSION)

    response = client.get("/admin")

    assert response.status_code == 200
    assert "View Data" in response.text
    assert f'action="/admin/accounts/{TARGET_ORG}/impersonate"' in response.text
    assert 'name="reason" value="Routine support check"' in response.text
    expected_csrf = us_lacey_csrf_token(
        session_token=SESSION,
        purpose=f"platform-admin-impersonate:{TARGET_ORG}",
    )
    assert expected_csrf in response.text


def test_start_impersonation_redirects_directly_into_support_view(monkeypatch) -> None:
    identity = SimpleNamespace(user_id=7, organization_id=8, account_status="PILOT")
    monkeypatch.setattr(
        admin_surface,
        "resolve_us_lacey_session",
        lambda token: identity,
    )
    monkeypatch.setattr(
        admin_surface,
        "_platform_admin_refresh_token",
        lambda token: token,
    )
    calls = []

    def start_impersonation(**kwargs):
        calls.append(kwargs)
        return {"session_id": 9001}

    monkeypatch.setattr(
        admin_surface,
        "start_readonly_impersonation_superadmin",
        start_impersonation,
    )

    client.cookies.set(US_LACEY_SESSION_COOKIE, SESSION)
    response = client.post(
        f"/admin/accounts/{TARGET_ORG}/impersonate",
        data={
            "reason": "Routine support check",
            "csrf_token": us_lacey_csrf_token(
                session_token=SESSION,
                purpose=f"platform-admin-impersonate:{TARGET_ORG}",
            ),
        },
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/admin/impersonation/operations"
    assert calls
    assert calls[0]["organization_id"] == TARGET_ORG
    assert calls[0]["reason"] == "Routine support check"
    set_cookie = response.headers["set-cookie"]
    assert IMPERSONATION_COOKIE in set_cookie
    assert "HttpOnly" in set_cookie
    assert "Path=/admin" in set_cookie
