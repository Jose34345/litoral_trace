"""Regression for finishing a counted evaluation shipment and renewing browser expiry."""
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from litoral_trace.us_lacey import evaluation
from litoral_trace.us_lacey.access import UsLaceyOperationalAccessError
from litoral_trace.web import us_lacey_pilot_app as pilot


def _entitlement(status):
    return SimpleNamespace(
        evaluation_status=status,
        evaluation_read_only=status in {"EXHAUSTED", "EXPIRED"},
    )


def test_exhausted_user_can_review_counted_shipment_but_cannot_upload_or_create(monkeypatch):
    identity = SimpleNamespace(organization_id=42, user_id=3, email="pilot@example.com")
    monkeypatch.setattr(pilot, "_operational_context", lambda token: (identity, _entitlement("EXHAUSTED")))
    observed = []

    def can_finish(*, organization_id, operation_public_id):
        observed.append((organization_id, operation_public_id))
        return operation_public_id == "counted-uuid"

    monkeypatch.setattr(pilot, "can_finish_exhausted_evaluation_operation", can_finish)
    assert pilot._operational_mutation_context(
        "valid-synthetic", review_operation_id="counted-uuid"
    )[0] is identity
    assert observed == [(42, "counted-uuid")]

    with pytest.raises(UsLaceyOperationalAccessError):
        pilot._operational_mutation_context("valid-synthetic")
    with pytest.raises(UsLaceyOperationalAccessError):
        pilot._operational_mutation_context(
            "valid-synthetic", review_operation_id="other-uuid"
        )


def test_expired_evaluation_never_reopens_review(monkeypatch):
    monkeypatch.setattr(
        pilot, "_operational_context",
        lambda token: (SimpleNamespace(organization_id=42), _entitlement("EXPIRED")),
    )
    monkeypatch.setattr(
        pilot, "can_finish_exhausted_evaluation_operation",
        lambda **kwargs: pytest.fail("expired evaluation should never query counted shipments"),
    )
    with pytest.raises(UsLaceyOperationalAccessError):
        pilot._operational_mutation_context(
            "synthetic", review_operation_id="counted-uuid"
        )


def test_counted_review_requires_tenant_operation_and_unexpired_state(monkeypatch):
    class FakeSession:
        def __init__(self, values):
            self.values = iter(values)
        def scalar(self, stmt):
            return next(self.values)
        def close(self):
            pass

    monkeypatch.setattr(evaluation, "set_tenant_db_context", lambda session, org_id: None)
    public_id = "00000000-0000-0000-0000-000000000001"
    active = SimpleNamespace(
        status="EXHAUSTED",
        inactive_expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
    )
    assert evaluation.can_finish_exhausted_evaluation_operation(
        organization_id=42,
        operation_public_id=public_id,
        session_factory=lambda: FakeSession([active, 9]),
    )
    assert not evaluation.can_finish_exhausted_evaluation_operation(
        organization_id=42,
        operation_public_id=public_id,
        session_factory=lambda: FakeSession([active, None]),
    )
    expired = SimpleNamespace(
        status="EXHAUSTED",
        inactive_expires_at=datetime.now(timezone.utc) - timedelta(seconds=1),
    )
    assert not evaluation.can_finish_exhausted_evaluation_operation(
        organization_id=42,
        operation_public_id=public_id,
        session_factory=lambda: FakeSession([expired]),
    )


def test_claimed_evaluation_cookie_follows_sliding_server_expiry(monkeypatch):
    identity = SimpleNamespace(organization_id=42)
    expiry = datetime.now(timezone.utc) + timedelta(days=6)
    entitlement = SimpleNamespace(evaluation_claimed=True, evaluation_can_claim=False)
    monkeypatch.setattr(pilot, "_operational_context", lambda token: (identity, entitlement))
    monkeypatch.setattr(pilot, "resolve_us_lacey_session", lambda token: identity)
    monkeypatch.setattr(
        pilot,
        "get_evaluation_state",
        lambda *, organization_id: SimpleNamespace(claimed=True, inactive_expires_at=expiry),
    )
    monkeypatch.setattr(
        pilot,
        "load_us_lacey_portal_config",
        lambda: SimpleNamespace(session_cookie_secure=True),
    )
    client = TestClient(pilot.app, follow_redirects=False)
    client.cookies.set(pilot.US_LACEY_SESSION_COOKIE, "synthetic")
    response = client.get("/evaluation/save")
    assert response.status_code == 303
    cookies = response.headers.get("set-cookie", "")
    assert "us_lacey_session=synthetic" in cookies
    assert "httponly" in cookies.lower()
    assert "secure" in cookies.lower()
    assert "max-age=" in cookies.lower()
