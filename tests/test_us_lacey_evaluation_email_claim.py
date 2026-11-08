"""Email possession is required before an evaluation work address is claimed."""
from datetime import datetime, timedelta, timezone
import hashlib
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from litoral_trace.us_lacey import evaluation_claim_email as claim
from litoral_trace.us_lacey.evaluation import UsLaceyEvaluationError
from litoral_trace.web import us_lacey_pilot_app as pilot


class FakeSession:
    def __init__(self, row=None):
        self.row = row
        self.params = None
        self.committed = 0
        self.rolled_back = 0
    def execute(self, statement, params):
        self.params = params
        return self
    def mappings(self):
        return self
    def one(self):
        return self.row if self.row is not None else {"organization_id": 5, "user_id": 12}
    def commit(self):
        self.committed += 1
    def rollback(self):
        self.rolled_back += 1
    def close(self):
        pass


def test_challenge_created_with_hashed_tokens_and_no_claim_before_delivery():
    session = FakeSession()
    sent = []

    email = claim.request_evaluation_claim_email(
        session_token="synthetic-private-session",
        work_email=" Admin@Importer.Example ",
        verification_delivery=lambda address, token: sent.append((address, token)),
        session_factory=lambda: session,
    )
    assert email == "admin@importer.example"
    assert sent[0][0] == email
    assert session.params["session_hash"] == hashlib.sha256(
        b"synthetic-private-session"
    ).hexdigest()
    assert session.params["challenge_hash"] == hashlib.sha256(
        sent[0][1].encode()
    ).hexdigest()
    assert session.committed == 1
    assert session.rolled_back == 0


def test_failed_delivery_rolls_back_verification_challenge():
    session = FakeSession()
    def fail_send(*args):
        raise RuntimeError("synthetic SMTP outage; no real email sent")
    with pytest.raises(UsLaceyEvaluationError, match="Unable to send"):
        claim.request_evaluation_claim_email(
            session_token="synthetic-private-session",
            work_email="admin@importer.example",
            verification_delivery=fail_send,
            session_factory=lambda: session,
        )
    assert session.rolled_back == 1
    assert session.committed == 0


def test_finish_evaluation_consumes_token_through_postgres_only():
    expiry = datetime.now(timezone.utc) + timedelta(days=7)
    session = FakeSession({
        "organization_id": 5, "user_id": 12,
        "evaluation_status": "ACTIVE",
        "successful_operations_used": 1, "operation_limit": 5,
        "inactive_expires_at": expiry,
    })
    result = claim.complete_evaluation_claim_email(
        session_token="synthetic-session",
        verification_token="synthetic-token",
        session_factory=lambda: session,
    )
    assert result.status == "ACTIVE"
    assert result.organization_id == 5
    assert result.inactive_expires_at == expiry
    assert session.committed == 1
    assert session.params["challenge_hash"] != "synthetic-token"


def test_portal_claim_requests_mail_and_does_not_claim_directly(monkeypatch):
    identity = SimpleNamespace(organization_id=5, user_id=12)
    entitlement = SimpleNamespace(evaluation_can_claim=True, evaluation_claimed=False)
    monkeypatch.setattr(pilot, "_operational_context", lambda _: (identity, entitlement))
    monkeypatch.setattr(
        pilot, "load_us_lacey_portal_config",
        lambda: SimpleNamespace(public_origin="https://lacey.example", session_cookie_secure=True),
    )
    delivered = []
    monkeypatch.setattr(
        pilot, "request_evaluation_claim_email",
        lambda **kwargs: delivered.append(kwargs["work_email"]) or kwargs["work_email"],
    )
    session_token = "synthetic-session"
    client = TestClient(pilot.app, follow_redirects=False)
    client.cookies.set(pilot.US_LACEY_SESSION_COOKIE, session_token)
    response = client.post(
        "/evaluation/claim",
        data={
            "work_email": "admin@importer.example",
            "csrf_token": pilot.us_lacey_csrf_token(
                session_token=session_token, purpose="evaluation:claim"
            ),
        },
    )
    assert response.status_code == 200
    assert delivered == ["admin@importer.example"]


def test_mail_scanner_get_does_not_claim_and_human_post_does(monkeypatch):
    identity = SimpleNamespace(organization_id=5, user_id=12)
    entitlement = SimpleNamespace(evaluation_can_claim=True, evaluation_claimed=False)
    monkeypatch.setattr(pilot, "_operational_context", lambda _: (identity, entitlement))
    monkeypatch.setattr(
        pilot, "load_us_lacey_portal_config",
        lambda: SimpleNamespace(session_cookie_secure=True),
    )
    monkeypatch.setattr(pilot, "safe_record_outreach_event", lambda **kwargs: None)
    verified = []
    monkeypatch.setattr(
        pilot, "complete_evaluation_claim_email",
        lambda **kwargs: (
            verified.append(kwargs["verification_token"])
            or SimpleNamespace(
                organization_id=5, successful_operations_used=1,
                operation_limit=5,
                inactive_expires_at=datetime.now(timezone.utc) + timedelta(days=7),
            )
        ),
    )
    session_token = "synthetic-session"
    client = TestClient(pilot.app, follow_redirects=False)
    client.cookies.set(pilot.US_LACEY_SESSION_COOKIE, session_token)
    get_response = client.get("/evaluation/verify?token=synthetic-link")
    assert get_response.status_code == 200
    assert verified == []
    post_response = client.post(
        "/evaluation/verify",
        data={
            "verification_token": "synthetic-link",
            "csrf_token": pilot.us_lacey_csrf_token(
                session_token=session_token, purpose="evaluation:verify"
            ),
        },
    )
    assert post_response.status_code == 303
    assert verified == ["synthetic-link"]
    assert "us_lacey_session=" in post_response.headers["set-cookie"]


def test_email_claim_migration_scopes_schema_creation_to_installation():
    """Migration 076 revoked this privilege; 080 must restore least privilege."""
    from pathlib import Path

    migration = (
        Path(__file__).resolve().parents[1]
        / "alembic"
        / "versions"
        / "080_us_lacey_evaluation_email_proof.py"
    ).read_text(encoding="utf-8")
    grant = 'op.execute(f"GRANT CREATE ON SCHEMA public TO {PLATFORM_ROLE}")'
    revoke = 'op.execute(f"REVOKE CREATE ON SCHEMA public FROM {PLATFORM_ROLE}")'
    assert migration.count(grant) == 1
    assert migration.count(revoke) == 1
    assert migration.index(grant) < migration.index('op.execute(f"SET LOCAL ROLE {PLATFORM_ROLE}")')
    assert migration.index(revoke) > migration.index('op.execute("RESET ROLE")')
    assert "_reset_platform_role()" in migration
