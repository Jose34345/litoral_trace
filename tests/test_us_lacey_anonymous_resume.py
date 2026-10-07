from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from litoral_trace.us_lacey._operations_core import UsLaceyOperationConflict
from litoral_trace.us_lacey.evaluation import UsLaceyEvaluationError
from litoral_trace.us_lacey.portal_auth import (
    US_LACEY_SESSION_COOKIE,
    UsLaceyPortalIdentity,
)
from litoral_trace.us_lacey.workflow import (
    UsLaceyWorkflowError,
    create_us_lacey_customer_operation,
)
from litoral_trace.web.us_lacey_pilot_app import app


NO_SLOT = "This evaluation has no available shipment slots right now."


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


class _SingleExistingOperations:
    def __init__(self) -> None:
        self.create_calls: list[dict[str, object]] = []

    def list_operations(self, **_kwargs):
        return [SimpleNamespace(public_id="OP-IN-PROGRESS", status="NEW")]

    def get_operation(self, **_kwargs):
        return SimpleNamespace(public_id="OP-IN-PROGRESS", status="NEW")

    def create_operation(self, **kwargs):
        self.create_calls.append(kwargs)
        return SimpleNamespace(public_id="OP-UNEXPECTED-NEW", status="NEW")


class _FiveExistingOperations(_SingleExistingOperations):
    def list_operations(self, **_kwargs):
        return [
            SimpleNamespace(public_id=f"OP-{index}", status="NEW")
            for index in range(1, 6)
        ]


def test_no_slot_preflight_resumes_the_single_existing_evaluation_shipment(monkeypatch):
    service = _SingleExistingOperations()
    access_calls: list[dict[str, object]] = []

    def no_capacity(**_kwargs):
        raise UsLaceyEvaluationError(NO_SLOT)

    monkeypatch.setattr(
        "litoral_trace.us_lacey.workflow.require_evaluation_creation_capacity",
        no_capacity,
    )
    monkeypatch.setattr(
        "litoral_trace.us_lacey.workflow.require_us_lacey_operational_access",
        lambda **kwargs: access_calls.append(kwargs),
    )

    result = create_us_lacey_customer_operation(
        organization_id=122,
        user_id=21,
        client_reference="LACEY-RETRY",
        line_references=("1",),
        operations=service,
    )

    assert result.public_id == "OP-IN-PROGRESS"
    assert service.create_calls == []
    assert access_calls == [
        {
            "organization_id": 122,
            "require_operation_slot": False,
            "require_mutation_access": True,
        }
    ]


def test_no_slot_preflight_does_not_mask_a_full_multi_shipment_evaluation(monkeypatch):
    service = _FiveExistingOperations()

    def no_capacity(**_kwargs):
        raise UsLaceyEvaluationError(NO_SLOT)

    monkeypatch.setattr(
        "litoral_trace.us_lacey.workflow.require_evaluation_creation_capacity",
        no_capacity,
    )

    with pytest.raises(UsLaceyWorkflowError, match="no available shipment slots"):
        create_us_lacey_customer_operation(
            organization_id=122,
            user_id=21,
            client_reference="LACEY-SIXTH",
            line_references=("1",),
            operations=service,
        )


def test_concurrent_create_conflict_resumes_the_operation_that_won_the_race(monkeypatch):
    service = _SingleExistingOperations()

    monkeypatch.setattr(
        "litoral_trace.us_lacey.workflow.require_evaluation_creation_capacity",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        "litoral_trace.us_lacey.workflow.require_us_lacey_operational_access",
        lambda **_kwargs: None,
    )

    def conflict(**_kwargs):
        raise UsLaceyOperationConflict(NO_SLOT)

    service.create_operation = conflict

    result = create_us_lacey_customer_operation(
        organization_id=122,
        user_id=21,
        client_reference="LACEY-RACE",
        line_references=("1",),
        operations=service,
    )

    assert result.public_id == "OP-IN-PROGRESS"


def test_stale_new_shipment_post_uploads_into_the_resumed_operation(monkeypatch):
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
        lambda **_kwargs: SimpleNamespace(public_id="OP-IN-PROGRESS"),
    )
    monkeypatch.setattr(
        "litoral_trace.web.us_lacey_pilot_app.upload_and_enqueue_us_lacey_document_batch",
        lambda **kwargs: uploaded.update(kwargs),
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
    assert uploaded["operation_public_id"] == "OP-IN-PROGRESS"
    assert len(uploaded["documents"]) == 1
