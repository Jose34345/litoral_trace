from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from litoral_trace.us_lacey import evaluation
from litoral_trace.us_lacey.evaluation import (
    EVALUATION_INACTIVITY_DAYS,
    EVALUATION_OPERATION_LIMIT,
    EVALUATION_RAW_RETENTION_HOURS,
    UsLaceyEvaluationError,
    normalize_work_email,
)
from litoral_trace.web.us_lacey_unified_app import app


ROOT = Path(__file__).resolve().parents[1]


def test_evaluation_product_contract_constants() -> None:
    assert EVALUATION_OPERATION_LIMIT == 5
    assert EVALUATION_INACTIVITY_DAYS == 7
    assert EVALUATION_RAW_RETENTION_HOURS == 4


@pytest.mark.parametrize(
    "email",
    (
        "buyer@gmail.com",
        "buyer@outlook.com",
        "buyer@yahoo.com",
        "buyer@proton.me",
    ),
)
def test_evaluation_claim_requires_work_email(email: str) -> None:
    with pytest.raises(UsLaceyEvaluationError, match="work email"):
        normalize_work_email(email)


def test_evaluation_claim_accepts_and_normalizes_work_email() -> None:
    assert normalize_work_email("  Compliance@Example-Lumber.com ") == (
        "compliance@example-lumber.com"
    )


def test_public_samples_are_ungated_read_only_and_do_not_create_session_cookie() -> None:
    client = TestClient(app)
    client.cookies.clear()

    importer = client.get("/try/importer", follow_redirects=False)
    broker = client.get("/try/broker", follow_redirects=False)
    reuse = client.get("/try/importer?step=2", follow_redirects=False)

    assert importer.status_code == 200
    assert broker.status_code == 200
    assert reuse.status_code == 200
    assert "sample · read-only" in importer.text
    assert "Importer" in importer.text
    assert "Customs Broker" in broker.text
    assert "verified supplier evidence" in reuse.text.lower()
    assert "reused" in reuse.text.lower()
    assert "set-cookie" not in importer.headers
    assert "set-cookie" not in broker.headers
    assert "set-cookie" not in reuse.headers
    assert "/try/importer?step=2" in importer.text
    assert "/sandbox/start?mode=own" in reuse.text
    assert "credit card" in importer.text.lower()


def test_claim_surface_requests_only_work_email_and_no_payment_identity_fields() -> None:
    source = (
        ROOT
        / "src"
        / "litoral_trace"
        / "templates"
        / "us_lacey"
        / "evaluation_claim.html"
    ).read_text(encoding="utf-8")

    assert 'name="work_email"' in source
    assert 'name="csrf_token"' in source
    for forbidden in (
        'name="password"',
        'name="company"',
        'name="full_name"',
        'name="phone"',
        'name="card',
        "Paddle.Checkout",
    ):
        assert forbidden not in source
    assert "no credit card" in source.lower()
    assert "4 hours" in source
    assert "7 days of inactivity" in source


def test_evaluation_ui_exposes_five_shipment_and_read_only_contract() -> None:
    sandbox = (
        ROOT
        / "src"
        / "litoral_trace"
        / "templates"
        / "us_lacey"
        / "sandbox_start.html"
    ).read_text(encoding="utf-8")
    base = (
        ROOT
        / "src"
        / "litoral_trace"
        / "templates"
        / "us_lacey"
        / "base.html"
    ).read_text(encoding="utf-8")
    new_operation = (
        ROOT
        / "src"
        / "litoral_trace"
        / "templates"
        / "us_lacey"
        / "new_operation.html"
    ).read_text(encoding="utf-8")
    operation_detail = (
        ROOT
        / "src"
        / "litoral_trace"
        / "templates"
        / "us_lacey"
        / "operation_detail.html"
    ).read_text(encoding="utf-8")
    upgrade = (
        ROOT
        / "src"
        / "litoral_trace"
        / "templates"
        / "us_lacey"
        / "evaluation_upgrade.html"
    ).read_text(encoding="utf-8")

    assert "5-shipment evaluation" in sandbox
    assert "Raw documents are permanently deleted after 4 hours" in sandbox
    assert "Private processing" in sandbox
    assert "Tenant isolated" in sandbox
    assert "Model improvement is off by default" in sandbox
    assert 'name="learning_consent"' not in sandbox
    assert "Evaluation progress" in base
    assert "Private evaluation" in base
    assert "Anonymous workspace" in base
    assert "data-evaluation-privacy-banner" in base
    assert "No card required" in base
    assert "Evaluation shipments remaining" in new_operation
    assert "Reprocessing does not consume another shipment" in new_operation
    assert "Model improvement is off by default" in operation_detail
    assert 'name="learning_consent"' in operation_detail
    assert "Raw file deletion scheduled for" in operation_detail
    assert "Raw source documents are never used for model improvement" in operation_detail
    assert "Five-shipment evaluation complete" in base
    assert "read-only" in upgrade.lower()
    assert "results remain available" in upgrade.lower()


def test_migration_encodes_success_only_exact_once_and_retention_contract() -> None:
    migration = (
        ROOT
        / "alembic"
        / "versions"
        / "076_us_lacey_product_led_evaluation.py"
    ).read_text(encoding="utf-8")

    assert 'revision = "076_us_lacey_product_led_evaluation"' in migration
    assert 'down_revision = "075_us_lacey_identity_and_product_bridge"' in migration
    assert "operation_limit = 5" in migration
    assert "raw_retention_hours = 4" in migration
    assert "interval '4 hours'" in migration
    assert "interval '7 days'" in migration
    assert "evaluation.status IN ('ANONYMOUS','ACTIVE','EXHAUSTED')" in migration
    assert "uq_us_lacey_evaluation_operations_operation" in migration
    assert "'READY_FOR_REVIEW'" in migration
    assert "'REVIEW_REQUIRED'" in migration
    assert "'COMPLETED'" in migration
    assert "current_status NOT IN" in migration
    assert "EVALUATION_OPERATION_2" in migration
    assert "EVALUATION_EXHAUSTED" in migration
    assert "PQL_QUALIFIED" in migration
    assert "plan_code = 'EVALUATION'" in migration
    assert "price_cents = 0" in migration
    assert "FORCE ROW LEVEL SECURITY" in migration


def test_operation_creation_serializes_evaluation_capacity() -> None:
    source = (
        ROOT
        / "src"
        / "litoral_trace"
        / "us_lacey"
        / "_operations_core.py"
    ).read_text(encoding="utf-8")

    assert "select(UsLaceyEvaluation)" in source
    assert ".with_for_update()" in source
    assert 'UsLaceyOperation.status != "FAILED"' in source
    assert '"ANONYMOUS"' in source
    assert '"EXHAUSTED"' in source


def test_growth_plane_contains_activation_and_pql_vocabulary() -> None:
    source = (
        ROOT
        / "src"
        / "litoral_trace"
        / "us_lacey"
        / "growth_attribution.py"
    ).read_text(encoding="utf-8")

    for event in (
        "SAMPLE_STARTED",
        "SAMPLE_REUSE_REACHED",
        "SAMPLE_COMPLETED",
        "OWN_SHIPMENT_STARTED",
        "OWN_SHIPMENT_PROCESSED",
        "EVALUATION_CLAIMED",
        "EVALUATION_OPERATION_2",
        "EVIDENCE_REUSED",
        "EVALUATION_EXHAUSTED",
        "UPGRADE_STARTED",
        "PQL_QUALIFIED",
    ):
        assert event in source
