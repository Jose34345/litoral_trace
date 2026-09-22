from __future__ import annotations

import base64
import hashlib
import os
from pathlib import Path
from uuid import uuid4
import zlib

import pytest
from sqlalchemy import create_engine, text

import litoral_trace.us_lacey.ingestion as ingestion_module
import litoral_trace.us_lacey.worker as worker_module
from litoral_trace.us_lacey.commercial import UsLaceyCommercialConfig
from litoral_trace.us_lacey.db import reset_us_lacey_engine_state
from litoral_trace.us_lacey.ingestion import UsLaceyIngestionService
from litoral_trace.us_lacey.operations import UsLaceyOperationService
from litoral_trace.us_lacey.regulatory_assessment_snapshot import (
    get_current_regulatory_assessment_view,
)
from litoral_trace.us_lacey.self_service import (
    register_us_lacey_company,
    verify_us_lacey_email,
)
from litoral_trace.us_lacey.worker import process_one_us_lacey_job
from litoral_trace.us_lacey.worker_db import reset_us_lacey_worker_engine_state
from litoral_trace.us_lacey.workflow import (
    create_us_lacey_customer_operation,
    upload_and_enqueue_us_lacey_document_batch,
)
from litoral_trace.web.us_lacey_operational_views import _review_field_groups
from tests.test_us_lacey_worker_postgres_integration import MemoryObjectStorage


pytestmark = pytest.mark.skipif(
    os.environ.get("ENABLE_POSTGRES_TESTS") != "1"
    or not os.environ.get("US_LACEY_DATABASE_URL")
    or not os.environ.get("US_LACEY_WORKER_DATABASE_URL"),
    reason="requires isolated U.S. PostgreSQL runtime and worker credentials",
)


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "us_lacey_quality_output"
FIXTURES = (
    (
        "01_Commercial_Invoice_Entry_Worksheet.pdf",
        "application/pdf",
        "57f2d05651c6207ad0f1cfc180578371e307e736912cc58222ee2252207db923",
    ),
    (
        "02_Botanical_Supplier_Declaration.pdf",
        "application/pdf",
        "8fb34eab3515ce28797214f765a6c6410f31a4dc2489076101b84206776c48be",
    ),
    (
        "03_BOM_Packing_List.xlsx",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "c1ab63c1a83284ee19cf11ebc5f4a83c9fd2ff5e95dba5c6171ef113417c13f4",
    ),
)


def _fixture_bytes(filename: str, expected_sha256: str) -> bytes:
    encoded = (
        FIXTURE_DIR / f"{filename}.zlib.b64"
    ).read_text(encoding="ascii")
    payload = zlib.decompress(base64.b64decode("".join(encoded.split())))
    assert hashlib.sha256(payload).hexdigest() == expected_sha256
    return payload


def _commercial_config() -> UsLaceyCommercialConfig:
    return UsLaceyCommercialConfig(
        price_cents=14900,
        monthly_operation_limit=100,
        payment_provider="MANUAL_BANK_TRANSFER",
        bank_transfer_instructions="CI-only payment instructions",
        terms_version="terms-quality-v1",
        privacy_version="privacy-quality-v1",
        beta_terms_version="beta-quality-v1",
        support_email="support@litoraltrace.com",
    )


def _activate_account(organization_id: int) -> None:
    runtime = create_engine(
        os.environ["US_LACEY_DATABASE_URL"],
        pool_pre_ping=True,
        hide_parameters=True,
    )
    try:
        with runtime.begin() as connection:
            connection.execute(
                text(
                    "SELECT set_config('app.current_organization_id', "
                    ":organization_id, true)"
                ),
                {"organization_id": str(organization_id)},
            )
            connection.execute(
                text(
                    "UPDATE public.us_lacey_organization_profiles "
                    "SET account_status='ACTIVE', updated_at=now() "
                    "WHERE organization_id=:organization_id"
                ),
                {"organization_id": organization_id},
            )
            connection.execute(
                text(
                    "UPDATE public.us_lacey_subscriptions "
                    "SET status='ACTIVE', started_at=coalesce(started_at, now()), "
                    "updated_at=now() WHERE organization_id=:organization_id"
                ),
                {"organization_id": organization_id},
            )
            connection.execute(
                text(
                    "UPDATE public.us_lacey_payments "
                    "SET status='VERIFIED', verified_at=now(), paid_at=now(), "
                    "updated_at=now() WHERE organization_id=:organization_id"
                ),
                {"organization_id": organization_id},
            )
    finally:
        runtime.dispose()


def _register_active_customer():
    suffix = uuid4().hex[:12]
    registered = register_us_lacey_company(
        legal_name=f"Quality Output Imports {suffix} LLC",
        business_type="CUSTOMS_BROKER",
        admin_name="Quality Output Reviewer",
        admin_email=f"quality-{suffix}@example.com",
        password="correct-horse-quality-123",
        commercial_config=_commercial_config(),
    )
    verify_us_lacey_email(registered.verification_token)
    _activate_account(registered.organization_id)
    return registered


def _database_quality_metrics(*, organization_id: int, operation_id: int):
    runtime = create_engine(
        os.environ["US_LACEY_DATABASE_URL"],
        pool_pre_ping=True,
        hide_parameters=True,
    )
    try:
        with runtime.begin() as connection:
            connection.execute(
                text(
                    "SELECT set_config('app.current_organization_id', "
                    ":organization_id, true)"
                ),
                {"organization_id": str(organization_id)},
            )
            plant_line_count = connection.execute(
                text(
                    "SELECT count(*) FROM public.us_lacey_ppq_plant_lines "
                    "WHERE organization_id=:organization_id "
                    "AND operation_id=:operation_id"
                ),
                {
                    "organization_id": organization_id,
                    "operation_id": operation_id,
                },
            ).scalar_one()
            taxonomic_conflicts = connection.execute(
                text(
                    "SELECT count(*) FROM public.reconciliation_issues "
                    "WHERE organization_id=:organization_id "
                    "AND operation_reference=:operation_reference "
                    "AND status='OPEN' "
                    "AND field_name IN ('genus','species')"
                ),
                {
                    "organization_id": organization_id,
                    "operation_reference": f"us_lacey:{operation_id}",
                },
            ).scalar_one()
        return int(plant_line_count), int(taxonomic_conflicts)
    finally:
        runtime.dispose()


def test_complete_three_document_golden_shipment_is_exception_first(monkeypatch):
    """The exact three-file commercial demo packet must produce a small review queue."""
    reset_us_lacey_engine_state()
    reset_us_lacey_worker_engine_state()
    storage = MemoryObjectStorage()
    monkeypatch.setattr(
        ingestion_module,
        "get_us_lacey_storage_client",
        lambda: storage,
    )
    monkeypatch.setattr(
        worker_module,
        "get_us_lacey_storage_client",
        lambda: storage,
    )

    registered = _register_active_customer()
    operations = UsLaceyOperationService()
    operation = create_us_lacey_customer_operation(
        organization_id=registered.organization_id,
        user_id=registered.user_id,
        client_reference="LT-SBX-2026-0921-COMPLETE",
        line_references=("1",),
    )

    documents = tuple(
        (
            filename,
            content_type,
            _fixture_bytes(filename, expected_sha),
            "UNKNOWN",
        )
        for filename, content_type, expected_sha in FIXTURES
    )
    queued = upload_and_enqueue_us_lacey_document_batch(
        organization_id=registered.organization_id,
        user_id=registered.user_id,
        operation_public_id=operation.public_id,
        documents=documents,
        ingestion=UsLaceyIngestionService(),
    )
    assert len(queued) == 3

    results = [
        process_one_us_lacey_job(worker_id=f"quality-output-worker-{index}")
        for index in range(1, 4)
    ]
    assert all(result.claimed for result in results)
    assert all(result.job_status == "COMPLETED" for result in results)

    detail = operations.get_detail(
        organization_id=registered.organization_id,
        operation_public_id=operation.public_id,
    )
    attention, supported, _settled = _review_field_groups(detail)

    operation_id = operations.get_internal_id(
        organization_id=registered.organization_id,
        operation_public_id=operation.public_id,
    )

    runtime = create_engine(
        os.environ["US_LACEY_DATABASE_URL"],
        pool_pre_ping=True,
        hide_parameters=True,
    )
    try:
        with runtime.begin() as connection:
            connection.execute(
                text(
                    "SELECT set_config('app.current_organization_id', "
                    ":organization_id, true)"
                ),
                {"organization_id": str(registered.organization_id)},
            )
            plant_line_count = int(
                connection.execute(
                    text(
                        "SELECT count(*) FROM public.us_lacey_ppq_plant_lines "
                        "WHERE organization_id=:organization_id "
                        "AND operation_id=:operation_id"
                    ),
                    {
                        "organization_id": registered.organization_id,
                        "operation_id": operation_id,
                    },
                ).scalar_one()
            )
            taxonomic_conflicts = int(
                connection.execute(
                    text(
                        "SELECT count(*) FROM public.reconciliation_issues "
                        "WHERE organization_id=:organization_id "
                        "AND operation_reference=:operation_reference "
                        "AND status='OPEN' "
                        "AND field_name IN ('genus','species')"
                    ),
                    {
                        "organization_id": registered.organization_id,
                        "operation_reference": f"us_lacey:{operation.public_id}",
                    },
                ).scalar_one()
            )
    finally:
        runtime.dispose()

    assert plant_line_count == 3, "phantom PPQ plant lines are forbidden"
    assert taxonomic_conflicts == 0, "contextually equivalent taxonomy must reconcile"
    assert len(attention) < 10, (
        f"Exception-First contract violated: {len(attention)} fields require action"
    )
    assert supported, "the golden packet must auto-resolve at least one field"

    regulatory = get_current_regulatory_assessment_view(
        organization_id=registered.organization_id,
        operation_public_id=operation.public_id,
    )
    assert regulatory is not None
    false_special_failures = [
        item
        for item in regulatory.payload.get("assessments", [])
        if item.get("rule_id") in {"SPECIAL_COMPOSITE", "SPECIAL_RECYCLED"}
        and item.get("status") == "FAIL"
    ]
    assert false_special_failures == []

    reset_us_lacey_worker_engine_state()
    reset_us_lacey_engine_state()
