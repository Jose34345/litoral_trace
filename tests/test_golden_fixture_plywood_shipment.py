from __future__ import annotations

import os
from pathlib import Path

import pytest
from sqlalchemy import select

import litoral_trace.us_lacey.ingestion as ingestion_module
import litoral_trace.us_lacey.worker as worker_module
from litoral_trace.db.models import (
    ReconciliationIssue,
    UsLaceyPpqPlantLine,
)
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.us_lacey.db import (
    get_us_lacey_db_session,
    reset_us_lacey_engine_state,
)
from litoral_trace.us_lacey.ingestion import UsLaceyIngestionService
from litoral_trace.us_lacey.operations import UsLaceyOperationService
from litoral_trace.us_lacey.regulatory_assessment_snapshot import (
    get_current_regulatory_assessment_view,
)
from litoral_trace.us_lacey.worker import process_one_us_lacey_job
from litoral_trace.us_lacey.worker_db import reset_us_lacey_worker_engine_state
from litoral_trace.us_lacey.workflow import (
    create_us_lacey_customer_operation,
    upload_and_enqueue_us_lacey_document_batch,
)
from litoral_trace.web.us_lacey_operational_views import _review_field_groups
from tests.test_us_lacey_worker_postgres_integration import (
    MemoryObjectStorage,
    _activate_account,
    _register_active_customer,
)


pytestmark = pytest.mark.skipif(
    os.environ.get("ENABLE_POSTGRES_TESTS") != "1"
    or not os.environ.get("US_LACEY_DATABASE_URL")
    or not os.environ.get("US_LACEY_WORKER_DATABASE_URL"),
    reason="requires isolated U.S. PostgreSQL runtime and worker credentials",
)


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "us_lacey_quality_output"
FIXTURE_FILES = (
    (
        "01_Commercial_Invoice_Entry_Worksheet.pdf",
        "application/pdf",
        "UNKNOWN",
    ),
    (
        "02_Botanical_Supplier_Declaration.pdf",
        "application/pdf",
        "UNKNOWN",
    ),
    (
        "03_BOM_Packing_List.xlsx",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "UNKNOWN",
    ),
)


def _documents() -> tuple[tuple[str, str, bytes, str], ...]:
    return tuple(
        (
            filename,
            content_type,
            (FIXTURE_DIR / filename).read_bytes(),
            role,
        )
        for filename, content_type, role in FIXTURE_FILES
    )


def test_golden_fixture_complete_shipment_is_exception_first(monkeypatch):
    """The complete three-file pack must produce a small, actionable review set."""
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

    registered, _email, suffix = _register_active_customer()
    _activate_account(registered.organization_id)

    operation = create_us_lacey_customer_operation(
        organization_id=registered.organization_id,
        user_id=registered.user_id,
        client_reference=f"GOLDEN-QOO-{suffix}",
        line_references=("1",),
    )

    queued = upload_and_enqueue_us_lacey_document_batch(
        organization_id=registered.organization_id,
        user_id=registered.user_id,
        operation_public_id=operation.public_id,
        documents=_documents(),
        ingestion=UsLaceyIngestionService(),
    )
    assert len(queued) == 3

    completed_job_ids: set[int] = set()
    for index in range(3):
        result = process_one_us_lacey_job(worker_id=f"golden-qoo-{index + 1}")
        assert result.claimed is True
        assert result.job_status == "COMPLETED"
        completed_job_ids.add(int(result.job_id))

    assert completed_job_ids == {int(item.job.id) for item in queued}

    service = UsLaceyOperationService()
    operation_id = service.get_internal_id(
        organization_id=registered.organization_id,
        operation_public_id=operation.public_id,
    )
    detail = service.get_detail(
        organization_id=registered.organization_id,
        operation_public_id=operation.public_id,
    )

    # Acceptance gate 1: the 8 BOM component rows must not become PPQ lines.
    session = get_us_lacey_db_session()
    try:
        set_tenant_db_context(session, registered.organization_id)
        plant_lines = session.scalars(
            select(UsLaceyPpqPlantLine)
            .where(
                UsLaceyPpqPlantLine.organization_id == registered.organization_id,
                UsLaceyPpqPlantLine.operation_id == operation_id,
            )
            .order_by(UsLaceyPpqPlantLine.ordinal)
        ).all()
        assert [line.line_reference for line in plant_lines] == ["1", "2", "3"]

        # Acceptance gate 2: contextual epithet/binomial equivalence must not
        # generate an open taxonomic reconciliation conflict.
        taxonomic_conflicts = session.scalars(
            select(ReconciliationIssue).where(
                ReconciliationIssue.organization_id == registered.organization_id,
                ReconciliationIssue.operation_reference
                == f"us_lacey:{operation.public_id}",
                ReconciliationIssue.field_name.in_(("genus", "species")),
                ReconciliationIssue.status == "OPEN",
            )
        ).all()
        assert taxonomic_conflicts == []
    finally:
        session.rollback()
        session.close()

    # Acceptance gate 3: SPECIAL pathways that simply do not apply are neutral.
    regulatory = get_current_regulatory_assessment_view(
        organization_id=registered.organization_id,
        operation_public_id=operation.public_id,
    )
    assert regulatory is not None
    special_assessments = [
        item
        for item in regulatory.payload.get("assessments", [])
        if item.get("rule_id") in {"SPECIAL_COMPOSITE", "SPECIAL_RECYCLED"}
    ]
    assert special_assessments
    assert all(item.get("status") != "FAIL" for item in special_assessments)

    # Acceptance gate 4: presentation must absorb both projection and canonical
    # vocabularies without hiding supported or review-required customer work.
    action_required, auto_resolved, _settled = _review_field_groups(detail)
    assert len(action_required) < 10, [
        (field.line_reference, field.field_name, field.status)
        for field in action_required
    ]

    action_ids = {field.id for field in action_required}
    auto_ids = {field.id for field in auto_resolved}
    assert {
        field.id for field in detail.fields if field.status == "REVIEW"
    } <= action_ids
    assert {
        field.id for field in detail.fields if field.status == "FOUND"
    } <= auto_ids

    reset_us_lacey_worker_engine_state()
    reset_us_lacey_engine_state()
