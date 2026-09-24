from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
from sqlalchemy import select

from litoral_trace.db.models import (
    DocumentExtractionRun,
    ExtractedDocumentField,
    ReconciliationIssue,
    UsLaceyOperation,
    UsLaceyOperationField,
    UsLaceyPpqPlantLine,
)
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.us_lacey.db import (
    get_us_lacey_db_session,
    reset_us_lacey_engine_state,
)
from litoral_trace.us_lacey.operations import UsLaceyOperationService
from litoral_trace.us_lacey.projection import project_assurance_document_to_us_lacey
from litoral_trace.us_lacey.workflow import create_us_lacey_customer_operation
from litoral_trace.web.us_lacey_operational_views import _review_field_groups
from tests.test_us_lacey_worker_postgres_integration import (
    _activate_account,
    _register_active_customer,
)
from tests.us_lacey_engine2_postgres import (
    add_test_document,
    engine2_postgres_engine,
    engine2_postgres_session_factory,
    tenant_session,
)


pytestmark = pytest.mark.skipif(
    os.environ.get("ENABLE_POSTGRES_TESTS") != "1"
    or not os.environ.get("US_LACEY_DATABASE_URL"),
    reason="Golden Pack 2 requires the isolated PostgreSQL gate.",
)


FIXTURE = Path(__file__).parent / "fixtures" / "us_lacey_golden_pack_2.json"


def _seed_extraction(factory, *, organization_id: int, assurance_document_id: int, fields):
    session = tenant_session(factory, organization_id)
    run = DocumentExtractionRun(
        organization_id=organization_id,
        assurance_document_id=assurance_document_id,
        engine="golden-pack-2",
        engine_version="1",
        status="COMPLETED",
    )
    session.add(run)
    session.flush()
    for index, field in enumerate(fields, start=1):
        session.add(
            ExtractedDocumentField(
                organization_id=organization_id,
                assurance_document_id=assurance_document_id,
                extraction_run_id=run.id,
                field_name=field["field_name"],
                original_value=field["value"],
                normalized_value=None,
                value_type="text",
                confidence=0.96,
                confidence_level="HIGH",
                source_page=1,
                source_locator=field["locator"],
                auto_accepted=True,
                needs_review=False,
            )
        )
    session.commit()
    session.close()


def _canonical_pair(issue: ReconciliationIssue) -> frozenset[str]:
    return frozenset(
        str(value).strip().casefold()
        for value in (issue.left_value, issue.right_value)
        if value is not None
    )


def test_golden_fixture_pack_2_projects_three_intentional_conflicts_and_botanical_line(
    engine2_postgres_session_factory,
):
    reset_us_lacey_engine_state()
    registered, _email, suffix = _register_active_customer()
    _activate_account(registered.organization_id)

    operation = create_us_lacey_customer_operation(
        organization_id=registered.organization_id,
        user_id=registered.user_id,
        client_reference=f"GOLDEN-PACK2-{suffix}",
        line_references=(),
    )
    operation_id = UsLaceyOperationService().get_internal_id(
        organization_id=registered.organization_id,
        operation_public_id=operation.public_id,
    )

    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
    assurance_ids: list[int] = []
    for document in payload["documents"]:
        _link, assurance_id, _vault, _sha = add_test_document(
            engine2_postgres_session_factory,
            organization_id=registered.organization_id,
            operation_id=operation_id,
            role=document["role"],
            filename=document["filename"],
            content=document["filename"].encode("utf-8"),
        )
        assurance_ids.append(assurance_id)
        _seed_extraction(
            engine2_postgres_session_factory,
            organization_id=registered.organization_id,
            assurance_document_id=assurance_id,
            fields=document["fields"],
        )

    session = tenant_session(engine2_postgres_session_factory, registered.organization_id)
    operation_row = session.get(UsLaceyOperation, operation_id)
    operation_row.document_count = len(assurance_ids)
    session.commit()
    session.close()

    results = []
    for assurance_id in assurance_ids:
        results.append(
            project_assurance_document_to_us_lacey(
                organization_id=registered.organization_id,
                operation_id=operation_id,
                assurance_document_id=assurance_id,
            )
        )

    session = tenant_session(engine2_postgres_session_factory, registered.organization_id)
    try:
        plant_lines = session.scalars(
            select(UsLaceyPpqPlantLine).where(
                UsLaceyPpqPlantLine.organization_id == registered.organization_id,
                UsLaceyPpqPlantLine.operation_id == operation_id,
            )
        ).all()
        assert [line.line_reference for line in plant_lines] == ["1"]

        hts = session.scalar(
            select(UsLaceyOperationField).where(
                UsLaceyOperationField.organization_id == registered.organization_id,
                UsLaceyOperationField.operation_id == operation_id,
                UsLaceyOperationField.merchandise_line_reference == "1",
                UsLaceyOperationField.field_name == "hts_code",
            )
        )
        assert hts is not None
        assert hts.normalized_value == "4407990190"

        botanical_fields = session.scalars(
            select(UsLaceyOperationField).where(
                UsLaceyOperationField.organization_id == registered.organization_id,
                UsLaceyOperationField.operation_id == operation_id,
                UsLaceyOperationField.merchandise_line_reference == "1",
                UsLaceyOperationField.field_name.in_(
                    ("genus", "species", "country_of_harvest")
                ),
            )
        ).all()
        assert {row.field_name for row in botanical_fields} == {
            "genus",
            "species",
            "country_of_harvest",
        }

        conflicts = session.scalars(
            select(ReconciliationIssue)
            .where(
                ReconciliationIssue.organization_id == registered.organization_id,
                ReconciliationIssue.operation_reference
                == f"us_lacey:{operation.public_id}",
                ReconciliationIssue.rule_code == "US_LACEY_FIELD_CONFLICT",
                ReconciliationIssue.status == "OPEN",
            )
            .order_by(ReconciliationIssue.id.asc())
        ).all()

        assert len(conflicts) == 3
        by_field = {issue.field_name: issue for issue in conflicts}
        assert set(by_field) == {
            "estimated_arrival_date",
            "entered_value",
            "country_of_harvest",
        }
        assert _canonical_pair(by_field["estimated_arrival_date"]) == {
            "2026-09-29",
            "2026-10-01",
        }
        assert _canonical_pair(by_field["entered_value"]) == {"48600", "49050"}
        assert _canonical_pair(by_field["country_of_harvest"]) == {
            "peru",
            "brazil",
        }
        assert all(issue.severity == "BLOCKING" for issue in conflicts)
    finally:
        session.close()

    detail = UsLaceyOperationService().get_detail(
        organization_id=registered.organization_id,
        operation_public_id=operation.public_id,
    )
    attention, _supported, _settled = _review_field_groups(detail)
    conflict_cards = {
        field.field_name: field
        for field in attention
        if field.status == "CONFLICT"
    }
    assert set(conflict_cards) == {
        "estimated_arrival_date",
        "entered_value",
        "country_of_harvest",
    }
    assert all(result.operation_status in {"REVIEW_REQUIRED", "READY_FOR_REVIEW"} for result in results)

    reset_us_lacey_engine_state()
