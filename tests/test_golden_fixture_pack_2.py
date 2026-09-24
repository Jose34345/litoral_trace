from __future__ import annotations

import os
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import select

from litoral_trace.db.models import (
    AssuranceDocument,
    DocumentExtractionRun,
    ExtractedDocumentField,
    ReconciliationIssue,
    UsLaceyPpqPlantLine,
    VaultDocument,
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


pytestmark = pytest.mark.skipif(
    os.environ.get("ENABLE_POSTGRES_TESTS") != "1"
    or not os.environ.get("US_LACEY_DATABASE_URL")
    or not os.environ.get("TEST_POSTGRES_MIGRATION_DATABASE_URL"),
    reason="requires the isolated U.S. PostgreSQL integration database",
)


def _add_extracted_document(
    *,
    organization_id: int,
    user_id: int,
    filename: str,
    fields: tuple[tuple[str, str, str], ...],
) -> int:
    session = get_us_lacey_db_session()
    try:
        set_tenant_db_context(session, organization_id)
        token = uuid4().hex
        vault = VaultDocument(
            organization_id=organization_id,
            created_by_user_id=user_id,
            original_filename=filename,
            content_type="application/pdf",
            size_bytes=1024,
            sha256=(token * 4)[:64],
            object_key=f"us-lacey/golden-pack2/{organization_id}/{token}",
            storage_backend="s3",
            storage_bucket="us-lacey-ci-private",
            document_type="OTHER_EVIDENCE",
            status="available",
        )
        session.add(vault)
        session.flush()

        assurance = AssuranceDocument(
            organization_id=organization_id,
            vault_document_id=vault.id,
            semantic_document_type="UNKNOWN",
            type_confidence=0.99,
            processing_status="NEEDS_REVIEW",
        )
        session.add(assurance)
        session.flush()

        run = DocumentExtractionRun(
            organization_id=organization_id,
            assurance_document_id=assurance.id,
            engine="assurance-deterministic-parser",
            engine_version="pack2-golden-v1",
            status="SUCCEEDED",
        )
        session.add(run)
        session.flush()

        for field_name, value, locator in fields:
            session.add(
                ExtractedDocumentField(
                    organization_id=organization_id,
                    assurance_document_id=assurance.id,
                    extraction_run_id=run.id,
                    field_name=field_name,
                    original_value=value,
                    normalized_value=value,
                    value_type="cell",
                    confidence=0.98,
                    confidence_level="HIGH",
                    source_page=1,
                    source_locator=locator,
                    auto_accepted=False,
                    needs_review=True,
                )
            )
        session.commit()
        return assurance.id
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


PACK2 = (
    (
        "01_Commercial_Invoice.pdf",
        "COMMERCIAL_INVOICE",
        (
            ("raw.table.1.Line", "1", "table:1;data_row:1;column:1"),
            ("raw.table.1.SKU", "CO-18", "table:1;data_row:1;column:2"),
            (
                "raw.table.1.Description",
                "Sawn cedar boards - Cedrela odorata",
                "table:1;data_row:1;column:3",
            ),
            ("raw.table.1.HTS", "4407.99.0190", "table:1;data_row:1;column:4"),
            ("raw.table.1.Qty", "18.000 m3 / 900 pcs", "table:1;data_row:1;column:5"),
            ("raw.table.1.Unit Price", "2700.00/m3", "table:1;data_row:1;column:6"),
            ("raw.table.1.Amount", "48600.00", "table:1;data_row:1;column:7"),
            ("raw.table.2.Entered Value", "48600.00", "table:2;data_row:1;column:1"),
            ("raw.table.3.Container", "TGHU5519023", "table:3;data_row:1;column:1"),
            ("raw.table.3.BOL", "ACE-MIA-260913-77", "table:3;data_row:1;column:2"),
        ),
    ),
    (
        "02_Bill_of_Lading.pdf",
        "BILL_OF_LADING",
        (
            ("raw.table.1.BOL", "ACE-MIA-260913-77", "table:1;data_row:1;column:1"),
            ("raw.table.1.Container", "TGHU5519023", "table:1;data_row:1;column:2"),
            ("raw.table.1.Consignee", "Timberline Imports LLC", "table:1;data_row:1;column:3"),
            ("raw.table.1.ETA", "2026-09-29", "table:1;data_row:1;column:4"),
        ),
    ),
    (
        "03_Packing_List.pdf",
        "PACKING_LIST",
        (
            ("raw.table.1.BOL", "ACE-MIA-260913-77", "table:1;data_row:1;column:1"),
            ("raw.table.1.Container", "TGHU5519023", "table:1;data_row:1;column:2"),
        ),
    ),
    (
        "04_Botanical_Declaration.pdf",
        "BOTANICAL_DECLARATION",
        (
            ("raw.table.1.Genus", "Cedrela", "table:1;data_row:1;column:1"),
            ("raw.table.1.Species", "odorata", "table:1;data_row:1;column:2"),
            ("raw.table.1.Country of Harvest", "Peru", "table:1;data_row:1;column:3"),
            ("raw.table.1.Plant Quantity", "18.000", "table:1;data_row:1;column:4"),
            ("raw.table.1.Unit", "m3", "table:1;data_row:1;column:5"),
        ),
    ),
    (
        "05_Supplier_Origin_Declaration.pdf",
        "SUPPLIER_DECLARATION",
        (
            ("raw.table.1.Genus", "Cedrela", "table:1;data_row:1;column:1"),
            ("raw.table.1.Species", "odorata", "table:1;data_row:1;column:2"),
            ("raw.table.1.Country of Harvest", "Brazil", "table:1;data_row:1;column:3"),
            ("raw.table.1.Plant Quantity", "18.000", "table:1;data_row:1;column:4"),
            ("raw.table.1.Unit", "m3", "table:1;data_row:1;column:5"),
        ),
    ),
    (
        "06_Entry_Worksheet.pdf",
        "CUSTOMS_ENTRY",
        (
            ("raw.table.1.Line", "1", "table:1;data_row:1;column:1"),
            ("raw.table.1.HTS", "4407.99.0190", "table:1;data_row:1;column:2"),
            (
                "raw.table.1.Description",
                "Cedrela odorata sawn boards",
                "table:1;data_row:1;column:3",
            ),
            ("raw.table.1.Entered Value", "49050.00", "table:1;data_row:1;column:4"),
        ),
    ),
    (
        "07_Arrival_Notice.pdf",
        "ARRIVAL_NOTICE",
        (
            ("raw.table.1.BOL", "ACE-MIA-260913-77", "table:1;data_row:1;column:1"),
            ("raw.table.1.Container", "TGHU5519023", "table:1;data_row:1;column:2"),
            ("raw.table.1.Consignee", "Timberline Imports LLC", "table:1;data_row:1;column:3"),
            ("raw.table.1.ETA", "2026-10-01", "table:1;data_row:1;column:4"),
        ),
    ),
)


def test_golden_fixture_pack_2_projects_exact_three_intentional_conflicts():
    reset_us_lacey_engine_state()
    registered, _email, suffix = _register_active_customer()
    _activate_account(registered.organization_id)

    operation = create_us_lacey_customer_operation(
        organization_id=registered.organization_id,
        user_id=registered.user_id,
        client_reference=f"GOLDEN-PACK2-{suffix}",
        line_references=(),
    )
    service = UsLaceyOperationService()
    operation_id = service.get_internal_id(
        organization_id=registered.organization_id,
        operation_public_id=operation.public_id,
    )

    for filename, role, fields in PACK2:
        document_id = _add_extracted_document(
            organization_id=registered.organization_id,
            user_id=registered.user_id,
            filename=filename,
            fields=fields,
        )
        service.attach_document(
            organization_id=registered.organization_id,
            operation_public_id=operation.public_id,
            assurance_document_id=document_id,
            document_role=role,
        )
        project_assurance_document_to_us_lacey(
            organization_id=registered.organization_id,
            operation_id=operation_id,
            assurance_document_id=document_id,
        )

    detail = service.get_detail(
        organization_id=registered.organization_id,
        operation_public_id=operation.public_id,
    )

    session = get_us_lacey_db_session()
    try:
        set_tenant_db_context(session, registered.organization_id)
        plant_lines = session.scalars(
            select(UsLaceyPpqPlantLine).where(
                UsLaceyPpqPlantLine.organization_id == registered.organization_id,
                UsLaceyPpqPlantLine.operation_id == operation_id,
            )
        ).all()
        assert [line.line_reference for line in plant_lines] == ["1"]

        open_issues = session.scalars(
            select(ReconciliationIssue)
            .where(
                ReconciliationIssue.organization_id == registered.organization_id,
                ReconciliationIssue.operation_reference == f"us_lacey:{operation.public_id}",
                ReconciliationIssue.status == "OPEN",
            )
            .order_by(ReconciliationIssue.id)
        ).all()
        assert len(open_issues) == 3
        issue_values = {
            issue.field_name: {str(issue.left_value), str(issue.right_value)}
            for issue in open_issues
        }
    finally:
        session.rollback()
        session.close()
    assert issue_values["estimated_arrival_date"] == {
        "2026-09-29",
        "2026-10-01",
    }
    assert {
        Decimal(value)
        for value in issue_values["entered_value_reconciliation"]
    } == {
        Decimal("48600.00"),
        Decimal("49050.00"),
    }
    assert issue_values["country_of_harvest"] == {"Peru", "Brazil"}

    conflict_fields = {
        field.field_name: field
        for field in detail.fields
        if field.status == "CONFLICT"
    }
    assert set(conflict_fields) == {
        "estimated_arrival_date",
        "entered_value",
        "country_of_harvest",
    }

    assert {
        candidate.normalized_value or candidate.original_value
        for candidate in conflict_fields["estimated_arrival_date"].candidates
    } == {"2026-09-29", "2026-10-01"}
    assert {
        Decimal(candidate.normalized_value or candidate.original_value)
        for candidate in conflict_fields["entered_value"].candidates
    } == {Decimal("48600.00"), Decimal("49050.00")}
    assert {
        candidate.normalized_value or candidate.original_value
        for candidate in conflict_fields["country_of_harvest"].candidates
    } == {"Peru", "Brazil"}

    plant_field_names = {
        field.field_name
        for field in detail.fields
        if field.line_reference == "1"
    }
    assert {"genus", "species", "country_of_harvest"}.issubset(plant_field_names)

    attention_fields, _auto_supported, _settled = _review_field_groups(detail)
    attention_conflicts = {
        field.field_name
        for field in attention_fields
        if field.status == "CONFLICT"
    }
    assert attention_conflicts == {
        "estimated_arrival_date",
        "entered_value",
        "country_of_harvest",
    }

    reset_us_lacey_engine_state()
