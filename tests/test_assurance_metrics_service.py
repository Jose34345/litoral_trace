from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from litoral_trace.assurance.metrics_service import AssuranceMetricsService
from litoral_trace.db.models import (
    AssuranceDocument,
    DocumentExtractionRun,
    ExtractedDocumentField,
    OperationalException,
    ReconciliationIssue,
)


def factory():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    AssuranceDocument.__table__.create(engine, checkfirst=True)
    DocumentExtractionRun.__table__.create(engine, checkfirst=True)
    ExtractedDocumentField.__table__.create(engine, checkfirst=True)
    ReconciliationIssue.__table__.create(engine, checkfirst=True)
    OperationalException.__table__.create(engine, checkfirst=True)
    return sessionmaker(bind=engine, expire_on_commit=False)


def test_snapshot_uses_latest_extraction_and_persisted_exception_timing():
    f = factory()
    session: Session = f()
    start = datetime(2026, 8, 28, 10, 0, tzinfo=timezone.utc)
    document = AssuranceDocument(
        organization_id=42,
        vault_document_id=1,
        semantic_document_type="INVOICE",
        type_confidence=0.98,
        processing_status="EXTRACTED",
        created_at=start,
        updated_at=start,
    )
    session.add(document)
    session.flush()
    old = DocumentExtractionRun(
        organization_id=42,
        assurance_document_id=document.id,
        engine="parser",
        engine_version="1.0",
        status="SUCCEEDED",
        started_at=start,
        completed_at=start + timedelta(seconds=20),
    )
    latest = DocumentExtractionRun(
        organization_id=42,
        assurance_document_id=document.id,
        engine="parser",
        engine_version="1.2",
        status="SUCCEEDED",
        started_at=start + timedelta(seconds=30),
        completed_at=start + timedelta(seconds=40),
    )
    session.add_all([old, latest])
    session.flush()
    session.add_all(
        [
            ExtractedDocumentField(
                organization_id=42,
                assurance_document_id=document.id,
                extraction_run_id=old.id,
                field_name="old.field",
                value_type="text",
                confidence=1.0,
                confidence_level="HIGH",
                auto_accepted=True,
                needs_review=False,
            ),
            ExtractedDocumentField(
                organization_id=42,
                assurance_document_id=document.id,
                extraction_run_id=latest.id,
                field_name="invoice.number",
                value_type="identifier",
                confidence=0.99,
                confidence_level="HIGH",
                auto_accepted=True,
                needs_review=False,
            ),
            ExtractedDocumentField(
                organization_id=42,
                assurance_document_id=document.id,
                extraction_run_id=latest.id,
                field_name="supplier.name",
                value_type="text",
                confidence=0.75,
                confidence_level="MEDIUM",
                auto_accepted=False,
                needs_review=False,
            ),
            ExtractedDocumentField(
                organization_id=42,
                assurance_document_id=document.id,
                extraction_run_id=latest.id,
                field_name="raw.document_text",
                value_type="text",
                confidence=0.90,
                confidence_level="HIGH",
                auto_accepted=False,
                needs_review=True,
            ),
        ]
    )
    session.add(
        ReconciliationIssue(
            organization_id=42,
            operation_reference="shipment:abc",
            fingerprint="b" * 64,
            rule_code="QTY_MISMATCH",
            severity="BLOCKING",
            status="OPEN",
            field_name="quantity",
            left_source="invoice",
            right_source="dispatch",
            explanation="Diferencia",
        )
    )
    session.add(
        OperationalException(
            organization_id=42,
            fingerprint="c" * 64,
            source_type="RECONCILIATION",
            operation_reference="shipment:abc",
            cause_code="QTY_MISMATCH",
            entity_type="OPERATION",
            entity_reference="shipment:abc",
            title="Cantidad",
            description="Diferencia",
            impact="BLOCKING",
            priority="CRITICAL",
            status="RESOLVED",
            recommended_action="Revisar",
            created_at=start,
            updated_at=start,
            resolved_at=start + timedelta(seconds=90),
        )
    )
    session.commit()
    session.close()

    snapshot = AssuranceMetricsService(session_factory=f).snapshot(organization_id=42)
    payload = snapshot.as_dict()

    assert payload["fields_detected"] == 2
    assert payload["fields_auto_accepted"] == 1
    assert payload["fields_manually_reviewed"] == 1
    assert payload["automatic_data_percentage"] == 50.0
    assert payload["reconciliation_issues"] == 1
    assert payload["blocking_issues"] == 1
    assert payload["completed_documents"] == 1
    assert payload["average_upload_to_extraction_seconds"] == 40.0
    assert payload["processing_seconds"] == 10.0
    assert payload["average_exception_resolution_seconds"] == 90.0
    assert payload["resolved_exception_count"] == 1
    assert payload["zero_friction_target_percentage"] == 70.0
    assert payload["zero_friction_target_met"] is False


def test_empty_tenant_metrics_are_zero_and_safe():
    snapshot = AssuranceMetricsService(session_factory=factory()).snapshot(organization_id=42)
    payload = snapshot.as_dict()
    assert payload["fields_detected"] == 0
    assert payload["automatic_data_percentage"] == 0.0
    assert payload["average_upload_to_extraction_seconds"] == 0.0
    assert payload["average_exception_resolution_seconds"] == 0.0
