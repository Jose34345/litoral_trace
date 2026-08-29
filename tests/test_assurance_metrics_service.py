from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from litoral_trace.assurance.metrics_service import AssuranceMetricsService
from litoral_trace.db.models import (
    AssuranceDocument,
    AuditLog,
    DocumentExtractionRun,
    ExtractedDocumentField,
    OperationalException,
    ReconciliationIssue,
    Shipment,
)
from litoral_trace.services.audit import AuditAction, build_audit_actor


def factory():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    AssuranceDocument.__table__.create(engine, checkfirst=True)
    DocumentExtractionRun.__table__.create(engine, checkfirst=True)
    ExtractedDocumentField.__table__.create(engine, checkfirst=True)
    ReconciliationIssue.__table__.create(engine, checkfirst=True)
    OperationalException.__table__.create(engine, checkfirst=True)
    Shipment.__table__.create(engine, checkfirst=True)
    AuditLog.__table__.create(engine, checkfirst=True)
    return sessionmaker(bind=engine, expire_on_commit=False)


def test_snapshot_covers_operational_metrics_and_manual_vs_lt_report():
    f = factory()
    session: Session = f()
    start = datetime(2026, 8, 28, 10, 0, tzinfo=timezone.utc)
    shipment_public_id = uuid4()

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
    auto_field = ExtractedDocumentField(
        organization_id=42,
        assurance_document_id=document.id,
        extraction_run_id=latest.id,
        field_name="invoice.number",
        value_type="identifier",
        confidence=0.99,
        confidence_level="HIGH",
        original_value="A-1",
        normalized_value="A-1",
        auto_accepted=True,
        needs_review=False,
    )
    human_field = ExtractedDocumentField(
        organization_id=42,
        assurance_document_id=document.id,
        extraction_run_id=latest.id,
        field_name="supplier.name",
        value_type="text",
        confidence=0.75,
        confidence_level="MEDIUM",
        original_value="Proveedor SA",
        normalized_value="Proveedor SA",
        auto_accepted=False,
        needs_review=False,
    )
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
            auto_field,
            human_field,
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
    session.flush()

    shipment = Shipment(
        public_id=shipment_public_id,
        organization_id=42,
        shipment_code="SHIP-METRICS-1",
        destination_country="DE",
        shipped_at=start + timedelta(seconds=120),
        status="DISPATCHED",
    )
    session.add(shipment)
    operation_reference = f"shipment:{shipment_public_id}"
    session.add(
        ReconciliationIssue(
            organization_id=42,
            operation_reference=operation_reference,
            fingerprint="b" * 64,
            rule_code="QTY_MISMATCH",
            severity="BLOCKING",
            status="OPEN",
            field_name="quantity",
            left_source="invoice",
            right_source="dispatch",
            explanation="Diferencia",
            created_at=start + timedelta(seconds=15),
        )
    )
    session.add_all(
        [
            OperationalException(
                organization_id=42,
                fingerprint="c" * 64,
                source_type="RECONCILIATION",
                operation_reference=operation_reference,
                cause_code="QTY_MISMATCH",
                entity_type="OPERATION",
                entity_reference=operation_reference,
                title="Cantidad",
                description="Diferencia",
                impact="BLOCKING",
                priority="CRITICAL",
                status="RESOLVED",
                recommended_action="Revisar",
                created_at=start + timedelta(seconds=10),
                updated_at=start + timedelta(seconds=100),
                resolved_at=start + timedelta(seconds=100),
            ),
            OperationalException(
                organization_id=42,
                fingerprint="d" * 64,
                source_type="PREFLIGHT",
                operation_reference=operation_reference,
                cause_code="LATE_BLOCK",
                entity_type="OPERATION",
                entity_reference=operation_reference,
                title="Bloqueo tardío",
                description="Detectado después del despacho",
                impact="BLOCKING",
                priority="HIGH",
                status="OPEN",
                recommended_action="Revisar",
                created_at=start + timedelta(seconds=130),
                updated_at=start + timedelta(seconds=130),
            ),
        ]
    )
    session.add(
        AuditLog(
            organization_id=42,
            action=AuditAction.ASSURANCE_REVIEW_CORRECT.value,
            entity_type="assurance_document",
            entity_id=document.id,
            before_data={
                "fields": [
                    {
                        "field_id": human_field.id,
                        "field_name": human_field.field_name,
                        "effective_value": "Proveedor SA",
                    }
                ]
            },
            after_data={
                "metadata": {"corrected_count": 1},
                "state_after": {
                    "fields": [
                        {
                            "field_id": human_field.id,
                            "field_name": human_field.field_name,
                            "effective_value": "Proveedor S.A.",
                        }
                    ]
                },
            },
            timestamp=start + timedelta(seconds=45),
        )
    )
    session.commit()
    session.close()

    service = AssuranceMetricsService(session_factory=f)
    service.set_manual_baseline(
        organization_id=42,
        manual_baseline_seconds=120,
        label="Revisión manual histórica",
        actor=build_audit_actor(
            organization_id=42,
            user_id=None,
            username="tester",
            role="admin",
        ),
    )
    payload = service.snapshot(organization_id=42).as_dict()

    assert payload["fields_detected"] == 2
    assert payload["fields_auto_accepted"] == 1
    assert payload["fields_manually_reviewed"] == 1
    assert payload["fields_manually_changed"] == 1
    assert payload["automatic_data_percentage"] == 50.0
    assert payload["auto_acceptance_percentage"] == 50.0
    assert payload["reconciliation_issues"] == 1
    assert payload["discrepancies_detected"] == 1
    assert payload["blocking_issues"] == 1
    assert payload["open_reconciliation_issue_count"] == 1
    assert payload["blocking_exceptions_before_dispatch"] == 1
    assert payload["completed_documents"] == 1
    assert payload["upload_to_extraction_seconds"] == 40.0
    assert payload["average_upload_to_extraction_seconds"] == 40.0
    assert payload["processing_seconds"] == 10.0
    assert payload["average_exception_resolution_seconds"] == 90.0
    assert payload["resolved_exception_count"] == 1
    assert payload["zero_friction_target_percentage"] == 70.0
    assert payload["zero_friction_target_met"] is False

    report = payload["manual_vs_lt"]
    assert report["available"] is True
    assert report["comparison_scope"] == "DOCUMENT_INTAKE_TO_EXTRACTION"
    assert report["manual_baseline_seconds"] == 120.0
    assert report["lt_average_seconds"] == 40.0
    assert report["time_reduction_percentage"] == 66.67
    assert report["target_reduction_percentage"] == 50.0
    assert report["target_met"] is True
    assert report["baseline_label"] == "Revisión manual histórica"
    assert "no representa todavía" in report["caveat"]


def test_empty_tenant_metrics_are_zero_safe_and_report_is_unavailable():
    snapshot = AssuranceMetricsService(session_factory=factory()).snapshot(organization_id=42)
    payload = snapshot.as_dict()
    assert payload["fields_detected"] == 0
    assert payload["automatic_data_percentage"] == 0.0
    assert payload["auto_acceptance_percentage"] == 0.0
    assert payload["average_upload_to_extraction_seconds"] == 0.0
    assert payload["average_exception_resolution_seconds"] == 0.0
    assert payload["blocking_exceptions_before_dispatch"] == 0
    assert payload["manual_vs_lt"]["available"] is False
    assert payload["manual_vs_lt"]["time_reduction_percentage"] is None


def test_manual_baseline_is_tenant_scoped_and_latest_value_wins():
    f = factory()
    service = AssuranceMetricsService(session_factory=f)
    actor_42 = build_audit_actor(
        organization_id=42,
        user_id=None,
        username="a",
        role="admin",
    )
    actor_77 = build_audit_actor(
        organization_id=77,
        user_id=None,
        username="b",
        role="admin",
    )
    service.set_manual_baseline(
        organization_id=42,
        manual_baseline_seconds=300,
        label="Primero",
        actor=actor_42,
    )
    service.set_manual_baseline(
        organization_id=42,
        manual_baseline_seconds=240,
        label="Último",
        actor=actor_42,
    )
    service.set_manual_baseline(
        organization_id=77,
        manual_baseline_seconds=999,
        label="Otro tenant",
        actor=actor_77,
    )

    payload_42 = service.snapshot(organization_id=42).as_dict()
    payload_77 = service.snapshot(organization_id=77).as_dict()
    assert payload_42["manual_vs_lt"]["manual_baseline_seconds"] == 240.0
    assert payload_42["manual_vs_lt"]["baseline_label"] == "Último"
    assert payload_77["manual_vs_lt"]["manual_baseline_seconds"] == 999.0
