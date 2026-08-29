from __future__ import annotations

from datetime import date
from decimal import Decimal

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from litoral_trace.assurance.domain import DocumentProcessingStatus
from litoral_trace.assurance.preflight import (
    PreflightInput,
    PreflightSignalState,
    PreflightStatus,
)
from litoral_trace.assurance.preflight_service import AssurancePreflightService
from litoral_trace.db.models import AssuranceDocument, DocumentEntityLink, ReconciliationIssue


def factory():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    AssuranceDocument.__table__.create(engine, checkfirst=True)
    DocumentEntityLink.__table__.create(engine, checkfirst=True)
    ReconciliationIssue.__table__.create(engine, checkfirst=True)
    return sessionmaker(bind=engine, expire_on_commit=False)


def ready_payload() -> PreflightInput:
    return PreflightInput(
        customer_reference="BUYER-1",
        market="US",
        product="WOOD",
        quantity=Decimal("10"),
        commitment_date=date(2026, 9, 15),
        stock_available=Decimal("20"),
        origin_state=PreflightSignalState.READY,
        genealogy_state=PreflightSignalState.READY,
        phytosanitary_state=PreflightSignalState.READY,
        eudr_state=PreflightSignalState.NOT_APPLICABLE,
    )


def seed_linked_document(
    f,
    *,
    organization_id: int,
    operation_reference: str,
    processing_status: str,
    error_code: str | None = None,
) -> int:
    session: Session = f()
    document = AssuranceDocument(
        organization_id=organization_id,
        vault_document_id=1000 + organization_id,
        semantic_document_type="INVOICE",
        type_confidence=0.99,
        processing_status=processing_status,
        last_error_code=error_code,
    )
    session.add(document)
    session.flush()
    session.add(
        DocumentEntityLink(
            organization_id=organization_id,
            assurance_document_id=document.id,
            entity_type="OPERATION",
            entity_reference=operation_reference,
            link_confidence=1.0,
            link_method="EXACT_IDENTIFIER",
            human_confirmed=False,
        )
    )
    session.commit()
    document_id = document.id
    session.close()
    return document_id


def test_failed_linked_document_blocks_ready_and_exposes_controlled_cause():
    f = factory()
    seed_linked_document(
        f,
        organization_id=42,
        operation_reference="OP-FAIL",
        processing_status=DocumentProcessingStatus.FAILED.value,
        error_code="DOCUMENT_PARSE_FAILED",
    )

    view = AssurancePreflightService(session_factory=f).evaluate(
        organization_id=42,
        operation_reference="OP-FAIL",
        payload=ready_payload(),
    )

    assert view.result.status == PreflightStatus.BLOCKED
    assert "RECONCILIATION_BLOCKING" in view.result.reason_codes
    assert any(
        "DOCUMENT_PROCESSING_FAILED" in reason.explanation
        and "DOCUMENT_PARSE_FAILED" in reason.explanation
        for reason in view.result.reasons
    )


def test_missing_required_fields_review_state_blocks_ready():
    f = factory()
    seed_linked_document(
        f,
        organization_id=42,
        operation_reference="OP-MISSING",
        processing_status=DocumentProcessingStatus.NEEDS_REVIEW.value,
        error_code="REQUIRED_FIELDS_MISSING",
    )

    view = AssurancePreflightService(session_factory=f).evaluate(
        organization_id=42,
        operation_reference="OP-MISSING",
        payload=ready_payload(),
    )

    assert view.result.status == PreflightStatus.BLOCKED
    assert any(
        "DOCUMENT_REVIEW_REQUIRED" in reason.explanation
        and "REQUIRED_FIELDS_MISSING" in reason.explanation
        for reason in view.result.reasons
    )


def test_scanned_pdf_ocr_required_is_fail_closed_until_real_ocr_exists():
    f = factory()
    seed_linked_document(
        f,
        organization_id=42,
        operation_reference="OP-SCAN",
        processing_status=DocumentProcessingStatus.NEEDS_REVIEW.value,
        error_code="OCR_REQUIRED",
    )

    view = AssurancePreflightService(session_factory=f).evaluate(
        organization_id=42,
        operation_reference="OP-SCAN",
        payload=ready_payload(),
    )

    assert view.result.status == PreflightStatus.BLOCKED
    assert any("OCR_REQUIRED" in reason.explanation for reason in view.result.reasons)


def test_processing_document_prevents_ready_without_claiming_failure():
    f = factory()
    seed_linked_document(
        f,
        organization_id=42,
        operation_reference="OP-PENDING",
        processing_status=DocumentProcessingStatus.PROCESSING.value,
    )

    view = AssurancePreflightService(session_factory=f).evaluate(
        organization_id=42,
        operation_reference="OP-PENDING",
        payload=ready_payload(),
    )

    assert view.result.status == PreflightStatus.CONDITIONAL
    assert any("DOCUMENT_PROCESSING_PENDING" in reason.explanation for reason in view.result.reasons)


def test_failed_document_from_other_tenant_or_operation_is_invisible():
    f = factory()
    seed_linked_document(
        f,
        organization_id=77,
        operation_reference="OP-TARGET",
        processing_status=DocumentProcessingStatus.FAILED.value,
    )
    seed_linked_document(
        f,
        organization_id=42,
        operation_reference="OP-OTHER",
        processing_status=DocumentProcessingStatus.FAILED.value,
    )

    view = AssurancePreflightService(session_factory=f).evaluate(
        organization_id=42,
        operation_reference="OP-TARGET",
        payload=ready_payload(),
    )

    assert view.result.status == PreflightStatus.READY
    assert view.result.reasons == ()
