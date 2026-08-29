from __future__ import annotations

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from litoral_trace.assurance.document_review import (
    AssuranceDocumentReviewError,
    AssuranceDocumentReviewService,
)
from litoral_trace.assurance.domain import DocumentProcessingStatus, ExtractionRunStatus
from litoral_trace.db.models import (
    AssuranceDocument,
    AuditLog,
    DocumentEntityLink,
    DocumentExtractionRun,
    ExtractedDocumentField,
    ReconciliationIssue,
    VaultDocument,
)
from litoral_trace.services.audit import AuditAction, build_audit_actor


def factory():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    VaultDocument.__table__.create(engine, checkfirst=True)
    AssuranceDocument.__table__.create(engine, checkfirst=True)
    DocumentExtractionRun.__table__.create(engine, checkfirst=True)
    ExtractedDocumentField.__table__.create(engine, checkfirst=True)
    DocumentEntityLink.__table__.create(engine, checkfirst=True)
    ReconciliationIssue.__table__.create(engine, checkfirst=True)
    AuditLog.__table__.create(engine, checkfirst=True)
    return sessionmaker(bind=engine, expire_on_commit=False)


def seed(factory, *, org: int = 42, error_code: str = "EXTRACTED_FIELDS_NEED_REVIEW"):
    session: Session = factory()
    vault = VaultDocument(
        organization_id=org,
        original_filename="factura.pdf",
        content_type="application/pdf",
        size_bytes=100,
        sha256="a" * 64,
        object_key=f"tenant/{org}/factura.pdf",
        storage_backend="s3",
        storage_bucket="test",
        document_type="OTHER_EVIDENCE",
        status="available",
    )
    session.add(vault)
    session.flush()
    document = AssuranceDocument(
        organization_id=org,
        vault_document_id=vault.id,
        semantic_document_type="INVOICE",
        type_confidence=0.97,
        processing_status=DocumentProcessingStatus.NEEDS_REVIEW.value,
        last_error_code=error_code,
        last_error_message="Revisión requerida",
    )
    session.add(document)
    session.flush()
    old_run = DocumentExtractionRun(
        organization_id=org,
        assurance_document_id=document.id,
        engine="parser",
        engine_version="1.0",
        status=ExtractionRunStatus.NEEDS_REVIEW.value,
    )
    session.add(old_run)
    session.flush()
    session.add(
        ExtractedDocumentField(
            organization_id=org,
            assurance_document_id=document.id,
            extraction_run_id=old_run.id,
            field_name="old.field",
            original_value="old",
            normalized_value="old",
            value_type="text",
            confidence=0.70,
            confidence_level="MEDIUM",
            auto_accepted=False,
            needs_review=True,
        )
    )
    latest = DocumentExtractionRun(
        organization_id=org,
        assurance_document_id=document.id,
        engine="parser",
        engine_version="1.2",
        status=ExtractionRunStatus.NEEDS_REVIEW.value,
    )
    session.add(latest)
    session.flush()
    review = ExtractedDocumentField(
        organization_id=org,
        assurance_document_id=document.id,
        extraction_run_id=latest.id,
        field_name="invoice.number",
        original_value="0001-25",
        normalized_value="0001-25",
        value_type="identifier",
        confidence=0.78,
        confidence_level="MEDIUM",
        source_page=1,
        source_locator="pdf:text",
        auto_accepted=False,
        needs_review=True,
    )
    accepted = ExtractedDocumentField(
        organization_id=org,
        assurance_document_id=document.id,
        extraction_run_id=latest.id,
        field_name="supplier.cuit",
        original_value="30-12345678-9",
        normalized_value="30123456789",
        value_type="identifier",
        confidence=0.98,
        confidence_level="HIGH",
        auto_accepted=True,
        needs_review=False,
    )
    raw = ExtractedDocumentField(
        organization_id=org,
        assurance_document_id=document.id,
        extraction_run_id=latest.id,
        field_name="raw.document_text",
        original_value="texto",
        normalized_value="texto",
        value_type="text",
        confidence=0.90,
        confidence_level="HIGH",
        auto_accepted=False,
        needs_review=True,
    )
    session.add_all([review, accepted, raw])
    session.flush()
    session.add(
        DocumentEntityLink(
            organization_id=org,
            assurance_document_id=document.id,
            entity_type="LOT",
            entity_reference="lote:7",
            link_confidence=0.99,
            link_method="EXACT_IDENTIFIER",
            human_confirmed=False,
        )
    )
    session.commit()
    result = (document.public_id, review.id, accepted.id)
    session.close()
    return result


def test_review_shows_only_latest_structured_fields_and_links():
    f = factory()
    public_id, _, _ = seed(f)
    view = AssuranceDocumentReviewService(session_factory=f).get(
        organization_id=42,
        assurance_public_id=public_id,
    )
    assert view.filename == "factura.pdf"
    assert view.structured_field_count == 2
    assert view.auto_accepted_count == 1
    assert view.review_count == 1
    assert {field.field_name for field in view.fields} == {"invoice.number", "supplier.cuit"}
    assert len(view.links) == 1
    assert view.issues == ()


def test_review_exposes_open_discrepancy_that_references_the_document():
    f = factory()
    public_id, _, _ = seed(f)
    session: Session = f()
    document = session.scalar(
        select(AssuranceDocument).where(AssuranceDocument.public_id == public_id)
    )
    session.add(
        ReconciliationIssue(
            organization_id=42,
            operation_reference="shipment:abc",
            fingerprint="d" * 64,
            rule_code="QTY_MISMATCH",
            severity="BLOCKING",
            status="OPEN",
            field_name="quantity",
            left_document_id=document.id,
            left_source="factura.pdf [quantity]",
            right_source="remito.pdf [quantity]",
            left_value="80",
            right_value="75",
            explanation="La cantidad no coincide.",
        )
    )
    session.commit()
    session.close()

    view = AssuranceDocumentReviewService(session_factory=f).get(
        organization_id=42,
        assurance_public_id=public_id,
    )
    assert len(view.issues) == 1
    assert view.issues[0].severity == "BLOCKING"
    assert view.issues[0].rule_code == "QTY_MISMATCH"
    assert "no coincide" in view.issues[0].explanation


def test_bulk_approval_is_audited_and_closes_only_narrow_review_condition():
    f = factory()
    public_id, review_id, _ = seed(f)
    service = AssuranceDocumentReviewService(session_factory=f)
    result = service.approve_fields(
        organization_id=42,
        assurance_public_id=public_id,
        field_ids=[review_id],
        actor=build_audit_actor(
            organization_id=42,
            user_id=9,
            username="operador",
            role="admin",
        ),
    )
    assert result.approved_count == 1
    assert result.remaining_review_count == 0
    assert result.processing_status == DocumentProcessingStatus.EXTRACTED.value

    session: Session = f()
    row = session.get(ExtractedDocumentField, review_id)
    audit = session.scalar(
        select(AuditLog).where(AuditLog.action == AuditAction.ASSURANCE_REVIEW_APPROVE.value)
    )
    assert row.needs_review is False
    assert row.auto_accepted is False
    assert audit is not None
    assert audit.user_id == 9
    session.close()


def test_approval_never_clears_ocr_or_other_source_blockers():
    f = factory()
    public_id, review_id, _ = seed(f, error_code="OCR_REQUIRED")
    result = AssuranceDocumentReviewService(session_factory=f).approve_fields(
        organization_id=42,
        assurance_public_id=public_id,
        field_ids=[review_id],
        actor=build_audit_actor(
            organization_id=42,
            user_id=9,
            username="operador",
            role="admin",
        ),
    )
    assert result.remaining_review_count == 0
    assert result.processing_status == DocumentProcessingStatus.NEEDS_REVIEW.value


def test_field_from_other_document_or_tenant_cannot_be_approved():
    f = factory()
    public_id, _, _ = seed(f, org=42)
    _, other_field_id, _ = seed(f, org=77)
    service = AssuranceDocumentReviewService(session_factory=f)
    try:
        service.approve_fields(
            organization_id=42,
            assurance_public_id=public_id,
            field_ids=[other_field_id],
            actor=build_audit_actor(
                organization_id=42,
                user_id=9,
                username="operador",
                role="admin",
            ),
        )
    except AssuranceDocumentReviewError:
        pass
    else:
        raise AssertionError("Cross-tenant field approval must fail closed")
