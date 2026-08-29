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
    DocumentExtractionRun,
    ExtractedDocumentField,
    VaultDocument,
)
from litoral_trace.services.audit import AuditAction, build_audit_actor


def factory():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    VaultDocument.__table__.create(engine, checkfirst=True)
    AssuranceDocument.__table__.create(engine, checkfirst=True)
    DocumentExtractionRun.__table__.create(engine, checkfirst=True)
    ExtractedDocumentField.__table__.create(engine, checkfirst=True)
    AuditLog.__table__.create(engine, checkfirst=True)
    return sessionmaker(bind=engine, expire_on_commit=False)


def seed(f, *, org: int = 42):
    session: Session = f()
    vault = VaultDocument(
        organization_id=org,
        original_filename="remito.pdf",
        content_type="application/pdf",
        size_bytes=100,
        sha256=(str(org)[-1] or "a") * 64,
        object_key=f"tenant/{org}/remito.pdf",
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
        semantic_document_type="DELIVERY_NOTE",
        type_confidence=0.95,
        processing_status=DocumentProcessingStatus.NEEDS_REVIEW.value,
        last_error_code="EXTRACTED_FIELDS_NEED_REVIEW",
        last_error_message="Revisión requerida",
    )
    session.add(document)
    session.flush()
    run = DocumentExtractionRun(
        organization_id=org,
        assurance_document_id=document.id,
        engine="parser",
        engine_version="1.2",
        status=ExtractionRunStatus.NEEDS_REVIEW.value,
    )
    session.add(run)
    session.flush()
    field = ExtractedDocumentField(
        organization_id=org,
        assurance_document_id=document.id,
        extraction_run_id=run.id,
        field_name="quantity",
        original_value="1.200,00",
        normalized_value="1200.00",
        value_type="number",
        confidence=0.72,
        confidence_level="MEDIUM",
        auto_accepted=False,
        needs_review=True,
    )
    session.add(field)
    session.commit()
    result = document.public_id, field.id
    session.close()
    return result


def test_human_correction_preserves_original_and_is_audited():
    f = factory()
    public_id, field_id = seed(f)
    result = AssuranceDocumentReviewService(session_factory=f).correct_fields(
        organization_id=42,
        assurance_public_id=public_id,
        corrections={field_id: "1.234,50"},
        actor=build_audit_actor(
            organization_id=42,
            user_id=9,
            username="operador",
            role="admin",
        ),
    )
    assert result.corrected_count == 1
    assert result.remaining_review_count == 0
    assert result.processing_status == DocumentProcessingStatus.EXTRACTED.value

    session: Session = f()
    field = session.get(ExtractedDocumentField, field_id)
    audit = session.scalar(
        select(AuditLog).where(AuditLog.action == AuditAction.ASSURANCE_REVIEW_CORRECT.value)
    )
    assert field is not None
    assert field.original_value == "1.200,00"
    assert field.normalized_value == "1234.50"
    assert field.needs_review is False
    assert field.auto_accepted is False
    assert audit is not None
    assert audit.user_id == 9
    assert audit.before_data["fields"][0]["effective_value"] == "1200.00"
    assert audit.after_data["state_after"]["fields"][0]["effective_value"] == "1234.50"
    session.close()


def test_human_correction_that_does_not_change_value_is_not_counted_as_changed():
    f = factory()
    public_id, field_id = seed(f)
    result = AssuranceDocumentReviewService(session_factory=f).correct_fields(
        organization_id=42,
        assurance_public_id=public_id,
        corrections={field_id: "1.200,00"},
        actor=build_audit_actor(
            organization_id=42,
            user_id=9,
            username="operador",
            role="admin",
        ),
    )
    assert result.corrected_count == 0
    assert result.remaining_review_count == 0


def test_cross_tenant_field_correction_fails_closed():
    f = factory()
    public_id, _ = seed(f, org=42)
    _, other_field_id = seed(f, org=77)
    try:
        AssuranceDocumentReviewService(session_factory=f).correct_fields(
            organization_id=42,
            assurance_public_id=public_id,
            corrections={other_field_id: "999"},
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
        raise AssertionError("Cross-tenant field correction must fail closed")
