from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

import litoral_trace.assurance.processing as processing
from litoral_trace.assurance.domain import (
    AssuranceDocumentType,
    DocumentProcessingStatus,
    ExtractionRunStatus,
)
from litoral_trace.assurance.parsers import DocumentParseError
from litoral_trace.assurance.processing import AssuranceProcessingService
from litoral_trace.db.models import (
    AssuranceDocument,
    AuditLog,
    DocumentExtractionRun,
    VaultDocument,
)
from litoral_trace.services.audit import AuditAction


def factory():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    VaultDocument.__table__.create(engine, checkfirst=True)
    AssuranceDocument.__table__.create(engine, checkfirst=True)
    DocumentExtractionRun.__table__.create(engine, checkfirst=True)
    AuditLog.__table__.create(engine, checkfirst=True)
    return sessionmaker(bind=engine, expire_on_commit=False)


class FakeVerifiedDownload:
    def iter_chunks(self, *, chunk_size: int):
        del chunk_size
        yield b"fake-document"


class FakeVaultService:
    @contextmanager
    def materialize_verified_download(self, *, organization_id: int, document_id):
        del organization_id, document_id
        yield FakeVerifiedDownload()


def seed(f, *, status: str = DocumentProcessingStatus.EXTRACTED.value):
    session: Session = f()
    vault = VaultDocument(
        organization_id=42,
        original_filename="factura.pdf",
        content_type="application/pdf",
        size_bytes=13,
        sha256="a" * 64,
        object_key="tenant/42/factura.pdf",
        storage_backend="s3",
        storage_bucket="test",
        document_type="OTHER_EVIDENCE",
        status="available",
    )
    session.add(vault)
    session.flush()
    document = AssuranceDocument(
        organization_id=42,
        vault_document_id=vault.id,
        semantic_document_type="INVOICE",
        type_confidence=0.99,
        processing_status=status,
    )
    session.add(document)
    session.commit()
    result = document.public_id
    session.close()
    return result


def patch_successful_pipeline(monkeypatch, *, version: str):
    monkeypatch.setattr(processing, "PARSER_ENGINE_VERSION", version)
    monkeypatch.setattr(
        processing,
        "parse_document",
        lambda filename, content: SimpleNamespace(
            file_kind="PDF",
            text="factura valida",
            tables=(),
            metadata={},
            ocr_required=False,
        ),
    )
    monkeypatch.setattr(
        processing,
        "classify_document",
        lambda filename, parsed: SimpleNamespace(
            document_type=AssuranceDocumentType.INVOICE,
            confidence=0.99,
            evidence=(),
        ),
    )
    monkeypatch.setattr(processing, "extract_structured_fields", lambda parsed: ())
    monkeypatch.setattr(processing, "missing_required_fields", lambda doc_type, candidates: ())
    monkeypatch.setattr(processing, "_persist_raw_parsed_fields", lambda *args, **kwargs: 0)
    monkeypatch.setattr(
        processing,
        "_persist_structured_fields",
        lambda *args, **kwargs: {
            "structured": 0,
            "auto_accepted": 0,
            "needs_review": 0,
            "conflicts": 0,
            "low_confidence": 0,
        },
    )
    monkeypatch.setattr(
        processing,
        "_persist_entity_links",
        lambda *args, **kwargs: {"linked": 0, "ambiguous": 0, "below_threshold": 0},
    )


def test_new_extractor_version_can_force_reprocess_without_duplicate_silent_run(monkeypatch):
    f = factory()
    public_id = seed(f, status=DocumentProcessingStatus.EXTRACTED.value)
    patch_successful_pipeline(monkeypatch, version="9.9.9")
    service = AssuranceProcessingService(
        session_factory=f,
        vault_service=FakeVaultService(),
    )

    # Normal processing of an already-terminal document is idempotent.
    assert service.process(
        organization_id=42,
        assurance_public_id=public_id,
        force_reprocess=False,
    ) == DocumentProcessingStatus.EXTRACTED.value
    session: Session = f()
    assert session.scalars(select(DocumentExtractionRun)).all() == []
    assert session.scalars(select(AuditLog)).all() == []
    session.close()

    # A deliberate reprocess creates a new reproducible run with the new version.
    assert service.process(
        organization_id=42,
        assurance_public_id=public_id,
        force_reprocess=True,
    ) == DocumentProcessingStatus.EXTRACTED.value
    session = f()
    runs = session.scalars(select(DocumentExtractionRun)).all()
    audits = session.scalars(select(AuditLog)).all()
    assert len(runs) == 1
    assert runs[0].engine_version == "9.9.9"
    assert runs[0].status == ExtractionRunStatus.SUCCEEDED.value
    assert runs[0].extraction_metadata["force_reprocess"] is True

    assert len(audits) == 1
    audit = audits[0]
    assert audit.organization_id == 42
    assert audit.user_id is None
    assert audit.username == "system"
    assert audit.action == AuditAction.ASSURANCE_EXTRACTION_COMPLETE.value
    assert audit.before_data == {"processing_status": DocumentProcessingStatus.PROCESSING.value}
    assert audit.after_data["outcome"] == "success"
    assert audit.after_data["actor_role"] == "system"
    assert audit.after_data["metadata"]["engine_version"] == "9.9.9"
    assert audit.after_data["metadata"]["force_reprocess"] is True
    assert audit.after_data["state_after"]["processing_status"] == DocumentProcessingStatus.EXTRACTED.value
    # Audit contains provenance and counts, never extracted document content.
    assert "factura valida" not in str(audit.after_data)
    session.close()


def test_parse_failure_is_terminal_failed_never_ready_and_is_safely_audited(monkeypatch):
    f = factory()
    public_id = seed(f, status=DocumentProcessingStatus.UPLOADED.value)
    monkeypatch.setattr(
        processing,
        "parse_document",
        lambda filename, content: (_ for _ in ()).throw(DocumentParseError("corrupto")),
    )
    service = AssuranceProcessingService(
        session_factory=f,
        vault_service=FakeVaultService(),
    )

    result = service.process(
        organization_id=42,
        assurance_public_id=public_id,
        force_reprocess=False,
    )
    assert result == DocumentProcessingStatus.FAILED.value

    session: Session = f()
    document = session.scalar(
        select(AssuranceDocument).where(AssuranceDocument.public_id == public_id)
    )
    run = session.scalar(
        select(DocumentExtractionRun)
        .where(DocumentExtractionRun.assurance_document_id == document.id)
        .order_by(DocumentExtractionRun.id.desc())
    )
    audits = session.scalars(select(AuditLog)).all()
    assert document.processing_status == DocumentProcessingStatus.FAILED.value
    assert document.processing_status != DocumentProcessingStatus.EXTRACTED.value
    assert document.last_error_code == "DOCUMENT_PARSE_FAILED"
    assert run.status == ExtractionRunStatus.FAILED.value
    assert run.error_code == "DOCUMENT_PARSE_FAILED"

    assert len(audits) == 1
    audit = audits[0]
    assert audit.action == AuditAction.ASSURANCE_EXTRACTION_FAILED.value
    assert audit.after_data["outcome"] == "failure"
    assert audit.after_data["metadata"]["error_code"] == "DOCUMENT_PARSE_FAILED"
    assert audit.after_data["state_after"]["processing_status"] == DocumentProcessingStatus.FAILED.value
    # Parser exception detail belongs to processing diagnostics, not the audit envelope.
    assert "corrupto" not in str(audit.after_data)
    assert "corrupto" not in str(audit.detail)
    session.close()
