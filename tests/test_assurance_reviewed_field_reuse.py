from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from litoral_trace.assurance.reconciliation_service import _accepted_fields_by_document
from litoral_trace.db.models import (
    AssuranceDocument,
    DocumentExtractionRun,
    ExtractedDocumentField,
    VaultDocument,
)


def test_human_approved_field_becomes_trusted_without_becoming_auto_accepted():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    for table in (
        VaultDocument.__table__,
        AssuranceDocument.__table__,
        DocumentExtractionRun.__table__,
        ExtractedDocumentField.__table__,
    ):
        table.create(engine, checkfirst=True)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    seed: Session = factory()

    vault = VaultDocument(
        organization_id=9,
        original_filename="remito.pdf",
        content_type="application/pdf",
        size_bytes=10,
        sha256="a" * 64,
        object_key="assurance/remito.pdf",
        storage_backend="s3",
        storage_bucket="test",
        document_type="OTHER_EVIDENCE",
        status="available",
    )
    seed.add(vault)
    seed.flush()
    document = AssuranceDocument(
        organization_id=9,
        vault_document_id=vault.id,
        processing_status="NEEDS_REVIEW",
    )
    seed.add(document)
    seed.flush()
    run = DocumentExtractionRun(
        organization_id=9,
        assurance_document_id=document.id,
        engine="test",
        engine_version="1",
        status="NEEDS_REVIEW",
    )
    seed.add(run)
    seed.flush()
    field = ExtractedDocumentField(
        organization_id=9,
        assurance_document_id=document.id,
        extraction_run_id=run.id,
        field_name="quantity",
        original_value="80",
        normalized_value="80",
        value_type="number",
        confidence=0.70,
        confidence_level="MEDIUM",
        auto_accepted=False,
        needs_review=True,
    )
    seed.add(field)
    seed.commit()

    values, _ = _accepted_fields_by_document(
        seed,
        organization_id=9,
        assurance_document_ids=(document.id,),
    )
    assert values == {}

    # Equivalent persisted state after explicit review approval.
    field.needs_review = False
    seed.commit()
    values, _ = _accepted_fields_by_document(
        seed,
        organization_id=9,
        assurance_document_ids=(document.id,),
    )
    assert values[document.id]["quantity"] == "80"
    assert field.auto_accepted is False
    seed.close()
