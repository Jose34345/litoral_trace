"""Tenant-scoped document intelligence metadata layered on top of Evidence Vault.

The immutable/original file remains owned by ``VaultDocument``. Assurance adds
semantic type, extraction provenance, machine-readable claims and operational
links without duplicating object-storage responsibilities.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    JSON,
    String,
    Text,
    UniqueConstraint,
    Uuid,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from litoral_trace.assurance.domain import (
    AssuranceDocumentType,
    DocumentProcessingStatus,
    ExtractionRunStatus,
)
from litoral_trace.db.base import Base


class AssuranceDocument(Base):
    """One semantic-processing record for an immutable Vault document."""

    __tablename__ = "assurance_documents"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    public_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), default=uuid4, nullable=False)
    organization_id: Mapped[int] = mapped_column(Integer, nullable=False)
    vault_document_id: Mapped[int] = mapped_column(Integer, nullable=False)
    semantic_document_type: Mapped[str] = mapped_column(
        String(64), nullable=False, default=AssuranceDocumentType.UNKNOWN.value
    )
    type_confidence: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    processing_status: Mapped[str] = mapped_column(
        String(32), nullable=False, default=DocumentProcessingStatus.UPLOADED.value
    )
    valid_from: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    valid_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    source_system: Mapped[str | None] = mapped_column(String(255), nullable=True)
    last_error_code: Mapped[str | None] = mapped_column(String(100), nullable=True)
    last_error_message: Mapped[str | None] = mapped_column(String(512), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["vault_document_id", "organization_id"],
            ["vault_documents.id", "vault_documents.organization_id"],
            name="fk_assurance_documents_vault_tenant",
            ondelete="CASCADE",
        ),
        UniqueConstraint("public_id", name="uq_assurance_documents_public_id"),
        UniqueConstraint(
            "id", "organization_id", name="uq_assurance_documents_id_organization_id"
        ),
        UniqueConstraint(
            "organization_id",
            "vault_document_id",
            name="uq_assurance_documents_tenant_vault_document",
        ),
        Index("ix_assurance_documents_organization_id", "organization_id"),
        Index(
            "ix_assurance_documents_tenant_processing",
            "organization_id",
            "processing_status",
        ),
        Index(
            "ix_assurance_documents_tenant_semantic_type",
            "organization_id",
            "semantic_document_type",
        ),
        Index(
            "ix_assurance_documents_tenant_valid_until",
            "organization_id",
            "valid_until",
        ),
    )


class DocumentExtractionRun(Base):
    """One reproducible extraction attempt over a Vault-backed document."""

    __tablename__ = "document_extraction_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    organization_id: Mapped[int] = mapped_column(Integer, nullable=False)
    assurance_document_id: Mapped[int] = mapped_column(Integer, nullable=False)
    engine: Mapped[str] = mapped_column(String(100), nullable=False)
    engine_version: Mapped[str] = mapped_column(String(100), nullable=False)
    status: Mapped[str] = mapped_column(
        String(32), nullable=False, default=ExtractionRunStatus.PENDING.value
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    extraction_metadata: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    error_code: Mapped[str | None] = mapped_column(String(100), nullable=True)
    error_detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["assurance_document_id", "organization_id"],
            ["assurance_documents.id", "assurance_documents.organization_id"],
            name="fk_document_extraction_runs_document_tenant",
            ondelete="CASCADE",
        ),
        UniqueConstraint(
            "id", "organization_id", name="uq_document_extraction_runs_id_organization_id"
        ),
        Index("ix_document_extraction_runs_organization_id", "organization_id"),
        Index(
            "ix_document_extraction_runs_tenant_document",
            "organization_id",
            "assurance_document_id",
        ),
    )


class ExtractedDocumentField(Base):
    """Field-level value with confidence and exact source provenance."""

    __tablename__ = "extracted_document_fields"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    organization_id: Mapped[int] = mapped_column(Integer, nullable=False)
    assurance_document_id: Mapped[int] = mapped_column(Integer, nullable=False)
    extraction_run_id: Mapped[int] = mapped_column(Integer, nullable=False)
    field_name: Mapped[str] = mapped_column(String(255), nullable=False)
    original_value: Mapped[str | None] = mapped_column(Text, nullable=True)
    normalized_value: Mapped[str | None] = mapped_column(Text, nullable=True)
    value_type: Mapped[str] = mapped_column(String(64), nullable=False, default="text")
    confidence: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    confidence_level: Mapped[str] = mapped_column(String(16), nullable=False, default="LOW")
    source_page: Mapped[int | None] = mapped_column(Integer, nullable=True)
    source_locator: Mapped[str | None] = mapped_column(Text, nullable=True)
    auto_accepted: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    needs_review: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["assurance_document_id", "organization_id"],
            ["assurance_documents.id", "assurance_documents.organization_id"],
            name="fk_extracted_fields_document_tenant",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["extraction_run_id", "organization_id"],
            ["document_extraction_runs.id", "document_extraction_runs.organization_id"],
            name="fk_extracted_fields_run_tenant",
            ondelete="CASCADE",
        ),
        Index("ix_extracted_document_fields_organization_id", "organization_id"),
        Index(
            "ix_extracted_fields_tenant_review",
            "organization_id",
            "needs_review",
        ),
        Index(
            "ix_extracted_fields_tenant_field",
            "organization_id",
            "field_name",
        ),
    )


class DocumentClaim(Base):
    """Machine-readable assertion whose evidence remains the original Vault file."""

    __tablename__ = "document_claims"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    organization_id: Mapped[int] = mapped_column(Integer, nullable=False)
    assurance_document_id: Mapped[int] = mapped_column(Integer, nullable=False)
    claim_type: Mapped[str] = mapped_column(String(100), nullable=False)
    issuer: Mapped[str | None] = mapped_column(String(255), nullable=True)
    subject_type: Mapped[str] = mapped_column(String(64), nullable=False)
    subject_reference: Mapped[str] = mapped_column(String(255), nullable=False)
    statement: Mapped[str] = mapped_column(Text, nullable=False)
    scope_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    valid_from: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    valid_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    integrity_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["assurance_document_id", "organization_id"],
            ["assurance_documents.id", "assurance_documents.organization_id"],
            name="fk_document_claims_document_tenant",
            ondelete="CASCADE",
        ),
        Index("ix_document_claims_organization_id", "organization_id"),
        Index(
            "ix_document_claims_tenant_subject",
            "organization_id",
            "subject_type",
            "subject_reference",
        ),
        Index(
            "ix_document_claims_tenant_valid_until",
            "organization_id",
            "valid_until",
        ),
    )


class DocumentEntityLink(Base):
    """Auditable document-to-operation relation without cross-tenant foreign keys."""

    __tablename__ = "document_entity_links"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    organization_id: Mapped[int] = mapped_column(Integer, nullable=False)
    assurance_document_id: Mapped[int] = mapped_column(Integer, nullable=False)
    entity_type: Mapped[str] = mapped_column(String(32), nullable=False)
    entity_reference: Mapped[str] = mapped_column(String(255), nullable=False)
    link_confidence: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    link_method: Mapped[str] = mapped_column(String(32), nullable=False)
    human_confirmed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["assurance_document_id", "organization_id"],
            ["assurance_documents.id", "assurance_documents.organization_id"],
            name="fk_document_entity_links_document_tenant",
            ondelete="CASCADE",
        ),
        UniqueConstraint(
            "organization_id",
            "assurance_document_id",
            "entity_type",
            "entity_reference",
            name="uq_document_entity_links_tenant_target",
        ),
        Index("ix_document_entity_links_organization_id", "organization_id"),
        Index(
            "ix_document_entity_links_tenant_target",
            "organization_id",
            "entity_type",
            "entity_reference",
        ),
    )
