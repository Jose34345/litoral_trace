"""Persistent document/evidence model for Assurance v1."""
from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    JSON,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from litoral_trace.assurance.domain import (
    DocumentProcessingStatus,
    DocumentType,
    ExtractionRunStatus,
)
from litoral_trace.db.base import Base, TimestampMixin


class AssuranceDocument(Base, TimestampMixin):
    """Original file plus machine-readable processing state and provenance."""

    __tablename__ = "assurance_documents"
    __table_args__ = (
        UniqueConstraint(
            "organization_id",
            "sha256",
            name="uq_assurance_document_org_sha256",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    organization_id: Mapped[int] = mapped_column(
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    filename: Mapped[str] = mapped_column(String(512), nullable=False)
    mime_type: Mapped[str] = mapped_column(String(255), nullable=False)
    file_size_bytes: Mapped[int] = mapped_column(Integer, nullable=False)
    storage_key: Mapped[str] = mapped_column(String(1024), nullable=False)
    sha256: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    document_type: Mapped[str] = mapped_column(
        String(64), nullable=False, default=DocumentType.UNKNOWN.value, index=True
    )
    processing_status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default=DocumentProcessingStatus.UPLOADED.value,
        index=True,
    )
    source_system: Mapped[str | None] = mapped_column(String(255), nullable=True)
    valid_from: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    valid_until: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, index=True
    )
    source_metadata: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)

    organization = relationship("Organization")
    extraction_runs: Mapped[list["DocumentExtractionRun"]] = relationship(
        back_populates="document", cascade="all, delete-orphan"
    )
    extracted_fields: Mapped[list["ExtractedField"]] = relationship(
        back_populates="document", cascade="all, delete-orphan"
    )
    claims: Mapped[list["DocumentClaim"]] = relationship(
        back_populates="document", cascade="all, delete-orphan"
    )
    entity_links: Mapped[list["DocumentEntityLink"]] = relationship(
        back_populates="document", cascade="all, delete-orphan"
    )


class DocumentExtractionRun(Base, TimestampMixin):
    """One reproducible attempt to extract structured facts from a document."""

    __tablename__ = "document_extraction_runs"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    organization_id: Mapped[int] = mapped_column(
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    document_id: Mapped[int] = mapped_column(
        ForeignKey("assurance_documents.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    engine: Mapped[str] = mapped_column(String(100), nullable=False)
    engine_version: Mapped[str] = mapped_column(String(100), nullable=False)
    status: Mapped[str] = mapped_column(
        String(32), nullable=False, default=ExtractionRunStatus.PENDING.value, index=True
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    error_code: Mapped[str | None] = mapped_column(String(100), nullable=True)
    error_detail: Mapped[str | None] = mapped_column(Text, nullable=True)

    document: Mapped[AssuranceDocument] = relationship(back_populates="extraction_runs")
    fields: Mapped[list["ExtractedField"]] = relationship(
        back_populates="extraction_run", cascade="all, delete-orphan"
    )


class ExtractedField(Base, TimestampMixin):
    """Field-level extraction with original value, normalized value and provenance."""

    __tablename__ = "extracted_fields"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    organization_id: Mapped[int] = mapped_column(
        ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, index=True
    )
    document_id: Mapped[int] = mapped_column(
        ForeignKey("assurance_documents.id", ondelete="CASCADE"), nullable=False, index=True
    )
    extraction_run_id: Mapped[int] = mapped_column(
        ForeignKey("document_extraction_runs.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    field_name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    original_value: Mapped[str | None] = mapped_column(Text, nullable=True)
    normalized_value: Mapped[str | None] = mapped_column(Text, nullable=True)
    value_type: Mapped[str] = mapped_column(String(64), nullable=False, default="text")
    confidence: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    confidence_level: Mapped[str] = mapped_column(String(16), nullable=False, default="LOW")
    source_page: Mapped[int | None] = mapped_column(Integer, nullable=True)
    source_locator: Mapped[str | None] = mapped_column(Text, nullable=True)
    auto_accepted: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    needs_review: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, index=True)

    document: Mapped[AssuranceDocument] = relationship(back_populates="extracted_fields")
    extraction_run: Mapped[DocumentExtractionRun] = relationship(back_populates="fields")


class DocumentClaim(Base, TimestampMixin):
    """Machine-readable assertion evidenced by an original document."""

    __tablename__ = "document_claims"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    organization_id: Mapped[int] = mapped_column(
        ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, index=True
    )
    document_id: Mapped[int] = mapped_column(
        ForeignKey("assurance_documents.id", ondelete="CASCADE"), nullable=False, index=True
    )
    claim_type: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    issuer: Mapped[str | None] = mapped_column(String(255), nullable=True)
    subject_type: Mapped[str] = mapped_column(String(64), nullable=False)
    subject_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    statement: Mapped[str] = mapped_column(Text, nullable=False)
    scope: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    valid_from: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    valid_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    integrity_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)

    document: Mapped[AssuranceDocument] = relationship(back_populates="claims")


class DocumentEntityLink(Base, TimestampMixin):
    """Auditable relation between evidence and operational entities."""

    __tablename__ = "document_entity_links"
    __table_args__ = (
        UniqueConstraint(
            "organization_id",
            "document_id",
            "entity_type",
            "entity_id",
            name="uq_document_entity_link",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    organization_id: Mapped[int] = mapped_column(
        ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, index=True
    )
    document_id: Mapped[int] = mapped_column(
        ForeignKey("assurance_documents.id", ondelete="CASCADE"), nullable=False, index=True
    )
    entity_type: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    entity_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    link_confidence: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    link_method: Mapped[str] = mapped_column(String(32), nullable=False)
    human_confirmed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    document: Mapped[AssuranceDocument] = relationship(back_populates="entity_links")
