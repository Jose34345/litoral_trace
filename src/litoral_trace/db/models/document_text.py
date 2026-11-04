"""Immutable multilingual source spans and translation provenance (Phase A)."""
from __future__ import annotations

from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Float,
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
from sqlalchemy.orm import Mapped, mapped_column, relationship

from litoral_trace.db.base import Base


class DocumentTextSpan(Base):
    """Immutable text actually observed in the original legal source document."""

    __tablename__ = "document_text_spans"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    public_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), default=uuid4, nullable=False)
    organization_id: Mapped[int] = mapped_column(Integer, nullable=False)
    assurance_document_id: Mapped[int] = mapped_column(Integer, nullable=False)
    extraction_run_id: Mapped[int] = mapped_column(Integer, nullable=False)
    page: Mapped[int] = mapped_column(Integer, nullable=False)
    block_id: Mapped[str] = mapped_column(String(255), nullable=False)
    table_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    row_index: Mapped[int | None] = mapped_column(Integer, nullable=True)
    column_index: Mapped[int | None] = mapped_column(Integer, nullable=True)
    bbox_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    source_locator: Mapped[str | None] = mapped_column(Text, nullable=True)
    original_text: Mapped[str] = mapped_column(Text, nullable=False)
    original_language: Mapped[str] = mapped_column(String(32), nullable=False)
    language_confidence: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    extraction_method: Mapped[str] = mapped_column(String(32), nullable=False)
    ocr_engine: Mapped[str | None] = mapped_column(String(100), nullable=True)
    ocr_engine_version: Mapped[str | None] = mapped_column(String(100), nullable=True)
    ocr_confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    translations: Mapped[list["DocumentTextTranslation"]] = relationship(
        back_populates="source_span", cascade="all, delete-orphan"
    )
    evidence_nodes: Mapped[list["SemanticEvidenceNode"]] = relationship(back_populates="source_span")

    __table_args__ = (
        ForeignKeyConstraint(
            ["assurance_document_id", "organization_id"],
            ["assurance_documents.id", "assurance_documents.organization_id"],
            name="fk_document_text_spans_assurance_tenant",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["extraction_run_id", "organization_id"],
            ["document_extraction_runs.id", "document_extraction_runs.organization_id"],
            name="fk_document_text_spans_extraction_run_tenant",
            ondelete="CASCADE",
        ),
        UniqueConstraint("id", "organization_id", name="uq_document_text_spans_id_org"),
        UniqueConstraint("public_id", name="uq_document_text_spans_public_id"),
        CheckConstraint("page > 0", name="ck_document_text_spans_page"),
        CheckConstraint(
            "language_confidence >= 0 AND language_confidence <= 1",
            name="ck_document_text_spans_language_confidence",
        ),
        CheckConstraint(
            "ocr_confidence IS NULL OR (ocr_confidence >= 0 AND ocr_confidence <= 1)",
            name="ck_document_text_spans_ocr_confidence",
        ),
        Index("ix_document_text_spans_org_document", "organization_id", "assurance_document_id"),
        Index("ix_document_text_spans_org_run", "organization_id", "extraction_run_id"),
        Index("ix_document_text_spans_org_language", "organization_id", "original_language"),
    )


class DocumentTextTranslation(Base):
    """Interpretation of a source span; never an independent evidence root."""

    __tablename__ = "document_text_translations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    public_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), default=uuid4, nullable=False)
    organization_id: Mapped[int] = mapped_column(Integer, nullable=False)
    source_span_id: Mapped[int] = mapped_column(Integer, nullable=False)
    source_language: Mapped[str] = mapped_column(String(32), nullable=False)
    target_language: Mapped[str] = mapped_column(String(32), nullable=False, default="en")
    translated_text: Mapped[str] = mapped_column(Text, nullable=False)
    provider: Mapped[str] = mapped_column(String(64), nullable=False)
    model_name: Mapped[str] = mapped_column(String(128), nullable=False)
    model_version: Mapped[str] = mapped_column(String(128), nullable=False)
    quality_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    input_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    output_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    source_span: Mapped[DocumentTextSpan] = relationship(back_populates="translations")

    __table_args__ = (
        ForeignKeyConstraint(
            ["source_span_id", "organization_id"],
            ["document_text_spans.id", "document_text_spans.organization_id"],
            name="fk_document_text_translations_span_tenant",
            ondelete="CASCADE",
        ),
        UniqueConstraint("id", "organization_id", name="uq_document_text_translations_id_org"),
        UniqueConstraint("public_id", name="uq_document_text_translations_public_id"),
        UniqueConstraint(
            "organization_id", "source_span_id", "target_language", "provider",
            "model_name", "model_version", "input_hash",
            name="uq_document_text_translations_identity",
        ),
        CheckConstraint(
            "quality_score IS NULL OR (quality_score >= 0 AND quality_score <= 1)",
            name="ck_document_text_translations_quality",
        ),
        Index("ix_document_text_translations_org_span", "organization_id", "source_span_id"),
        Index("ix_document_text_translations_org_target", "organization_id", "target_language"),
    )
