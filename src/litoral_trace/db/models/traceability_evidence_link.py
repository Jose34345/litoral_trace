"""Tenant-safe contextual evidence links for industrial traceability subjects."""
from __future__ import annotations

from datetime import date, datetime
from typing import Final
from uuid import UUID, uuid4

from sqlalchemy import (
    CheckConstraint,
    Date,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    Uuid,
    func,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from litoral_trace.db.base import Base


TRACEABILITY_EVIDENCE_TYPES: Final[frozenset[str]] = frozenset(
    {
        "ORIGIN_AUTHORIZATION",
        "FOREST_GUIDE",
        "REMITO",
        "INVOICE",
        "CERTIFICATE",
        "TRANSPORT",
        "GEOSPATIAL",
        "SUPPLIER_DECLARATION",
        "OTHER",
    }
)

TRACEABILITY_EVIDENCE_SUBJECT_TYPES: Final[frozenset[str]] = frozenset(
    {"SOURCE_LOTE", "TRACEABILITY_EVENT", "TRACEABILITY_BATCH", "SHIPMENT"}
)


class TraceabilityEvidenceLink(Base):
    """Append-only link between one Vault object and one traceability subject."""

    __tablename__ = "traceability_evidence_links"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    public_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), default=uuid4, nullable=False
    )
    organization_id: Mapped[int] = mapped_column(Integer, nullable=False)
    vault_document_id: Mapped[int] = mapped_column(Integer, nullable=False)

    source_lote_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    traceability_event_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    traceability_batch_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    shipment_id: Mapped[int | None] = mapped_column(Integer, nullable=True)

    evidence_type: Mapped[str] = mapped_column(String(40), nullable=False)
    reference_number: Mapped[str | None] = mapped_column(String(160), nullable=True)
    issuer: Mapped[str | None] = mapped_column(String(200), nullable=True)
    document_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    valid_from: Mapped[date | None] = mapped_column(Date, nullable=True)
    valid_until: Mapped[date | None] = mapped_column(Date, nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_by_user_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey(
            "users.id",
            ondelete="SET NULL",
            name="fk_traceability_evidence_links_created_by_user_id",
        ),
        nullable=True,
    )
    unlinked_by_user_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey(
            "users.id",
            ondelete="SET NULL",
            name="fk_traceability_evidence_links_unlinked_by_user_id",
        ),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    unlinked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        UniqueConstraint("public_id", name="uq_traceability_evidence_links_public_id"),
        ForeignKeyConstraint(
            ["vault_document_id", "organization_id"],
            ["vault_documents.id", "vault_documents.organization_id"],
            name="fk_traceability_evidence_links_vault_tenant",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["source_lote_id", "organization_id"],
            ["lotes.id", "lotes.organization_id"],
            name="fk_traceability_evidence_links_source_lote_tenant",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["traceability_event_id", "organization_id"],
            ["traceability_events.id", "traceability_events.organization_id"],
            name="fk_traceability_evidence_links_event_tenant",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["traceability_batch_id", "organization_id"],
            ["traceability_batches.id", "traceability_batches.organization_id"],
            name="fk_traceability_evidence_links_batch_tenant",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["shipment_id", "organization_id"],
            ["shipments.id", "shipments.organization_id"],
            name="fk_traceability_evidence_links_shipment_tenant",
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            "evidence_type IN ('ORIGIN_AUTHORIZATION','FOREST_GUIDE','REMITO','INVOICE','CERTIFICATE','TRANSPORT','GEOSPATIAL','SUPPLIER_DECLARATION','OTHER')",
            name="ck_traceability_evidence_links_type",
        ),
        CheckConstraint(
            "(CASE WHEN source_lote_id IS NOT NULL THEN 1 ELSE 0 END + "
            "CASE WHEN traceability_event_id IS NOT NULL THEN 1 ELSE 0 END + "
            "CASE WHEN traceability_batch_id IS NOT NULL THEN 1 ELSE 0 END + "
            "CASE WHEN shipment_id IS NOT NULL THEN 1 ELSE 0 END) = 1",
            name="ck_traceability_evidence_links_exactly_one_subject",
        ),
        CheckConstraint(
            "unlinked_by_user_id IS NULL OR unlinked_at IS NOT NULL",
            name="ck_traceability_evidence_links_unlink_state",
        ),
        CheckConstraint(
            "valid_from IS NULL OR valid_until IS NULL OR valid_until >= valid_from",
            name="ck_traceability_evidence_links_validity_range",
        ),
        Index(
            "ix_traceability_evidence_links_tenant_created",
            "organization_id",
            "created_at",
        ),
        Index(
            "ix_traceability_evidence_links_tenant_vault",
            "organization_id",
            "vault_document_id",
        ),
        Index(
            "uq_traceability_evidence_active_source",
            "vault_document_id",
            "source_lote_id",
            unique=True,
            postgresql_where=text("unlinked_at IS NULL AND source_lote_id IS NOT NULL"),
        ),
        Index(
            "uq_traceability_evidence_active_event",
            "vault_document_id",
            "traceability_event_id",
            unique=True,
            postgresql_where=text("unlinked_at IS NULL AND traceability_event_id IS NOT NULL"),
        ),
        Index(
            "uq_traceability_evidence_active_batch",
            "vault_document_id",
            "traceability_batch_id",
            unique=True,
            postgresql_where=text("unlinked_at IS NULL AND traceability_batch_id IS NOT NULL"),
        ),
        Index(
            "uq_traceability_evidence_active_shipment",
            "vault_document_id",
            "shipment_id",
            unique=True,
            postgresql_where=text("unlinked_at IS NULL AND shipment_id IS NOT NULL"),
        ),
    )

    @property
    def subject_type(self) -> str:
        if self.source_lote_id is not None:
            return "SOURCE_LOTE"
        if self.traceability_event_id is not None:
            return "TRACEABILITY_EVENT"
        if self.traceability_batch_id is not None:
            return "TRACEABILITY_BATCH"
        return "SHIPMENT"
