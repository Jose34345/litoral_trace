"""Minimal supplier identity learned from Assurance documents.

This is intentionally not an ERP supplier master. It exists only so high-
confidence documentary identity can be reused and linked across operational
files without asking the user to re-enter the same supplier.
"""
from __future__ import annotations

from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    UniqueConstraint,
    Uuid,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from litoral_trace.db.base import Base


class AssuranceSupplier(Base):
    """Tenant-scoped supplier identity inferred from deterministic evidence only."""

    __tablename__ = "assurance_suppliers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    public_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), default=uuid4, nullable=False)
    organization_id: Mapped[int] = mapped_column(Integer, nullable=False)
    cuit: Mapped[str] = mapped_column(String(11), nullable=False)
    display_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    normalized_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    status: Mapped[str] = mapped_column(
        String(24), nullable=False, default="AUTO_CREATED", server_default="AUTO_CREATED"
    )
    source_assurance_document_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["source_assurance_document_id", "organization_id"],
            ["assurance_documents.id", "assurance_documents.organization_id"],
            name="fk_assurance_suppliers_source_document_tenant",
            # The provenance document is part of the audit chain. RESTRICT avoids
            # a composite SET NULL attempting to null the non-null tenant column.
            ondelete="RESTRICT",
        ),
        CheckConstraint("length(cuit) = 11", name="ck_assurance_suppliers_cuit_length"),
        CheckConstraint(
            "status IN ('AUTO_CREATED','CONFIRMED','NEEDS_REVIEW')",
            name="ck_assurance_suppliers_status",
        ),
        UniqueConstraint("public_id", name="uq_assurance_suppliers_public_id"),
        UniqueConstraint("id", "organization_id", name="uq_assurance_suppliers_id_org"),
        UniqueConstraint(
            "organization_id", "cuit", name="uq_assurance_suppliers_tenant_cuit"
        ),
        Index("ix_assurance_suppliers_organization_id", "organization_id"),
        Index(
            "ix_assurance_suppliers_tenant_name",
            "organization_id",
            "normalized_name",
        ),
        Index(
            "ix_assurance_suppliers_tenant_status",
            "organization_id",
            "status",
        ),
    )
