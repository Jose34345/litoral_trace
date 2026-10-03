"""Reusable, tenant-scoped supplier evidence for U.S. Lacey workflows.

The tables in this module persist small structured identities, evidence metadata,
and claims only.  Source file bytes remain in the existing Vault/Storage layers;
this model stores references and SHA-256 fingerprints, never document blobs.
"""
from __future__ import annotations

from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import (
    CheckConstraint,
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
)
from sqlalchemy.orm import Mapped, mapped_column

from litoral_trace.db.base import Base


class UsLaceySupplier(Base):
    """Minimal reusable supplier identity inside one tenant."""

    __tablename__ = "us_lacey_supplier"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    public_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), default=uuid4, nullable=False)
    organization_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
    )
    supplier_key: Mapped[str] = mapped_column(String(128), nullable=False)
    display_name: Mapped[str] = mapped_column(String(255), nullable=False)
    normalized_name: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(
        String(16), nullable=False, default="ACTIVE", server_default="ACTIVE"
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    __table_args__ = (
        UniqueConstraint("public_id", name="uq_us_lacey_supplier_public_id"),
        UniqueConstraint("id", "organization_id", name="uq_us_lacey_supplier_id_org"),
        UniqueConstraint(
            "organization_id",
            "supplier_key",
            name="uq_us_lacey_supplier_org_key",
        ),
        CheckConstraint(
            "status IN ('ACTIVE','INACTIVE','NEEDS_REVIEW')",
            name="ck_us_lacey_supplier_status",
        ),
        Index("ix_us_lacey_supplier_org", "organization_id"),
        Index(
            "ix_us_lacey_supplier_org_normalized_name",
            "organization_id",
            "normalized_name",
        ),
    )


class UsLaceySupplierProduct(Base):
    """Exact supplier/product identity that may reuse verified evidence."""

    __tablename__ = "us_lacey_supplier_product"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    public_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), default=uuid4, nullable=False)
    organization_id: Mapped[int] = mapped_column(Integer, nullable=False)
    supplier_id: Mapped[int] = mapped_column(Integer, nullable=False)
    product_key: Mapped[str] = mapped_column(String(128), nullable=False)
    sku: Mapped[str | None] = mapped_column(String(128), nullable=True)
    display_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    normalized_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    status: Mapped[str] = mapped_column(
        String(16), nullable=False, default="ACTIVE", server_default="ACTIVE"
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["supplier_id", "organization_id"],
            ["us_lacey_supplier.id", "us_lacey_supplier.organization_id"],
            name="fk_us_lacey_supplier_product_supplier_tenant",
            ondelete="CASCADE",
        ),
        UniqueConstraint(
            "public_id", name="uq_us_lacey_supplier_product_public_id"
        ),
        UniqueConstraint(
            "id",
            "organization_id",
            name="uq_us_lacey_supplier_product_id_org",
        ),
        UniqueConstraint(
            "organization_id",
            "supplier_id",
            "product_key",
            name="uq_us_lacey_supplier_product_identity",
        ),
        CheckConstraint(
            "status IN ('ACTIVE','INACTIVE','NEEDS_REVIEW')",
            name="ck_us_lacey_supplier_product_status",
        ),
        Index(
            "ix_us_lacey_supplier_product_org_supplier",
            "organization_id",
            "supplier_id",
        ),
        Index(
            "ix_us_lacey_supplier_product_org_key",
            "organization_id",
            "product_key",
        ),
        Index(
            "ix_us_lacey_supplier_product_org_sku",
            "organization_id",
            "sku",
        ),
    )


class UsLaceySupplierEvidence(Base):
    """Verified evidence metadata reusable for one exact supplier product."""

    __tablename__ = "us_lacey_supplier_evidence"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    public_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), default=uuid4, nullable=False)
    organization_id: Mapped[int] = mapped_column(Integer, nullable=False)
    supplier_product_id: Mapped[int] = mapped_column(Integer, nullable=False)
    evidence_type: Mapped[str] = mapped_column(String(64), nullable=False)
    document_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    source_reference: Mapped[str | None] = mapped_column(String(512), nullable=True)
    valid_from: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    valid_until: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    verified_by_user_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )
    verified_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    status: Mapped[str] = mapped_column(
        String(16), nullable=False, default="VERIFIED", server_default="VERIFIED"
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["supplier_product_id", "organization_id"],
            ["us_lacey_supplier_product.id", "us_lacey_supplier_product.organization_id"],
            name="fk_us_lacey_supplier_evidence_product_tenant",
            ondelete="CASCADE",
        ),
        UniqueConstraint(
            "public_id", name="uq_us_lacey_supplier_evidence_public_id"
        ),
        UniqueConstraint(
            "id",
            "organization_id",
            name="uq_us_lacey_supplier_evidence_id_org",
        ),
        UniqueConstraint(
            "organization_id",
            "supplier_product_id",
            "document_hash",
            "evidence_type",
            name="uq_us_lacey_supplier_evidence_document",
        ),
        CheckConstraint(
            "length(document_hash) = 64",
            name="ck_us_lacey_supplier_evidence_hash_length",
        ),
        CheckConstraint(
            "valid_until > valid_from",
            name="ck_us_lacey_supplier_evidence_valid_window",
        ),
        CheckConstraint(
            "status IN ('VERIFIED','REVOKED')",
            name="ck_us_lacey_supplier_evidence_status",
        ),
        Index(
            "ix_us_lacey_supplier_evidence_org_product_validity",
            "organization_id",
            "supplier_product_id",
            "status",
            "valid_until",
        ),
        Index(
            "ix_us_lacey_supplier_evidence_org_hash",
            "organization_id",
            "document_hash",
        ),
    )


class UsLaceyEvidenceClaim(Base):
    """One structured field assertion backed by reusable supplier evidence."""

    __tablename__ = "us_lacey_evidence_claim"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    organization_id: Mapped[int] = mapped_column(Integer, nullable=False)
    evidence_id: Mapped[int] = mapped_column(Integer, nullable=False)
    field_name: Mapped[str] = mapped_column(String(100), nullable=False)
    field_value: Mapped[str] = mapped_column(Text, nullable=False)
    normalized_value: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["evidence_id", "organization_id"],
            ["us_lacey_supplier_evidence.id", "us_lacey_supplier_evidence.organization_id"],
            name="fk_us_lacey_evidence_claim_evidence_tenant",
            ondelete="CASCADE",
        ),
        UniqueConstraint(
            "id",
            "organization_id",
            name="uq_us_lacey_evidence_claim_id_org",
        ),
        UniqueConstraint(
            "organization_id",
            "evidence_id",
            "field_name",
            name="uq_us_lacey_evidence_claim_field",
        ),
        Index(
            "ix_us_lacey_evidence_claim_org_evidence_field",
            "organization_id",
            "evidence_id",
            "field_name",
        ),
        Index(
            "ix_us_lacey_evidence_claim_org_field",
            "organization_id",
            "field_name",
        ),
    )
