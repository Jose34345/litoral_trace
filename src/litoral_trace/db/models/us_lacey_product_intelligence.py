"""Tenant-scoped Product Intelligence snapshots for U.S. Lacey operations.

This persistence layer is intentionally non-canonical.  It records source-backed
product composition derived from one exact source-set revision and must never be
treated as regulatory truth by itself.
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
    JSON,
    String,
    UniqueConstraint,
    Uuid,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from litoral_trace.db.base import Base


class UsLaceyProductIntelligenceSnapshot(Base):
    """Immutable payload for Product Intelligence on one sealed source generation."""

    __tablename__ = "us_lacey_product_intelligence_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    public_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), default=uuid4, nullable=False)
    organization_id: Mapped[int] = mapped_column(Integer, nullable=False)
    operation_id: Mapped[int] = mapped_column(Integer, nullable=False)
    source_set_revision_id: Mapped[int] = mapped_column(Integer, nullable=False)
    generation: Mapped[int] = mapped_column(Integer, nullable=False)
    source_set_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(24), nullable=False)

    document_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    eligible_document_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    recognized_bom_table_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    unique_sku_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    component_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    material_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    issue_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    payload_json: Mapped[dict] = mapped_column(JSON, nullable=False)
    error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_message: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    finalized_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        ForeignKeyConstraint(
            ["operation_id", "organization_id"],
            ["us_lacey_operations.id", "us_lacey_operations.organization_id"],
            name="fk_lacey_pi_snapshot_operation_tenant",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["source_set_revision_id", "organization_id"],
            ["us_lacey_source_set_revisions.id", "us_lacey_source_set_revisions.organization_id"],
            name="fk_lacey_pi_snapshot_revision_tenant",
            ondelete="CASCADE",
        ),
        UniqueConstraint("id", "organization_id", name="uq_lacey_pi_snapshot_id_org"),
        UniqueConstraint("public_id", name="uq_lacey_pi_snapshot_public_id"),
        UniqueConstraint(
            "organization_id",
            "source_set_revision_id",
            name="uq_lacey_pi_snapshot_revision",
        ),
        CheckConstraint("generation > 0", name="ck_lacey_pi_snapshot_generation"),
        CheckConstraint(
            "status IN ('READY','PARTIAL','FAILED','NOT_APPLICABLE','STALE')",
            name="ck_lacey_pi_snapshot_status",
        ),
        CheckConstraint("document_count >= 0", name="ck_lacey_pi_snapshot_document_count"),
        CheckConstraint("eligible_document_count >= 0", name="ck_lacey_pi_snapshot_eligible_count"),
        CheckConstraint("recognized_bom_table_count >= 0", name="ck_lacey_pi_snapshot_table_count"),
        CheckConstraint("unique_sku_count >= 0", name="ck_lacey_pi_snapshot_sku_count"),
        CheckConstraint("component_count >= 0", name="ck_lacey_pi_snapshot_component_count"),
        CheckConstraint("material_count >= 0", name="ck_lacey_pi_snapshot_material_count"),
        CheckConstraint("issue_count >= 0", name="ck_lacey_pi_snapshot_issue_count"),
        Index("ix_lacey_pi_snapshot_org_operation", "organization_id", "operation_id"),
        Index("ix_lacey_pi_snapshot_org_status", "organization_id", "status"),
    )
