"""Tenant-scoped deterministic regulatory assessment snapshots for U.S. Lacey.

This persistence layer is deliberately non-canonical. It records reproducible
rule-scoped assessments for one exact source-set generation and ruleset version.
It must never be treated as final filing/compliance truth by itself.
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


class UsLaceyRegulatoryAssessmentSnapshot(Base):
    """Immutable rule output for one source revision and ruleset version."""

    __tablename__ = "us_lacey_regulatory_assessment_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    public_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), default=uuid4, nullable=False)
    organization_id: Mapped[int] = mapped_column(Integer, nullable=False)
    operation_id: Mapped[int] = mapped_column(Integer, nullable=False)
    source_set_revision_id: Mapped[int] = mapped_column(Integer, nullable=False)
    generation: Mapped[int] = mapped_column(Integer, nullable=False)
    source_set_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    ruleset_version: Mapped[str] = mapped_column(String(96), nullable=False)
    input_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="CURRENT")
    assessment_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    indeterminate_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    payload_json: Mapped[dict] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    finalized_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        ForeignKeyConstraint(
            ["operation_id", "organization_id"],
            ["us_lacey_operations.id", "us_lacey_operations.organization_id"],
            name="fk_lacey_reg_assessment_operation_tenant",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["source_set_revision_id", "organization_id"],
            ["us_lacey_source_set_revisions.id", "us_lacey_source_set_revisions.organization_id"],
            name="fk_lacey_reg_assessment_revision_tenant",
            ondelete="CASCADE",
        ),
        UniqueConstraint("id", "organization_id", name="uq_lacey_reg_assessment_id_org"),
        UniqueConstraint("public_id", name="uq_lacey_reg_assessment_public_id"),
        UniqueConstraint(
            "organization_id",
            "source_set_revision_id",
            "ruleset_version",
            name="uq_lacey_reg_assessment_revision_ruleset",
        ),
        CheckConstraint("generation > 0", name="ck_lacey_reg_assessment_generation"),
        CheckConstraint(
            "status IN ('CURRENT','STALE')",
            name="ck_lacey_reg_assessment_status",
        ),
        CheckConstraint("assessment_count >= 0", name="ck_lacey_reg_assessment_count"),
        CheckConstraint("indeterminate_count >= 0", name="ck_lacey_reg_indeterminate_count"),
        CheckConstraint(
            "indeterminate_count <= assessment_count",
            name="ck_lacey_reg_indeterminate_le_assessment",
        ),
        Index("ix_lacey_reg_assessment_org_operation", "organization_id", "operation_id"),
        Index("ix_lacey_reg_assessment_org_status", "organization_id", "status"),
    )
