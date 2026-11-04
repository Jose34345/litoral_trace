"""Tenant-scoped persisted discrepancies produced by Assurance reconciliation."""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKeyConstraint,
    Index,
    Integer,
    JSON,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    Uuid,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from litoral_trace.assurance.domain import (
    ReconciliationIssueStatus,
    ReconciliationSeverity,
)
from litoral_trace.db.base import Base


class ReconciliationIssue(Base):
    """Idempotent discrepancy with explicit source evidence and lifecycle."""

    __tablename__ = "reconciliation_issues"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    public_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), default=uuid4, nullable=False)
    organization_id: Mapped[int] = mapped_column(Integer, nullable=False)
    operation_reference: Mapped[str] = mapped_column(String(255), nullable=False)
    fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    rule_code: Mapped[str] = mapped_column(String(100), nullable=False)
    severity: Mapped[str] = mapped_column(
        String(16), nullable=False, default=ReconciliationSeverity.WARNING.value
    )
    status: Mapped[str] = mapped_column(
        String(32), nullable=False, default=ReconciliationIssueStatus.OPEN.value
    )
    field_name: Mapped[str | None] = mapped_column(String(100), nullable=True)
    us_lacey_operation_field_id: Mapped[int | None] = mapped_column(Integer, nullable=True)

    left_document_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    right_document_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    left_source: Mapped[str] = mapped_column(String(512), nullable=False)
    right_source: Mapped[str | None] = mapped_column(String(512), nullable=True)
    left_value: Mapped[str | None] = mapped_column(Text, nullable=True)
    right_value: Mapped[str | None] = mapped_column(Text, nullable=True)
    delta_numeric: Mapped[Decimal | None] = mapped_column(Numeric(24, 8), nullable=True)

    explanation: Mapped[str] = mapped_column(Text, nullable=False)
    evidence_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    resolution_justification: Mapped[str | None] = mapped_column(Text, nullable=True)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["left_document_id", "organization_id"],
            ["assurance_documents.id", "assurance_documents.organization_id"],
            name="fk_reconciliation_issues_left_document_tenant",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["right_document_id", "organization_id"],
            ["assurance_documents.id", "assurance_documents.organization_id"],
            name="fk_reconciliation_issues_right_document_tenant",
            ondelete="CASCADE",
        ),
        UniqueConstraint("public_id", name="uq_reconciliation_issues_public_id"),
        UniqueConstraint(
            "organization_id",
            "fingerprint",
            name="uq_reconciliation_issues_tenant_fingerprint",
        ),
        CheckConstraint(
            "length(fingerprint) = 64",
            name="ck_reconciliation_issues_fingerprint",
        ),
        CheckConstraint(
            "severity IN ('INFO','WARNING','BLOCKING')",
            name="ck_reconciliation_issues_severity",
        ),
        CheckConstraint(
            "status IN ('OPEN','ACCEPTED_WITH_JUSTIFICATION','RESOLVED')",
            name="ck_reconciliation_issues_status",
        ),
        Index("ix_reconciliation_issues_organization_id", "organization_id"),
        Index(
            "ix_reconciliation_issues_tenant_operation_status",
            "organization_id",
            "operation_reference",
            "status",
        ),
        Index(
            "ix_reconciliation_issues_tenant_severity_status",
            "organization_id",
            "severity",
            "status",
        ),
        Index(
            "ix_reconciliation_issues_tenant_rule",
            "organization_id",
            "rule_code",
        ),
        Index(
            "ix_reconciliation_issues_us_lacey_field",
            "organization_id",
            "us_lacey_operation_field_id",
        ),
    )
