"""Tenant-scoped actionable exceptions derived from Assurance controls."""
from __future__ import annotations

from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import (
    DateTime,
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
    OperationalExceptionPriority,
    OperationalExceptionSource,
    OperationalExceptionStatus,
    ReconciliationSeverity,
)
from litoral_trace.db.base import Base


class OperationalException(Base):
    """One actionable operational problem without duplicating its source evidence."""

    __tablename__ = "operational_exceptions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    public_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), default=uuid4, nullable=False)
    organization_id: Mapped[int] = mapped_column(Integer, nullable=False)
    fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    source_type: Mapped[str] = mapped_column(
        String(32), nullable=False, default=OperationalExceptionSource.MANUAL.value
    )
    source_reference: Mapped[str | None] = mapped_column(String(255), nullable=True)
    operation_reference: Mapped[str] = mapped_column(String(255), nullable=False)
    cause_code: Mapped[str] = mapped_column(String(100), nullable=False)
    entity_type: Mapped[str] = mapped_column(String(64), nullable=False)
    entity_reference: Mapped[str] = mapped_column(String(255), nullable=False)
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    impact: Mapped[str] = mapped_column(
        String(16), nullable=False, default=ReconciliationSeverity.WARNING.value
    )
    priority: Mapped[str] = mapped_column(
        String(16), nullable=False, default=OperationalExceptionPriority.MEDIUM.value
    )
    status: Mapped[str] = mapped_column(
        String(32), nullable=False, default=OperationalExceptionStatus.OPEN.value
    )
    assigned_to_user_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    assigned_to_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    due_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    recommended_action: Mapped[str] = mapped_column(Text, nullable=False)
    source_snapshot: Mapped[dict[str, object] | None] = mapped_column(JSON, nullable=True)
    resolution_note: Mapped[str | None] = mapped_column(Text, nullable=True)
    resolved_by_user_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    __table_args__ = (
        UniqueConstraint("public_id", name="uq_operational_exceptions_public_id"),
        UniqueConstraint(
            "organization_id",
            "fingerprint",
            name="uq_operational_exceptions_tenant_fingerprint",
        ),
        Index("ix_operational_exceptions_organization_id", "organization_id"),
        Index(
            "ix_operational_exceptions_tenant_status_priority_due",
            "organization_id",
            "status",
            "priority",
            "due_at",
        ),
        Index(
            "ix_operational_exceptions_tenant_operation",
            "organization_id",
            "operation_reference",
        ),
        Index(
            "ix_operational_exceptions_tenant_assignee_status",
            "organization_id",
            "assigned_to_user_id",
            "status",
        ),
        Index(
            "ix_operational_exceptions_tenant_source",
            "organization_id",
            "source_type",
            "source_reference",
        ),
    )
