"""Immutable operation-scoped audit trail for the U.S. Lacey workspace."""
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
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from litoral_trace.db.base import Base


class UsLaceyOperationEvent(Base):
    """Append-only System-of-Record event for one tenant-owned operation."""

    __tablename__ = "us_lacey_operation_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    public_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True),
        default=uuid4,
        nullable=False,
    )
    organization_id: Mapped[int] = mapped_column(Integer, nullable=False)
    operation_id: Mapped[int] = mapped_column(Integer, nullable=False)
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    actor_type: Mapped[str] = mapped_column(String(16), nullable=False)
    actor_identity: Mapped[str] = mapped_column(String(255), nullable=False)
    event_type: Mapped[str] = mapped_column(String(48), nullable=False)
    event_key: Mapped[str | None] = mapped_column(String(255), nullable=True)
    details: Mapped[dict] = mapped_column(
        JSON().with_variant(JSONB(), "postgresql"),
        nullable=False,
        default=dict,
        server_default=text("'{}'"),
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["operation_id", "organization_id"],
            ["us_lacey_operations.id", "us_lacey_operations.organization_id"],
            name="fk_us_lacey_operation_events_operation_tenant",
            ondelete="CASCADE",
        ),
        UniqueConstraint(
            "public_id",
            name="uq_us_lacey_operation_events_public_id",
        ),
        UniqueConstraint(
            "id",
            "organization_id",
            name="uq_us_lacey_operation_events_id_org",
        ),
        UniqueConstraint(
            "organization_id",
            "operation_id",
            "event_key",
            name="uq_us_lacey_operation_events_event_key",
        ),
        CheckConstraint(
            "actor_type IN ('SYSTEM','USER')",
            name="ck_us_lacey_operation_events_actor_type",
        ),
        CheckConstraint(
            "event_type IN ("
            "'CREATED','DOCUMENT_UPLOADED','EXTRACTED','HUMAN_REVIEW',"
            "'EVIDENCE_REUSED','PACKAGE_GENERATED'"
            ")",
            name="ck_us_lacey_operation_events_event_type",
        ),
        Index(
            "ix_us_lacey_operation_events_org_operation_time",
            "organization_id",
            "operation_id",
            "timestamp",
            "id",
        ),
    )
