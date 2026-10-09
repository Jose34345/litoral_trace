"""Product-led evaluation state for U.S. Lacey activation."""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from litoral_trace.db.base import Base


class UsLaceyEvaluation(Base):
    __tablename__ = "us_lacey_evaluations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    organization_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
    )
    status: Mapped[str] = mapped_column(
        String(24),
        nullable=False,
        default="ANONYMOUS",
    )
    work_email: Mapped[str | None] = mapped_column(String(255), nullable=True)
    operation_limit: Mapped[int] = mapped_column(Integer, nullable=False, default=5)
    successful_operations_used: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
    )
    claimed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    last_activity_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    inactive_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    raw_retention_hours: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=4,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    __table_args__ = (
        UniqueConstraint(
            "organization_id",
            name="uq_us_lacey_evaluations_organization",
        ),
        CheckConstraint(
            "status IN ('ANONYMOUS','ACTIVE','EXHAUSTED','EXPIRED')",
            name="ck_us_lacey_evaluations_status",
        ),
        CheckConstraint(
            "operation_limit = 5",
            name="ck_us_lacey_evaluations_operation_limit",
        ),
        CheckConstraint(
            "successful_operations_used >= 0 "
            "AND successful_operations_used <= operation_limit",
            name="ck_us_lacey_evaluations_usage",
        ),
        CheckConstraint(
            "raw_retention_hours = 4",
            name="ck_us_lacey_evaluations_raw_retention",
        ),
        CheckConstraint(
            "(status = 'ANONYMOUS' AND work_email IS NULL "
            "AND claimed_at IS NULL AND inactive_expires_at IS NULL) "
            "OR "
            "(status IN ('ACTIVE','EXHAUSTED') "
            "AND work_email IS NOT NULL AND claimed_at IS NOT NULL "
            "AND inactive_expires_at IS NOT NULL) "
            "OR "
            "(status = 'EXPIRED')",
            name="ck_us_lacey_evaluations_claim_state",
        ),
        Index(
            "ix_us_lacey_evaluations_status_expiry",
            "status",
            "inactive_expires_at",
        ),
    )


class UsLaceyEvaluationOperation(Base):
    __tablename__ = "us_lacey_evaluation_operations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    organization_id: Mapped[int] = mapped_column(Integer, nullable=False)
    operation_id: Mapped[int] = mapped_column(Integer, nullable=False)
    counted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["operation_id", "organization_id"],
            ["us_lacey_operations.id", "us_lacey_operations.organization_id"],
            name="fk_us_lacey_evaluation_operations_operation_tenant",
            ondelete="CASCADE",
        ),
        UniqueConstraint(
            "organization_id",
            "operation_id",
            name="uq_us_lacey_evaluation_operations_operation",
        ),
        Index(
            "ix_us_lacey_evaluation_operations_org_counted",
            "organization_id",
            "counted_at",
        ),
    )


class UsLaceyEvaluationRawPurgeJob(Base):
    __tablename__ = "us_lacey_evaluation_raw_purge_jobs"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    organization_id: Mapped[int] = mapped_column(Integer, nullable=False)
    vault_document_id: Mapped[int] = mapped_column(Integer, nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    state: Mapped[str] = mapped_column(
        String(24),
        nullable=False,
        default="PENDING",
    )
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    locked_by: Mapped[str | None] = mapped_column(String(255), nullable=True)
    locked_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    heartbeat_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    last_error: Mapped[str | None] = mapped_column(String(2000), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["vault_document_id", "organization_id"],
            ["vault_documents.id", "vault_documents.organization_id"],
            name="fk_us_lacey_eval_raw_purge_vault_tenant",
            ondelete="CASCADE",
        ),
        UniqueConstraint(
            "organization_id",
            "vault_document_id",
            name="uq_us_lacey_eval_raw_purge_document",
        ),
        CheckConstraint(
            "state IN ('PENDING','RUNNING','RETRY','COMPLETED','FAILED')",
            name="ck_us_lacey_eval_raw_purge_state",
        ),
        CheckConstraint(
            "attempt_count >= 0",
            name="ck_us_lacey_eval_raw_purge_attempt_count",
        ),
        Index(
            "ix_us_lacey_eval_raw_purge_claim",
            "state",
            "available_at",
            "expires_at",
            "id",
        ),
    )
