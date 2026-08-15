"""Persistent tenant-scoped identity for successful XLSX batch imports."""
from __future__ import annotations

from datetime import datetime
from typing import Final
from uuid import UUID, uuid4

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    JSON,
    String,
    UniqueConstraint,
    Uuid,
    func,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from litoral_trace.db.base import Base


BATCH_IMPORT_STATUSES: Final[frozenset[str]] = frozenset(
    {
        "processing",
        "completed",
    }
)


class BatchImport(Base):
    """
    Transactional idempotency record for one tenant XLSX import.

    A processing row is created and completed inside the same transaction as
    the lotes and audit events. Failed imports roll back completely, including
    this record.
    """

    __tablename__ = "batch_imports"

    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )
    public_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True),
        default=uuid4,
        nullable=False,
    )
    organization_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey(
            "organizations.id",
            ondelete="RESTRICT",
            name="fk_batch_imports_organization_id",
        ),
        nullable=False,
    )
    created_by_user_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey(
            "users.id",
            ondelete="SET NULL",
            name="fk_batch_imports_created_by_user_id",
        ),
        nullable=True,
    )

    idempotency_key: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
    )
    source_sha256: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
    )
    source_filename: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
    )

    status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="processing",
        server_default="processing",
    )
    total_rows: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
    )
    inserted_rows: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default="0",
    )
    lote_ids: Mapped[list[int]] = mapped_column(
        JSON,
        nullable=False,
        default=list,
        server_default=text("'[]'"),
    )
    identifiers: Mapped[list[str]] = mapped_column(
        JSON,
        nullable=False,
        default=list,
        server_default=text("'[]'"),
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    __table_args__ = (
        UniqueConstraint(
            "public_id",
            name="uq_batch_imports_public_id",
        ),
        UniqueConstraint(
            "organization_id",
            "idempotency_key",
            name="uq_batch_imports_tenant_idempotency_key",
        ),
        UniqueConstraint(
            "id",
            "organization_id",
            name="uq_batch_imports_id_organization_id",
        ),
        CheckConstraint(
            "length(trim(idempotency_key)) > 0",
            name="ck_batch_imports_idempotency_key_not_blank",
        ),
        CheckConstraint(
            "length(source_sha256) = 64",
            name="ck_batch_imports_source_sha256_length",
        ),
        CheckConstraint(
            "length(trim(source_filename)) > 0",
            name="ck_batch_imports_source_filename_not_blank",
        ),
        CheckConstraint(
            "status IN ('processing', 'completed')",
            name="ck_batch_imports_status",
        ),
        CheckConstraint(
            "total_rows > 0",
            name="ck_batch_imports_total_rows_positive",
        ),
        CheckConstraint(
            "inserted_rows >= 0 AND inserted_rows <= total_rows",
            name="ck_batch_imports_inserted_rows_range",
        ),
        CheckConstraint(
            (
                "("
                "status = 'processing' "
                "AND completed_at IS NULL "
                "AND inserted_rows = 0"
                ") OR ("
                "status = 'completed' "
                "AND completed_at IS NOT NULL "
                "AND inserted_rows = total_rows"
                ")"
            ),
            name="ck_batch_imports_completion_state",
        ),
        Index(
            "ix_batch_imports_organization_id",
            "organization_id",
        ),
        Index(
            "ix_batch_imports_created_by_user_id",
            "created_by_user_id",
        ),
        Index(
            "ix_batch_imports_tenant_created_at",
            "organization_id",
            "created_at",
        ),
        Index(
            "ix_batch_imports_tenant_source_sha256",
            "organization_id",
            "source_sha256",
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<BatchImport id={self.id} "
            f"public_id={self.public_id} "
            f"org={self.organization_id} "
            f"status='{self.status}'>"
        )
