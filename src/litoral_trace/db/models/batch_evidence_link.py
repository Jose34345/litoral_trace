"""Persistent tenant-scoped linkage between batch imports and Vault evidence."""
from __future__ import annotations

from datetime import datetime
from typing import Final
from uuid import UUID, uuid4

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    UniqueConstraint,
    Uuid,
    func,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from litoral_trace.db.base import Base


BATCH_EVIDENCE_TYPES: Final[frozenset[str]] = frozenset(
    {
        "SOURCE_WORKBOOK",
        "SUPPORTING_EVIDENCE",
        "COMPLIANCE_EVIDENCE",
    }
)


class BatchEvidenceLink(Base):
    """
    Append-only evidence-link history for one tenant batch import.

    Active links have ``unlinked_at IS NULL``. Unlinking never deletes the
    relationship row, preserving a durable audit/history anchor.
    """

    __tablename__ = "batch_evidence_links"

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
        nullable=False,
    )
    batch_import_id: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
    )
    vault_document_id: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
    )
    evidence_type: Mapped[str] = mapped_column(
        String(40),
        nullable=False,
    )
    created_by_user_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey(
            "users.id",
            ondelete="SET NULL",
            name="fk_batch_evidence_links_created_by_user_id",
        ),
        nullable=True,
    )
    unlinked_by_user_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey(
            "users.id",
            ondelete="SET NULL",
            name="fk_batch_evidence_links_unlinked_by_user_id",
        ),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    unlinked_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    __table_args__ = (
        UniqueConstraint(
            "public_id",
            name="uq_batch_evidence_links_public_id",
        ),
        ForeignKeyConstraint(
            ["batch_import_id", "organization_id"],
            ["batch_imports.id", "batch_imports.organization_id"],
            name="fk_batch_evidence_links_batch_tenant",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["vault_document_id", "organization_id"],
            ["vault_documents.id", "vault_documents.organization_id"],
            name="fk_batch_evidence_links_vault_tenant",
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            (
                "evidence_type IN ("
                "'SOURCE_WORKBOOK', "
                "'SUPPORTING_EVIDENCE', "
                "'COMPLIANCE_EVIDENCE'"
                ")"
            ),
            name="ck_batch_evidence_links_evidence_type",
        ),
        CheckConstraint(
            (
                "unlinked_by_user_id IS NULL "
                "OR unlinked_at IS NOT NULL"
            ),
            name="ck_batch_evidence_links_unlink_state",
        ),
        Index(
            "ix_batch_evidence_links_organization_id",
            "organization_id",
        ),
        Index(
            "ix_batch_evidence_links_tenant_batch_created",
            "organization_id",
            "batch_import_id",
            "created_at",
        ),
        Index(
            "ix_batch_evidence_links_tenant_vault_created",
            "organization_id",
            "vault_document_id",
            "created_at",
        ),
        Index(
            "uq_batch_evidence_links_active_pair",
            "batch_import_id",
            "vault_document_id",
            unique=True,
            postgresql_where=text("unlinked_at IS NULL"),
        ),
        Index(
            "uq_batch_evidence_links_active_source",
            "batch_import_id",
            unique=True,
            postgresql_where=text(
                "unlinked_at IS NULL "
                "AND evidence_type = 'SOURCE_WORKBOOK'"
            ),
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<BatchEvidenceLink id={self.id} "
            f"public_id={self.public_id} "
            f"org={self.organization_id} "
            f"type='{self.evidence_type}' "
            f"active={self.unlinked_at is None}>"
        )
