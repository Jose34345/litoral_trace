"""Persistent tenant-scoped metadata for enterprise Vault objects."""
from __future__ import annotations

from datetime import datetime
from typing import Final
from uuid import UUID, uuid4

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
    Uuid,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from litoral_trace.db.base import Base


VAULT_DOCUMENT_TYPES: Final[frozenset[str]] = frozenset(
    {
        "PDF_CERTIFICADO",
        "DDS_JSON_TRACES",
        "REMITO_EXCEL",
        "OTHER_EVIDENCE",
    }
)

VAULT_DOCUMENT_STATUSES: Final[frozenset[str]] = frozenset(
    {
        "pending_upload",
        "available",
        "upload_failed",
        "delete_pending",
        "delete_failed",
        "deleted",
    }
)

VAULT_STORAGE_BACKENDS: Final[frozenset[str]] = frozenset(
    {
        "s3",
    }
)


class VaultDocument(Base):
    """Metadata record for one private object stored outside PostgreSQL."""

    __tablename__ = "vault_documents"

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
            name="fk_vault_documents_organization_id",
        ),
        nullable=False,
    )
    created_by_user_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey(
            "users.id",
            ondelete="SET NULL",
            name="fk_vault_documents_created_by_user_id",
        ),
        nullable=True,
    )

    original_filename: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
    )
    content_type: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
    )
    size_bytes: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
    )
    sha256: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
    )

    object_key: Mapped[str] = mapped_column(
        String(512),
        nullable=False,
    )
    storage_backend: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="s3",
        server_default="s3",
    )
    storage_bucket: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
    )
    storage_etag: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
    )
    storage_version_id: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
    )

    document_type: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
    )
    status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="pending_upload",
        server_default="pending_upload",
    )

    idempotency_key: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
    )
    last_error_code: Mapped[str | None] = mapped_column(
        String(100),
        nullable=True,
    )
    last_error_message: Mapped[str | None] = mapped_column(
        String(512),
        nullable=True,
    )

    deleted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    __table_args__ = (
        UniqueConstraint(
            "public_id",
            name="uq_vault_documents_public_id",
        ),
        UniqueConstraint(
            "object_key",
            name="uq_vault_documents_object_key",
        ),
        UniqueConstraint(
            "organization_id",
            "idempotency_key",
            name="uq_vault_documents_tenant_idempotency_key",
        ),
        UniqueConstraint(
            "id",
            "organization_id",
            name="uq_vault_documents_id_organization_id",
        ),
        CheckConstraint(
            "size_bytes > 0",
            name="ck_vault_documents_size_bytes_positive",
        ),
        CheckConstraint(
            "length(sha256) = 64",
            name="ck_vault_documents_sha256_length",
        ),
        CheckConstraint(
            "length(trim(original_filename)) > 0",
            name="ck_vault_documents_filename_not_blank",
        ),
        CheckConstraint(
            "length(trim(content_type)) > 0",
            name="ck_vault_documents_content_type_not_blank",
        ),
        CheckConstraint(
            "length(trim(object_key)) > 0",
            name="ck_vault_documents_object_key_not_blank",
        ),
        CheckConstraint(
            "length(trim(storage_bucket)) > 0",
            name="ck_vault_documents_storage_bucket_not_blank",
        ),
        CheckConstraint(
            "storage_backend IN ('s3')",
            name="ck_vault_documents_storage_backend",
        ),
        CheckConstraint(
            (
                "document_type IN ("
                "'PDF_CERTIFICADO', "
                "'DDS_JSON_TRACES', "
                "'REMITO_EXCEL', "
                "'OTHER_EVIDENCE'"
                ")"
            ),
            name="ck_vault_documents_document_type",
        ),
        CheckConstraint(
            (
                "status IN ("
                "'pending_upload', "
                "'available', "
                "'upload_failed', "
                "'delete_pending', "
                "'delete_failed', "
                "'deleted'"
                ")"
            ),
            name="ck_vault_documents_status",
        ),
        CheckConstraint(
            (
                "("
                "status = 'deleted' AND deleted_at IS NOT NULL"
                ") OR ("
                "status <> 'deleted' AND deleted_at IS NULL"
                ")"
            ),
            name="ck_vault_documents_deleted_at_state",
        ),
        CheckConstraint(
            (
                "status NOT IN ('upload_failed', 'delete_failed') "
                "OR last_error_code IS NOT NULL"
            ),
            name="ck_vault_documents_failure_has_error_code",
        ),
        Index(
            "ix_vault_documents_organization_id",
            "organization_id",
        ),
        Index(
            "ix_vault_documents_created_by_user_id",
            "created_by_user_id",
        ),
        Index(
            "ix_vault_documents_tenant_created_at",
            "organization_id",
            "created_at",
        ),
        Index(
            "ix_vault_documents_tenant_type_created_at",
            "organization_id",
            "document_type",
            "created_at",
        ),
        Index(
            "ix_vault_documents_tenant_status_created_at",
            "organization_id",
            "status",
            "created_at",
        ),
        Index(
            "ix_vault_documents_tenant_sha256",
            "organization_id",
            "sha256",
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<VaultDocument id={self.id} "
            f"public_id={self.public_id} "
            f"org={self.organization_id} "
            f"status='{self.status}'>"
        )
