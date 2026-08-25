"""Tenant-scoped remembered mappings for Smart Import workbooks."""
from __future__ import annotations

from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import (
    Boolean,
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


class SmartImportProfile(Base):
    """Remember one confirmed source-header mapping inside one organization."""

    __tablename__ = "smart_import_profiles"

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
            ondelete="CASCADE",
            name="fk_smart_import_profiles_organization_id",
        ),
        nullable=False,
    )
    created_by_user_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey(
            "users.id",
            ondelete="SET NULL",
            name="fk_smart_import_profiles_created_by_user_id",
        ),
        nullable=True,
    )
    updated_by_user_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey(
            "users.id",
            ondelete="SET NULL",
            name="fk_smart_import_profiles_updated_by_user_id",
        ),
        nullable=True,
    )

    name: Mapped[str] = mapped_column(
        String(120),
        nullable=False,
    )
    schema_kind: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="lotes",
        server_default="lotes",
    )
    sheet_name: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
    )
    header_fingerprint: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
    )
    header_signature: Mapped[list[str]] = mapped_column(
        JSON,
        nullable=False,
        default=list,
        server_default=text("'[]'"),
    )
    mapping_json: Mapped[dict[str, str]] = mapped_column(
        JSON,
        nullable=False,
        default=dict,
        server_default=text("'{}'"),
    )

    version: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=1,
        server_default="1",
    )
    use_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default="0",
    )
    active: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
        server_default=text("true"),
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
    last_used_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    __table_args__ = (
        UniqueConstraint(
            "public_id",
            name="uq_smart_import_profiles_public_id",
        ),
        UniqueConstraint(
            "organization_id",
            "schema_kind",
            "name",
            name="uq_smart_import_profiles_tenant_schema_name",
        ),
        CheckConstraint(
            "length(trim(name)) > 0",
            name="ck_smart_import_profiles_name_not_blank",
        ),
        CheckConstraint(
            "length(trim(sheet_name)) > 0",
            name="ck_smart_import_profiles_sheet_name_not_blank",
        ),
        CheckConstraint(
            "length(header_fingerprint) = 64",
            name="ck_smart_import_profiles_fingerprint_length",
        ),
        CheckConstraint(
            "schema_kind = 'lotes'",
            name="ck_smart_import_profiles_schema_kind",
        ),
        CheckConstraint(
            "version > 0",
            name="ck_smart_import_profiles_version_positive",
        ),
        CheckConstraint(
            "use_count >= 0",
            name="ck_smart_import_profiles_use_count_non_negative",
        ),
        Index(
            "ix_smart_import_profiles_organization_id",
            "organization_id",
        ),
        Index(
            "ix_smart_import_profiles_tenant_fingerprint",
            "organization_id",
            "schema_kind",
            "header_fingerprint",
        ),
        Index(
            "ix_smart_import_profiles_tenant_active",
            "organization_id",
            "active",
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<SmartImportProfile id={self.id} "
            f"org={self.organization_id} "
            f"name={self.name!r} v={self.version}>"
        )
