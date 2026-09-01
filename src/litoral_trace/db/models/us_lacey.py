"""Tenant-scoped persistence for the U.S. Lacey pilot platform."""
from __future__ import annotations

from datetime import date, datetime
from uuid import UUID, uuid4

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Date,
    DateTime,
    Float,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    Uuid,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from litoral_trace.db.base import Base
from litoral_trace.us_lacey.domain import (
    UsLaceyAccountStatus,
    UsLaceyBusinessType,
    UsLaceyFieldStatus,
    UsLaceyOperationStatus,
)


class UsLaceyOrganizationProfile(Base):
    """U.S. commercial profile layered over the existing Organization."""

    __tablename__ = "us_lacey_organization_profiles"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    organization_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
    )
    legal_name: Mapped[str] = mapped_column(String(255), nullable=False)
    country_code: Mapped[str] = mapped_column(String(2), nullable=False, default="US")
    address_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    business_type: Mapped[str] = mapped_column(
        String(32), nullable=False, default=UsLaceyBusinessType.OTHER.value
    )
    admin_contact_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    admin_contact_email: Mapped[str | None] = mapped_column(String(255), nullable=True)
    billing_email: Mapped[str | None] = mapped_column(String(255), nullable=True)
    account_status: Mapped[str] = mapped_column(
        String(24), nullable=False, default=UsLaceyAccountStatus.PENDING_EMAIL.value
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    __table_args__ = (
        UniqueConstraint("organization_id", name="uq_us_lacey_org_profiles_org"),
        UniqueConstraint("id", "organization_id", name="uq_us_lacey_org_profiles_id_org"),
        CheckConstraint("country_code = 'US'", name="ck_us_lacey_org_profiles_country_us"),
        CheckConstraint(
            "business_type IN ('IMPORTER','CUSTOMS_BROKER','OTHER')",
            name="ck_us_lacey_org_profiles_business_type",
        ),
        CheckConstraint(
            "account_status IN ('PENDING_EMAIL','PAYMENT_PENDING','PILOT','ACTIVE','SUSPENDED')",
            name="ck_us_lacey_org_profiles_status",
        ),
        Index("ix_us_lacey_org_profiles_organization_id", "organization_id"),
    )


class UsLaceyOperation(Base):
    """One customer shipment/document-preparation operation."""

    __tablename__ = "us_lacey_operations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    public_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), default=uuid4, nullable=False)
    organization_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("organizations.id", ondelete="RESTRICT"), nullable=False
    )
    created_by_user_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True
    )
    client_reference: Mapped[str] = mapped_column(String(255), nullable=False)
    importer_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    consignee_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    broker_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    supplier_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    operation_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    status: Mapped[str] = mapped_column(
        String(32), nullable=False, default=UsLaceyOperationStatus.NEW.value
    )
    document_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    merchandise_line_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    review_result: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    __table_args__ = (
        UniqueConstraint("public_id", name="uq_us_lacey_operations_public_id"),
        UniqueConstraint("id", "organization_id", name="uq_us_lacey_operations_id_org"),
        UniqueConstraint(
            "organization_id", "client_reference", name="uq_us_lacey_operations_org_reference"
        ),
        CheckConstraint("document_count >= 0", name="ck_us_lacey_operations_document_count"),
        CheckConstraint(
            "merchandise_line_count >= 0", name="ck_us_lacey_operations_line_count"
        ),
        CheckConstraint(
            "status IN ('NEW','PROCESSING','REVIEW_REQUIRED','READY_FOR_REVIEW','COMPLETED','FAILED')",
            name="ck_us_lacey_operations_status",
        ),
        Index("ix_us_lacey_operations_organization_id", "organization_id"),
        Index("ix_us_lacey_operations_org_status", "organization_id", "status"),
        Index("ix_us_lacey_operations_org_created", "organization_id", "created_at"),
    )


class UsLaceyOperationDocument(Base):
    """Versioned link from a U.S. operation to an immutable Assurance/Vault document."""

    __tablename__ = "us_lacey_operation_documents"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    organization_id: Mapped[int] = mapped_column(Integer, nullable=False)
    operation_id: Mapped[int] = mapped_column(Integer, nullable=False)
    assurance_document_id: Mapped[int] = mapped_column(Integer, nullable=False)
    document_role: Mapped[str] = mapped_column(String(64), nullable=False, default="UNKNOWN")
    version_number: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    is_current: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["operation_id", "organization_id"],
            ["us_lacey_operations.id", "us_lacey_operations.organization_id"],
            name="fk_us_lacey_operation_documents_operation_tenant",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["assurance_document_id", "organization_id"],
            ["assurance_documents.id", "assurance_documents.organization_id"],
            name="fk_us_lacey_operation_documents_assurance_tenant",
            ondelete="RESTRICT",
        ),
        UniqueConstraint("id", "organization_id", name="uq_us_lacey_operation_documents_id_org"),
        UniqueConstraint(
            "organization_id",
            "operation_id",
            "assurance_document_id",
            "version_number",
            name="uq_us_lacey_operation_documents_version",
        ),
        CheckConstraint("version_number > 0", name="ck_us_lacey_operation_documents_version"),
        Index("ix_us_lacey_operation_documents_organization_id", "organization_id"),
        Index(
            "ix_us_lacey_operation_documents_org_operation",
            "organization_id",
            "operation_id",
        ),
    )


class UsLaceyPpqShipment(Base):
    """Exactly one PPQ shipment header (fields 1–10) per operation."""

    __tablename__ = "us_lacey_ppq_shipments"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    organization_id: Mapped[int] = mapped_column(Integer, nullable=False)
    operation_id: Mapped[int] = mapped_column(Integer, nullable=False)
    estimated_arrival_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    __table_args__ = (
        ForeignKeyConstraint(
            ["operation_id", "organization_id"],
            ["us_lacey_operations.id", "us_lacey_operations.organization_id"],
            name="fk_us_lacey_ppq_shipments_operation_tenant",
            ondelete="CASCADE",
        ),
        UniqueConstraint("id", "organization_id", name="uq_us_lacey_ppq_shipments_id_org"),
        UniqueConstraint("organization_id", "operation_id", name="uq_us_lacey_ppq_shipments_operation"),
        Index("ix_us_lacey_ppq_shipments_org_operation", "organization_id", "operation_id"),
    )


class UsLaceyPpqPlantLine(Base):
    """One explicit PPQ plant/species/country/quantity declaration line."""

    __tablename__ = "us_lacey_ppq_plant_lines"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    public_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), default=uuid4, nullable=False)
    organization_id: Mapped[int] = mapped_column(Integer, nullable=False)
    operation_id: Mapped[int] = mapped_column(Integer, nullable=False)
    line_reference: Mapped[str] = mapped_column(String(100), nullable=False)
    ordinal: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    __table_args__ = (
        ForeignKeyConstraint(
            ["operation_id", "organization_id"],
            ["us_lacey_operations.id", "us_lacey_operations.organization_id"],
            name="fk_us_lacey_ppq_plant_lines_operation_tenant",
            ondelete="CASCADE",
        ),
        UniqueConstraint("id", "organization_id", name="uq_us_lacey_ppq_plant_lines_id_org"),
        UniqueConstraint("public_id", name="uq_us_lacey_ppq_plant_lines_public_id"),
        UniqueConstraint("organization_id", "operation_id", "line_reference", name="uq_us_lacey_ppq_plant_lines_reference"),
        UniqueConstraint("organization_id", "operation_id", "ordinal", name="uq_us_lacey_ppq_plant_lines_ordinal"),
        CheckConstraint("ordinal > 0", name="ck_us_lacey_ppq_plant_lines_ordinal"),
        Index("ix_us_lacey_ppq_plant_lines_org_operation", "organization_id", "operation_id"),
    )


class UsLaceyOperationField(Base):
    """Operation-level structured value with evidence, confidence and human review."""

    __tablename__ = "us_lacey_operation_fields"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    organization_id: Mapped[int] = mapped_column(Integer, nullable=False)
    operation_id: Mapped[int] = mapped_column(Integer, nullable=False)
    merchandise_line_reference: Mapped[str] = mapped_column(String(100), nullable=False, default="1")
    field_name: Mapped[str] = mapped_column(String(100), nullable=False)
    field_scope: Mapped[str] = mapped_column(String(16), nullable=False, default="PLANT_LINE")
    plant_line_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    original_value: Mapped[str | None] = mapped_column(Text, nullable=True)
    normalized_value: Mapped[str | None] = mapped_column(Text, nullable=True)
    field_status: Mapped[str] = mapped_column(
        String(24), nullable=False, default=UsLaceyFieldStatus.MISSING.value
    )
    confidence: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    source_assurance_document_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    source_page: Mapped[int | None] = mapped_column(Integer, nullable=True)
    source_locator: Mapped[str | None] = mapped_column(Text, nullable=True)
    extractor: Mapped[str | None] = mapped_column(String(100), nullable=True)
    extractor_version: Mapped[str | None] = mapped_column(String(100), nullable=True)
    human_value: Mapped[str | None] = mapped_column(Text, nullable=True)
    reviewed_by_user_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True
    )
    reviewed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    validation_status: Mapped[str] = mapped_column(String(24), nullable=False, default="MISSING")
    validation_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    not_required_reason_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["plant_line_id", "organization_id"],
            ["us_lacey_ppq_plant_lines.id", "us_lacey_ppq_plant_lines.organization_id"],
            name="fk_us_lacey_operation_fields_plant_line_tenant",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["operation_id", "organization_id"],
            ["us_lacey_operations.id", "us_lacey_operations.organization_id"],
            name="fk_us_lacey_operation_fields_operation_tenant",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["source_assurance_document_id", "organization_id"],
            ["assurance_documents.id", "assurance_documents.organization_id"],
            name="fk_us_lacey_operation_fields_source_document_tenant",
            ondelete="RESTRICT",
        ),
        UniqueConstraint("id", "organization_id", name="uq_us_lacey_operation_fields_id_org"),
        UniqueConstraint(
            "organization_id",
            "operation_id",
            "merchandise_line_reference",
            "field_name",
            name="uq_us_lacey_operation_fields_operation_line_field",
        ),
        CheckConstraint("confidence >= 0 AND confidence <= 1", name="ck_us_lacey_fields_confidence"),
        CheckConstraint(
            "field_status IN ('FOUND','MATCHED','MISSING','REVIEW','NOT_REQUIRED')",
            name="ck_us_lacey_fields_status",
        ),
        CheckConstraint("field_scope IN ('SHIPMENT','PLANT_LINE')", name="ck_us_lacey_fields_scope"),
        CheckConstraint(
            "validation_status IN ('VALID','INVALID','MISSING','REVIEW_REQUIRED')",
            name="ck_us_lacey_fields_validation_status",
        ),
        CheckConstraint(
            "(field_scope = 'SHIPMENT' AND plant_line_id IS NULL) OR "
            "(field_scope = 'PLANT_LINE' AND plant_line_id IS NOT NULL)",
            name="ck_us_lacey_fields_scope_line",
        ),
        Index("ix_us_lacey_operation_fields_organization_id", "organization_id"),
        Index(
            "ix_us_lacey_operation_fields_org_operation",
            "organization_id",
            "operation_id",
        ),
        Index(
            "ix_us_lacey_operation_fields_org_status",
            "organization_id",
            "field_status",
        ),
    )


class UsLaceyFieldCandidate(Base):
    """Every plausible extracted value; candidates are never silently collapsed."""

    __tablename__ = "us_lacey_field_candidates"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    organization_id: Mapped[int] = mapped_column(Integer, nullable=False)
    operation_id: Mapped[int] = mapped_column(Integer, nullable=False)
    operation_field_id: Mapped[int] = mapped_column(Integer, nullable=False)
    source_assurance_document_id: Mapped[int] = mapped_column(Integer, nullable=False)
    original_value: Mapped[str] = mapped_column(Text, nullable=False)
    normalized_value: Mapped[str | None] = mapped_column(Text, nullable=True)
    validation_status: Mapped[str] = mapped_column(String(24), nullable=False)
    validation_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    confidence: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    source_page: Mapped[int | None] = mapped_column(Integer, nullable=True)
    source_locator: Mapped[str | None] = mapped_column(Text, nullable=True)
    extractor: Mapped[str | None] = mapped_column(String(100), nullable=True)
    extractor_version: Mapped[str | None] = mapped_column(String(100), nullable=True)
    fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    decision: Mapped[str] = mapped_column(String(16), nullable=False, default="PENDING")
    decided_by_user_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    decided_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (
        ForeignKeyConstraint(["operation_id", "organization_id"], ["us_lacey_operations.id", "us_lacey_operations.organization_id"], name="fk_us_lacey_field_candidates_operation_tenant", ondelete="CASCADE"),
        ForeignKeyConstraint(["operation_field_id", "organization_id"], ["us_lacey_operation_fields.id", "us_lacey_operation_fields.organization_id"], name="fk_us_lacey_field_candidates_field_tenant", ondelete="CASCADE"),
        ForeignKeyConstraint(["source_assurance_document_id", "organization_id"], ["assurance_documents.id", "assurance_documents.organization_id"], name="fk_us_lacey_field_candidates_document_tenant", ondelete="RESTRICT"),
        UniqueConstraint("id", "organization_id", name="uq_us_lacey_field_candidates_id_org"),
        UniqueConstraint("organization_id", "fingerprint", name="uq_us_lacey_field_candidates_fingerprint"),
        CheckConstraint("confidence >= 0 AND confidence <= 1", name="ck_us_lacey_field_candidates_confidence"),
        CheckConstraint("validation_status IN ('VALID','INVALID','MISSING','REVIEW_REQUIRED')", name="ck_us_lacey_field_candidates_validation"),
        CheckConstraint("decision IN ('PENDING','SELECTED','REJECTED')", name="ck_us_lacey_field_candidates_decision"),
        Index("ix_us_lacey_field_candidates_org_field", "organization_id", "operation_field_id"),
        Index("ix_us_lacey_field_candidates_org_operation", "organization_id", "operation_id"),
    )
