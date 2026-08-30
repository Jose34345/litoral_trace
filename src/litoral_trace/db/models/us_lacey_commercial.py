"""Commercial, legal-acceptance and durable-job models for the U.S. Lacey product."""
from __future__ import annotations

from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import (
    CheckConstraint,
    DateTime,
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


class UsLaceySubscription(Base):
    __tablename__ = "us_lacey_subscriptions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    public_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), default=uuid4, nullable=False)
    organization_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False
    )
    plan_code: Mapped[str] = mapped_column(String(64), nullable=False, default="PRIVATE_BETA")
    currency: Mapped[str] = mapped_column(String(3), nullable=False, default="USD")
    price_cents: Mapped[int] = mapped_column(Integer, nullable=False)
    monthly_operation_limit: Mapped[int] = mapped_column(Integer, nullable=False)
    used_operations: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    status: Mapped[str] = mapped_column(String(24), nullable=False, default="PENDING")
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    renews_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    __table_args__ = (
        UniqueConstraint("public_id", name="uq_us_lacey_subscriptions_public_id"),
        UniqueConstraint("organization_id", name="uq_us_lacey_subscriptions_org"),
        UniqueConstraint("id", "organization_id", name="uq_us_lacey_subscriptions_id_org"),
        CheckConstraint("currency = 'USD'", name="ck_us_lacey_subscriptions_currency_usd"),
        CheckConstraint("price_cents > 0", name="ck_us_lacey_subscriptions_price_positive"),
        CheckConstraint(
            "monthly_operation_limit > 0", name="ck_us_lacey_subscriptions_limit_positive"
        ),
        CheckConstraint("used_operations >= 0", name="ck_us_lacey_subscriptions_usage_nonnegative"),
        CheckConstraint(
            "status IN ('PENDING','ACTIVE','PAST_DUE','CANCELED')",
            name="ck_us_lacey_subscriptions_status",
        ),
        Index("ix_us_lacey_subscriptions_org_status", "organization_id", "status"),
    )


class UsLaceyPayment(Base):
    __tablename__ = "us_lacey_payments"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    public_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), default=uuid4, nullable=False)
    organization_id: Mapped[int] = mapped_column(Integer, nullable=False)
    subscription_id: Mapped[int] = mapped_column(Integer, nullable=False)
    provider: Mapped[str] = mapped_column(String(40), nullable=False, default="MANUAL_BANK_TRANSFER")
    amount_cents: Mapped[int] = mapped_column(Integer, nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False, default="USD")
    payment_reference: Mapped[str] = mapped_column(String(64), nullable=False)
    customer_reference: Mapped[str | None] = mapped_column(String(255), nullable=True)
    status: Mapped[str] = mapped_column(String(24), nullable=False, default="PENDING")
    paid_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    verified_by_user_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["subscription_id", "organization_id"],
            ["us_lacey_subscriptions.id", "us_lacey_subscriptions.organization_id"],
            name="fk_us_lacey_payments_subscription_tenant",
            ondelete="CASCADE",
        ),
        UniqueConstraint("public_id", name="uq_us_lacey_payments_public_id"),
        UniqueConstraint("payment_reference", name="uq_us_lacey_payments_reference"),
        UniqueConstraint("id", "organization_id", name="uq_us_lacey_payments_id_org"),
        CheckConstraint("amount_cents > 0", name="ck_us_lacey_payments_amount_positive"),
        CheckConstraint("currency = 'USD'", name="ck_us_lacey_payments_currency_usd"),
        CheckConstraint(
            "provider IN ('MANUAL_BANK_TRANSFER','WISE','STRIPE')",
            name="ck_us_lacey_payments_provider",
        ),
        CheckConstraint(
            "status IN ('PENDING','VERIFIED','REJECTED','REFUNDED')",
            name="ck_us_lacey_payments_status",
        ),
        Index("ix_us_lacey_payments_org_status", "organization_id", "status"),
        Index("ix_us_lacey_payments_reference", "payment_reference"),
    )


class UsLaceyTermsAcceptance(Base):
    __tablename__ = "us_lacey_terms_acceptances"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    organization_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False
    )
    user_id: Mapped[int] = mapped_column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    document_type: Mapped[str] = mapped_column(String(32), nullable=False)
    document_version: Mapped[str] = mapped_column(String(64), nullable=False)
    accepted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "organization_id", "user_id", "document_type", "document_version",
            name="uq_us_lacey_terms_acceptance_version",
        ),
        CheckConstraint(
            "document_type IN ('TERMS','PRIVACY','PRIVATE_BETA')",
            name="ck_us_lacey_terms_acceptance_type",
        ),
        Index("ix_us_lacey_terms_acceptances_org", "organization_id"),
    )


class UsLaceyProcessingJob(Base):
    __tablename__ = "us_lacey_processing_jobs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    public_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), default=uuid4, nullable=False)
    organization_id: Mapped[int] = mapped_column(Integer, nullable=False)
    operation_id: Mapped[int] = mapped_column(Integer, nullable=False)
    assurance_document_id: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="QUEUED")
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=3)
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    locked_by: Mapped[str | None] = mapped_column(String(255), nullable=True)
    locked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error_code: Mapped[str | None] = mapped_column(String(100), nullable=True)
    last_error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["operation_id", "organization_id"],
            ["us_lacey_operations.id", "us_lacey_operations.organization_id"],
            name="fk_us_lacey_processing_jobs_operation_tenant",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["assurance_document_id", "organization_id"],
            ["assurance_documents.id", "assurance_documents.organization_id"],
            name="fk_us_lacey_processing_jobs_document_tenant",
            ondelete="CASCADE",
        ),
        UniqueConstraint("public_id", name="uq_us_lacey_processing_jobs_public_id"),
        UniqueConstraint(
            "organization_id", "operation_id", "assurance_document_id",
            name="uq_us_lacey_processing_jobs_document_once",
        ),
        UniqueConstraint("id", "organization_id", name="uq_us_lacey_processing_jobs_id_org"),
        CheckConstraint(
            "status IN ('QUEUED','RUNNING','RETRY','COMPLETED','FAILED')",
            name="ck_us_lacey_processing_jobs_status",
        ),
        CheckConstraint("attempt_count >= 0", name="ck_us_lacey_processing_jobs_attempt_nonnegative"),
        CheckConstraint("max_attempts > 0", name="ck_us_lacey_processing_jobs_max_attempts_positive"),
        Index("ix_us_lacey_processing_jobs_queue", "status", "available_at", "created_at"),
        Index("ix_us_lacey_processing_jobs_org_status", "organization_id", "status"),
    )
