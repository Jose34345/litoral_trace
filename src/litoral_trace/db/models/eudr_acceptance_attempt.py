"""Auditable ACCEPTANCE-only EUDR V3 transport attempts."""
from __future__ import annotations

from datetime import datetime
from typing import Final
from uuid import UUID, uuid4

from sqlalchemy import (
    Boolean,
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
)
from sqlalchemy.orm import Mapped, mapped_column

from litoral_trace.db.base import Base, TimestampMixin


EUDR_ACCEPTANCE_ATTEMPT_STATES: Final[frozenset[str]] = frozenset(
    {
        "PREPARED",
        "SENT",
        "REMOTE_ACCEPTED",
        "REMOTE_REJECTED",
        "TRANSPORT_ERROR",
    }
)
EUDR_ACCEPTANCE_OPERATIONS: Final[frozenset[str]] = frozenset({"SUBMIT_DDS"})


class EudrAcceptanceAttempt(Base, TimestampMixin):
    """One idempotent, tenant-safe attempt against non-legal ACCEPTANCE.

    The row deliberately stores no username, authentication key, WS-Security
    nonce or raw XML. Those belong to deployment secrets / transient memory.
    Hashes make the outbound contract auditable without turning PostgreSQL into
    a credential or message-body store.
    """

    __tablename__ = "eudr_acceptance_attempts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    public_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), default=uuid4, nullable=False)
    organization_id: Mapped[int] = mapped_column(Integer, nullable=False)
    candidate_id: Mapped[int] = mapped_column(Integer, nullable=False)

    environment: Mapped[str] = mapped_column(String(16), nullable=False, default="ACCEPTANCE")
    operation: Mapped[str] = mapped_column(String(24), nullable=False, default="SUBMIT_DDS")
    state: Mapped[str] = mapped_column(String(24), nullable=False, default="PREPARED")

    operator_role: Mapped[str] = mapped_column(String(32), nullable=False)
    country_of_activity: Mapped[str] = mapped_column(String(2), nullable=False)
    border_cross_country: Mapped[str | None] = mapped_column(String(2), nullable=True)
    internal_reference_number: Mapped[str] = mapped_column(String(120), nullable=False)
    geo_location_confidential: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="false"
    )

    candidate_payload_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    wire_contract_profile: Mapped[str] = mapped_column(String(120), nullable=False)
    wire_contract_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    request_body_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    envelope_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    response_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    request_body_bytes: Mapped[int] = mapped_column(Integer, nullable=False)

    remote_uuid: Mapped[str | None] = mapped_column(String(120), nullable=True)
    remote_reference_number: Mapped[str | None] = mapped_column(String(160), nullable=True)
    remote_verification_number: Mapped[str | None] = mapped_column(String(160), nullable=True)
    remote_status: Mapped[str | None] = mapped_column(String(80), nullable=True)
    http_status: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error_code: Mapped[str | None] = mapped_column(String(120), nullable=True)
    error_summary: Mapped[str | None] = mapped_column(Text, nullable=True)

    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_by_user_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey(
            "users.id",
            ondelete="SET NULL",
            name="fk_eudr_acceptance_attempts_created_by_user_id",
        ),
        nullable=True,
    )

    __table_args__ = (
        UniqueConstraint("id", "organization_id", name="uq_eudr_acceptance_attempts_id_org"),
        UniqueConstraint("public_id", name="uq_eudr_acceptance_attempts_public_id"),
        UniqueConstraint(
            "candidate_id",
            "request_body_sha256",
            name="uq_eudr_acceptance_attempts_candidate_body",
        ),
        ForeignKeyConstraint(
            ["organization_id"],
            ["organizations.id"],
            name="fk_eudr_acceptance_attempts_organization_id",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["candidate_id", "organization_id"],
            ["eudr_dds_candidates.id", "eudr_dds_candidates.organization_id"],
            name="fk_eudr_acceptance_attempts_candidate_tenant",
            ondelete="RESTRICT",
        ),
        CheckConstraint("environment = 'ACCEPTANCE'", name="ck_eudr_acceptance_attempts_environment"),
        CheckConstraint("operation = 'SUBMIT_DDS'", name="ck_eudr_acceptance_attempts_operation"),
        CheckConstraint(
            "state IN ('PREPARED','SENT','REMOTE_ACCEPTED','REMOTE_REJECTED','TRANSPORT_ERROR')",
            name="ck_eudr_acceptance_attempts_state",
        ),
        CheckConstraint(
            "operator_role IN ('OPERATOR')",
            name="ck_eudr_acceptance_attempts_operator_role",
        ),
        CheckConstraint("length(candidate_payload_sha256) = 64", name="ck_eudr_acceptance_attempts_candidate_hash"),
        CheckConstraint("length(wire_contract_sha256) = 64", name="ck_eudr_acceptance_attempts_contract_hash"),
        CheckConstraint("length(request_body_sha256) = 64", name="ck_eudr_acceptance_attempts_body_hash"),
        CheckConstraint("envelope_sha256 IS NULL OR length(envelope_sha256) = 64", name="ck_eudr_acceptance_attempts_envelope_hash"),
        CheckConstraint("response_sha256 IS NULL OR length(response_sha256) = 64", name="ck_eudr_acceptance_attempts_response_hash"),
        CheckConstraint("request_body_bytes > 0", name="ck_eudr_acceptance_attempts_body_bytes"),
        Index(
            "ix_eudr_acceptance_attempts_tenant_candidate_created",
            "organization_id",
            "candidate_id",
            "created_at",
        ),
        Index(
            "ix_eudr_acceptance_attempts_tenant_state_created",
            "organization_id",
            "state",
            "created_at",
        ),
    )
