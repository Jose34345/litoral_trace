"""Passive, privacy-bounded reliability telemetry for U.S. Lacey paid pilots."""
from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from uuid import UUID, uuid4

from sqlalchemy import (
    BigInteger,
    Boolean,
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
)
from sqlalchemy.orm import Mapped, mapped_column

from litoral_trace.db.base import Base


class PilotQualityTrigger(StrEnum):
    INITIAL_PROCESS = "INITIAL_PROCESS"
    REPROCESS = "REPROCESS"
    WATCHDOG = "WATCHDOG"


class PilotIncidentSeverity(StrEnum):
    P0 = "P0"
    P1 = "P1"


class PilotIncidentStatus(StrEnum):
    OPEN = "OPEN"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    FIX_IN_PROGRESS = "FIX_IN_PROGRESS"
    FIX_READY = "FIX_READY"
    FIX_DEPLOYED = "FIX_DEPLOYED"
    RETESTED = "RETESTED"
    CLOSED = "CLOSED"


class UsLaceyPilotQualitySnapshot(Base):
    """PII-free technical counters captured after one processing generation."""

    __tablename__ = "us_lacey_pilot_quality_snapshots"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    organization_id: Mapped[int] = mapped_column(Integer, nullable=False)
    operation_id: Mapped[int] = mapped_column(Integer, nullable=False)
    attribution_session_id: Mapped[UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        nullable=True,
    )
    trigger: Mapped[str] = mapped_column(String(24), nullable=False)

    source_set_fingerprint: Mapped[str | None] = mapped_column(String(64), nullable=True)
    engine_version: Mapped[str | None] = mapped_column(String(100), nullable=True)
    canonical_publisher_version: Mapped[str | None] = mapped_column(String(100), nullable=True)

    document_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    valid_document_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    logical_document_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    document_type_counts: Mapped[dict[str, int]] = mapped_column(
        JSON, nullable=False, default=dict
    )

    commercial_line_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    canonical_line_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    auto_resolved_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    action_required_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    confirmed_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    conflict_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    total_field_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    processing_duration_ms: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    export_ready: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["operation_id", "organization_id"],
            ["us_lacey_operations.id", "us_lacey_operations.organization_id"],
            name="fk_lacey_pilot_snapshot_operation_tenant",
            ondelete="CASCADE",
        ),
        UniqueConstraint(
            "id",
            "organization_id",
            name="uq_lacey_pilot_snapshot_id_org",
        ),
        CheckConstraint(
            "trigger IN ('INITIAL_PROCESS','REPROCESS','WATCHDOG')",
            name="ck_lacey_pilot_snapshot_trigger",
        ),
        CheckConstraint(
            "document_count >= 0 AND valid_document_count >= 0 "
            "AND logical_document_count >= 0",
            name="ck_lacey_pilot_snapshot_document_counts",
        ),
        CheckConstraint(
            "commercial_line_count >= 0 AND canonical_line_count >= 0",
            name="ck_lacey_pilot_snapshot_line_counts",
        ),
        CheckConstraint(
            "auto_resolved_count >= 0 AND action_required_count >= 0 "
            "AND confirmed_count >= 0 AND conflict_count >= 0 "
            "AND total_field_count >= 0",
            name="ck_lacey_pilot_snapshot_field_counts",
        ),
        CheckConstraint(
            "processing_duration_ms IS NULL OR processing_duration_ms >= 0",
            name="ck_lacey_pilot_snapshot_processing_ms",
        ),
        Index(
            "ix_lacey_pilot_snapshot_org_operation_created",
            "organization_id",
            "operation_id",
            "created_at",
        ),
        Index(
            "ix_lacey_pilot_snapshot_attribution_created",
            "attribution_session_id",
            "created_at",
        ),
    )


class UsLaceyPilotIncident(Base):
    """One deduplicated technical anomaly emitted by PilotQualityGuard."""

    __tablename__ = "us_lacey_pilot_incidents"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    incident_public_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), default=uuid4, nullable=False
    )
    organization_id: Mapped[int] = mapped_column(Integer, nullable=False)
    operation_id: Mapped[int] = mapped_column(Integer, nullable=False)
    quality_snapshot_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    attribution_session_id: Mapped[UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        nullable=True,
    )

    detector_code: Mapped[str] = mapped_column(String(64), nullable=False)
    severity: Mapped[str] = mapped_column(String(8), nullable=False)
    status: Mapped[str] = mapped_column(
        String(24), nullable=False, default=PilotIncidentStatus.OPEN.value
    )
    fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    diagnostic_manifest: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)

    acknowledged_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    fixed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["operation_id", "organization_id"],
            ["us_lacey_operations.id", "us_lacey_operations.organization_id"],
            name="fk_lacey_pilot_incident_operation_tenant",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["quality_snapshot_id", "organization_id"],
            [
                "us_lacey_pilot_quality_snapshots.id",
                "us_lacey_pilot_quality_snapshots.organization_id",
            ],
            name="fk_lacey_pilot_incident_snapshot_tenant",
            ondelete="RESTRICT",
        ),
        UniqueConstraint(
            "incident_public_id",
            name="uq_lacey_pilot_incident_public_id",
        ),
        UniqueConstraint(
            "fingerprint",
            name="uq_lacey_pilot_incident_fingerprint",
        ),
        CheckConstraint(
            "severity IN ('P0','P1')",
            name="ck_lacey_pilot_incident_severity",
        ),
        CheckConstraint(
            "status IN ('OPEN','ACKNOWLEDGED','FIX_IN_PROGRESS','FIX_READY',"
            "'FIX_DEPLOYED','RETESTED','CLOSED')",
            name="ck_lacey_pilot_incident_status",
        ),
        CheckConstraint(
            "length(fingerprint) = 64",
            name="ck_lacey_pilot_incident_fingerprint_length",
        ),
        Index(
            "ix_lacey_pilot_incident_org_status_created",
            "organization_id",
            "status",
            "created_at",
        ),
        Index(
            "ix_lacey_pilot_incident_operation_created",
            "organization_id",
            "operation_id",
            "created_at",
        ),
        Index(
            "ix_lacey_pilot_incident_attribution_created",
            "attribution_session_id",
            "created_at",
        ),
    )
