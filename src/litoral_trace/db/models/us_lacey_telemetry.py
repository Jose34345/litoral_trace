"""De-identified product-learning telemetry for U.S. Lacey workflows."""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from uuid import UUID, uuid4

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    JSON,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    Uuid,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from litoral_trace.db.base import Base


class TelemetryRunOrigin(StrEnum):
    SANDBOX = "SANDBOX"
    PRODUCTION = "PRODUCTION"


class TelemetryFieldActionType(StrEnum):
    CONFIRMED = "CONFIRMED"
    CORRECTED = "CORRECTED"
    REJECTED = "REJECTED"
    UNREVIEWED = "UNREVIEWED"


class TelemetryRun(Base):
    __tablename__ = "us_lacey_telemetry_runs"

    run_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid4)
    origin: Mapped[str] = mapped_column(String(16), nullable=False)
    learning_opt_in: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    document_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    document_type_counts: Mapped[dict[str, int]] = mapped_column(JSON, nullable=False, default=dict)
    processing_total_ms: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    reviewed_field_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    confirmed_field_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    corrected_field_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    rejected_field_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    unreviewed_field_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    human_correction_rate: Mapped[Decimal | None] = mapped_column(Numeric(6, 5), nullable=True)
    mean_prediction_confidence: Mapped[Decimal | None] = mapped_column(Numeric(6, 5), nullable=True)
    engine_version: Mapped[str | None] = mapped_column(String(64), nullable=True)
    telemetry_schema_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    finalized_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (
        CheckConstraint("origin IN ('SANDBOX','PRODUCTION')", name="ck_us_lacey_telemetry_runs_origin"),
        CheckConstraint("document_count >= 0", name="ck_us_lacey_telemetry_runs_document_count"),
        CheckConstraint("processing_total_ms IS NULL OR processing_total_ms >= 0", name="ck_us_lacey_telemetry_runs_processing_ms"),
        CheckConstraint(
            "reviewed_field_count >= 0 AND confirmed_field_count >= 0 "
            "AND corrected_field_count >= 0 AND rejected_field_count >= 0 "
            "AND unreviewed_field_count >= 0",
            name="ck_us_lacey_telemetry_runs_counts_nonnegative",
        ),
        CheckConstraint(
            "reviewed_field_count = confirmed_field_count + corrected_field_count + rejected_field_count",
            name="ck_us_lacey_telemetry_runs_reviewed_consistency",
        ),
        CheckConstraint(
            "human_correction_rate IS NULL OR (human_correction_rate >= 0 AND human_correction_rate <= 1)",
            name="ck_us_lacey_telemetry_runs_correction_rate",
        ),
        CheckConstraint(
            "mean_prediction_confidence IS NULL OR "
            "(mean_prediction_confidence >= 0 AND mean_prediction_confidence <= 1)",
            name="ck_us_lacey_telemetry_runs_confidence",
        ),
        Index("ix_us_lacey_telemetry_runs_origin_created", "origin", "created_at"),
    )


class TelemetryFieldAction(Base):
    __tablename__ = "us_lacey_telemetry_field_actions"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    telemetry_run_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("us_lacey_telemetry_runs.run_id", ondelete="CASCADE"),
        nullable=False,
    )
    field_instance_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), default=uuid4, nullable=False)
    field_name: Mapped[str] = mapped_column(String(128), nullable=False)
    document_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    prediction_confidence: Mapped[Decimal | None] = mapped_column(Numeric(6, 5), nullable=True)
    action_taken: Mapped[str] = mapped_column(String(16), nullable=False)
    deidentified_fragment: Mapped[str | None] = mapped_column(Text, nullable=True)
    target_value: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (
        UniqueConstraint("telemetry_run_id", "field_instance_id", name="uq_us_lacey_telemetry_field_instance"),
        CheckConstraint(
            "action_taken IN ('CONFIRMED','CORRECTED','REJECTED','UNREVIEWED')",
            name="ck_us_lacey_telemetry_field_action",
        ),
        CheckConstraint(
            "prediction_confidence IS NULL OR (prediction_confidence >= 0 AND prediction_confidence <= 1)",
            name="ck_us_lacey_telemetry_field_confidence",
        ),
        CheckConstraint(
            "action_taken = 'CORRECTED' OR (deidentified_fragment IS NULL AND target_value IS NULL)",
            name="ck_us_lacey_telemetry_corrected_payload",
        ),
        Index("ix_us_lacey_telemetry_field_run", "telemetry_run_id"),
        Index("ix_us_lacey_telemetry_field_document_action", "document_type", "action_taken"),
        Index("ix_us_lacey_telemetry_field_name_action", "field_name", "action_taken"),
    )
