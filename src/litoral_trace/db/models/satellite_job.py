"""Durable tenant-owned satellite job records."""
from __future__ import annotations

from datetime import date, datetime
from typing import TYPE_CHECKING

from sqlalchemy import (
    CheckConstraint,
    Date,
    DateTime,
    Float,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    Uuid,
    func,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from litoral_trace.db.base import Base


if TYPE_CHECKING:
    from litoral_trace.db.models.lote import Lote
    from litoral_trace.db.models.organization import Organization
    from litoral_trace.db.models.satellite_job_result import SatelliteJobResult
    from litoral_trace.db.models.satellite_ndvi import SatelliteNdviObservation


class SatelliteJob(Base):
    """Persistent queue foundation for tenant-scoped satellite processing."""

    __tablename__ = "satellite_jobs"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    organization_id: Mapped[int] = mapped_column(Integer, nullable=False)
    lote_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    job_type: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        default="ndvi_timeseries",
    )
    status: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default="queued",
    )
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=3)
    next_attempt_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    locked_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    locked_by: Mapped[str | None] = mapped_column(String(255), nullable=True)
    heartbeat_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    lease_token: Mapped[str | None] = mapped_column(Uuid(as_uuid=False), nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    finished_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    error_code: Mapped[str | None] = mapped_column(String(100), nullable=True)
    error_message: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    idempotency_key: Mapped[str | None] = mapped_column(String(255), nullable=True)
    request_start_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    request_end_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    max_cloud_pct: Mapped[float | None] = mapped_column(Float, nullable=True)
    geometry_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    algorithm_version: Mapped[str | None] = mapped_column(String(50), nullable=True)
    polygon_wkt_snapshot: Mapped[str | None] = mapped_column(Text, nullable=True)
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

    organization: Mapped[Organization] = relationship(
        "Organization",
        back_populates="satellite_jobs",
        overlaps="lote,satellite_jobs",
    )
    lote: Mapped[Lote | None] = relationship(
        "Lote",
        back_populates="satellite_jobs",
        overlaps="organization,satellite_jobs",
    )
    observations: Mapped[list[SatelliteNdviObservation]] = relationship(
        "SatelliteNdviObservation",
        back_populates="satellite_job",
        overlaps="organization,satellite_job",
    )
    result_snapshot: Mapped[SatelliteJobResult | None] = relationship(
        "SatelliteJobResult",
        back_populates="satellite_job",
        uselist=False,
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["organization_id"],
            ["organizations.id"],
            ondelete="CASCADE",
            name="fk_satellite_jobs_organization_id",
        ),
        ForeignKeyConstraint(
            ["lote_id", "organization_id"],
            ["lotes.id", "lotes.organization_id"],
            ondelete="CASCADE",
            name="fk_satellite_jobs_lote_tenant",
        ),
        UniqueConstraint(
            "organization_id",
            "idempotency_key",
            name="uq_satellite_jobs_tenant_idempotency_key",
        ),
        UniqueConstraint(
            "id",
            "organization_id",
            name="uq_satellite_jobs_id_organization_id",
        ),
        CheckConstraint(
            "status IN ('queued', 'running', 'succeeded', 'failed')",
            name="ck_satellite_jobs_status",
        ),
        CheckConstraint(
            "job_type IN ('ndvi_timeseries')",
            name="ck_satellite_jobs_job_type",
        ),
        CheckConstraint(
            "attempt_count >= 0",
            name="ck_satellite_jobs_attempt_count_non_negative",
        ),
        CheckConstraint(
            "max_attempts > 0",
            name="ck_satellite_jobs_max_attempts_positive",
        ),
        CheckConstraint(
            "attempt_count <= max_attempts",
            name="ck_satellite_jobs_attempt_count_lte_max_attempts",
        ),
        CheckConstraint(
            "("
            "max_cloud_pct IS NULL "
            "OR (max_cloud_pct >= 0.0 AND max_cloud_pct <= 100.0)"
            ")",
            name="ck_satellite_jobs_max_cloud_pct_range",
        ),
        CheckConstraint(
            "("
            "job_type <> 'ndvi_timeseries' "
            "OR ("
            "lote_id IS NOT NULL "
            "AND request_start_date IS NOT NULL "
            "AND request_end_date IS NOT NULL "
            "AND max_cloud_pct IS NOT NULL "
            "AND geometry_hash IS NOT NULL "
            "AND algorithm_version IS NOT NULL "
            "AND polygon_wkt_snapshot IS NOT NULL"
            ")"
            ")",
            name="ck_satellite_jobs_ndvi_timeseries_payload",
        ),
        CheckConstraint(
            "("
            "request_start_date IS NULL "
            "OR request_end_date IS NULL "
            "OR request_start_date <= request_end_date"
            ")",
            name="ck_satellite_jobs_date_window",
        ),
        Index("ix_satellite_jobs_organization_id", "organization_id"),
        Index("ix_satellite_jobs_lote_id", "lote_id"),
        Index(
            "ix_satellite_jobs_status_next_attempt_created_at",
            "status",
            "next_attempt_at",
            "created_at",
        ),
        Index(
            "ix_satellite_jobs_tenant_history",
            "organization_id",
            "created_at",
        ),
        Index(
            "ix_satellite_jobs_tenant_lote_history",
            "organization_id",
            "lote_id",
            "created_at",
        ),
        Index(
            "ix_satellite_jobs_running_heartbeat_at",
            "heartbeat_at",
            "id",
            postgresql_where=text(
                "status = 'running' AND heartbeat_at IS NOT NULL"
            ),
        ),
        Index(
            "uq_satellite_jobs_lease_token_non_null",
            "lease_token",
            unique=True,
            postgresql_where=text("lease_token IS NOT NULL"),
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<SatelliteJob id={self.id} "
            f"org={self.organization_id} "
            f"type='{self.job_type}' "
            f"status='{self.status}'>"
        )
