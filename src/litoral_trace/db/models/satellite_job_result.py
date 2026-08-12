"""Immutable per-job satellite result snapshots."""
from __future__ import annotations

from datetime import datetime
from typing import Any, TYPE_CHECKING

from sqlalchemy import (
    JSON,
    DateTime,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from litoral_trace.db.base import Base


if TYPE_CHECKING:
    from litoral_trace.db.models.satellite_job import SatelliteJob


RESULT_PAYLOAD_TYPE = JSON().with_variant(
    JSONB(astext_type=Text()),
    "postgresql",
)


class SatelliteJobResult(Base):
    """Immutable domain snapshot for exactly one successful satellite job."""

    __tablename__ = "satellite_job_results"

    satellite_job_id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
    )
    organization_id: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
    )
    lote_id: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
    )
    result_schema_version: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
    )
    geometry_hash: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
    )
    algorithm_version: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
    )
    result_payload: Mapped[dict[str, Any]] = mapped_column(
        RESULT_PAYLOAD_TYPE,
        nullable=False,
    )
    payload_sha256: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    satellite_job: Mapped[SatelliteJob] = relationship(
        "SatelliteJob",
        back_populates="result_snapshot",
        uselist=False,
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["organization_id"],
            ["organizations.id"],
            ondelete="RESTRICT",
            name="fk_satellite_job_results_organization_id",
        ),
        ForeignKeyConstraint(
            ["satellite_job_id", "organization_id"],
            ["satellite_jobs.id", "satellite_jobs.organization_id"],
            ondelete="RESTRICT",
            name="fk_satellite_job_results_job_tenant",
        ),
        ForeignKeyConstraint(
            ["lote_id", "organization_id"],
            ["lotes.id", "lotes.organization_id"],
            ondelete="RESTRICT",
            name="fk_satellite_job_results_lote_tenant",
        ),
        Index(
            "ix_satellite_job_results_organization_id",
            "organization_id",
        ),
        Index(
            "ix_satellite_job_results_lote_id",
            "lote_id",
        ),
        Index(
            "ix_satellite_job_results_payload_sha256",
            "payload_sha256",
        ),
        Index(
            "ix_satellite_job_results_tenant_created_at",
            "organization_id",
            "created_at",
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<SatelliteJobResult satellite_job_id={self.satellite_job_id} "
            f"org={self.organization_id} "
            f"schema='{self.result_schema_version}'>"
        )
