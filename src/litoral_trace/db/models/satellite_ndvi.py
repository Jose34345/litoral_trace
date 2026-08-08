"""Modelo ORM SatelliteNdviObservation para persistencia histórica de telemetría satelital."""
from __future__ import annotations

from datetime import date, datetime
from typing import TYPE_CHECKING

from sqlalchemy import (
    String,
    Float,
    Integer,
    Date,
    DateTime,
    ForeignKey,
    UniqueConstraint,
    Index,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from litoral_trace.db.base import Base


if TYPE_CHECKING:
    from litoral_trace.db.models.organization import Organization
    from litoral_trace.db.models.lote import Lote


class SatelliteNdviObservation(Base):
    """Persistencia histórica de mediciones NDVI Copernicus Sentinel-2
    reducidas sobre polígono real de lote.
    """

    __tablename__ = "satellite_ndvi_observations"

    id: Mapped[int] = mapped_column(
        primary_key=True,
        autoincrement=True,
    )

    organization_id: Mapped[int] = mapped_column(
        ForeignKey(
            "organizations.id",
            ondelete="CASCADE",
        ),
        nullable=False,
    )

    lote_id: Mapped[int] = mapped_column(
        ForeignKey(
            "lotes.id",
            ondelete="CASCADE",
        ),
        nullable=False,
    )

    observation_date: Mapped[date] = mapped_column(
        Date,
        nullable=False,
    )

    ndvi_mean: Mapped[float] = mapped_column(
        Float,
        nullable=False,
    )

    ndvi_min: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
    )

    ndvi_max: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
    )

    ndvi_std: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
    )

    cloud_percentage: Mapped[float] = mapped_column(
        Float,
        nullable=False,
        default=0.0,
    )

    valid_pixel_count: Mapped[int | None] = mapped_column(
        Integer,
        nullable=True,
    )

    valid_pixel_percentage: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
    )

    satellite: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        default="Sentinel-2",
    )

    collection: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        default="COPERNICUS/S2_SR_HARMONIZED",
    )

    geometry_hash: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
    )

    algorithm_version: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
    )

    processing_date: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    # Relaciones
    organization: Mapped[Organization] = relationship(
        "Organization",
    )

    lote: Mapped[Lote] = relationship(
        "Lote",
    )

    __table_args__ = (
        # Restricción de unicidad por organización, lote, fecha y geometría.
        UniqueConstraint(
            "organization_id",
            "lote_id",
            "observation_date",
            "geometry_hash",
            name="uq_satellite_obs_tenant_lote_date_hash",
        ),

        # Índices existentes en la base de datos.
        # Se mantienen los nombres históricos para evitar
        # drift de schema detectado por Alembic.
        Index(
            "ix_sat_obs_organization_id",
            "organization_id",
        ),

        Index(
            "ix_sat_obs_lote_id",
            "lote_id",
        ),

        Index(
            "ix_sat_obs_observation_date",
            "observation_date",
        ),

        Index(
            "ix_sat_obs_geometry_hash",
            "geometry_hash",
        ),

        # Índice compuesto para consultas históricas
        # por organización, lote y fecha.
        Index(
            "ix_sat_obs_tenant_lote_date",
            "organization_id",
            "lote_id",
            "observation_date",
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<SatelliteNdviObservation "
            f"lote_id={self.lote_id} "
            f"date={self.observation_date} "
            f"ndvi={self.ndvi_mean}>"
        )
