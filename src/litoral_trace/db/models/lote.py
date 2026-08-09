"""Modelo Lote geoespacial de activos foresto-industriales."""

from __future__ import annotations

from typing import TYPE_CHECKING

from geoalchemy2 import Geometry
from sqlalchemy import Float, ForeignKey, Index, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from litoral_trace.db.base import Base, TimestampMixin


if TYPE_CHECKING:
    from litoral_trace.db.models.organization import Organization
    from litoral_trace.db.models.satellite_job import SatelliteJob


GEOM_COLUMN_TYPE = Geometry(
    geometry_type="POLYGON",
    srid=4326,
    spatial_index=False,
).with_variant(Text(), "sqlite")


class Lote(Base, TimestampMixin):
    """Lote/Rodal geoespacial con soporte PostGIS/WKT y balance de masas EUDR."""

    __tablename__ = "lotes"

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
        index=True,
    )

    identificador: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        index=True,
    )

    productor_id: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        index=True,
    )

    producto_forestal: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
    )

    hectareas: Mapped[float] = mapped_column(
        Float,
        nullable=False,
        default=0.0,
    )

    # ============================================================
    # Geolocalización WGS84
    # ============================================================

    latitud: Mapped[float] = mapped_column(
        Float,
        nullable=False,
    )

    longitud: Mapped[float] = mapped_column(
        Float,
        nullable=False,
    )

    # WKT original conservado por compatibilidad.
    polygon_wkt: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )

    # Geometría PostGIS real.
    #
    # POLYGON:
    #   El lote se representa como un polígono.
    #
    # SRID 4326:
    #   WGS84, coordenadas geográficas latitud/longitud.
    #
    # nullable=True:
    #   Permite lotes que todavía no tengan geometría.
    geom: Mapped[object | None] = mapped_column(
        GEOM_COLUMN_TYPE,
        nullable=True,
    )

    # ============================================================
    # Compliance EUDR y balance de masas
    # ============================================================

    estatus: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        default="Pendiente",
        index=True,
    )

    volumen_ingresado_ton: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
        default=0.0,
    )

    volumen_exportar_ton: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
        default=0.0,
    )

    # ============================================================
    # Relaciones
    # ============================================================

    organization: Mapped[Organization] = relationship(
        "Organization",
        back_populates="lotes",
    )
    satellite_jobs: Mapped[list[SatelliteJob]] = relationship(
        "SatelliteJob",
        back_populates="lote",
        overlaps="organization,satellite_jobs",
    )

    # ============================================================
    # Índices
    # ============================================================

    __table_args__ = (
        UniqueConstraint(
            "id",
            "organization_id",
            name="uq_lotes_id_organization_id",
        ),
        Index(
            "ix_lotes_geom_gist",
            "geom",
            postgresql_using="gist",
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<Lote id={self.id} "
            f"identificador='{self.identificador}' "
            f"estatus='{self.estatus}'>"
        )
