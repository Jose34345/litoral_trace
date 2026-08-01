"""Modelo Lote geoespacial de activos foresto-industriales."""
from __future__ import annotations
from typing import TYPE_CHECKING
from sqlalchemy import Float, String, Text, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship
from litoral_trace.db.base import Base, TimestampMixin

if TYPE_CHECKING:
    from litoral_trace.db.models.organization import Organization

class Lote(Base, TimestampMixin):
    """Lote/Rodal geoespacial con soporte PostGIS/WKT y balance de masas EUDR."""
    __tablename__ = "lotes"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    organization_id: Mapped[int] = mapped_column(ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, index=True)
    
    identificador: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    productor_id: Mapped[str] = mapped_column(String(100), nullable=False, index=True)  # CUIT / Guía Forestal / SACVeFor
    producto_forestal: Mapped[str] = mapped_column(String(100), nullable=False)
    hectareas: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    
    # Geometría (Centroide WGS84 + Polígono WKT)
    latitud: Mapped[float] = mapped_column(Float, nullable=False)
    longitud: Mapped[float] = mapped_column(Float, nullable=False)
    polygon_wkt: Mapped[str | None] = mapped_column(Text, nullable=True)
    
    # Métricas de Compliance & Balance de Masas
    estatus: Mapped[str] = mapped_column(String(50), nullable=False, default="Pendiente", index=True)  # Verde, Rojo, Pendiente
    volumen_ingresado_ton: Mapped[float | None] = mapped_column(Float, nullable=True, default=0.0)
    volumen_exportar_ton: Mapped[float | None] = mapped_column(Float, nullable=True, default=0.0)

    # Relaciones
    organization: Mapped[Organization] = relationship("Organization", back_populates="lotes")

    def __repr__(self) -> str:
        return f"<Lote id={self.id} identificador='{self.identificador}' estatus='{self.estatus}'>"
