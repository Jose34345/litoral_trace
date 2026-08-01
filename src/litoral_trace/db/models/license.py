"""Modelo License para control de licencias y cuotas de uso SaaS."""
from __future__ import annotations
from datetime import datetime
from typing import TYPE_CHECKING
from sqlalchemy import String, Integer, Float, Boolean, DateTime, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship
from litoral_trace.db.base import Base, TimestampMixin

if TYPE_CHECKING:
    from litoral_trace.db.models.organization import Organization

class License(Base, TimestampMixin):
    """Licencia y límites de cuota por organización."""
    __tablename__ = "licenses"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    organization_id: Mapped[int] = mapped_column(ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, index=True)
    
    plan_type: Mapped[str] = mapped_column(String(50), nullable=False, default="pro")  # pro, enterprise, custom
    max_lotes: Mapped[int] = mapped_column(Integer, nullable=False, default=100)
    max_volume_tons: Mapped[float] = mapped_column(Float, nullable=False, default=10000.0)
    max_batch_rows: Mapped[int] = mapped_column(Integer, nullable=False, default=500)
    
    valid_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    # Relaciones
    organization: Mapped[Organization] = relationship("Organization", back_populates="licenses")

    def __repr__(self) -> str:
        return f"<License id={self.id} plan='{self.plan_type}' max_lotes={self.max_lotes}>"
