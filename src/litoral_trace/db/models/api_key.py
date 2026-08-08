"""Modelo ApiKey para integraciones B2B seguras."""
from __future__ import annotations
from datetime import datetime
from typing import TYPE_CHECKING
import sqlalchemy as sa
from sqlalchemy import String, Boolean, DateTime, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship
from litoral_trace.db.base import Base, TimestampMixin

if TYPE_CHECKING:
    from litoral_trace.db.models.organization import Organization
    from litoral_trace.db.models.user import User

class ApiKey(Base, TimestampMixin):
    """Clave de API para autenticación programática de clientes B2B (ERP/SACVeFor/VUCE)."""
    __tablename__ = "api_keys"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    organization_id: Mapped[int] = mapped_column(ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, index=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    key_prefix: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    key_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    permissions: Mapped[dict | None] = mapped_column(sa.JSON, nullable=True)
    
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_used_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # Relaciones
    organization: Mapped[Organization] = relationship("Organization", back_populates="api_keys")
    user: Mapped[User] = relationship("User", back_populates="api_keys")

    def __repr__(self) -> str:
        return f"<ApiKey id={self.id} prefix='{self.key_prefix}*' name='{self.name}'>"
