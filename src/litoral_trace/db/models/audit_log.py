"""Modelo AuditLog para trazabilidad inmutable y auditoría aduanera."""
from __future__ import annotations
from datetime import datetime
from typing import TYPE_CHECKING
import sqlalchemy as sa
from sqlalchemy import String, Text, ForeignKey, DateTime, func
from sqlalchemy.orm import Mapped, mapped_column, relationship
from litoral_trace.db.base import Base, TimestampMixin

if TYPE_CHECKING:
    from litoral_trace.db.models.organization import Organization
    from litoral_trace.db.models.user import User

class AuditLog(Base, TimestampMixin):
    """Registro inmutable de auditoría para trazabilidad legal y aduanera EUDR."""
    __tablename__ = "audit_logs"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    organization_id: Mapped[int] = mapped_column(ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, index=True)
    user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)
    username: Mapped[str | None] = mapped_column(String(100), nullable=True)
    
    action: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    entity_type: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    entity_id: Mapped[int | None] = mapped_column(nullable=True, index=True)
    
    before_data: Mapped[dict | None] = mapped_column(sa.JSON, nullable=True)
    after_data: Mapped[dict | None] = mapped_column(sa.JSON, nullable=True)
    detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    ip_address: Mapped[str | None] = mapped_column(String(45), nullable=True)
    
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False, index=True)

    # Relaciones
    organization: Mapped[Organization] = relationship("Organization", back_populates="audit_logs")
    user: Mapped[User | None] = relationship("User", back_populates="audit_logs")

    def __repr__(self) -> str:
        return f"<AuditLog id={self.id} action='{self.action}' username='{self.username}'>"
