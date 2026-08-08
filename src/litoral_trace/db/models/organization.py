"""Modelo Organization - Entidad raíz multi-tenant."""
from __future__ import annotations
from typing import TYPE_CHECKING
from sqlalchemy import String, Text, Boolean
from sqlalchemy.orm import Mapped, mapped_column, relationship
from litoral_trace.db.base import Base, TimestampMixin

if TYPE_CHECKING:
    from litoral_trace.db.models.user import User
    from litoral_trace.db.models.lote import Lote
    from litoral_trace.db.models.audit_log import AuditLog
    from litoral_trace.db.models.api_key import ApiKey
    from litoral_trace.db.models.license import License
    from litoral_trace.db.models.user_session import UserSession

class Organization(Base, TimestampMixin):
    """Organización / Empresa para aislamiento estricto multi-tenant."""
    __tablename__ = "organizations"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    slug: Mapped[str] = mapped_column(String(100), nullable=False, unique=True, index=True)
    tax_id: Mapped[str | None] = mapped_column(String(50), nullable=True, unique=True, index=True)  # CUIT / CUIL
    tier: Mapped[str] = mapped_column(String(50), nullable=False, default="pro")  # free, pro, enterprise
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    # Relaciones
    users: Mapped[list[User]] = relationship("User", back_populates="organization", cascade="all, delete-orphan")
    lotes: Mapped[list[Lote]] = relationship("Lote", back_populates="organization", cascade="all, delete-orphan")
    audit_logs: Mapped[list[AuditLog]] = relationship("AuditLog", back_populates="organization", cascade="all, delete-orphan")
    api_keys: Mapped[list[ApiKey]] = relationship("ApiKey", back_populates="organization", cascade="all, delete-orphan")
    licenses: Mapped[list[License]] = relationship("License", back_populates="organization", cascade="all, delete-orphan")
    sessions: Mapped[list[UserSession]] = relationship("UserSession", back_populates="organization", cascade="all, delete-orphan")

    def __repr__(self) -> str:
        return f"<Organization id={self.id} slug='{self.slug}' tier='{self.tier}'>"
