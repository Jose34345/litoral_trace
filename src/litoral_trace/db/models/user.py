"""Modelo User con autenticación y RBAC."""
from __future__ import annotations
from datetime import datetime
from typing import TYPE_CHECKING
from sqlalchemy import String, Boolean, DateTime, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship
from litoral_trace.db.base import Base, TimestampMixin

if TYPE_CHECKING:
    from litoral_trace.db.models.organization import Organization
    from litoral_trace.db.models.audit_log import AuditLog
    from litoral_trace.db.models.api_key import ApiKey
    from litoral_trace.db.models.user_session import UserSession

class User(Base, TimestampMixin):
    """Usuario del sistema con asignación multi-tenant a una Organización."""
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    organization_id: Mapped[int] = mapped_column(ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, index=True)
    
    email: Mapped[str] = mapped_column(String(255), nullable=False, unique=True, index=True)
    username: Mapped[str] = mapped_column(String(100), nullable=False, unique=True, index=True)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    role: Mapped[str] = mapped_column(String(50), nullable=False, default="cliente")  # superadmin, admin, manager, auditor, cliente
    full_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    last_login_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # Relaciones
    organization: Mapped[Organization] = relationship("Organization", back_populates="users")
    audit_logs: Mapped[list[AuditLog]] = relationship("AuditLog", back_populates="user")
    api_keys: Mapped[list[ApiKey]] = relationship("ApiKey", back_populates="user")
    sessions: Mapped[list[UserSession]] = relationship("UserSession", back_populates="user")

    def __repr__(self) -> str:
        return f"<User id={self.id} username='{self.username}' role='{self.role}'>"
