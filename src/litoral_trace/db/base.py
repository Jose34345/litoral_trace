"""Base declarativa y mixins comunes para SQLAlchemy 2.x."""
from __future__ import annotations
from datetime import datetime, timezone
from sqlalchemy import DateTime, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

class Base(DeclarativeBase):
    """Base declarativa para la arquitectura de Litoral Trace."""
    pass

class TimestampMixin:
    """Mixin para marcas de tiempo auditables en UTC."""
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
