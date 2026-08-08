"""Base declarativa y mixins comunes para SQLAlchemy 2.x."""
from __future__ import annotations
import os
from datetime import datetime, timezone
from sqlalchemy import DateTime, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def _disable_geoalchemy_sqlite_admin_for_non_production() -> None:
    """Evita hooks SpatiaLite en entornos locales donde sólo usamos SQLite simple."""
    env_value = os.environ.get("ENVIRONMENT", "").strip().lower()
    if env_value in {"production", "prod"}:
        return

    try:
        from geoalchemy2.admin.dialects import sqlite as geoalchemy_sqlite
    except Exception:
        return

    def _noop(*args, **kwargs):
        return None

    geoalchemy_sqlite.before_create = _noop
    geoalchemy_sqlite.after_create = _noop
    geoalchemy_sqlite.before_drop = _noop
    geoalchemy_sqlite.after_drop = _noop


_disable_geoalchemy_sqlite_admin_for_non_production()


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
