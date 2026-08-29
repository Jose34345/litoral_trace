"""Gestor de conexiones de base de datos para Litoral Trace."""
from __future__ import annotations

from typing import Any

from litoral_trace.config import get_settings
from litoral_trace.config.settings import (
    normalize_database_url,
    resolve_runtime_database_url,
)

_engine: Any | None = None
_session_factory: Any | None = None


def _is_production_environment() -> bool:
    return get_settings().is_production


def _is_test_environment() -> bool:
    return get_settings().is_test


def _normalize_postgres_url(db_url: str) -> str:
    return normalize_database_url(db_url)


def _normalize_database_url(db_url: str) -> str:
    return normalize_database_url(db_url)


def _get_application_database_url_from_environment() -> str | None:
    return resolve_runtime_database_url(get_settings())


def _get_test_database_url() -> str:
    return get_settings().database.resolved_test_database_url


def reset_engine_state() -> None:
    """Reinicia el engine cacheado y el session factory asociado."""
    global _engine, _session_factory

    if _engine is not None:
        _engine.dispose()

    _engine = None
    _session_factory = None


def get_database_url() -> str:
    """Obtiene la URL de la base de datos desde settings, secrets o fallback local."""
    if _is_test_environment():
        return _get_test_database_url()

    db_url = _get_application_database_url_from_environment()

    if not db_url:
        try:
            import streamlit as st

            db_url = st.secrets.get("DB_URL")
        except Exception:
            db_url = None

    if db_url:
        db_url = _normalize_database_url(db_url)

    if _is_production_environment():
        if not db_url or not db_url.startswith("postgresql+psycopg://"):
            raise RuntimeError(
                "Production requires a valid PostgreSQL DATABASE_URL; no fallback to SQLite is allowed."
            )

    if not db_url:
        return "sqlite:///litoral_trace_prod.db"

    return db_url


def get_engine() -> Any:
    """Devuelve o inicializa la instancia singleton del Engine de SQLAlchemy con Pooling."""
    global _engine
    if _engine is not None:
        return _engine

    try:
        from sqlalchemy import create_engine

        db_url = get_database_url()
        if db_url.startswith("sqlite"):
            _engine = create_engine(
                db_url,
                connect_args={"check_same_thread": False},
                echo=False,
            )
        else:
            _engine = create_engine(
                db_url,
                pool_size=10,
                max_overflow=20,
                pool_recycle=1800,
                pool_pre_ping=True,
                echo=False,
            )
        return _engine
    except ImportError:
        return None


def get_session_factory() -> Any:
    """Devuelve un factory de sesiones para SQLAlchemy."""
    global _session_factory

    try:
        from sqlalchemy.orm import sessionmaker

        engine = get_engine()
        if _session_factory is not None:
            return _session_factory
        if engine:
            # El contexto RLS de tenant es local a cada transacción. En una API
            # request-scoped no debemos forzar un refresh implícito luego del
            # commit, porque ese refresh nace en una transacción nueva sin GUC.
            _session_factory = sessionmaker(
                bind=engine,
                autoflush=False,
                autocommit=False,
                expire_on_commit=False,
            )
            return _session_factory
    except ImportError:
        return None


def get_db_session() -> Any:
    """Obtiene una nueva sesion de base de datos."""
    try:
        factory = get_session_factory()
        if factory:
            return factory()
    except ImportError:
        return None
