"""Gestor de Conexiones de Base de Datos PostgreSQL + PostGIS con Pooling de Alto Rendimiento."""
from __future__ import annotations
import os
from typing import Any

_engine: Any | None = None
_session_factory: Any | None = None


def _is_production_environment() -> bool:
    env_value = os.environ.get("ENVIRONMENT", "").strip().lower()
    return env_value in {"production", "prod"}


def _normalize_postgres_url(db_url: str) -> str:
    if db_url.startswith("postgres://"):
        return db_url.replace("postgres://", "postgresql+psycopg://", 1)
    if db_url.startswith("postgresql://"):
        return db_url.replace("postgresql://", "postgresql+psycopg://", 1)
    return db_url


def get_database_url() -> str:
    """Obtiene la URL de la base de datos desde variables de entorno, secrets o fallback local."""
    db_url = os.environ.get("DATABASE_URL") or os.environ.get("POSTGRES_URL") or os.environ.get("DB_URL")

    if not db_url:
        try:
            import streamlit as st
            db_url = st.secrets.get("DB_URL")
        except Exception:
            db_url = None

    if db_url:
        db_url = _normalize_postgres_url(db_url)

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
            _session_factory = sessionmaker(
                bind=engine,
                autoflush=False,
                autocommit=False,
            )
            return _session_factory
    except ImportError:
        return None


def get_db_session() -> Any:
    """Obtiene una nueva sesión de base de datos."""
    try:
        factory = get_session_factory()
        if factory:
            return factory()
    except ImportError:
        return None
