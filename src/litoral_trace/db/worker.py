"""Dedicated worker claim connection utilities."""
from __future__ import annotations

from typing import Any

from litoral_trace.config import get_settings
from litoral_trace.config.settings import normalize_database_url, resolve_worker_database_url

_worker_engine: Any | None = None
_worker_session_factory: Any | None = None


def reset_worker_engine_state() -> None:
    global _worker_engine, _worker_session_factory

    if _worker_engine is not None:
        _worker_engine.dispose()

    _worker_engine = None
    _worker_session_factory = None


def get_worker_database_url() -> str:
    settings = get_settings()
    worker_database_url = resolve_worker_database_url(settings)

    if not worker_database_url:
        raise RuntimeError(
            "WORKER_DATABASE_URL es obligatorio para el worker satelital y no "
            "puede derivarse de MIGRATION_DATABASE_URL ni de URLs de test."
        )

    normalized_worker_database_url = normalize_database_url(worker_database_url)
    if not normalized_worker_database_url.startswith("postgresql+psycopg://"):
        raise RuntimeError(
            "WORKER_DATABASE_URL debe ser una URL PostgreSQL valida para el worker."
        )

    forbidden_urls = {
        value
        for value in (
            settings.database.resolved_migration_database_url,
            settings.database.test_database_url,
            settings.database.test_postgres_database_url,
        )
        if value
    }
    if normalized_worker_database_url in {
        normalize_database_url(value)
        for value in forbidden_urls
    }:
        raise RuntimeError(
            "WORKER_DATABASE_URL debe ser explicita y no puede reutilizar "
            "credenciales de migracion ni de test."
        )

    return normalized_worker_database_url


def get_worker_engine() -> Any:
    global _worker_engine
    if _worker_engine is not None:
        return _worker_engine

    from sqlalchemy import create_engine

    _worker_engine = create_engine(
        get_worker_database_url(),
        pool_size=5,
        max_overflow=5,
        pool_recycle=1800,
        pool_pre_ping=True,
        echo=False,
    )
    return _worker_engine


def get_worker_session_factory() -> Any:
    global _worker_session_factory
    if _worker_session_factory is not None:
        return _worker_session_factory

    from sqlalchemy.orm import sessionmaker

    _worker_session_factory = sessionmaker(
        bind=get_worker_engine(),
        autoflush=False,
        autocommit=False,
    )
    return _worker_session_factory


def get_worker_db_session() -> Any:
    factory = get_worker_session_factory()
    return factory() if factory else None
