"""Dedicated database connection for U.S. Lacey processing workers."""
from __future__ import annotations

from typing import Any
from urllib.parse import urlsplit
import os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from litoral_trace.config.settings import normalize_database_url
from litoral_trace.us_lacey.config import load_us_lacey_runtime_config

_worker_engine: Any | None = None
_worker_session_factory: Any | None = None


class UsLaceyWorkerConfigurationError(RuntimeError):
    pass


def _identity(url: str) -> tuple[str, int | None, str, str]:
    normalized = normalize_database_url(url)
    parsed = urlsplit(normalized.replace("postgresql+psycopg://", "postgresql://", 1))
    return (
        (parsed.hostname or "").lower(),
        parsed.port,
        (parsed.path or "").lstrip("/"),
        parsed.username or "",
    )


def reset_us_lacey_worker_engine_state() -> None:
    global _worker_engine, _worker_session_factory
    if _worker_engine is not None:
        _worker_engine.dispose()
    _worker_engine = None
    _worker_session_factory = None


def get_us_lacey_worker_database_url() -> str:
    raw = str(os.environ.get("US_LACEY_WORKER_DATABASE_URL", "")).strip()
    if not raw:
        raise UsLaceyWorkerConfigurationError(
            "US_LACEY_WORKER_DATABASE_URL is required for U.S. processing workers."
        )
    worker_url = normalize_database_url(raw)
    if not worker_url.startswith("postgresql+psycopg://"):
        raise UsLaceyWorkerConfigurationError(
            "US_LACEY_WORKER_DATABASE_URL must be PostgreSQL."
        )

    runtime_url = load_us_lacey_runtime_config().database_url
    worker_host, worker_port, worker_db, worker_user = _identity(worker_url)
    runtime_host, runtime_port, runtime_db, runtime_user = _identity(runtime_url)
    if (worker_host, worker_port, worker_db) != (runtime_host, runtime_port, runtime_db):
        raise UsLaceyWorkerConfigurationError(
            "The U.S. worker must target the same isolated U.S. database as the web runtime."
        )
    if not worker_user or worker_user == runtime_user:
        raise UsLaceyWorkerConfigurationError(
            "The U.S. worker must use credentials distinct from the web runtime role."
        )
    return worker_url


def get_us_lacey_worker_engine() -> Any:
    global _worker_engine
    if _worker_engine is None:
        _worker_engine = create_engine(
            get_us_lacey_worker_database_url(),
            pool_size=3,
            max_overflow=2,
            pool_recycle=1800,
            pool_pre_ping=True,
            echo=False,
        )
    return _worker_engine


def get_us_lacey_worker_session_factory() -> Any:
    global _worker_session_factory
    if _worker_session_factory is None:
        _worker_session_factory = sessionmaker(
            bind=get_us_lacey_worker_engine(),
            autoflush=False,
            autocommit=False,
            expire_on_commit=False,
        )
    return _worker_session_factory


def get_us_lacey_worker_db_session() -> Any:
    return get_us_lacey_worker_session_factory()()
