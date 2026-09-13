"""Database engine dedicated to the U.S. Lacey pilot.

This module never consults Litoral Trace's generic DATABASE_URL and therefore
cannot silently connect the pilot to the Argentina runtime database.
"""
from __future__ import annotations

from typing import Any

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from litoral_trace.us_lacey.config import load_us_lacey_runtime_config

_engine: Any | None = None
_session_factory: Any | None = None


def reset_us_lacey_engine_state() -> None:
    global _engine, _session_factory
    if _engine is not None:
        _engine.dispose()
    _engine = None
    _session_factory = None


def get_us_lacey_database_url() -> str:
    return load_us_lacey_runtime_config().database_url


def get_us_lacey_engine() -> Any:
    global _engine
    if _engine is None:
        _engine = create_engine(
            get_us_lacey_database_url(),
            pool_size=5,
            max_overflow=10,
            pool_recycle=1800,
            pool_pre_ping=True,
            echo=False,
        )
    return _engine


def get_us_lacey_session_factory() -> Any:
    global _session_factory
    if _session_factory is None:
        _session_factory = sessionmaker(
            bind=get_us_lacey_engine(),
            autoflush=False,
            autocommit=False,
            expire_on_commit=False,
        )
    return _session_factory


def get_us_lacey_db_session() -> Any:
    return get_us_lacey_session_factory()()
