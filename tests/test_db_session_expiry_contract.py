from __future__ import annotations

from sqlalchemy import create_engine

import litoral_trace.db.engine as db_engine


def test_runtime_sessions_keep_loaded_state_after_commit(monkeypatch):
    """RLS tenant context is transaction-local, so commit must not force ORM refreshes."""
    engine = create_engine("sqlite+pysqlite:///:memory:")

    monkeypatch.setattr(db_engine, "_session_factory", None)
    monkeypatch.setattr(db_engine, "get_engine", lambda: engine)

    factory = db_engine.get_session_factory()
    assert factory is not None

    session = factory()
    try:
        assert session.expire_on_commit is False
    finally:
        session.close()
        engine.dispose()
