"""Fail-closed schema compatibility probe for the isolated U.S. Lacey runtime.

The application runtime never migrates the database. It only compares the single
repository Alembic head with the revision recorded by PostgreSQL and reports a
boolean readiness signal. Migration credentials stay outside the Render runtime.
"""
from __future__ import annotations

from functools import lru_cache
import logging
from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import text

from litoral_trace.us_lacey.db import get_us_lacey_engine


_LOG = logging.getLogger("litoral_trace.us_lacey.schema_compatibility")


@lru_cache(maxsize=1)
def required_us_lacey_schema_revision() -> str:
    """Return the repository's single canonical Alembic head.

    A release with multiple heads is not safely deployable and therefore raises.
    """
    repository_root = Path(__file__).resolve().parents[3]
    config = Config(str(repository_root / "alembic.ini"))
    config.set_main_option("script_location", str(repository_root / "alembic"))
    script = ScriptDirectory.from_config(config)
    heads = tuple(script.get_heads())
    if len(heads) != 1:
        raise RuntimeError(
            f"Expected exactly one canonical Alembic head; found {len(heads)}."
        )
    return heads[0]


def probe_us_lacey_schema_compatibility() -> bool:
    """Return True only when the runtime database is exactly at the code-required head.

    The query is deliberately read-only. Error details and connection information are
    logged server-side only and are never returned to HTTP clients.
    """
    try:
        required = required_us_lacey_schema_revision()
        with get_us_lacey_engine().connect() as connection:
            current = connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one()
    except Exception:
        _LOG.exception("us_lacey_schema_compatibility_probe_failed")
        return False

    if str(current) != required:
        _LOG.warning(
            "us_lacey_schema_revision_mismatch required=%s current=%s",
            required,
            current,
        )
        return False

    _LOG.info("us_lacey_schema_revision_ready revision=%s", required)
    return True
