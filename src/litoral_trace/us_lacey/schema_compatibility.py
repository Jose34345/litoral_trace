"""Fail-closed schema compatibility probe for the U.S. Lacey runtime.

The runtime never migrates the database. It verifies that PostgreSQL contains
the minimum Alembic revision required by the Lacey domain. Unrelated descendant
migrations may advance the repository/database head without invalidating Lacey.
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

# 073 adds the append-only tenant-scoped operation audit trail required by
# the enterprise System-of-Record workspace contract.
_REQUIRED_US_LACEY_SCHEMA_REVISION = "073_us_lacey_operation_audit_trail"


@lru_cache(maxsize=1)
def _repository_script_directory() -> ScriptDirectory:
    repository_root = Path(__file__).resolve().parents[3]
    config = Config(str(repository_root / "alembic.ini"))
    config.set_main_option("script_location", str(repository_root / "alembic"))
    return ScriptDirectory.from_config(config)


@lru_cache(maxsize=1)
def required_us_lacey_schema_revision() -> str:
    """Return the minimum Alembic revision required by Lacey."""
    script = _repository_script_directory()
    if script.get_revision(_REQUIRED_US_LACEY_SCHEMA_REVISION) is None:
        raise RuntimeError(
            "Required U.S. Lacey Alembic revision is not present in the repository."
        )
    return _REQUIRED_US_LACEY_SCHEMA_REVISION


def _revision_satisfies_required(*, current: str, required: str) -> bool:
    if current == required:
        return True

    script = _repository_script_directory()
    try:
        revisions = script.iterate_revisions(current, "base")
    except Exception:
        _LOG.exception(
            "us_lacey_schema_revision_graph_lookup_failed current=%s",
            current,
        )
        return False

    return any(revision.revision == required for revision in revisions)


def probe_us_lacey_schema_compatibility() -> bool:
    """Return True when the database includes the required Lacey revision."""
    try:
        required = required_us_lacey_schema_revision()
        with get_us_lacey_engine().connect() as connection:
            current = str(
                connection.execute(
                    text("SELECT version_num FROM alembic_version")
                ).scalar_one()
            )
    except Exception:
        _LOG.exception("us_lacey_schema_compatibility_probe_failed")
        return False

    if not _revision_satisfies_required(
        current=current,
        required=required,
    ):
        _LOG.warning(
            "us_lacey_schema_revision_mismatch required=%s current=%s",
            required,
            current,
        )
        return False

    _LOG.info(
        "us_lacey_schema_revision_ready required=%s current=%s",
        required,
        current,
    )
    return True
