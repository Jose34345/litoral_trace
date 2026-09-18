from __future__ import annotations

import os

import pytest
from sqlalchemy import create_engine, text

from litoral_trace.us_lacey.schema_compatibility import (
    required_us_lacey_schema_revision,
)


pytestmark = pytest.mark.skipif(
    os.getenv("ENABLE_POSTGRES_TESTS") != "1",
    reason="PostgreSQL integration is disabled.",
)


def test_runtime_role_reads_only_schema_revision_and_matches_repository_head() -> None:
    runtime_url = os.environ["US_LACEY_DATABASE_URL"]
    audit_url = os.environ["US_LACEY_TEST_AUDIT_DATABASE_URL"]

    runtime = create_engine(runtime_url, pool_pre_ping=True, hide_parameters=True)
    with runtime.connect() as connection:
        current_user = connection.execute(text("SELECT current_user")).scalar_one()
        current_revision = connection.execute(
            text("SELECT version_num FROM alembic_version")
        ).scalar_one()

    audit = create_engine(audit_url, pool_pre_ping=True, hide_parameters=True)
    with audit.connect() as connection:
        privileges = connection.execute(
            text(
                """
                SELECT
                    has_table_privilege(
                        'litoral_trace_app',
                        'public.alembic_version',
                        'SELECT'
                    ) AS runtime_select,
                    has_table_privilege(
                        'litoral_trace_us_lacey_worker',
                        'public.alembic_version',
                        'SELECT'
                    ) AS worker_select,
                    has_table_privilege(
                        'public',
                        'public.alembic_version',
                        'SELECT'
                    ) AS public_select
                """
            )
        ).mappings().one()

    runtime.dispose()
    audit.dispose()

    assert current_user == "litoral_trace_app"
    assert current_revision == required_us_lacey_schema_revision()
    assert dict(privileges) == {
        "runtime_select": True,
        "worker_select": False,
        "public_select": False,
    }
