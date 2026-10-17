"""Allow the U.S. Lacey runtime to read only the Alembic schema revision.

Revision ID: 051_lacey_schema_readiness
Revises: 050_lacey_regulatory_assessment_snapshots
"""
from __future__ import annotations

from alembic import op


revision = "051_lacey_schema_readiness"
down_revision = "050_lacey_regulatory_assessment_snapshots"
branch_labels = None
depends_on = None

RUNTIME_ROLE = "litoral_trace_app"
WORKER_LOGIN_ROLE = "litoral_trace_us_lacey_worker"


def upgrade() -> None:
    # The application readiness probe needs only one metadata SELECT. Migration
    # ownership remains separate and no write capability is granted to runtime.
    op.execute("REVOKE ALL PRIVILEGES ON TABLE public.alembic_version FROM PUBLIC")
    op.execute(
        f"GRANT SELECT ON TABLE public.alembic_version TO {RUNTIME_ROLE}"
    )
    # Explicitly preserve the worker boundary even if a previous environment had
    # accumulated broad grants.
    op.execute(
        f"REVOKE ALL PRIVILEGES ON TABLE public.alembic_version FROM {WORKER_LOGIN_ROLE}"
    )


def downgrade() -> None:
    op.execute(
        f"REVOKE SELECT ON TABLE public.alembic_version FROM {RUNTIME_ROLE}"
    )
