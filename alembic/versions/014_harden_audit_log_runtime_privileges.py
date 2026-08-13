"""Harden runtime audit log privileges for append-only evidence.

Revision ID: 014_harden_audit_log_runtime_privileges
Revises: 013_add_satellite_job_results
Create Date: 2026-08-12 00:00:00.000000
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op


revision: str = "014_harden_audit_log_runtime_privileges"
down_revision: Union[str, Sequence[str], None] = "013_add_satellite_job_results"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


RUNTIME_ROLE = "litoral_trace_app"


def upgrade() -> None:
    op.execute(
        "GRANT SELECT, INSERT ON TABLE public.audit_logs "
        f"TO {RUNTIME_ROLE}"
    )
    op.execute(
        "GRANT USAGE, SELECT ON SEQUENCE public.audit_logs_id_seq "
        f"TO {RUNTIME_ROLE}"
    )
    op.execute(
        "REVOKE UPDATE, DELETE ON TABLE public.audit_logs "
        f"FROM {RUNTIME_ROLE}"
    )


def downgrade() -> None:
    op.execute(
        "GRANT UPDATE, DELETE ON TABLE public.audit_logs "
        f"TO {RUNTIME_ROLE}"
    )
