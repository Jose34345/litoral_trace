"""Add tenant-scoped Assurance operational exceptions.

Revision ID: 032_assurance_operational_exceptions
Revises: 031_assurance_reconciliation
Create Date: 2026-08-27 20:40:00.000000
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "032_assurance_operational_exceptions"
down_revision: Union[str, Sequence[str], None] = "031_assurance_reconciliation"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

RUNTIME_ROLE = "litoral_trace_app"
WORKER_EXECUTOR_ROLE = "litoral_trace_worker_executor"
TENANT_CONTEXT_SQL = (
    "NULLIF(current_setting('app.current_organization_id', true), '')::integer"
)
TABLE = "operational_exceptions"


def _enable_rls() -> None:
    tenant_match = f"organization_id = {TENANT_CONTEXT_SQL}"
    op.execute(f"ALTER TABLE public.{TABLE} ENABLE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE public.{TABLE} FORCE ROW LEVEL SECURITY")
    op.execute(
        f"CREATE POLICY {TABLE}_tenant_select ON public.{TABLE} "
        f"FOR SELECT USING ({tenant_match})"
    )
    op.execute(
        f"CREATE POLICY {TABLE}_tenant_insert ON public.{TABLE} "
        f"FOR INSERT WITH CHECK ({tenant_match})"
    )
    op.execute(
        f"CREATE POLICY {TABLE}_tenant_update ON public.{TABLE} "
        f"FOR UPDATE USING ({tenant_match}) WITH CHECK ({tenant_match})"
    )


def _grant_runtime() -> None:
    op.execute(f"REVOKE ALL PRIVILEGES ON TABLE public.{TABLE} FROM PUBLIC")
    op.execute(
        f"GRANT SELECT, INSERT, UPDATE ON TABLE public.{TABLE} TO {RUNTIME_ROLE}"
    )
    op.execute(
        f"REVOKE ALL PRIVILEGES ON TABLE public.{TABLE} FROM {WORKER_EXECUTOR_ROLE}"
    )
    sequence = f"{TABLE}_id_seq"
    op.execute(f"REVOKE ALL PRIVILEGES ON SEQUENCE public.{sequence} FROM PUBLIC")
    op.execute(
        f"GRANT USAGE, SELECT ON SEQUENCE public.{sequence} TO {RUNTIME_ROLE}"
    )
    op.execute(
        f"REVOKE ALL PRIVILEGES ON SEQUENCE public.{sequence} FROM {WORKER_EXECUTOR_ROLE}"
    )


def upgrade() -> None:
    op.create_table(
        TABLE,
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column(
            "public_id",
            sa.Uuid(),
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("fingerprint", sa.String(length=64), nullable=False),
        sa.Column(
            "source_type",
            sa.String(length=32),
            nullable=False,
            server_default="MANUAL",
        ),
        sa.Column("source_reference", sa.String(length=255), nullable=True),
        sa.Column("operation_reference", sa.String(length=255), nullable=False),
        sa.Column("cause_code", sa.String(length=100), nullable=False),
        sa.Column("entity_type", sa.String(length=64), nullable=False),
        sa.Column("entity_reference", sa.String(length=255), nullable=False),
        sa.Column("title", sa.String(length=255), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column(
            "impact", sa.String(length=16), nullable=False, server_default="WARNING"
        ),
        sa.Column(
            "priority", sa.String(length=16), nullable=False, server_default="MEDIUM"
        ),
        sa.Column(
            "status", sa.String(length=32), nullable=False, server_default="OPEN"
        ),
        sa.Column("assigned_to_user_id", sa.Integer(), nullable=True),
        sa.Column("assigned_to_name", sa.String(length=255), nullable=True),
        sa.Column("due_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("recommended_action", sa.Text(), nullable=False),
        sa.Column("source_snapshot", sa.JSON(), nullable=True),
        sa.Column("resolution_note", sa.Text(), nullable=True),
        sa.Column("resolved_by_user_id", sa.Integer(), nullable=True),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.CheckConstraint(
            "length(fingerprint) = 64",
            name="ck_operational_exceptions_fingerprint",
        ),
        sa.CheckConstraint(
            "source_type IN ('RECONCILIATION','PREFLIGHT','MANUAL')",
            name="ck_operational_exceptions_source_type",
        ),
        sa.CheckConstraint(
            "impact IN ('INFO','WARNING','BLOCKING')",
            name="ck_operational_exceptions_impact",
        ),
        sa.CheckConstraint(
            "priority IN ('LOW','MEDIUM','HIGH','CRITICAL')",
            name="ck_operational_exceptions_priority",
        ),
        sa.CheckConstraint(
            "status IN ('OPEN','IN_PROGRESS','RESOLVED','DISMISSED')",
            name="ck_operational_exceptions_status",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_operational_exceptions"),
        sa.UniqueConstraint("public_id", name="uq_operational_exceptions_public_id"),
        sa.UniqueConstraint(
            "organization_id",
            "fingerprint",
            name="uq_operational_exceptions_tenant_fingerprint",
        ),
    )
    op.create_index(
        "ix_operational_exceptions_organization_id",
        TABLE,
        ["organization_id"],
    )
    op.create_index(
        "ix_operational_exceptions_tenant_status_priority_due",
        TABLE,
        ["organization_id", "status", "priority", "due_at"],
    )
    op.create_index(
        "ix_operational_exceptions_tenant_operation",
        TABLE,
        ["organization_id", "operation_reference"],
    )
    op.create_index(
        "ix_operational_exceptions_tenant_assignee_status",
        TABLE,
        ["organization_id", "assigned_to_user_id", "status"],
    )
    op.create_index(
        "ix_operational_exceptions_tenant_source",
        TABLE,
        ["organization_id", "source_type", "source_reference"],
    )

    _enable_rls()
    _grant_runtime()


def downgrade() -> None:
    sequence = f"{TABLE}_id_seq"
    op.execute(
        f"REVOKE USAGE, SELECT ON SEQUENCE public.{sequence} FROM {RUNTIME_ROLE}"
    )
    op.execute(
        f"REVOKE SELECT, INSERT, UPDATE ON TABLE public.{TABLE} FROM {RUNTIME_ROLE}"
    )
    for action in ("update", "insert", "select"):
        op.execute(
            f"DROP POLICY IF EXISTS {TABLE}_tenant_{action} ON public.{TABLE}"
        )
    op.execute(f"ALTER TABLE public.{TABLE} NO FORCE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE public.{TABLE} DISABLE ROW LEVEL SECURITY")
    op.drop_index("ix_operational_exceptions_tenant_source", table_name=TABLE)
    op.drop_index(
        "ix_operational_exceptions_tenant_assignee_status", table_name=TABLE
    )
    op.drop_index("ix_operational_exceptions_tenant_operation", table_name=TABLE)
    op.drop_index(
        "ix_operational_exceptions_tenant_status_priority_due", table_name=TABLE
    )
    op.drop_index("ix_operational_exceptions_organization_id", table_name=TABLE)
    op.drop_table(TABLE)
