"""Add tenant-scoped Assurance reconciliation issues.

Revision ID: 031_assurance_reconciliation
Revises: 030_assurance_document_intelligence
Create Date: 2026-08-27 18:20:00.000000
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "031_assurance_reconciliation"
down_revision: Union[str, Sequence[str], None] = "030_assurance_document_intelligence"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

RUNTIME_ROLE = "litoral_trace_app"
WORKER_EXECUTOR_ROLE = "litoral_trace_worker_executor"
TENANT_CONTEXT_SQL = (
    "NULLIF(current_setting('app.current_organization_id', true), '')::integer"
)
TABLE = "reconciliation_issues"


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
        sa.Column("operation_reference", sa.String(length=255), nullable=False),
        sa.Column("fingerprint", sa.String(length=64), nullable=False),
        sa.Column("rule_code", sa.String(length=100), nullable=False),
        sa.Column(
            "severity", sa.String(length=16), nullable=False, server_default="WARNING"
        ),
        sa.Column(
            "status", sa.String(length=32), nullable=False, server_default="OPEN"
        ),
        sa.Column("field_name", sa.String(length=100), nullable=True),
        sa.Column("left_document_id", sa.Integer(), nullable=True),
        sa.Column("right_document_id", sa.Integer(), nullable=True),
        sa.Column("left_source", sa.String(length=512), nullable=False),
        sa.Column("right_source", sa.String(length=512), nullable=True),
        sa.Column("left_value", sa.Text(), nullable=True),
        sa.Column("right_value", sa.Text(), nullable=True),
        sa.Column("delta_numeric", sa.Numeric(24, 8), nullable=True),
        sa.Column("explanation", sa.Text(), nullable=False),
        sa.Column("evidence_json", sa.JSON(), nullable=True),
        sa.Column("resolution_justification", sa.Text(), nullable=True),
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
            name="ck_reconciliation_issues_fingerprint",
        ),
        sa.CheckConstraint(
            "severity IN ('INFO','WARNING','BLOCKING')",
            name="ck_reconciliation_issues_severity",
        ),
        sa.CheckConstraint(
            "status IN ('OPEN','ACCEPTED_WITH_JUSTIFICATION','RESOLVED')",
            name="ck_reconciliation_issues_status",
        ),
        sa.ForeignKeyConstraint(
            ["left_document_id", "organization_id"],
            ["assurance_documents.id", "assurance_documents.organization_id"],
            name="fk_reconciliation_issues_left_document_tenant",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["right_document_id", "organization_id"],
            ["assurance_documents.id", "assurance_documents.organization_id"],
            name="fk_reconciliation_issues_right_document_tenant",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_reconciliation_issues"),
        sa.UniqueConstraint("public_id", name="uq_reconciliation_issues_public_id"),
        sa.UniqueConstraint(
            "organization_id",
            "fingerprint",
            name="uq_reconciliation_issues_tenant_fingerprint",
        ),
    )
    op.create_index(
        "ix_reconciliation_issues_organization_id",
        TABLE,
        ["organization_id"],
    )
    op.create_index(
        "ix_reconciliation_issues_tenant_operation_status",
        TABLE,
        ["organization_id", "operation_reference", "status"],
    )
    op.create_index(
        "ix_reconciliation_issues_tenant_severity_status",
        TABLE,
        ["organization_id", "severity", "status"],
    )
    op.create_index(
        "ix_reconciliation_issues_tenant_rule",
        TABLE,
        ["organization_id", "rule_code"],
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
    op.drop_index("ix_reconciliation_issues_tenant_rule", table_name=TABLE)
    op.drop_index(
        "ix_reconciliation_issues_tenant_severity_status", table_name=TABLE
    )
    op.drop_index(
        "ix_reconciliation_issues_tenant_operation_status", table_name=TABLE
    )
    op.drop_index("ix_reconciliation_issues_organization_id", table_name=TABLE)
    op.drop_table(TABLE)
