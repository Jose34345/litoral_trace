"""Persist deterministic regulatory assessments for exact U.S. Lacey source sets.

Revision ID: 050_lacey_regulatory_assessment_snapshots
Revises: 049_lacey_product_intelligence_snapshots
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "050_lacey_regulatory_assessment_snapshots"
down_revision = "049_lacey_product_intelligence_snapshots"
branch_labels = None
depends_on = None

RUNTIME_ROLE = "litoral_trace_app"
WORKER_EXECUTOR_ROLE = "litoral_trace_worker_executor"
TENANT_CONTEXT_SQL = "NULLIF(current_setting('app.current_organization_id', true), '')::integer"
TABLE = "us_lacey_regulatory_assessment_snapshots"


def _secure() -> None:
    predicate = f"organization_id = {TENANT_CONTEXT_SQL}"
    op.execute(f"ALTER TABLE public.{TABLE} ENABLE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE public.{TABLE} FORCE ROW LEVEL SECURITY")
    for action, clause in (
        ("select", "USING"),
        ("insert", "WITH CHECK"),
        ("update", "USING"),
        ("delete", "USING"),
    ):
        suffix = f"{clause} ({predicate})"
        if action == "update":
            suffix += f" WITH CHECK ({predicate})"
        op.execute(
            f"CREATE POLICY {TABLE}_tenant_{action} ON public.{TABLE} "
            f"FOR {action.upper()} {suffix}"
        )
    op.execute(f"REVOKE ALL PRIVILEGES ON TABLE public.{TABLE} FROM PUBLIC")
    op.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.{TABLE} TO {RUNTIME_ROLE}")
    op.execute(f"REVOKE ALL PRIVILEGES ON SEQUENCE public.{TABLE}_id_seq FROM PUBLIC")
    op.execute(f"GRANT USAGE, SELECT ON SEQUENCE public.{TABLE}_id_seq TO {RUNTIME_ROLE}")
    # Queue workers continue through the tenant-scoped application service seam.
    op.execute(f"REVOKE ALL PRIVILEGES ON TABLE public.{TABLE} FROM {WORKER_EXECUTOR_ROLE}")
    op.execute(f"REVOKE ALL PRIVILEGES ON SEQUENCE public.{TABLE}_id_seq FROM {WORKER_EXECUTOR_ROLE}")


def upgrade() -> None:
    op.create_table(
        TABLE,
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("public_id", sa.Uuid(), nullable=False),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("operation_id", sa.Integer(), nullable=False),
        sa.Column("source_set_revision_id", sa.Integer(), nullable=False),
        sa.Column("generation", sa.Integer(), nullable=False),
        sa.Column("source_set_fingerprint", sa.String(64), nullable=False),
        sa.Column("ruleset_version", sa.String(96), nullable=False),
        sa.Column("input_fingerprint", sa.String(64), nullable=False),
        sa.Column("status", sa.String(16), nullable=False, server_default="CURRENT"),
        sa.Column("assessment_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("indeterminate_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("payload_json", sa.JSON(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column("finalized_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["operation_id", "organization_id"],
            ["us_lacey_operations.id", "us_lacey_operations.organization_id"],
            name="fk_lacey_reg_assessment_operation_tenant",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["source_set_revision_id", "organization_id"],
            ["us_lacey_source_set_revisions.id", "us_lacey_source_set_revisions.organization_id"],
            name="fk_lacey_reg_assessment_revision_tenant",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint("id", "organization_id", name="uq_lacey_reg_assessment_id_org"),
        sa.UniqueConstraint("public_id", name="uq_lacey_reg_assessment_public_id"),
        sa.UniqueConstraint(
            "organization_id",
            "source_set_revision_id",
            "ruleset_version",
            name="uq_lacey_reg_assessment_revision_ruleset",
        ),
        sa.CheckConstraint("generation > 0", name="ck_lacey_reg_assessment_generation"),
        sa.CheckConstraint("status IN ('CURRENT','STALE')", name="ck_lacey_reg_assessment_status"),
        sa.CheckConstraint("assessment_count >= 0", name="ck_lacey_reg_assessment_count"),
        sa.CheckConstraint("indeterminate_count >= 0", name="ck_lacey_reg_indeterminate_count"),
        sa.CheckConstraint(
            "indeterminate_count <= assessment_count",
            name="ck_lacey_reg_indeterminate_le_assessment",
        ),
    )
    op.create_index(
        "ix_lacey_reg_assessment_org_operation",
        TABLE,
        ["organization_id", "operation_id"],
    )
    op.create_index(
        "ix_lacey_reg_assessment_org_status",
        TABLE,
        ["organization_id", "status"],
    )
    _secure()


def downgrade() -> None:
    for action in ("delete", "update", "insert", "select"):
        op.execute(f"DROP POLICY IF EXISTS {TABLE}_tenant_{action} ON public.{TABLE}")
    op.drop_table(TABLE)
