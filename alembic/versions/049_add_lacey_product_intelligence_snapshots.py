"""Persist Product Intelligence snapshots for exact U.S. Lacey source sets.

Revision ID: 049_lacey_product_intelligence_snapshots
Revises: 048_lacey_source_set_revisions
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "049_lacey_product_intelligence_snapshots"
down_revision = "048_lacey_source_set_revisions"
branch_labels = None
depends_on = None

RUNTIME_ROLE = "litoral_trace_app"
WORKER_EXECUTOR_ROLE = "litoral_trace_worker_executor"
TENANT_CONTEXT_SQL = "NULLIF(current_setting('app.current_organization_id', true), '')::integer"
TABLE = "us_lacey_product_intelligence_snapshots"


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
    # Queue workers receive access only through the existing application service seam.
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
        sa.Column("status", sa.String(24), nullable=False),
        sa.Column("document_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("eligible_document_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("recognized_bom_table_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("unique_sku_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("component_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("material_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("issue_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("payload_json", sa.JSON(), nullable=False),
        sa.Column("error_code", sa.String(64), nullable=True),
        sa.Column("error_message", sa.String(255), nullable=True),
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
            name="fk_lacey_pi_snapshot_operation_tenant",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["source_set_revision_id", "organization_id"],
            ["us_lacey_source_set_revisions.id", "us_lacey_source_set_revisions.organization_id"],
            name="fk_lacey_pi_snapshot_revision_tenant",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint("id", "organization_id", name="uq_lacey_pi_snapshot_id_org"),
        sa.UniqueConstraint("public_id", name="uq_lacey_pi_snapshot_public_id"),
        sa.UniqueConstraint(
            "organization_id",
            "source_set_revision_id",
            name="uq_lacey_pi_snapshot_revision",
        ),
        sa.CheckConstraint("generation > 0", name="ck_lacey_pi_snapshot_generation"),
        sa.CheckConstraint(
            "status IN ('READY','PARTIAL','FAILED','NOT_APPLICABLE','STALE')",
            name="ck_lacey_pi_snapshot_status",
        ),
        sa.CheckConstraint("document_count >= 0", name="ck_lacey_pi_snapshot_document_count"),
        sa.CheckConstraint("eligible_document_count >= 0", name="ck_lacey_pi_snapshot_eligible_count"),
        sa.CheckConstraint("recognized_bom_table_count >= 0", name="ck_lacey_pi_snapshot_table_count"),
        sa.CheckConstraint("unique_sku_count >= 0", name="ck_lacey_pi_snapshot_sku_count"),
        sa.CheckConstraint("component_count >= 0", name="ck_lacey_pi_snapshot_component_count"),
        sa.CheckConstraint("material_count >= 0", name="ck_lacey_pi_snapshot_material_count"),
        sa.CheckConstraint("issue_count >= 0", name="ck_lacey_pi_snapshot_issue_count"),
    )
    op.create_index(
        "ix_lacey_pi_snapshot_org_operation",
        TABLE,
        ["organization_id", "operation_id"],
    )
    op.create_index(
        "ix_lacey_pi_snapshot_org_status",
        TABLE,
        ["organization_id", "status"],
    )
    _secure()


def downgrade() -> None:
    for action in ("delete", "update", "insert", "select"):
        op.execute(f"DROP POLICY IF EXISTS {TABLE}_tenant_{action} ON public.{TABLE}")
    op.drop_table(TABLE)
