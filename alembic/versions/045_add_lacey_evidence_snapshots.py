"""Add immutable U.S. Lacey evidence snapshots.

Revision ID: 045_lacey_evidence_snapshots
Revises: 044_platform_admin_control_plane
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "045_lacey_evidence_snapshots"
down_revision: Union[str, Sequence[str], None] = "044_platform_admin_control_plane"
branch_labels = None
depends_on = None

TABLES = (
    "us_lacey_evidence_snapshots",
    "us_lacey_evidence_snapshot_documents",
)
RUNTIME_ROLE = "litoral_trace_app"
WORKER_EXECUTOR_ROLE = "litoral_trace_worker_executor"
TENANT_CONTEXT_SQL = "NULLIF(current_setting('app.current_organization_id', true), '')::integer"


def _rls(table: str) -> None:
    predicate = f"organization_id = {TENANT_CONTEXT_SQL}"
    op.execute(f"ALTER TABLE public.{table} ENABLE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE public.{table} FORCE ROW LEVEL SECURITY")
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
            f"CREATE POLICY {table}_tenant_{action} ON public.{table} "
            f"FOR {action.upper()} {suffix}"
        )


def _grant_runtime(table: str) -> None:
    op.execute(f"REVOKE ALL PRIVILEGES ON TABLE public.{table} FROM PUBLIC")
    op.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.{table} TO {RUNTIME_ROLE}")
    op.execute(f"REVOKE ALL PRIVILEGES ON TABLE public.{table} FROM {WORKER_EXECUTOR_ROLE}")
    op.execute(f"REVOKE ALL PRIVILEGES ON SEQUENCE public.{table}_id_seq FROM PUBLIC")
    op.execute(f"GRANT USAGE, SELECT ON SEQUENCE public.{table}_id_seq TO {RUNTIME_ROLE}")
    op.execute(f"REVOKE ALL PRIVILEGES ON SEQUENCE public.{table}_id_seq FROM {WORKER_EXECUTOR_ROLE}")


def upgrade() -> None:
    op.create_table(
        "us_lacey_evidence_snapshots",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("public_id", sa.Uuid(), nullable=False),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("operation_id", sa.Integer(), nullable=False),
        sa.Column("generation", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("source_set_fingerprint", sa.String(64), nullable=False),
        sa.Column("graph_version", sa.String(64), nullable=False),
        sa.Column("ontology_version", sa.String(64), nullable=False),
        sa.Column("translation_pipeline_version", sa.String(64), nullable=False),
        sa.Column("document_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("node_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("conflict_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("finalized_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["operation_id", "organization_id"],
            ["us_lacey_operations.id", "us_lacey_operations.organization_id"],
            name="fk_lacey_evidence_snapshots_operation_tenant",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint("id", "organization_id", name="uq_lacey_evidence_snapshots_id_org"),
        sa.UniqueConstraint("public_id", name="uq_lacey_evidence_snapshots_public_id"),
        sa.UniqueConstraint(
            "organization_id", "operation_id", "generation",
            name="uq_lacey_evidence_snapshots_generation",
        ),
        sa.UniqueConstraint(
            "organization_id", "operation_id", "source_set_fingerprint",
            name="uq_lacey_evidence_snapshots_fingerprint",
        ),
        sa.CheckConstraint("generation > 0", name="ck_lacey_evidence_snapshots_generation"),
        sa.CheckConstraint(
            "status IN ('BUILDING','CURRENT','SUPERSEDED','FAILED')",
            name="ck_lacey_evidence_snapshots_status",
        ),
        sa.CheckConstraint("document_count >= 0", name="ck_lacey_evidence_snapshots_document_count"),
        sa.CheckConstraint("node_count >= 0", name="ck_lacey_evidence_snapshots_node_count"),
        sa.CheckConstraint("conflict_count >= 0", name="ck_lacey_evidence_snapshots_conflict_count"),
    )
    op.create_index(
        "ix_lacey_evidence_snapshots_org_operation",
        "us_lacey_evidence_snapshots",
        ["organization_id", "operation_id"],
    )
    op.create_index(
        "ix_lacey_evidence_snapshots_org_status",
        "us_lacey_evidence_snapshots",
        ["organization_id", "status"],
    )

    op.create_table(
        "us_lacey_evidence_snapshot_documents",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("snapshot_id", sa.Integer(), nullable=False),
        sa.Column("operation_document_id", sa.Integer(), nullable=False),
        sa.Column("assurance_document_id", sa.Integer(), nullable=False),
        sa.Column("extraction_run_id", sa.Integer(), nullable=False),
        sa.Column("source_sha256", sa.String(64), nullable=False),
        sa.Column("document_role", sa.String(64), nullable=False, server_default="UNKNOWN"),
        sa.Column("processing_result", sa.String(16), nullable=False, server_default="SUCCEEDED"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.ForeignKeyConstraint(
            ["snapshot_id", "organization_id"],
            ["us_lacey_evidence_snapshots.id", "us_lacey_evidence_snapshots.organization_id"],
            name="fk_lacey_snapshot_documents_snapshot_tenant",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["operation_document_id", "organization_id"],
            ["us_lacey_operation_documents.id", "us_lacey_operation_documents.organization_id"],
            name="fk_lacey_snapshot_documents_operation_document_tenant",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["assurance_document_id", "organization_id"],
            ["assurance_documents.id", "assurance_documents.organization_id"],
            name="fk_lacey_snapshot_documents_assurance_tenant",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["extraction_run_id", "organization_id"],
            ["document_extraction_runs.id", "document_extraction_runs.organization_id"],
            name="fk_lacey_snapshot_documents_extraction_run_tenant",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint("id", "organization_id", name="uq_lacey_snapshot_documents_id_org"),
        sa.UniqueConstraint(
            "organization_id", "snapshot_id", "operation_document_id",
            name="uq_lacey_snapshot_documents_member",
        ),
        sa.CheckConstraint(
            "processing_result IN ('SUCCEEDED','FAILED','SKIPPED')",
            name="ck_lacey_snapshot_documents_result",
        ),
    )
    op.create_index(
        "ix_lacey_snapshot_documents_org_snapshot",
        "us_lacey_evidence_snapshot_documents",
        ["organization_id", "snapshot_id"],
    )
    op.create_index(
        "ix_lacey_snapshot_documents_org_assurance",
        "us_lacey_evidence_snapshot_documents",
        ["organization_id", "assurance_document_id"],
    )

    op.add_column(
        "us_lacey_operations",
        sa.Column("current_evidence_snapshot_id", sa.Integer(), nullable=True),
    )
    op.create_foreign_key(
        "fk_us_lacey_operations_current_snapshot_tenant",
        "us_lacey_operations",
        "us_lacey_evidence_snapshots",
        ["current_evidence_snapshot_id", "organization_id"],
        ["id", "organization_id"],
    )
    op.create_index(
        "ix_us_lacey_operations_current_snapshot",
        "us_lacey_operations",
        ["organization_id", "current_evidence_snapshot_id"],
    )

    for table in TABLES:
        _rls(table)
        _grant_runtime(table)


def downgrade() -> None:
    op.drop_index("ix_us_lacey_operations_current_snapshot", table_name="us_lacey_operations")
    op.drop_constraint(
        "fk_us_lacey_operations_current_snapshot_tenant",
        "us_lacey_operations",
        type_="foreignkey",
    )
    op.drop_column("us_lacey_operations", "current_evidence_snapshot_id")

    for table in reversed(TABLES):
        for action in ("delete", "update", "insert", "select"):
            op.execute(f"DROP POLICY IF EXISTS {table}_tenant_{action} ON public.{table}")
        op.drop_table(table)
