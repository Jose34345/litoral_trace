"""Persist explicitly sealed U.S. Lacey source-set generations.

Revision ID: 048_lacey_source_set_revisions
Revises: 047_lacey_semantic_graph
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "048_lacey_source_set_revisions"
down_revision = "047_lacey_semantic_graph"
branch_labels = None
depends_on = None

RUNTIME_ROLE = "litoral_trace_app"
TENANT_CONTEXT_SQL = "NULLIF(current_setting('app.current_organization_id', true), '')::integer"


def _secure(table: str) -> None:
    predicate = f"organization_id = {TENANT_CONTEXT_SQL}"
    op.execute(f"ALTER TABLE public.{table} ENABLE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE public.{table} FORCE ROW LEVEL SECURITY")
    for action, clause in (("select", "USING"), ("insert", "WITH CHECK"), ("update", "USING"), ("delete", "USING")):
        suffix = f"{clause} ({predicate})" + (f" WITH CHECK ({predicate})" if action == "update" else "")
        op.execute(f"CREATE POLICY {table}_tenant_{action} ON public.{table} FOR {action.upper()} {suffix}")
    op.execute(f"REVOKE ALL PRIVILEGES ON TABLE public.{table} FROM PUBLIC")
    op.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.{table} TO {RUNTIME_ROLE}")
    op.execute(f"REVOKE ALL PRIVILEGES ON SEQUENCE public.{table}_id_seq FROM PUBLIC")
    op.execute(f"GRANT USAGE, SELECT ON SEQUENCE public.{table}_id_seq TO {RUNTIME_ROLE}")


def upgrade() -> None:
    op.create_table(
        "us_lacey_source_set_revisions",
        sa.Column("id", sa.Integer(), primary_key=True), sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("operation_id", sa.Integer(), nullable=False), sa.Column("generation", sa.Integer(), nullable=False),
        sa.Column("source_set_fingerprint", sa.String(64), nullable=False), sa.Column("document_count", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False), sa.Column("is_current", sa.Boolean(), nullable=False),
        sa.Column("claimed_at", sa.DateTime(timezone=True)), sa.Column("finalized_at", sa.DateTime(timezone=True)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.ForeignKeyConstraint(["operation_id", "organization_id"], ["us_lacey_operations.id", "us_lacey_operations.organization_id"], ondelete="CASCADE"),
        sa.UniqueConstraint("id", "organization_id"), sa.UniqueConstraint("organization_id", "operation_id", "generation"),
        sa.CheckConstraint("generation > 0"), sa.CheckConstraint("document_count > 0"),
        sa.CheckConstraint("status IN ('OPEN','SEALED','FINALIZING','FINALIZED','SUPERSEDED')"),
    )
    op.create_index("ix_lacey_source_revision_org_operation_current", "us_lacey_source_set_revisions", ["organization_id", "operation_id", "is_current"])
    op.create_index("ix_lacey_source_revision_org_operation_fingerprint", "us_lacey_source_set_revisions", ["organization_id", "operation_id", "source_set_fingerprint"])
    op.create_table(
        "us_lacey_source_set_members",
        sa.Column("id", sa.Integer(), primary_key=True), sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("source_set_revision_id", sa.Integer(), nullable=False), sa.Column("operation_document_id", sa.Integer(), nullable=False), sa.Column("assurance_document_id", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.ForeignKeyConstraint(["source_set_revision_id", "organization_id"], ["us_lacey_source_set_revisions.id", "us_lacey_source_set_revisions.organization_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["operation_document_id", "organization_id"], ["us_lacey_operation_documents.id", "us_lacey_operation_documents.organization_id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["assurance_document_id", "organization_id"], ["assurance_documents.id", "assurance_documents.organization_id"], ondelete="RESTRICT"),
        sa.UniqueConstraint("id", "organization_id"), sa.UniqueConstraint("organization_id", "source_set_revision_id", "operation_document_id"),
    )
    op.create_index("ix_lacey_source_member_org_revision", "us_lacey_source_set_members", ["organization_id", "source_set_revision_id"])
    _secure("us_lacey_source_set_revisions")
    _secure("us_lacey_source_set_members")


def downgrade() -> None:
    for table in ("us_lacey_source_set_members", "us_lacey_source_set_revisions"):
        for action in ("delete", "update", "insert", "select"):
            op.execute(f"DROP POLICY IF EXISTS {table}_tenant_{action} ON public.{table}")
        op.drop_table(table)
