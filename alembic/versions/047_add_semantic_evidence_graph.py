"""Add persistent semantic evidence graph foundations.

Revision ID: 047_lacey_semantic_graph
Revises: 046_lacey_multilingual_spans
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "047_lacey_semantic_graph"
down_revision: Union[str, Sequence[str], None] = "046_lacey_multilingual_spans"
branch_labels = None
depends_on = None

TABLES = (
    "semantic_evidence_nodes",
    "semantic_snapshot_nodes",
    "semantic_evidence_edges",
)
RUNTIME_ROLE = "litoral_trace_app"
WORKER_EXECUTOR_ROLE = "litoral_trace_worker_executor"
TENANT_CONTEXT_SQL = "NULLIF(current_setting('app.current_organization_id', true), '')::integer"


def _rls(table: str) -> None:
    predicate = f"organization_id = {TENANT_CONTEXT_SQL}"
    op.execute(f"ALTER TABLE public.{table} ENABLE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE public.{table} FORCE ROW LEVEL SECURITY")
    for action, clause in (("select", "USING"), ("insert", "WITH CHECK"), ("update", "USING"), ("delete", "USING")):
        suffix = f"{clause} ({predicate})"
        if action == "update":
            suffix += f" WITH CHECK ({predicate})"
        op.execute(f"CREATE POLICY {table}_tenant_{action} ON public.{table} FOR {action.upper()} {suffix}")


def _grant_runtime(table: str) -> None:
    op.execute(f"REVOKE ALL PRIVILEGES ON TABLE public.{table} FROM PUBLIC")
    op.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.{table} TO {RUNTIME_ROLE}")
    op.execute(f"REVOKE ALL PRIVILEGES ON TABLE public.{table} FROM {WORKER_EXECUTOR_ROLE}")
    op.execute(f"REVOKE ALL PRIVILEGES ON SEQUENCE public.{table}_id_seq FROM PUBLIC")
    op.execute(f"GRANT USAGE, SELECT ON SEQUENCE public.{table}_id_seq TO {RUNTIME_ROLE}")
    op.execute(f"REVOKE ALL PRIVILEGES ON SEQUENCE public.{table}_id_seq FROM {WORKER_EXECUTOR_ROLE}")


def upgrade() -> None:
    op.create_table(
        "semantic_evidence_nodes",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("public_id", sa.Uuid(), nullable=False),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("assurance_document_id", sa.Integer(), nullable=False),
        sa.Column("extraction_run_id", sa.Integer(), nullable=False),
        sa.Column("source_span_id", sa.Integer(), nullable=False),
        sa.Column("target_field", sa.String(100), nullable=False),
        sa.Column("semantic_role", sa.String(100), nullable=False),
        sa.Column("scope", sa.String(64), nullable=False),
        sa.Column("local_entity_key", sa.String(512), nullable=True),
        sa.Column("original_value", sa.Text(), nullable=False),
        sa.Column("normalized_value", sa.Text(), nullable=True),
        sa.Column("evidence_class", sa.String(16), nullable=False),
        sa.Column("document_type", sa.String(64), nullable=False),
        sa.Column("extraction_confidence", sa.Float(), nullable=False, server_default="0"),
        sa.Column("authority_score", sa.Float(), nullable=False, server_default="0"),
        sa.Column("candidate_score", sa.Float(), nullable=False, server_default="0"),
        sa.Column("fingerprint", sa.String(64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.ForeignKeyConstraint(
            ["assurance_document_id", "organization_id"],
            ["assurance_documents.id", "assurance_documents.organization_id"],
            name="fk_semantic_evidence_nodes_assurance_tenant",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["extraction_run_id", "organization_id"],
            ["document_extraction_runs.id", "document_extraction_runs.organization_id"],
            name="fk_semantic_evidence_nodes_extraction_run_tenant",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["source_span_id", "organization_id"],
            ["document_text_spans.id", "document_text_spans.organization_id"],
            name="fk_semantic_evidence_nodes_span_tenant",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint("id", "organization_id", name="uq_semantic_evidence_nodes_id_org"),
        sa.UniqueConstraint("public_id", name="uq_semantic_evidence_nodes_public_id"),
        sa.UniqueConstraint("organization_id", "fingerprint", name="uq_semantic_evidence_nodes_fingerprint"),
        sa.CheckConstraint(
            "evidence_class IN ('EXPLICIT','DERIVED','INFERRED')",
            name="ck_semantic_evidence_nodes_class",
        ),
        sa.CheckConstraint(
            "extraction_confidence >= 0 AND extraction_confidence <= 1",
            name="ck_semantic_evidence_nodes_confidence",
        ),
    )
    op.create_index("ix_semantic_evidence_nodes_org_document", "semantic_evidence_nodes", ["organization_id", "assurance_document_id"])
    op.create_index("ix_semantic_evidence_nodes_org_field", "semantic_evidence_nodes", ["organization_id", "target_field"])
    op.create_index("ix_semantic_evidence_nodes_org_local_entity", "semantic_evidence_nodes", ["organization_id", "local_entity_key"])

    op.create_table(
        "semantic_snapshot_nodes",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("snapshot_id", sa.Integer(), nullable=False),
        sa.Column("evidence_node_id", sa.Integer(), nullable=False),
        sa.Column("canonical_entity_id", sa.String(128), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.ForeignKeyConstraint(
            ["snapshot_id", "organization_id"],
            ["us_lacey_evidence_snapshots.id", "us_lacey_evidence_snapshots.organization_id"],
            name="fk_semantic_snapshot_nodes_snapshot_tenant",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["evidence_node_id", "organization_id"],
            ["semantic_evidence_nodes.id", "semantic_evidence_nodes.organization_id"],
            name="fk_semantic_snapshot_nodes_node_tenant",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint("id", "organization_id", name="uq_semantic_snapshot_nodes_id_org"),
        sa.UniqueConstraint(
            "organization_id", "snapshot_id", "evidence_node_id",
            name="uq_semantic_snapshot_nodes_member",
        ),
    )
    op.create_index("ix_semantic_snapshot_nodes_org_snapshot", "semantic_snapshot_nodes", ["organization_id", "snapshot_id"])
    op.create_index("ix_semantic_snapshot_nodes_org_canonical", "semantic_snapshot_nodes", ["organization_id", "snapshot_id", "canonical_entity_id"])

    op.create_table(
        "semantic_evidence_edges",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("snapshot_id", sa.Integer(), nullable=False),
        sa.Column("from_node_id", sa.Integer(), nullable=False),
        sa.Column("to_node_id", sa.Integer(), nullable=False),
        sa.Column("relation_type", sa.String(32), nullable=False),
        sa.Column("confidence", sa.Float(), nullable=False, server_default="1"),
        sa.Column("reason_code", sa.String(100), nullable=True),
        sa.Column("metadata_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.ForeignKeyConstraint(
            ["snapshot_id", "organization_id"],
            ["us_lacey_evidence_snapshots.id", "us_lacey_evidence_snapshots.organization_id"],
            name="fk_semantic_evidence_edges_snapshot_tenant",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["from_node_id", "organization_id"],
            ["semantic_evidence_nodes.id", "semantic_evidence_nodes.organization_id"],
            name="fk_semantic_evidence_edges_from_node_tenant",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["to_node_id", "organization_id"],
            ["semantic_evidence_nodes.id", "semantic_evidence_nodes.organization_id"],
            name="fk_semantic_evidence_edges_to_node_tenant",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint("id", "organization_id", name="uq_semantic_evidence_edges_id_org"),
        sa.UniqueConstraint(
            "organization_id", "snapshot_id", "from_node_id", "to_node_id", "relation_type",
            name="uq_semantic_evidence_edges_relation",
        ),
        sa.CheckConstraint(
            "relation_type IN ('CORROBORATES','CONTRADICTS','SAME_ENTITY_AS','SUPERSEDES','OUT_OF_SCOPE','DERIVED_FROM')",
            name="ck_semantic_evidence_edges_relation_type",
        ),
        sa.CheckConstraint("confidence >= 0 AND confidence <= 1", name="ck_semantic_evidence_edges_confidence"),
    )
    op.create_index("ix_semantic_evidence_edges_org_snapshot", "semantic_evidence_edges", ["organization_id", "snapshot_id"])
    op.create_index("ix_semantic_evidence_edges_org_from", "semantic_evidence_edges", ["organization_id", "from_node_id"])
    op.create_index("ix_semantic_evidence_edges_org_to", "semantic_evidence_edges", ["organization_id", "to_node_id"])

    for table in TABLES:
        _rls(table)
        _grant_runtime(table)


def downgrade() -> None:
    for table in reversed(TABLES):
        for action in ("delete", "update", "insert", "select"):
            op.execute(f"DROP POLICY IF EXISTS {table}_tenant_{action} ON public.{table}")
        op.drop_table(table)
