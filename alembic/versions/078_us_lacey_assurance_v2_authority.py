"""Assurance V2 append-only decision, source and identity authority overlay.

Revision ID: 078_us_lacey_assurance_v2_authority
Revises: 077_us_lacey_outreach_human_signals
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "078_us_lacey_assurance_v2_authority"
down_revision = "077_us_lacey_outreach_human_signals"
branch_labels = None
depends_on = None

RUNTIME = "litoral_trace_app"
GUC = "NULLIF(current_setting('app.current_organization_id', true), '')::integer"
DECISIONS = "us_lacey_assurance_v2_decisions"
SOURCES = "us_lacey_assurance_v2_decision_sources"
MEMORY = "us_lacey_assurance_v2_memory_links"
IDENTITY = "us_lacey_assurance_v2_identity_events"
TABLES = (DECISIONS, SOURCES, MEMORY, IDENTITY)


def _fk(local: list[str], remote: list[str], **kw):
    return sa.ForeignKeyConstraint(local, remote, **kw)


def _secure_append_only(table: str) -> None:
    predicate = f"organization_id = {GUC}"
    op.execute(f"ALTER TABLE public.{table} ENABLE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE public.{table} FORCE ROW LEVEL SECURITY")
    op.execute(
        f"CREATE POLICY {table}_tenant_select ON public.{table} "
        f"FOR SELECT TO {RUNTIME} USING ({predicate})"
    )
    op.execute(
        f"CREATE POLICY {table}_tenant_insert ON public.{table} "
        f"FOR INSERT TO {RUNTIME} WITH CHECK ({predicate})"
    )
    op.execute(
        f"REVOKE ALL PRIVILEGES ON TABLE public.{table} "
        "FROM PUBLIC, anon, authenticated"
    )
    op.execute(f"GRANT SELECT, INSERT ON TABLE public.{table} TO {RUNTIME}")
    op.execute(f"REVOKE UPDATE, DELETE ON TABLE public.{table} FROM {RUNTIME}")
    op.execute(
        f"REVOKE ALL PRIVILEGES ON SEQUENCE public.{table}_id_seq "
        "FROM PUBLIC, anon, authenticated"
    )
    op.execute(f"GRANT USAGE, SELECT ON SEQUENCE public.{table}_id_seq TO {RUNTIME}")


def upgrade() -> None:
    # Tenant-bound authenticated actor FK, without assuming a user id implies tenant.
    op.create_unique_constraint("uq_users_id_org_assurance_v2", "users", ["id", "organization_id"])
    op.create_table(
        DECISIONS,
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("public_id", sa.Uuid(), nullable=False),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("operation_id", sa.Integer(), nullable=False),
        sa.Column("source_set_revision_id", sa.Integer(), nullable=False),
        sa.Column("source_set_fingerprint", sa.String(64), nullable=False),
        sa.Column("generation", sa.Integer(), nullable=False),
        sa.Column("actor_user_id", sa.Integer(), nullable=False),
        sa.Column("action", sa.String(16), nullable=False),
        sa.Column("field_name", sa.String(100), nullable=False),
        sa.Column("line_reference", sa.String(100)),
        sa.Column("selected_value", sa.Text()),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("context_json", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("supersedes_decision_id", sa.Integer()),
        sa.Column("audit_event_id", sa.Integer(), nullable=False),
        sa.Column("idempotency_key", sa.String(160), nullable=False),
        sa.Column("decided_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        _fk(["operation_id", "organization_id"], ["us_lacey_operations.id", "us_lacey_operations.organization_id"], ondelete="CASCADE"),
        _fk(["source_set_revision_id", "organization_id"], ["us_lacey_source_set_revisions.id", "us_lacey_source_set_revisions.organization_id"], ondelete="CASCADE"),
        _fk(["audit_event_id", "organization_id"], ["us_lacey_operation_events.id", "us_lacey_operation_events.organization_id"], ondelete="CASCADE"),
        _fk(["actor_user_id", "organization_id"], ["users.id", "users.organization_id"], ondelete="CASCADE"),
        _fk(["supersedes_decision_id", "organization_id"], [f"{DECISIONS}.id", f"{DECISIONS}.organization_id"], ondelete="CASCADE"),
        sa.UniqueConstraint("id", "organization_id", name="uq_assurance_v2_decision_tenant"),
        sa.UniqueConstraint("public_id", name="uq_assurance_v2_decision_public"),
        sa.UniqueConstraint("organization_id", "operation_id", "idempotency_key", name="uq_assurance_v2_decision_idempotent"),
        sa.UniqueConstraint("organization_id", "supersedes_decision_id", name="uq_assurance_v2_decision_one_successor"),
        sa.CheckConstraint("action IN ('ACCEPT','REJECT','CORRECT','SUPERSEDE')", name="ck_assurance_v2_decision_action"),
        sa.CheckConstraint("generation > 0", name="ck_assurance_v2_decision_generation"),
        sa.CheckConstraint("length(trim(reason)) > 0", name="ck_assurance_v2_decision_reason"),
        sa.CheckConstraint("(action IN ('REJECT','SUPERSEDE')) OR (selected_value IS NOT NULL AND length(trim(selected_value)) > 0)", name="ck_assurance_v2_decision_value"),
    )
    op.create_index("ix_assurance_v2_decision_case", DECISIONS, ["organization_id", "operation_id", "source_set_revision_id"])

    op.create_table(
        SOURCES,
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("decision_id", sa.Integer(), nullable=False),
        sa.Column("assurance_document_id", sa.Integer(), nullable=False),
        sa.Column("operation_document_id", sa.Integer(), nullable=False),
        sa.Column("document_version_number", sa.Integer(), nullable=False),
        sa.Column("semantic_evidence_node_id", sa.Integer()),
        sa.Column("source_operation_field_id", sa.Integer()),
        sa.Column("source_span_id", sa.Integer()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        _fk(["decision_id", "organization_id"], [f"{DECISIONS}.id", f"{DECISIONS}.organization_id"], ondelete="CASCADE"),
        _fk(["assurance_document_id", "organization_id"], ["assurance_documents.id", "assurance_documents.organization_id"], ondelete="CASCADE"),
        _fk(["operation_document_id", "organization_id"], ["us_lacey_operation_documents.id", "us_lacey_operation_documents.organization_id"], ondelete="CASCADE"),
        _fk(["semantic_evidence_node_id", "organization_id"], ["semantic_evidence_nodes.id", "semantic_evidence_nodes.organization_id"], ondelete="CASCADE"),
        _fk(["source_operation_field_id", "organization_id"], ["us_lacey_operation_fields.id", "us_lacey_operation_fields.organization_id"], ondelete="CASCADE"),
        _fk(["source_span_id", "organization_id"], ["document_text_spans.id", "document_text_spans.organization_id"], ondelete="CASCADE"),
        sa.UniqueConstraint("id", "organization_id", name="uq_assurance_v2_decision_source_tenant"),
    )
    op.create_index("ix_assurance_v2_decision_source_decision", SOURCES, ["organization_id", "decision_id"])

    op.create_table(
        MEMORY,
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("public_id", sa.Uuid(), nullable=False),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("decision_id", sa.Integer(), nullable=False),
        sa.Column("evidence_claim_id", sa.Integer(), nullable=False),
        sa.Column("supplier_product_id", sa.Integer(), nullable=False),
        sa.Column("assurance_document_id", sa.Integer(), nullable=False),
        sa.Column("source_set_revision_id", sa.Integer(), nullable=False),
        sa.Column("context_json", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        _fk(["decision_id", "organization_id"], [f"{DECISIONS}.id", f"{DECISIONS}.organization_id"], ondelete="CASCADE"),
        _fk(["evidence_claim_id", "organization_id"], ["us_lacey_evidence_claim.id", "us_lacey_evidence_claim.organization_id"], ondelete="CASCADE"),
        _fk(["supplier_product_id", "organization_id"], ["us_lacey_supplier_product.id", "us_lacey_supplier_product.organization_id"], ondelete="CASCADE"),
        _fk(["assurance_document_id", "organization_id"], ["assurance_documents.id", "assurance_documents.organization_id"], ondelete="CASCADE"),
        _fk(["source_set_revision_id", "organization_id"], ["us_lacey_source_set_revisions.id", "us_lacey_source_set_revisions.organization_id"], ondelete="CASCADE"),
        sa.UniqueConstraint("public_id", name="uq_assurance_v2_memory_public"),
        sa.UniqueConstraint("id", "organization_id", name="uq_assurance_v2_memory_tenant"),
        sa.UniqueConstraint("organization_id", "decision_id", "evidence_claim_id", name="uq_assurance_v2_memory_once"),
    )
    op.create_index("ix_assurance_v2_memory_product", MEMORY, ["organization_id", "supplier_product_id"])

    op.create_table(
        IDENTITY,
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("public_id", sa.Uuid(), nullable=False),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("operation_id", sa.Integer(), nullable=False),
        sa.Column("actor_user_id", sa.Integer(), nullable=False),
        sa.Column("entity_type", sa.String(24), nullable=False),
        sa.Column("source_supplier_id", sa.Integer()),
        sa.Column("target_supplier_id", sa.Integer()),
        sa.Column("source_product_id", sa.Integer()),
        sa.Column("target_product_id", sa.Integer()),
        sa.Column("alias_value", sa.String(512)),
        sa.Column("action", sa.String(16), nullable=False),
        sa.Column("reverses_event_id", sa.Integer()),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("audit_event_id", sa.Integer(), nullable=False),
        sa.Column("idempotency_key", sa.String(160), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        _fk(["operation_id", "organization_id"], ["us_lacey_operations.id", "us_lacey_operations.organization_id"], ondelete="CASCADE"),
        _fk(["audit_event_id", "organization_id"], ["us_lacey_operation_events.id", "us_lacey_operation_events.organization_id"], ondelete="CASCADE"),
        _fk(["actor_user_id", "organization_id"], ["users.id", "users.organization_id"], ondelete="CASCADE"),
        _fk(["source_supplier_id", "organization_id"], ["us_lacey_supplier.id", "us_lacey_supplier.organization_id"], ondelete="CASCADE"),
        _fk(["target_supplier_id", "organization_id"], ["us_lacey_supplier.id", "us_lacey_supplier.organization_id"], ondelete="CASCADE"),
        _fk(["source_product_id", "organization_id"], ["us_lacey_supplier_product.id", "us_lacey_supplier_product.organization_id"], ondelete="CASCADE"),
        _fk(["target_product_id", "organization_id"], ["us_lacey_supplier_product.id", "us_lacey_supplier_product.organization_id"], ondelete="CASCADE"),
        _fk(["reverses_event_id", "organization_id"], [f"{IDENTITY}.id", f"{IDENTITY}.organization_id"], ondelete="CASCADE"),
        sa.UniqueConstraint("public_id", name="uq_assurance_v2_identity_public"),
        sa.UniqueConstraint("id", "organization_id", name="uq_assurance_v2_identity_tenant"),
        sa.UniqueConstraint("organization_id", "operation_id", "idempotency_key", name="uq_assurance_v2_identity_idempotent"),
        sa.CheckConstraint("action IN ('ALIAS_ADD','ALIAS_REMOVE','MERGE','UNMERGE')", name="ck_assurance_v2_identity_action"),
        sa.CheckConstraint("entity_type IN ('SUPPLIER','SUPPLIER_PRODUCT')", name="ck_assurance_v2_identity_type"),
        sa.CheckConstraint("length(trim(reason)) > 0", name="ck_assurance_v2_identity_reason"),
    )
    op.create_index("ix_assurance_v2_identity_case", IDENTITY, ["organization_id", "entity_type", "source_supplier_id", "source_product_id"])

    for table in TABLES:
        _secure_append_only(table)


def downgrade() -> None:
    for table in reversed(TABLES):
        for action in ("insert", "select"):
            op.execute(f"DROP POLICY IF EXISTS {table}_tenant_{action} ON public.{table}")
        op.drop_table(table)
    op.drop_constraint("uq_users_id_org_assurance_v2", "users", type_="unique")
