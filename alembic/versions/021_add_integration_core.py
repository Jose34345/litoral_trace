"""Add tenant-safe integration core and generic ERP staging tables.

Revision ID: 021_add_integration_core
Revises: 020_add_traceability_evidence_links
Create Date: 2026-08-22 20:15:00.000000
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "021_add_integration_core"
down_revision: Union[str, Sequence[str], None] = "020_add_traceability_evidence_links"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

RUNTIME_ROLE = "litoral_trace_app"
WORKER_EXECUTOR_ROLE = "litoral_trace_worker_executor"
TENANT_CONTEXT_SQL = "NULLIF(current_setting('app.current_organization_id', true), '')::integer"
TABLES = (
    "integration_connections",
    "integration_sync_runs",
    "external_entities",
    "external_references",
    "integration_documents",
    "integration_events",
)


def _create_rls(table: str) -> None:
    tenant_match = f"organization_id = {TENANT_CONTEXT_SQL}"
    op.execute(f"ALTER TABLE public.{table} ENABLE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE public.{table} FORCE ROW LEVEL SECURITY")
    op.execute(
        f"CREATE POLICY {table}_tenant_select ON public.{table} "
        f"FOR SELECT USING ({tenant_match})"
    )
    op.execute(
        f"CREATE POLICY {table}_tenant_insert ON public.{table} "
        f"FOR INSERT WITH CHECK ({tenant_match})"
    )
    if table != "integration_events":
        op.execute(
            f"CREATE POLICY {table}_tenant_update ON public.{table} "
            f"FOR UPDATE USING ({tenant_match}) WITH CHECK ({tenant_match})"
        )


def _drop_rls(table: str) -> None:
    for action in ("update", "insert", "select"):
        op.execute(f"DROP POLICY IF EXISTS {table}_tenant_{action} ON public.{table}")
    op.execute(f"ALTER TABLE public.{table} NO FORCE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE public.{table} DISABLE ROW LEVEL SECURITY")


def _grant_runtime_access(table: str) -> None:
    op.execute(f"REVOKE ALL PRIVILEGES ON TABLE public.{table} FROM PUBLIC")
    op.execute(f"REVOKE ALL PRIVILEGES ON SEQUENCE public.{table}_id_seq FROM PUBLIC")
    privileges = "SELECT, INSERT" if table == "integration_events" else "SELECT, INSERT, UPDATE"
    op.execute(f"GRANT {privileges} ON TABLE public.{table} TO {RUNTIME_ROLE}")
    op.execute(
        f"GRANT USAGE, SELECT ON SEQUENCE public.{table}_id_seq TO {RUNTIME_ROLE}"
    )
    op.execute(f"REVOKE ALL PRIVILEGES ON TABLE public.{table} FROM {WORKER_EXECUTOR_ROLE}")
    op.execute(
        f"REVOKE ALL PRIVILEGES ON SEQUENCE public.{table}_id_seq FROM {WORKER_EXECUTOR_ROLE}"
    )


def upgrade() -> None:
    op.create_table(
        "integration_connections",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.Uuid(), nullable=False, server_default=sa.text("gen_random_uuid()")),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=160), nullable=False),
        sa.Column("connector_type", sa.String(length=40), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False, server_default="ACTIVE"),
        sa.Column("secret_ref", sa.String(length=255), nullable=True),
        sa.Column("config_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.PrimaryKeyConstraint("id", name="pk_integration_connections"),
        sa.UniqueConstraint("id", "organization_id", name="uq_integration_connections_id_org"),
        sa.UniqueConstraint("public_id", name="uq_integration_connections_public_id"),
        sa.ForeignKeyConstraint(
            ["organization_id"], ["organizations.id"],
            name="fk_integration_connections_organization_id", ondelete="RESTRICT"
        ),
        sa.CheckConstraint("connector_type IN ('GENERIC_ERP')", name="ck_integration_connections_connector_type"),
        sa.CheckConstraint("status IN ('ACTIVE','DISABLED')", name="ck_integration_connections_status"),
    )
    op.create_index(
        "uq_integration_connections_tenant_name_ci",
        "integration_connections",
        ["organization_id", sa.text("lower(name)")],
        unique=True,
    )
    op.create_index(
        "ix_integration_connections_tenant_type_status",
        "integration_connections",
        ["organization_id", "connector_type", "status"],
    )

    op.create_table(
        "integration_sync_runs",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.Uuid(), nullable=False, server_default=sa.text("gen_random_uuid()")),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("connection_id", sa.Integer(), nullable=False),
        sa.Column("idempotency_key_hash", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False, server_default="RUNNING"),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("records_seen", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("records_created", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("records_updated", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("records_unchanged", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("records_conflict", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("error_summary", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id", name="pk_integration_sync_runs"),
        sa.UniqueConstraint("id", "organization_id", name="uq_integration_sync_runs_id_org"),
        sa.UniqueConstraint("public_id", name="uq_integration_sync_runs_public_id"),
        sa.UniqueConstraint("connection_id", "idempotency_key_hash", name="uq_integration_sync_runs_connection_idempotency"),
        sa.ForeignKeyConstraint(
            ["connection_id", "organization_id"],
            ["integration_connections.id", "integration_connections.organization_id"],
            name="fk_integration_sync_runs_connection_tenant", ondelete="RESTRICT",
        ),
        sa.CheckConstraint("status IN ('RUNNING','SUCCEEDED','PARTIAL','FAILED')", name="ck_integration_sync_runs_status"),
        sa.CheckConstraint(
            "records_seen >= 0 AND records_created >= 0 AND records_updated >= 0 "
            "AND records_unchanged >= 0 AND records_conflict >= 0",
            name="ck_integration_sync_runs_counts",
        ),
    )
    op.create_index("ix_integration_sync_runs_tenant_started", "integration_sync_runs", ["organization_id", "started_at"])
    op.create_index("ix_integration_sync_runs_tenant_connection_status", "integration_sync_runs", ["organization_id", "connection_id", "status"])

    op.create_table(
        "external_entities",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.Uuid(), nullable=False, server_default=sa.text("gen_random_uuid()")),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("connection_id", sa.Integer(), nullable=False),
        sa.Column("last_sync_run_id", sa.Integer(), nullable=True),
        sa.Column("entity_type", sa.String(length=24), nullable=False),
        sa.Column("external_id", sa.String(length=200), nullable=False),
        sa.Column("payload_hash", sa.String(length=64), nullable=False),
        sa.Column("payload_json", sa.JSON(), nullable=False),
        sa.Column("normalized_json", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False, server_default="STAGED"),
        sa.Column("source_updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("reconciled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("conflict_reason", sa.String(length=500), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.PrimaryKeyConstraint("id", name="pk_external_entities"),
        sa.UniqueConstraint("id", "organization_id", name="uq_external_entities_id_org"),
        sa.UniqueConstraint("public_id", name="uq_external_entities_public_id"),
        sa.UniqueConstraint("connection_id", "entity_type", "external_id", name="uq_external_entities_connection_type_external"),
        sa.ForeignKeyConstraint(
            ["connection_id", "organization_id"],
            ["integration_connections.id", "integration_connections.organization_id"],
            name="fk_external_entities_connection_tenant", ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["last_sync_run_id", "organization_id"],
            ["integration_sync_runs.id", "integration_sync_runs.organization_id"],
            name="fk_external_entities_sync_run_tenant", ondelete="RESTRICT",
        ),
        sa.CheckConstraint("entity_type IN ('SUPPLIER','PRODUCT','RECEIPT','SHIPMENT')", name="ck_external_entities_type"),
        sa.CheckConstraint("status IN ('STAGED','RECONCILED','CONFLICT','IGNORED')", name="ck_external_entities_status"),
        sa.CheckConstraint("length(payload_hash) = 64", name="ck_external_entities_payload_hash"),
    )
    op.create_index("ix_external_entities_tenant_status_type", "external_entities", ["organization_id", "status", "entity_type"])
    op.create_index("ix_external_entities_tenant_connection_updated", "external_entities", ["organization_id", "connection_id", "updated_at"])

    op.create_table(
        "external_references",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.Uuid(), nullable=False, server_default=sa.text("gen_random_uuid()")),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("external_entity_id", sa.Integer(), nullable=False),
        sa.Column("target_type", sa.String(length=32), nullable=False),
        sa.Column("target_reference", sa.String(length=200), nullable=False),
        sa.Column("reconciled_by_user_id", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.PrimaryKeyConstraint("id", name="pk_external_references"),
        sa.UniqueConstraint("id", "organization_id", name="uq_external_references_id_org"),
        sa.UniqueConstraint("public_id", name="uq_external_references_public_id"),
        sa.UniqueConstraint("external_entity_id", name="uq_external_references_entity"),
        sa.ForeignKeyConstraint(
            ["external_entity_id", "organization_id"],
            ["external_entities.id", "external_entities.organization_id"],
            name="fk_external_references_entity_tenant", ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["reconciled_by_user_id"], ["users.id"],
            name="fk_external_references_reconciled_by_user_id", ondelete="SET NULL",
        ),
        sa.CheckConstraint(
            "target_type IN ('LOTE','TRACEABILITY_BATCH','TRACEABILITY_EVENT','SHIPMENT')",
            name="ck_external_references_target_type",
        ),
    )
    op.create_index("ix_external_references_tenant_target", "external_references", ["organization_id", "target_type", "target_reference"])

    op.create_table(
        "integration_documents",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.Uuid(), nullable=False, server_default=sa.text("gen_random_uuid()")),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("external_entity_id", sa.Integer(), nullable=True),
        sa.Column("vault_document_id", sa.Integer(), nullable=False),
        sa.Column("document_type", sa.String(length=64), nullable=False),
        sa.Column("source_reference", sa.String(length=200), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.PrimaryKeyConstraint("id", name="pk_integration_documents"),
        sa.UniqueConstraint("id", "organization_id", name="uq_integration_documents_id_org"),
        sa.UniqueConstraint("public_id", name="uq_integration_documents_public_id"),
        sa.UniqueConstraint("vault_document_id", "external_entity_id", name="uq_integration_documents_vault_entity"),
        sa.ForeignKeyConstraint(
            ["external_entity_id", "organization_id"],
            ["external_entities.id", "external_entities.organization_id"],
            name="fk_integration_documents_entity_tenant", ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["vault_document_id", "organization_id"],
            ["vault_documents.id", "vault_documents.organization_id"],
            name="fk_integration_documents_vault_tenant", ondelete="RESTRICT",
        ),
    )
    op.create_index("ix_integration_documents_tenant_entity", "integration_documents", ["organization_id", "external_entity_id"])

    op.create_table(
        "integration_events",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.Uuid(), nullable=False, server_default=sa.text("gen_random_uuid()")),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("connection_id", sa.Integer(), nullable=True),
        sa.Column("sync_run_id", sa.Integer(), nullable=True),
        sa.Column("external_entity_id", sa.Integer(), nullable=True),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("metadata_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.PrimaryKeyConstraint("id", name="pk_integration_events"),
        sa.UniqueConstraint("id", "organization_id", name="uq_integration_events_id_org"),
        sa.UniqueConstraint("public_id", name="uq_integration_events_public_id"),
        sa.ForeignKeyConstraint(
            ["connection_id", "organization_id"],
            ["integration_connections.id", "integration_connections.organization_id"],
            name="fk_integration_events_connection_tenant", ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["sync_run_id", "organization_id"],
            ["integration_sync_runs.id", "integration_sync_runs.organization_id"],
            name="fk_integration_events_sync_run_tenant", ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["external_entity_id", "organization_id"],
            ["external_entities.id", "external_entities.organization_id"],
            name="fk_integration_events_entity_tenant", ondelete="RESTRICT",
        ),
    )
    op.create_index("ix_integration_events_tenant_created", "integration_events", ["organization_id", "created_at"])

    for table in TABLES:
        _create_rls(table)
        _grant_runtime_access(table)


def downgrade() -> None:
    for table in reversed(TABLES):
        op.execute(f"REVOKE ALL PRIVILEGES ON TABLE public.{table} FROM {RUNTIME_ROLE}")
        op.execute(f"REVOKE ALL PRIVILEGES ON SEQUENCE public.{table}_id_seq FROM {RUNTIME_ROLE}")
        _drop_rls(table)

    op.drop_index("ix_integration_events_tenant_created", table_name="integration_events")
    op.drop_table("integration_events")
    op.drop_index("ix_integration_documents_tenant_entity", table_name="integration_documents")
    op.drop_table("integration_documents")
    op.drop_index("ix_external_references_tenant_target", table_name="external_references")
    op.drop_table("external_references")
    op.drop_index("ix_external_entities_tenant_connection_updated", table_name="external_entities")
    op.drop_index("ix_external_entities_tenant_status_type", table_name="external_entities")
    op.drop_table("external_entities")
    op.drop_index("ix_integration_sync_runs_tenant_connection_status", table_name="integration_sync_runs")
    op.drop_index("ix_integration_sync_runs_tenant_started", table_name="integration_sync_runs")
    op.drop_table("integration_sync_runs")
    op.drop_index("ix_integration_connections_tenant_type_status", table_name="integration_connections")
    op.drop_index("uq_integration_connections_tenant_name_ci", table_name="integration_connections")
    op.drop_table("integration_connections")
