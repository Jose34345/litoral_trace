"""Add tenant-scoped Assurance document intelligence schema.

Revision ID: 030_assurance_document_intelligence
Revises: 029_add_smart_import_profiles
Create Date: 2026-08-27 16:45:00.000000
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "030_assurance_document_intelligence"
down_revision: Union[str, Sequence[str], None] = "029_add_smart_import_profiles"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

RUNTIME_ROLE = "litoral_trace_app"
WORKER_EXECUTOR_ROLE = "litoral_trace_worker_executor"
TENANT_CONTEXT_SQL = (
    "NULLIF(current_setting('app.current_organization_id', true), '')::integer"
)

_TABLES = (
    "assurance_documents",
    "document_extraction_runs",
    "extracted_document_fields",
    "document_claims",
    "document_entity_links",
)


def _enable_rls(table: str) -> None:
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
    op.execute(
        f"CREATE POLICY {table}_tenant_update ON public.{table} "
        f"FOR UPDATE USING ({tenant_match}) WITH CHECK ({tenant_match})"
    )


def _disable_rls(table: str) -> None:
    for action in ("update", "insert", "select"):
        op.execute(
            f"DROP POLICY IF EXISTS {table}_tenant_{action} ON public.{table}"
        )
    op.execute(f"ALTER TABLE public.{table} NO FORCE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE public.{table} DISABLE ROW LEVEL SECURITY")


def _grant_runtime(table: str) -> None:
    op.execute(f"REVOKE ALL PRIVILEGES ON TABLE public.{table} FROM PUBLIC")
    op.execute(
        f"GRANT SELECT, INSERT, UPDATE ON TABLE public.{table} TO {RUNTIME_ROLE}"
    )
    op.execute(
        f"REVOKE ALL PRIVILEGES ON TABLE public.{table} FROM {WORKER_EXECUTOR_ROLE}"
    )
    sequence = f"{table}_id_seq"
    op.execute(f"REVOKE ALL PRIVILEGES ON SEQUENCE public.{sequence} FROM PUBLIC")
    op.execute(
        f"GRANT USAGE, SELECT ON SEQUENCE public.{sequence} TO {RUNTIME_ROLE}"
    )
    op.execute(
        f"REVOKE ALL PRIVILEGES ON SEQUENCE public.{sequence} FROM {WORKER_EXECUTOR_ROLE}"
    )


def _revoke_runtime(table: str) -> None:
    sequence = f"{table}_id_seq"
    op.execute(
        f"REVOKE USAGE, SELECT ON SEQUENCE public.{sequence} FROM {RUNTIME_ROLE}"
    )
    op.execute(
        f"REVOKE SELECT, INSERT, UPDATE ON TABLE public.{table} FROM {RUNTIME_ROLE}"
    )


def upgrade() -> None:
    op.create_table(
        "assurance_documents",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column(
            "public_id",
            sa.Uuid(),
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("vault_document_id", sa.Integer(), nullable=False),
        sa.Column(
            "semantic_document_type",
            sa.String(length=64),
            nullable=False,
            server_default="UNKNOWN",
        ),
        sa.Column("type_confidence", sa.Float(), nullable=False, server_default="0"),
        sa.Column(
            "processing_status",
            sa.String(length=32),
            nullable=False,
            server_default="UPLOADED",
        ),
        sa.Column("valid_from", sa.DateTime(timezone=True), nullable=True),
        sa.Column("valid_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("source_system", sa.String(length=255), nullable=True),
        sa.Column("last_error_code", sa.String(length=100), nullable=True),
        sa.Column("last_error_message", sa.String(length=512), nullable=True),
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
            "semantic_document_type IN ('INVOICE','DELIVERY_NOTE','FOREST_GUIDE',"
            "'PHYTOSANITARY_CERTIFICATE','CUSTOMS_DOCUMENT','SPREADSHEET','UNKNOWN')",
            name="ck_assurance_documents_semantic_type",
        ),
        sa.CheckConstraint(
            "processing_status IN ('UPLOADED','PROCESSING','EXTRACTED','NEEDS_REVIEW','FAILED')",
            name="ck_assurance_documents_processing_status",
        ),
        sa.CheckConstraint(
            "type_confidence >= 0 AND type_confidence <= 1",
            name="ck_assurance_documents_type_confidence",
        ),
        sa.ForeignKeyConstraint(
            ["vault_document_id", "organization_id"],
            ["vault_documents.id", "vault_documents.organization_id"],
            name="fk_assurance_documents_vault_tenant",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_assurance_documents"),
        sa.UniqueConstraint("public_id", name="uq_assurance_documents_public_id"),
        sa.UniqueConstraint(
            "id", "organization_id", name="uq_assurance_documents_id_organization_id"
        ),
        sa.UniqueConstraint(
            "organization_id",
            "vault_document_id",
            name="uq_assurance_documents_tenant_vault_document",
        ),
    )
    op.create_index(
        "ix_assurance_documents_organization_id",
        "assurance_documents",
        ["organization_id"],
    )
    op.create_index(
        "ix_assurance_documents_tenant_processing",
        "assurance_documents",
        ["organization_id", "processing_status"],
    )
    op.create_index(
        "ix_assurance_documents_tenant_semantic_type",
        "assurance_documents",
        ["organization_id", "semantic_document_type"],
    )
    op.create_index(
        "ix_assurance_documents_tenant_valid_until",
        "assurance_documents",
        ["organization_id", "valid_until"],
    )

    op.create_table(
        "document_extraction_runs",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("assurance_document_id", sa.Integer(), nullable=False),
        sa.Column("engine", sa.String(length=100), nullable=False),
        sa.Column("engine_version", sa.String(length=100), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="PENDING"),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("extraction_metadata", sa.JSON(), nullable=True),
        sa.Column("error_code", sa.String(length=100), nullable=True),
        sa.Column("error_detail", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.CheckConstraint(
            "status IN ('PENDING','RUNNING','SUCCEEDED','NEEDS_REVIEW','FAILED')",
            name="ck_document_extraction_runs_status",
        ),
        sa.ForeignKeyConstraint(
            ["assurance_document_id", "organization_id"],
            ["assurance_documents.id", "assurance_documents.organization_id"],
            name="fk_document_extraction_runs_document_tenant",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_document_extraction_runs"),
        sa.UniqueConstraint(
            "id", "organization_id", name="uq_document_extraction_runs_id_organization_id"
        ),
    )
    op.create_index(
        "ix_document_extraction_runs_organization_id",
        "document_extraction_runs",
        ["organization_id"],
    )
    op.create_index(
        "ix_document_extraction_runs_tenant_document",
        "document_extraction_runs",
        ["organization_id", "assurance_document_id"],
    )

    op.create_table(
        "extracted_document_fields",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("assurance_document_id", sa.Integer(), nullable=False),
        sa.Column("extraction_run_id", sa.Integer(), nullable=False),
        sa.Column("field_name", sa.String(length=255), nullable=False),
        sa.Column("original_value", sa.Text(), nullable=True),
        sa.Column("normalized_value", sa.Text(), nullable=True),
        sa.Column("value_type", sa.String(length=64), nullable=False, server_default="text"),
        sa.Column("confidence", sa.Float(), nullable=False, server_default="0"),
        sa.Column("confidence_level", sa.String(length=16), nullable=False, server_default="LOW"),
        sa.Column("source_page", sa.Integer(), nullable=True),
        sa.Column("source_locator", sa.Text(), nullable=True),
        sa.Column("auto_accepted", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("needs_review", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.CheckConstraint(
            "confidence >= 0 AND confidence <= 1",
            name="ck_extracted_document_fields_confidence",
        ),
        sa.CheckConstraint(
            "confidence_level IN ('HIGH','MEDIUM','LOW')",
            name="ck_extracted_document_fields_confidence_level",
        ),
        sa.CheckConstraint(
            "source_page IS NULL OR source_page > 0",
            name="ck_extracted_document_fields_source_page",
        ),
        sa.ForeignKeyConstraint(
            ["assurance_document_id", "organization_id"],
            ["assurance_documents.id", "assurance_documents.organization_id"],
            name="fk_extracted_fields_document_tenant",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["extraction_run_id", "organization_id"],
            ["document_extraction_runs.id", "document_extraction_runs.organization_id"],
            name="fk_extracted_fields_run_tenant",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_extracted_document_fields"),
    )
    op.create_index(
        "ix_extracted_document_fields_organization_id",
        "extracted_document_fields",
        ["organization_id"],
    )
    op.create_index(
        "ix_extracted_fields_tenant_review",
        "extracted_document_fields",
        ["organization_id", "needs_review"],
    )
    op.create_index(
        "ix_extracted_fields_tenant_field",
        "extracted_document_fields",
        ["organization_id", "field_name"],
    )

    op.create_table(
        "document_claims",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("assurance_document_id", sa.Integer(), nullable=False),
        sa.Column("claim_type", sa.String(length=100), nullable=False),
        sa.Column("issuer", sa.String(length=255), nullable=True),
        sa.Column("subject_type", sa.String(length=64), nullable=False),
        sa.Column("subject_reference", sa.String(length=255), nullable=False),
        sa.Column("statement", sa.Text(), nullable=False),
        sa.Column("scope_json", sa.JSON(), nullable=True),
        sa.Column("valid_from", sa.DateTime(timezone=True), nullable=True),
        sa.Column("valid_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("integrity_hash", sa.String(length=64), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.CheckConstraint(
            "integrity_hash IS NULL OR length(integrity_hash) = 64",
            name="ck_document_claims_integrity_hash",
        ),
        sa.ForeignKeyConstraint(
            ["assurance_document_id", "organization_id"],
            ["assurance_documents.id", "assurance_documents.organization_id"],
            name="fk_document_claims_document_tenant",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_document_claims"),
    )
    op.create_index(
        "ix_document_claims_organization_id",
        "document_claims",
        ["organization_id"],
    )
    op.create_index(
        "ix_document_claims_tenant_subject",
        "document_claims",
        ["organization_id", "subject_type", "subject_reference"],
    )
    op.create_index(
        "ix_document_claims_tenant_valid_until",
        "document_claims",
        ["organization_id", "valid_until"],
    )

    op.create_table(
        "document_entity_links",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("assurance_document_id", sa.Integer(), nullable=False),
        sa.Column("entity_type", sa.String(length=32), nullable=False),
        sa.Column("entity_reference", sa.String(length=255), nullable=False),
        sa.Column("link_confidence", sa.Float(), nullable=False, server_default="0"),
        sa.Column("link_method", sa.String(length=32), nullable=False),
        sa.Column("human_confirmed", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.CheckConstraint(
            "entity_type IN ('SUPPLIER','LOT','ORDER','SHIPMENT','OPERATION')",
            name="ck_document_entity_links_entity_type",
        ),
        sa.CheckConstraint(
            "link_method IN ('EXACT_IDENTIFIER','NORMALIZED_IDENTIFIER','HEURISTIC','HUMAN_CONFIRMED')",
            name="ck_document_entity_links_link_method",
        ),
        sa.CheckConstraint(
            "link_confidence >= 0 AND link_confidence <= 1",
            name="ck_document_entity_links_confidence",
        ),
        sa.ForeignKeyConstraint(
            ["assurance_document_id", "organization_id"],
            ["assurance_documents.id", "assurance_documents.organization_id"],
            name="fk_document_entity_links_document_tenant",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_document_entity_links"),
        sa.UniqueConstraint(
            "organization_id",
            "assurance_document_id",
            "entity_type",
            "entity_reference",
            name="uq_document_entity_links_tenant_target",
        ),
    )
    op.create_index(
        "ix_document_entity_links_organization_id",
        "document_entity_links",
        ["organization_id"],
    )
    op.create_index(
        "ix_document_entity_links_tenant_target",
        "document_entity_links",
        ["organization_id", "entity_type", "entity_reference"],
    )

    for table in _TABLES:
        _enable_rls(table)
        _grant_runtime(table)


def downgrade() -> None:
    for table in reversed(_TABLES):
        _revoke_runtime(table)
        _disable_rls(table)

    op.drop_table("document_entity_links")
    op.drop_table("document_claims")
    op.drop_table("extracted_document_fields")
    op.drop_table("document_extraction_runs")
    op.drop_table("assurance_documents")
