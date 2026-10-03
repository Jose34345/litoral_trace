"""Add isolated tenant-scoped U.S. Lacey pilot core tables.

Revision ID: 034_us_lacey_pilot_core
Revises: 033_assurance_suppliers
Create Date: 2026-08-29 23:20:00.000000
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "034_us_lacey_pilot_core"
down_revision: Union[str, Sequence[str], None] = "033_assurance_suppliers"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

RUNTIME_ROLE = "litoral_trace_app"
WORKER_EXECUTOR_ROLE = "litoral_trace_worker_executor"
TENANT_CONTEXT_SQL = (
    "NULLIF(current_setting('app.current_organization_id', true), '')::integer"
)
TABLES = (
    "us_lacey_organization_profiles",
    "us_lacey_operations",
    "us_lacey_operation_documents",
    "us_lacey_operation_fields",
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
    op.execute(
        f"CREATE POLICY {table}_tenant_delete ON public.{table} "
        f"FOR DELETE USING ({tenant_match})"
    )


def _grant_runtime(table: str) -> None:
    op.execute(f"REVOKE ALL PRIVILEGES ON TABLE public.{table} FROM PUBLIC")
    op.execute(
        f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.{table} TO {RUNTIME_ROLE}"
    )
    op.execute(
        f"REVOKE ALL PRIVILEGES ON TABLE public.{table} FROM {WORKER_EXECUTOR_ROLE}"
    )
    sequence = f"{table}_id_seq"
    op.execute(f"REVOKE ALL PRIVILEGES ON SEQUENCE public.{sequence} FROM PUBLIC")
    op.execute(f"GRANT USAGE, SELECT ON SEQUENCE public.{sequence} TO {RUNTIME_ROLE}")
    op.execute(
        f"REVOKE ALL PRIVILEGES ON SEQUENCE public.{sequence} FROM {WORKER_EXECUTOR_ROLE}"
    )


def upgrade() -> None:
    op.create_table(
        "us_lacey_organization_profiles",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("legal_name", sa.String(length=255), nullable=False),
        sa.Column("country_code", sa.String(length=2), nullable=False, server_default="US"),
        sa.Column("address_text", sa.Text(), nullable=True),
        sa.Column("business_type", sa.String(length=32), nullable=False, server_default="OTHER"),
        sa.Column("admin_contact_name", sa.String(length=255), nullable=True),
        sa.Column("admin_contact_email", sa.String(length=255), nullable=True),
        sa.Column("billing_email", sa.String(length=255), nullable=True),
        sa.Column("account_status", sa.String(length=24), nullable=False, server_default="PILOT"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.ForeignKeyConstraint(["organization_id"], ["organizations.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id", name="pk_us_lacey_organization_profiles"),
        sa.UniqueConstraint("organization_id", name="uq_us_lacey_org_profiles_org"),
        sa.UniqueConstraint("id", "organization_id", name="uq_us_lacey_org_profiles_id_org"),
        sa.CheckConstraint("country_code = 'US'", name="ck_us_lacey_org_profiles_country_us"),
        sa.CheckConstraint("business_type IN ('IMPORTER','CUSTOMS_BROKER','OTHER')", name="ck_us_lacey_org_profiles_business_type"),
        sa.CheckConstraint("account_status IN ('PILOT','ACTIVE','SUSPENDED')", name="ck_us_lacey_org_profiles_status"),
    )
    op.create_index("ix_us_lacey_org_profiles_organization_id", "us_lacey_organization_profiles", ["organization_id"])

    op.create_table(
        "us_lacey_operations",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.Uuid(), nullable=False, server_default=sa.text("gen_random_uuid()")),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("created_by_user_id", sa.Integer(), nullable=True),
        sa.Column("client_reference", sa.String(length=255), nullable=False),
        sa.Column("importer_name", sa.String(length=255), nullable=True),
        sa.Column("consignee_name", sa.String(length=255), nullable=True),
        sa.Column("broker_name", sa.String(length=255), nullable=True),
        sa.Column("supplier_name", sa.String(length=255), nullable=True),
        sa.Column("operation_date", sa.Date(), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="NEW"),
        sa.Column("document_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("merchandise_line_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("review_result", sa.String(length=64), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.ForeignKeyConstraint(["organization_id"], ["organizations.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["created_by_user_id"], ["users.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id", name="pk_us_lacey_operations"),
        sa.UniqueConstraint("public_id", name="uq_us_lacey_operations_public_id"),
        sa.UniqueConstraint("id", "organization_id", name="uq_us_lacey_operations_id_org"),
        sa.UniqueConstraint("organization_id", "client_reference", name="uq_us_lacey_operations_org_reference"),
        sa.CheckConstraint("document_count >= 0", name="ck_us_lacey_operations_document_count"),
        sa.CheckConstraint("merchandise_line_count >= 0", name="ck_us_lacey_operations_line_count"),
        sa.CheckConstraint("status IN ('NEW','PROCESSING','REVIEW_REQUIRED','READY_FOR_REVIEW','COMPLETED','FAILED')", name="ck_us_lacey_operations_status"),
    )
    op.create_index("ix_us_lacey_operations_organization_id", "us_lacey_operations", ["organization_id"])
    op.create_index("ix_us_lacey_operations_org_status", "us_lacey_operations", ["organization_id", "status"])
    op.create_index("ix_us_lacey_operations_org_created", "us_lacey_operations", ["organization_id", "created_at"])

    op.create_table(
        "us_lacey_operation_documents",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("operation_id", sa.Integer(), nullable=False),
        sa.Column("assurance_document_id", sa.Integer(), nullable=False),
        sa.Column("document_role", sa.String(length=64), nullable=False, server_default="UNKNOWN"),
        sa.Column("version_number", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("is_current", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.ForeignKeyConstraint(["operation_id", "organization_id"], ["us_lacey_operations.id", "us_lacey_operations.organization_id"], name="fk_us_lacey_operation_documents_operation_tenant", ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["assurance_document_id", "organization_id"], ["assurance_documents.id", "assurance_documents.organization_id"], name="fk_us_lacey_operation_documents_assurance_tenant", ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id", name="pk_us_lacey_operation_documents"),
        sa.UniqueConstraint("id", "organization_id", name="uq_us_lacey_operation_documents_id_org"),
        sa.UniqueConstraint("organization_id", "operation_id", "assurance_document_id", "version_number", name="uq_us_lacey_operation_documents_version"),
        sa.CheckConstraint("version_number > 0", name="ck_us_lacey_operation_documents_version"),
    )
    op.create_index("ix_us_lacey_operation_documents_organization_id", "us_lacey_operation_documents", ["organization_id"])
    op.create_index("ix_us_lacey_operation_documents_org_operation", "us_lacey_operation_documents", ["organization_id", "operation_id"])

    op.create_table(
        "us_lacey_operation_fields",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("operation_id", sa.Integer(), nullable=False),
        sa.Column("merchandise_line_reference", sa.String(length=100), nullable=False, server_default="1"),
        sa.Column("field_name", sa.String(length=100), nullable=False),
        sa.Column("original_value", sa.Text(), nullable=True),
        sa.Column("normalized_value", sa.Text(), nullable=True),
        sa.Column("field_status", sa.String(length=24), nullable=False, server_default="MISSING"),
        sa.Column("confidence", sa.Float(), nullable=False, server_default="0"),
        sa.Column("source_assurance_document_id", sa.Integer(), nullable=True),
        sa.Column("source_page", sa.Integer(), nullable=True),
        sa.Column("source_locator", sa.Text(), nullable=True),
        sa.Column("extractor", sa.String(length=100), nullable=True),
        sa.Column("extractor_version", sa.String(length=100), nullable=True),
        sa.Column("human_value", sa.Text(), nullable=True),
        sa.Column("reviewed_by_user_id", sa.Integer(), nullable=True),
        sa.Column("reviewed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.ForeignKeyConstraint(["operation_id", "organization_id"], ["us_lacey_operations.id", "us_lacey_operations.organization_id"], name="fk_us_lacey_operation_fields_operation_tenant", ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["source_assurance_document_id", "organization_id"], ["assurance_documents.id", "assurance_documents.organization_id"], name="fk_us_lacey_operation_fields_source_document_tenant", ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["reviewed_by_user_id"], ["users.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id", name="pk_us_lacey_operation_fields"),
        sa.UniqueConstraint("id", "organization_id", name="uq_us_lacey_operation_fields_id_org"),
        sa.UniqueConstraint("organization_id", "operation_id", "merchandise_line_reference", "field_name", name="uq_us_lacey_operation_fields_operation_line_field"),
        sa.CheckConstraint("confidence >= 0 AND confidence <= 1", name="ck_us_lacey_fields_confidence"),
        sa.CheckConstraint("field_status IN ('FOUND','MATCHED','MISSING','REVIEW','NOT_REQUIRED')", name="ck_us_lacey_fields_status"),
    )
    op.create_index("ix_us_lacey_operation_fields_organization_id", "us_lacey_operation_fields", ["organization_id"])
    op.create_index("ix_us_lacey_operation_fields_org_operation", "us_lacey_operation_fields", ["organization_id", "operation_id"])
    op.create_index("ix_us_lacey_operation_fields_org_status", "us_lacey_operation_fields", ["organization_id", "field_status"])

    for table in TABLES:
        _enable_rls(table)
        _grant_runtime(table)


def downgrade() -> None:
    for table in reversed(TABLES):
        sequence = f"{table}_id_seq"
        op.execute(f"REVOKE USAGE, SELECT ON SEQUENCE public.{sequence} FROM {RUNTIME_ROLE}")
        op.execute(f"REVOKE SELECT, INSERT, UPDATE, DELETE ON TABLE public.{table} FROM {RUNTIME_ROLE}")
        for action in ("delete", "update", "insert", "select"):
            op.execute(f"DROP POLICY IF EXISTS {table}_tenant_{action} ON public.{table}")
        op.execute(f"ALTER TABLE public.{table} NO FORCE ROW LEVEL SECURITY")
        op.execute(f"ALTER TABLE public.{table} DISABLE ROW LEVEL SECURITY")

    op.drop_index("ix_us_lacey_operation_fields_org_status", table_name="us_lacey_operation_fields")
    op.drop_index("ix_us_lacey_operation_fields_org_operation", table_name="us_lacey_operation_fields")
    op.drop_index("ix_us_lacey_operation_fields_organization_id", table_name="us_lacey_operation_fields")
    op.drop_table("us_lacey_operation_fields")

    op.drop_index("ix_us_lacey_operation_documents_org_operation", table_name="us_lacey_operation_documents")
    op.drop_index("ix_us_lacey_operation_documents_organization_id", table_name="us_lacey_operation_documents")
    op.drop_table("us_lacey_operation_documents")

    op.drop_index("ix_us_lacey_operations_org_created", table_name="us_lacey_operations")
    op.drop_index("ix_us_lacey_operations_org_status", table_name="us_lacey_operations")
    op.drop_index("ix_us_lacey_operations_organization_id", table_name="us_lacey_operations")
    op.drop_table("us_lacey_operations")

    op.drop_index("ix_us_lacey_org_profiles_organization_id", table_name="us_lacey_organization_profiles")
    op.drop_table("us_lacey_organization_profiles")
