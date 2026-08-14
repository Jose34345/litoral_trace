"""Add persistent enterprise Vault document metadata with tenant RLS.

Revision ID: 016_add_vault_documents
Revises: 015_add_satellite_queue_metrics
Create Date: 2026-08-14 00:00:00.000000
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "016_add_vault_documents"
down_revision: Union[str, Sequence[str], None] = (
    "015_add_satellite_queue_metrics"
)
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


RUNTIME_ROLE = "litoral_trace_app"
WORKER_EXECUTOR_ROLE = "litoral_trace_worker_executor"
TENANT_CONTEXT_SQL = (
    "NULLIF(current_setting('app.current_organization_id', true), '')::integer"
)


def _create_vault_document_rls() -> None:
    tenant_match_sql = f"organization_id = {TENANT_CONTEXT_SQL}"

    op.execute(
        "ALTER TABLE public.vault_documents ENABLE ROW LEVEL SECURITY"
    )
    op.execute(
        "ALTER TABLE public.vault_documents FORCE ROW LEVEL SECURITY"
    )
    op.execute(
        "CREATE POLICY vault_documents_tenant_select "
        "ON public.vault_documents "
        "FOR SELECT "
        f"USING ({tenant_match_sql})"
    )
    op.execute(
        "CREATE POLICY vault_documents_tenant_insert "
        "ON public.vault_documents "
        "FOR INSERT "
        f"WITH CHECK ({tenant_match_sql})"
    )
    op.execute(
        "CREATE POLICY vault_documents_tenant_update "
        "ON public.vault_documents "
        "FOR UPDATE "
        f"USING ({tenant_match_sql}) "
        f"WITH CHECK ({tenant_match_sql})"
    )


def _drop_vault_document_rls() -> None:
    op.execute(
        "DROP POLICY IF EXISTS vault_documents_tenant_update "
        "ON public.vault_documents"
    )
    op.execute(
        "DROP POLICY IF EXISTS vault_documents_tenant_insert "
        "ON public.vault_documents"
    )
    op.execute(
        "DROP POLICY IF EXISTS vault_documents_tenant_select "
        "ON public.vault_documents"
    )
    op.execute(
        "ALTER TABLE public.vault_documents NO FORCE ROW LEVEL SECURITY"
    )
    op.execute(
        "ALTER TABLE public.vault_documents DISABLE ROW LEVEL SECURITY"
    )


def _grant_runtime_access() -> None:
    op.execute(
        "REVOKE ALL PRIVILEGES ON TABLE public.vault_documents FROM PUBLIC"
    )
    op.execute(
        "REVOKE ALL PRIVILEGES "
        "ON SEQUENCE public.vault_documents_id_seq FROM PUBLIC"
    )
    op.execute(
        "GRANT SELECT, INSERT, UPDATE "
        "ON TABLE public.vault_documents "
        f"TO {RUNTIME_ROLE}"
    )
    op.execute(
        "GRANT USAGE, SELECT "
        "ON SEQUENCE public.vault_documents_id_seq "
        f"TO {RUNTIME_ROLE}"
    )
    op.execute(
        "REVOKE ALL PRIVILEGES "
        "ON TABLE public.vault_documents "
        f"FROM {WORKER_EXECUTOR_ROLE}"
    )
    op.execute(
        "REVOKE ALL PRIVILEGES "
        "ON SEQUENCE public.vault_documents_id_seq "
        f"FROM {WORKER_EXECUTOR_ROLE}"
    )


def _revoke_runtime_access() -> None:
    op.execute(
        "REVOKE USAGE, SELECT "
        "ON SEQUENCE public.vault_documents_id_seq "
        f"FROM {RUNTIME_ROLE}"
    )
    op.execute(
        "REVOKE SELECT, INSERT, UPDATE "
        "ON TABLE public.vault_documents "
        f"FROM {RUNTIME_ROLE}"
    )


def upgrade() -> None:
    op.create_table(
        "vault_documents",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column(
            "public_id",
            sa.Uuid(),
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("created_by_user_id", sa.Integer(), nullable=True),
        sa.Column("original_filename", sa.String(length=255), nullable=False),
        sa.Column("content_type", sa.String(length=255), nullable=False),
        sa.Column("size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("sha256", sa.String(length=64), nullable=False),
        sa.Column("object_key", sa.String(length=512), nullable=False),
        sa.Column(
            "storage_backend",
            sa.String(length=32),
            nullable=False,
            server_default="s3",
        ),
        sa.Column("storage_bucket", sa.String(length=255), nullable=False),
        sa.Column("storage_etag", sa.String(length=255), nullable=True),
        sa.Column("storage_version_id", sa.String(length=255), nullable=True),
        sa.Column("document_type", sa.String(length=50), nullable=False),
        sa.Column(
            "status",
            sa.String(length=32),
            nullable=False,
            server_default="pending_upload",
        ),
        sa.Column("idempotency_key", sa.String(length=255), nullable=True),
        sa.Column("last_error_code", sa.String(length=100), nullable=True),
        sa.Column("last_error_message", sa.String(length=512), nullable=True),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
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
            "size_bytes > 0",
            name="ck_vault_documents_size_bytes_positive",
        ),
        sa.CheckConstraint(
            "length(sha256) = 64",
            name="ck_vault_documents_sha256_length",
        ),
        sa.CheckConstraint(
            "length(trim(original_filename)) > 0",
            name="ck_vault_documents_filename_not_blank",
        ),
        sa.CheckConstraint(
            "length(trim(content_type)) > 0",
            name="ck_vault_documents_content_type_not_blank",
        ),
        sa.CheckConstraint(
            "length(trim(object_key)) > 0",
            name="ck_vault_documents_object_key_not_blank",
        ),
        sa.CheckConstraint(
            "length(trim(storage_bucket)) > 0",
            name="ck_vault_documents_storage_bucket_not_blank",
        ),
        sa.CheckConstraint(
            "storage_backend IN ('s3')",
            name="ck_vault_documents_storage_backend",
        ),
        sa.CheckConstraint(
            "document_type IN ("
            "'PDF_CERTIFICADO', "
            "'DDS_JSON_TRACES', "
            "'REMITO_EXCEL', "
            "'OTHER_EVIDENCE'"
            ")",
            name="ck_vault_documents_document_type",
        ),
        sa.CheckConstraint(
            "status IN ("
            "'pending_upload', "
            "'available', "
            "'upload_failed', "
            "'delete_pending', "
            "'delete_failed', "
            "'deleted'"
            ")",
            name="ck_vault_documents_status",
        ),
        sa.CheckConstraint(
            "(status = 'deleted' AND deleted_at IS NOT NULL) "
            "OR (status <> 'deleted' AND deleted_at IS NULL)",
            name="ck_vault_documents_deleted_at_state",
        ),
        sa.CheckConstraint(
            "status NOT IN ('upload_failed', 'delete_failed') "
            "OR last_error_code IS NOT NULL",
            name="ck_vault_documents_failure_has_error_code",
        ),
        sa.ForeignKeyConstraint(
            ["organization_id"],
            ["organizations.id"],
            name="fk_vault_documents_organization_id",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["created_by_user_id"],
            ["users.id"],
            name="fk_vault_documents_created_by_user_id",
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_vault_documents"),
        sa.UniqueConstraint(
            "public_id",
            name="uq_vault_documents_public_id",
        ),
        sa.UniqueConstraint(
            "object_key",
            name="uq_vault_documents_object_key",
        ),
        sa.UniqueConstraint(
            "organization_id",
            "idempotency_key",
            name="uq_vault_documents_tenant_idempotency_key",
        ),
    )

    op.create_index(
        "ix_vault_documents_organization_id",
        "vault_documents",
        ["organization_id"],
    )
    op.create_index(
        "ix_vault_documents_created_by_user_id",
        "vault_documents",
        ["created_by_user_id"],
    )
    op.create_index(
        "ix_vault_documents_tenant_created_at",
        "vault_documents",
        ["organization_id", "created_at"],
    )
    op.create_index(
        "ix_vault_documents_tenant_type_created_at",
        "vault_documents",
        ["organization_id", "document_type", "created_at"],
    )
    op.create_index(
        "ix_vault_documents_tenant_status_created_at",
        "vault_documents",
        ["organization_id", "status", "created_at"],
    )
    op.create_index(
        "ix_vault_documents_tenant_sha256",
        "vault_documents",
        ["organization_id", "sha256"],
    )

    _create_vault_document_rls()
    _grant_runtime_access()


def downgrade() -> None:
    _revoke_runtime_access()
    _drop_vault_document_rls()
    op.drop_table("vault_documents")