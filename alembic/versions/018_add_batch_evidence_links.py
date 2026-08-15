"""Add tenant-safe BatchImport <-> VaultDocument evidence linkage.

Revision ID: 018_add_batch_evidence_links
Revises: 017_add_batch_import_idempotency
Create Date: 2026-08-15 00:00:00.000000
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "018_add_batch_evidence_links"
down_revision: Union[str, Sequence[str], None] = (
    "017_add_batch_import_idempotency"
)
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


RUNTIME_ROLE = "litoral_trace_app"
WORKER_EXECUTOR_ROLE = "litoral_trace_worker_executor"
TENANT_CONTEXT_SQL = (
    "NULLIF(current_setting('app.current_organization_id', true), '')::integer"
)


def _create_rls() -> None:
    tenant_match_sql = f"organization_id = {TENANT_CONTEXT_SQL}"

    op.execute(
        "ALTER TABLE public.batch_evidence_links "
        "ENABLE ROW LEVEL SECURITY"
    )
    op.execute(
        "ALTER TABLE public.batch_evidence_links "
        "FORCE ROW LEVEL SECURITY"
    )
    op.execute(
        "CREATE POLICY batch_evidence_links_tenant_select "
        "ON public.batch_evidence_links "
        "FOR SELECT "
        f"USING ({tenant_match_sql})"
    )
    op.execute(
        "CREATE POLICY batch_evidence_links_tenant_insert "
        "ON public.batch_evidence_links "
        "FOR INSERT "
        f"WITH CHECK ({tenant_match_sql})"
    )
    op.execute(
        "CREATE POLICY batch_evidence_links_tenant_update "
        "ON public.batch_evidence_links "
        "FOR UPDATE "
        f"USING ({tenant_match_sql}) "
        f"WITH CHECK ({tenant_match_sql})"
    )


def _drop_rls() -> None:
    op.execute(
        "DROP POLICY IF EXISTS batch_evidence_links_tenant_update "
        "ON public.batch_evidence_links"
    )
    op.execute(
        "DROP POLICY IF EXISTS batch_evidence_links_tenant_insert "
        "ON public.batch_evidence_links"
    )
    op.execute(
        "DROP POLICY IF EXISTS batch_evidence_links_tenant_select "
        "ON public.batch_evidence_links"
    )
    op.execute(
        "ALTER TABLE public.batch_evidence_links "
        "NO FORCE ROW LEVEL SECURITY"
    )
    op.execute(
        "ALTER TABLE public.batch_evidence_links "
        "DISABLE ROW LEVEL SECURITY"
    )


def _grant_runtime_access() -> None:
    op.execute(
        "REVOKE ALL PRIVILEGES "
        "ON TABLE public.batch_evidence_links FROM PUBLIC"
    )
    op.execute(
        "REVOKE ALL PRIVILEGES "
        "ON SEQUENCE public.batch_evidence_links_id_seq FROM PUBLIC"
    )
    op.execute(
        "GRANT SELECT, INSERT, UPDATE "
        "ON TABLE public.batch_evidence_links "
        f"TO {RUNTIME_ROLE}"
    )
    op.execute(
        "GRANT USAGE, SELECT "
        "ON SEQUENCE public.batch_evidence_links_id_seq "
        f"TO {RUNTIME_ROLE}"
    )
    op.execute(
        "REVOKE ALL PRIVILEGES "
        "ON TABLE public.batch_evidence_links "
        f"FROM {WORKER_EXECUTOR_ROLE}"
    )
    op.execute(
        "REVOKE ALL PRIVILEGES "
        "ON SEQUENCE public.batch_evidence_links_id_seq "
        f"FROM {WORKER_EXECUTOR_ROLE}"
    )


def _revoke_runtime_access() -> None:
    op.execute(
        "REVOKE USAGE, SELECT "
        "ON SEQUENCE public.batch_evidence_links_id_seq "
        f"FROM {RUNTIME_ROLE}"
    )
    op.execute(
        "REVOKE SELECT, INSERT, UPDATE "
        "ON TABLE public.batch_evidence_links "
        f"FROM {RUNTIME_ROLE}"
    )


def upgrade() -> None:
    # Composite tenant keys make cross-tenant linkage impossible at FK level,
    # even if a future service bug supplies a mismatched organization_id.
    op.create_unique_constraint(
        "uq_batch_imports_id_organization_id",
        "batch_imports",
        ["id", "organization_id"],
    )
    op.create_unique_constraint(
        "uq_vault_documents_id_organization_id",
        "vault_documents",
        ["id", "organization_id"],
    )

    op.create_table(
        "batch_evidence_links",
        sa.Column(
            "id",
            sa.Integer(),
            autoincrement=True,
            nullable=False,
        ),
        sa.Column(
            "public_id",
            sa.Uuid(),
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "organization_id",
            sa.Integer(),
            nullable=False,
        ),
        sa.Column(
            "batch_import_id",
            sa.Integer(),
            nullable=False,
        ),
        sa.Column(
            "vault_document_id",
            sa.Integer(),
            nullable=False,
        ),
        sa.Column(
            "evidence_type",
            sa.String(length=40),
            nullable=False,
        ),
        sa.Column(
            "created_by_user_id",
            sa.Integer(),
            nullable=True,
        ),
        sa.Column(
            "unlinked_by_user_id",
            sa.Integer(),
            nullable=True,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column(
            "unlinked_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.CheckConstraint(
            (
                "evidence_type IN ("
                "'SOURCE_WORKBOOK', "
                "'SUPPORTING_EVIDENCE', "
                "'COMPLIANCE_EVIDENCE'"
                ")"
            ),
            name="ck_batch_evidence_links_evidence_type",
        ),
        sa.CheckConstraint(
            (
                "unlinked_by_user_id IS NULL "
                "OR unlinked_at IS NOT NULL"
            ),
            name="ck_batch_evidence_links_unlink_state",
        ),
        sa.ForeignKeyConstraint(
            ["batch_import_id", "organization_id"],
            ["batch_imports.id", "batch_imports.organization_id"],
            name="fk_batch_evidence_links_batch_tenant",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["vault_document_id", "organization_id"],
            ["vault_documents.id", "vault_documents.organization_id"],
            name="fk_batch_evidence_links_vault_tenant",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["created_by_user_id"],
            ["users.id"],
            name="fk_batch_evidence_links_created_by_user_id",
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["unlinked_by_user_id"],
            ["users.id"],
            name="fk_batch_evidence_links_unlinked_by_user_id",
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint(
            "id",
            name="pk_batch_evidence_links",
        ),
        sa.UniqueConstraint(
            "public_id",
            name="uq_batch_evidence_links_public_id",
        ),
    )

    op.create_index(
        "ix_batch_evidence_links_organization_id",
        "batch_evidence_links",
        ["organization_id"],
    )
    op.create_index(
        "ix_batch_evidence_links_tenant_batch_created",
        "batch_evidence_links",
        ["organization_id", "batch_import_id", "created_at"],
    )
    op.create_index(
        "ix_batch_evidence_links_tenant_vault_created",
        "batch_evidence_links",
        ["organization_id", "vault_document_id", "created_at"],
    )
    op.create_index(
        "uq_batch_evidence_links_active_pair",
        "batch_evidence_links",
        ["batch_import_id", "vault_document_id"],
        unique=True,
        postgresql_where=sa.text("unlinked_at IS NULL"),
    )
    op.create_index(
        "uq_batch_evidence_links_active_source",
        "batch_evidence_links",
        ["batch_import_id"],
        unique=True,
        postgresql_where=sa.text(
            "unlinked_at IS NULL "
            "AND evidence_type = 'SOURCE_WORKBOOK'"
        ),
    )

    _create_rls()
    _grant_runtime_access()


def downgrade() -> None:
    _revoke_runtime_access()
    _drop_rls()

    op.drop_index(
        "uq_batch_evidence_links_active_source",
        table_name="batch_evidence_links",
    )
    op.drop_index(
        "uq_batch_evidence_links_active_pair",
        table_name="batch_evidence_links",
    )
    op.drop_index(
        "ix_batch_evidence_links_tenant_vault_created",
        table_name="batch_evidence_links",
    )
    op.drop_index(
        "ix_batch_evidence_links_tenant_batch_created",
        table_name="batch_evidence_links",
    )
    op.drop_index(
        "ix_batch_evidence_links_organization_id",
        table_name="batch_evidence_links",
    )
    op.drop_table("batch_evidence_links")

    op.drop_constraint(
        "uq_vault_documents_id_organization_id",
        "vault_documents",
        type_="unique",
    )
    op.drop_constraint(
        "uq_batch_imports_id_organization_id",
        "batch_imports",
        type_="unique",
    )
