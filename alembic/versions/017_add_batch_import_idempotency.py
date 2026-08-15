"""Add persistent batch import idempotency and tenant lote uniqueness.

Revision ID: 017_add_batch_import_idempotency
Revises: 016_add_vault_documents
Create Date: 2026-08-15 00:00:00.000000
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "017_add_batch_import_idempotency"
down_revision: Union[str, Sequence[str], None] = "016_add_vault_documents"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


RUNTIME_ROLE = "litoral_trace_app"
WORKER_EXECUTOR_ROLE = "litoral_trace_worker_executor"
TENANT_CONTEXT_SQL = (
    "NULLIF(current_setting('app.current_organization_id', true), '')::integer"
)
LOTE_TENANT_IDENTIFIER_INDEX = "uq_lotes_tenant_identificador_ci"


def _assert_existing_lotes_are_unique() -> None:
    """Fail closed instead of silently deleting or merging existing business data."""

    bind = op.get_bind()
    duplicate_exists = bind.execute(
        sa.text(
            """
            SELECT 1
            FROM public.lotes
            GROUP BY organization_id, lower(identificador)
            HAVING count(*) > 1
            LIMIT 1
            """
        )
    ).scalar()

    if duplicate_exists is not None:
        raise RuntimeError(
            "Cannot enforce tenant lote identifier uniqueness: "
            "existing duplicates must be resolved explicitly first."
        )


def _create_batch_import_rls() -> None:
    tenant_match_sql = f"organization_id = {TENANT_CONTEXT_SQL}"

    op.execute(
        "ALTER TABLE public.batch_imports ENABLE ROW LEVEL SECURITY"
    )
    op.execute(
        "ALTER TABLE public.batch_imports FORCE ROW LEVEL SECURITY"
    )
    op.execute(
        "CREATE POLICY batch_imports_tenant_select "
        "ON public.batch_imports "
        "FOR SELECT "
        f"USING ({tenant_match_sql})"
    )
    op.execute(
        "CREATE POLICY batch_imports_tenant_insert "
        "ON public.batch_imports "
        "FOR INSERT "
        f"WITH CHECK ({tenant_match_sql})"
    )
    op.execute(
        "CREATE POLICY batch_imports_tenant_update "
        "ON public.batch_imports "
        "FOR UPDATE "
        f"USING ({tenant_match_sql}) "
        f"WITH CHECK ({tenant_match_sql})"
    )


def _drop_batch_import_rls() -> None:
    op.execute(
        "DROP POLICY IF EXISTS batch_imports_tenant_update "
        "ON public.batch_imports"
    )
    op.execute(
        "DROP POLICY IF EXISTS batch_imports_tenant_insert "
        "ON public.batch_imports"
    )
    op.execute(
        "DROP POLICY IF EXISTS batch_imports_tenant_select "
        "ON public.batch_imports"
    )
    op.execute(
        "ALTER TABLE public.batch_imports NO FORCE ROW LEVEL SECURITY"
    )
    op.execute(
        "ALTER TABLE public.batch_imports DISABLE ROW LEVEL SECURITY"
    )


def _grant_runtime_access() -> None:
    op.execute(
        "REVOKE ALL PRIVILEGES ON TABLE public.batch_imports FROM PUBLIC"
    )
    op.execute(
        "REVOKE ALL PRIVILEGES "
        "ON SEQUENCE public.batch_imports_id_seq FROM PUBLIC"
    )
    op.execute(
        "GRANT SELECT, INSERT, UPDATE "
        "ON TABLE public.batch_imports "
        f"TO {RUNTIME_ROLE}"
    )
    op.execute(
        "GRANT USAGE, SELECT "
        "ON SEQUENCE public.batch_imports_id_seq "
        f"TO {RUNTIME_ROLE}"
    )
    op.execute(
        "REVOKE ALL PRIVILEGES "
        "ON TABLE public.batch_imports "
        f"FROM {WORKER_EXECUTOR_ROLE}"
    )
    op.execute(
        "REVOKE ALL PRIVILEGES "
        "ON SEQUENCE public.batch_imports_id_seq "
        f"FROM {WORKER_EXECUTOR_ROLE}"
    )


def _revoke_runtime_access() -> None:
    op.execute(
        "REVOKE USAGE, SELECT "
        "ON SEQUENCE public.batch_imports_id_seq "
        f"FROM {RUNTIME_ROLE}"
    )
    op.execute(
        "REVOKE SELECT, INSERT, UPDATE "
        "ON TABLE public.batch_imports "
        f"FROM {RUNTIME_ROLE}"
    )


def upgrade() -> None:
    _assert_existing_lotes_are_unique()

    op.execute(
        "CREATE UNIQUE INDEX "
        f"{LOTE_TENANT_IDENTIFIER_INDEX} "
        "ON public.lotes (organization_id, lower(identificador))"
    )

    op.create_table(
        "batch_imports",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column(
            "public_id",
            sa.Uuid(),
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("created_by_user_id", sa.Integer(), nullable=True),
        sa.Column("idempotency_key", sa.String(length=255), nullable=False),
        sa.Column("source_sha256", sa.String(length=64), nullable=False),
        sa.Column("source_filename", sa.String(length=255), nullable=False),
        sa.Column(
            "status",
            sa.String(length=32),
            nullable=False,
            server_default="processing",
        ),
        sa.Column("total_rows", sa.Integer(), nullable=False),
        sa.Column(
            "inserted_rows",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
        sa.Column(
            "lote_ids",
            sa.JSON(),
            nullable=False,
            server_default=sa.text("'[]'::json"),
        ),
        sa.Column(
            "identifiers",
            sa.JSON(),
            nullable=False,
            server_default=sa.text("'[]'::json"),
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column(
            "completed_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.CheckConstraint(
            "length(trim(idempotency_key)) > 0",
            name="ck_batch_imports_idempotency_key_not_blank",
        ),
        sa.CheckConstraint(
            "length(source_sha256) = 64",
            name="ck_batch_imports_source_sha256_length",
        ),
        sa.CheckConstraint(
            "length(trim(source_filename)) > 0",
            name="ck_batch_imports_source_filename_not_blank",
        ),
        sa.CheckConstraint(
            "status IN ('processing', 'completed')",
            name="ck_batch_imports_status",
        ),
        sa.CheckConstraint(
            "total_rows > 0",
            name="ck_batch_imports_total_rows_positive",
        ),
        sa.CheckConstraint(
            "inserted_rows >= 0 AND inserted_rows <= total_rows",
            name="ck_batch_imports_inserted_rows_range",
        ),
        sa.CheckConstraint(
            "("
            "status = 'processing' "
            "AND completed_at IS NULL "
            "AND inserted_rows = 0"
            ") OR ("
            "status = 'completed' "
            "AND completed_at IS NOT NULL "
            "AND inserted_rows = total_rows"
            ")",
            name="ck_batch_imports_completion_state",
        ),
        sa.ForeignKeyConstraint(
            ["organization_id"],
            ["organizations.id"],
            name="fk_batch_imports_organization_id",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["created_by_user_id"],
            ["users.id"],
            name="fk_batch_imports_created_by_user_id",
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_batch_imports"),
        sa.UniqueConstraint(
            "public_id",
            name="uq_batch_imports_public_id",
        ),
        sa.UniqueConstraint(
            "organization_id",
            "idempotency_key",
            name="uq_batch_imports_tenant_idempotency_key",
        ),
    )

    op.create_index(
        "ix_batch_imports_organization_id",
        "batch_imports",
        ["organization_id"],
    )
    op.create_index(
        "ix_batch_imports_created_by_user_id",
        "batch_imports",
        ["created_by_user_id"],
    )
    op.create_index(
        "ix_batch_imports_tenant_created_at",
        "batch_imports",
        ["organization_id", "created_at"],
    )
    op.create_index(
        "ix_batch_imports_tenant_source_sha256",
        "batch_imports",
        ["organization_id", "source_sha256"],
    )

    _create_batch_import_rls()
    _grant_runtime_access()


def downgrade() -> None:
    _revoke_runtime_access()
    _drop_batch_import_rls()
    op.drop_table("batch_imports")
    op.execute(
        f"DROP INDEX IF EXISTS public.{LOTE_TENANT_IDENTIFIER_INDEX}"
    )
