"""Add tenant-scoped remembered Smart Import mappings.

Revision ID: 029_add_smart_import_profiles
Revises: 028_platform_definer_rls
Create Date: 2026-08-24 23:55:00.000000
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "029_add_smart_import_profiles"
down_revision: Union[str, Sequence[str], None] = "028_platform_definer_rls"
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
        "ALTER TABLE public.smart_import_profiles ENABLE ROW LEVEL SECURITY"
    )
    op.execute(
        "ALTER TABLE public.smart_import_profiles FORCE ROW LEVEL SECURITY"
    )
    op.execute(
        "CREATE POLICY smart_import_profiles_tenant_select "
        "ON public.smart_import_profiles FOR SELECT "
        f"USING ({tenant_match_sql})"
    )
    op.execute(
        "CREATE POLICY smart_import_profiles_tenant_insert "
        "ON public.smart_import_profiles FOR INSERT "
        f"WITH CHECK ({tenant_match_sql})"
    )
    op.execute(
        "CREATE POLICY smart_import_profiles_tenant_update "
        "ON public.smart_import_profiles FOR UPDATE "
        f"USING ({tenant_match_sql}) WITH CHECK ({tenant_match_sql})"
    )


def _drop_rls() -> None:
    op.execute(
        "DROP POLICY IF EXISTS smart_import_profiles_tenant_update "
        "ON public.smart_import_profiles"
    )
    op.execute(
        "DROP POLICY IF EXISTS smart_import_profiles_tenant_insert "
        "ON public.smart_import_profiles"
    )
    op.execute(
        "DROP POLICY IF EXISTS smart_import_profiles_tenant_select "
        "ON public.smart_import_profiles"
    )
    op.execute(
        "ALTER TABLE public.smart_import_profiles NO FORCE ROW LEVEL SECURITY"
    )
    op.execute(
        "ALTER TABLE public.smart_import_profiles DISABLE ROW LEVEL SECURITY"
    )


def _grant_runtime_access() -> None:
    op.execute(
        "REVOKE ALL PRIVILEGES ON TABLE public.smart_import_profiles FROM PUBLIC"
    )
    op.execute(
        "REVOKE ALL PRIVILEGES ON SEQUENCE public.smart_import_profiles_id_seq FROM PUBLIC"
    )
    op.execute(
        "GRANT SELECT, INSERT, UPDATE ON TABLE public.smart_import_profiles "
        f"TO {RUNTIME_ROLE}"
    )
    op.execute(
        "GRANT USAGE, SELECT ON SEQUENCE public.smart_import_profiles_id_seq "
        f"TO {RUNTIME_ROLE}"
    )
    op.execute(
        "REVOKE ALL PRIVILEGES ON TABLE public.smart_import_profiles "
        f"FROM {WORKER_EXECUTOR_ROLE}"
    )
    op.execute(
        "REVOKE ALL PRIVILEGES ON SEQUENCE public.smart_import_profiles_id_seq "
        f"FROM {WORKER_EXECUTOR_ROLE}"
    )


def _revoke_runtime_access() -> None:
    op.execute(
        "REVOKE USAGE, SELECT ON SEQUENCE public.smart_import_profiles_id_seq "
        f"FROM {RUNTIME_ROLE}"
    )
    op.execute(
        "REVOKE SELECT, INSERT, UPDATE ON TABLE public.smart_import_profiles "
        f"FROM {RUNTIME_ROLE}"
    )


def upgrade() -> None:
    op.create_table(
        "smart_import_profiles",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column(
            "public_id",
            sa.Uuid(),
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("created_by_user_id", sa.Integer(), nullable=True),
        sa.Column("updated_by_user_id", sa.Integer(), nullable=True),
        sa.Column("name", sa.String(length=120), nullable=False),
        sa.Column(
            "schema_kind",
            sa.String(length=32),
            nullable=False,
            server_default="lotes",
        ),
        sa.Column("sheet_name", sa.String(length=255), nullable=False),
        sa.Column("header_fingerprint", sa.String(length=64), nullable=False),
        sa.Column(
            "header_signature",
            sa.JSON(),
            nullable=False,
            server_default=sa.text("'[]'::json"),
        ),
        sa.Column(
            "mapping_json",
            sa.JSON(),
            nullable=False,
            server_default=sa.text("'{}'::json"),
        ),
        sa.Column("version", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("use_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
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
        sa.Column("last_used_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "length(trim(name)) > 0",
            name="ck_smart_import_profiles_name_not_blank",
        ),
        sa.CheckConstraint(
            "length(trim(sheet_name)) > 0",
            name="ck_smart_import_profiles_sheet_name_not_blank",
        ),
        sa.CheckConstraint(
            "length(header_fingerprint) = 64",
            name="ck_smart_import_profiles_fingerprint_length",
        ),
        sa.CheckConstraint(
            "schema_kind = 'lotes'",
            name="ck_smart_import_profiles_schema_kind",
        ),
        sa.CheckConstraint(
            "version > 0",
            name="ck_smart_import_profiles_version_positive",
        ),
        sa.CheckConstraint(
            "use_count >= 0",
            name="ck_smart_import_profiles_use_count_non_negative",
        ),
        sa.ForeignKeyConstraint(
            ["organization_id"],
            ["organizations.id"],
            name="fk_smart_import_profiles_organization_id",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["created_by_user_id"],
            ["users.id"],
            name="fk_smart_import_profiles_created_by_user_id",
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["updated_by_user_id"],
            ["users.id"],
            name="fk_smart_import_profiles_updated_by_user_id",
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_smart_import_profiles"),
        sa.UniqueConstraint(
            "public_id",
            name="uq_smart_import_profiles_public_id",
        ),
        sa.UniqueConstraint(
            "organization_id",
            "schema_kind",
            "name",
            name="uq_smart_import_profiles_tenant_schema_name",
        ),
    )
    op.create_index(
        "ix_smart_import_profiles_organization_id",
        "smart_import_profiles",
        ["organization_id"],
    )
    op.create_index(
        "ix_smart_import_profiles_tenant_fingerprint",
        "smart_import_profiles",
        ["organization_id", "schema_kind", "header_fingerprint"],
    )
    op.create_index(
        "ix_smart_import_profiles_tenant_active",
        "smart_import_profiles",
        ["organization_id", "active"],
    )

    _create_rls()
    _grant_runtime_access()


def downgrade() -> None:
    _revoke_runtime_access()
    _drop_rls()
    op.drop_table("smart_import_profiles")
