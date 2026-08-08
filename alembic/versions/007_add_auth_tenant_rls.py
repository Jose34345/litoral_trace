"""Add auth bootstrap functions and tenant RLS for users/session tables.

Revision ID: 007_add_auth_tenant_rls
Revises: 006_add_core_tenant_rls
Create Date: 2026-08-08 12:00:00.000000
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op


revision: str = "007_add_auth_tenant_rls"
down_revision: Union[str, Sequence[str], None] = "006_add_core_tenant_rls"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


TENANT_CONTEXT_SQL = (
    "NULLIF(current_setting('app.current_organization_id', true), '')::integer"
)

AUTH_DIRECT_TENANT_TABLES: tuple[tuple[str, str], ...] = (
    ("users", "organization_id"),
    ("user_sessions", "organization_id"),
)


def _create_direct_tenant_policies(table_name: str, column_name: str) -> None:
    tenant_match_sql = f"{column_name} = {TENANT_CONTEXT_SQL}"

    op.execute(f"ALTER TABLE public.{table_name} ENABLE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE public.{table_name} FORCE ROW LEVEL SECURITY")
    op.execute(
        f"CREATE POLICY {table_name}_tenant_select "
        f"ON public.{table_name} "
        f"FOR SELECT "
        f"USING ({tenant_match_sql})"
    )
    op.execute(
        f"CREATE POLICY {table_name}_tenant_insert "
        f"ON public.{table_name} "
        f"FOR INSERT "
        f"WITH CHECK ({tenant_match_sql})"
    )
    op.execute(
        f"CREATE POLICY {table_name}_tenant_update "
        f"ON public.{table_name} "
        f"FOR UPDATE "
        f"USING ({tenant_match_sql}) "
        f"WITH CHECK ({tenant_match_sql})"
    )
    op.execute(
        f"CREATE POLICY {table_name}_tenant_delete "
        f"ON public.{table_name} "
        f"FOR DELETE "
        f"USING ({tenant_match_sql})"
    )


def _drop_direct_tenant_policies(table_name: str) -> None:
    op.execute(
        f"DROP POLICY IF EXISTS {table_name}_tenant_delete ON public.{table_name}"
    )
    op.execute(
        f"DROP POLICY IF EXISTS {table_name}_tenant_update ON public.{table_name}"
    )
    op.execute(
        f"DROP POLICY IF EXISTS {table_name}_tenant_insert ON public.{table_name}"
    )
    op.execute(
        f"DROP POLICY IF EXISTS {table_name}_tenant_select ON public.{table_name}"
    )
    op.execute(f"ALTER TABLE public.{table_name} NO FORCE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE public.{table_name} DISABLE ROW LEVEL SECURITY")


def _create_auth_bootstrap_functions() -> None:
    op.execute(
        """
        CREATE FUNCTION public.bootstrap_auth_user_by_username(lookup_username text)
        RETURNS TABLE (
            id integer,
            organization_id integer,
            password_hash text,
            is_active boolean
        )
        LANGUAGE sql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
            SELECT
                users.id,
                users.organization_id,
                users.password_hash,
                users.is_active
            FROM public.users AS users
            WHERE users.username = lookup_username
        $$;
        """
    )
    op.execute(
        """
        CREATE FUNCTION public.bootstrap_auth_session_by_token_hash(lookup_token_hash text)
        RETURNS TABLE (
            id integer,
            user_id integer,
            organization_id integer
        )
        LANGUAGE sql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
            SELECT
                user_sessions.id,
                user_sessions.user_id,
                user_sessions.organization_id
            FROM public.user_sessions AS user_sessions
            WHERE user_sessions.token_hash = lookup_token_hash
            FOR UPDATE
        $$;
        """
    )
    op.execute(
        "REVOKE ALL ON FUNCTION public.bootstrap_auth_user_by_username(text) "
        "FROM PUBLIC"
    )
    op.execute(
        "REVOKE ALL ON FUNCTION public.bootstrap_auth_session_by_token_hash(text) "
        "FROM PUBLIC"
    )
    op.execute(
        "GRANT EXECUTE ON FUNCTION public.bootstrap_auth_user_by_username(text) "
        "TO litoral_trace_app"
    )
    op.execute(
        "GRANT EXECUTE ON FUNCTION public.bootstrap_auth_session_by_token_hash(text) "
        "TO litoral_trace_app"
    )


def _drop_auth_bootstrap_functions() -> None:
    op.execute(
        "REVOKE ALL ON FUNCTION public.bootstrap_auth_session_by_token_hash(text) "
        "FROM litoral_trace_app"
    )
    op.execute(
        "REVOKE ALL ON FUNCTION public.bootstrap_auth_user_by_username(text) "
        "FROM litoral_trace_app"
    )
    op.execute("DROP FUNCTION IF EXISTS public.bootstrap_auth_session_by_token_hash(text)")
    op.execute("DROP FUNCTION IF EXISTS public.bootstrap_auth_user_by_username(text)")


def upgrade() -> None:
    _create_auth_bootstrap_functions()
    for table_name, column_name in AUTH_DIRECT_TENANT_TABLES:
        _create_direct_tenant_policies(table_name, column_name)


def downgrade() -> None:
    for table_name, _ in reversed(AUTH_DIRECT_TENANT_TABLES):
        _drop_direct_tenant_policies(table_name)
    _drop_auth_bootstrap_functions()
