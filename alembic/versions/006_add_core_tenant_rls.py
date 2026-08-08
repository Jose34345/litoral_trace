"""Add core tenant row-level security.

Revision ID: 006_add_core_tenant_rls
Revises: 005_add_user_sessions
Create Date: 2026-08-08 00:00:00.000000
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op


revision: str = "006_add_core_tenant_rls"
down_revision: Union[str, Sequence[str], None] = "005_add_user_sessions"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


TENANT_CONTEXT_SQL = (
    "NULLIF(current_setting('app.current_organization_id', true), '')::integer"
)


DIRECT_TENANT_TABLES: tuple[tuple[str, str], ...] = (
    ("organizations", "id"),
    ("lotes", "organization_id"),
    ("satellite_ndvi_observations", "organization_id"),
    ("api_keys", "organization_id"),
    ("licenses", "organization_id"),
    ("audit_logs", "organization_id"),
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


def upgrade() -> None:
    for table_name, column_name in DIRECT_TENANT_TABLES:
        _create_direct_tenant_policies(table_name, column_name)


def downgrade() -> None:
    for table_name, _ in reversed(DIRECT_TENANT_TABLES):
        _drop_direct_tenant_policies(table_name)
