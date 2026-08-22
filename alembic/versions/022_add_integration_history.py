"""Add immutable external payload history and integration actor attribution.

Revision ID: 022_add_integration_history
Revises: 021_add_integration_core
Create Date: 2026-08-22 20:28:00.000000
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "022_add_integration_history"
down_revision: Union[str, Sequence[str], None] = "021_add_integration_core"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

RUNTIME_ROLE = "litoral_trace_app"
WORKER_EXECUTOR_ROLE = "litoral_trace_worker_executor"
TENANT_CONTEXT_SQL = "NULLIF(current_setting('app.current_organization_id', true), '')::integer"
TABLE = "external_entity_versions"


def upgrade() -> None:
    op.create_table(
        TABLE,
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.Uuid(), nullable=False, server_default=sa.text("gen_random_uuid()")),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("external_entity_id", sa.Integer(), nullable=False),
        sa.Column("sync_run_id", sa.Integer(), nullable=False),
        sa.Column("payload_hash", sa.String(length=64), nullable=False),
        sa.Column("payload_json", sa.JSON(), nullable=False),
        sa.Column("normalized_json", sa.JSON(), nullable=False),
        sa.Column("source_updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.PrimaryKeyConstraint("id", name="pk_external_entity_versions"),
        sa.UniqueConstraint("id", "organization_id", name="uq_external_entity_versions_id_org"),
        sa.UniqueConstraint("public_id", name="uq_external_entity_versions_public_id"),
        sa.UniqueConstraint("external_entity_id", "payload_hash", name="uq_external_entity_versions_entity_hash"),
        sa.ForeignKeyConstraint(
            ["external_entity_id", "organization_id"],
            ["external_entities.id", "external_entities.organization_id"],
            name="fk_external_entity_versions_entity_tenant",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["sync_run_id", "organization_id"],
            ["integration_sync_runs.id", "integration_sync_runs.organization_id"],
            name="fk_external_entity_versions_sync_run_tenant",
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint("length(payload_hash) = 64", name="ck_external_entity_versions_payload_hash"),
    )
    op.create_index(
        "ix_external_entity_versions_tenant_entity_created",
        TABLE,
        ["organization_id", "external_entity_id", "created_at"],
    )

    tenant_match = f"organization_id = {TENANT_CONTEXT_SQL}"
    op.execute(f"ALTER TABLE public.{TABLE} ENABLE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE public.{TABLE} FORCE ROW LEVEL SECURITY")
    op.execute(
        f"CREATE POLICY {TABLE}_tenant_select ON public.{TABLE} "
        f"FOR SELECT USING ({tenant_match})"
    )
    op.execute(
        f"CREATE POLICY {TABLE}_tenant_insert ON public.{TABLE} "
        f"FOR INSERT WITH CHECK ({tenant_match})"
    )
    op.execute(f"REVOKE ALL PRIVILEGES ON TABLE public.{TABLE} FROM PUBLIC")
    op.execute(f"REVOKE ALL PRIVILEGES ON SEQUENCE public.{TABLE}_id_seq FROM PUBLIC")
    op.execute(f"GRANT SELECT, INSERT ON TABLE public.{TABLE} TO {RUNTIME_ROLE}")
    op.execute(f"GRANT USAGE, SELECT ON SEQUENCE public.{TABLE}_id_seq TO {RUNTIME_ROLE}")
    op.execute(f"REVOKE ALL PRIVILEGES ON TABLE public.{TABLE} FROM {WORKER_EXECUTOR_ROLE}")
    op.execute(f"REVOKE ALL PRIVILEGES ON SEQUENCE public.{TABLE}_id_seq FROM {WORKER_EXECUTOR_ROLE}")

    op.add_column("integration_events", sa.Column("actor_user_id", sa.Integer(), nullable=True))
    op.create_foreign_key(
        "fk_integration_events_actor_user_id",
        "integration_events",
        "users",
        ["actor_user_id"],
        ["id"],
        ondelete="SET NULL",
    )


def downgrade() -> None:
    op.drop_constraint(
        "fk_integration_events_actor_user_id",
        "integration_events",
        type_="foreignkey",
    )
    op.drop_column("integration_events", "actor_user_id")

    op.execute(f"REVOKE USAGE, SELECT ON SEQUENCE public.{TABLE}_id_seq FROM {RUNTIME_ROLE}")
    op.execute(f"REVOKE SELECT, INSERT ON TABLE public.{TABLE} FROM {RUNTIME_ROLE}")
    op.execute(f"DROP POLICY IF EXISTS {TABLE}_tenant_insert ON public.{TABLE}")
    op.execute(f"DROP POLICY IF EXISTS {TABLE}_tenant_select ON public.{TABLE}")
    op.execute(f"ALTER TABLE public.{TABLE} NO FORCE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE public.{TABLE} DISABLE ROW LEVEL SECURITY")
    op.drop_index("ix_external_entity_versions_tenant_entity_created", table_name=TABLE)
    op.drop_table(TABLE)
