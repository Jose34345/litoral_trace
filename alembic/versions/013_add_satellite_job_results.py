"""Add immutable satellite job result snapshots.

Revision ID: 013_add_satellite_job_results
Revises: 012_add_satellite_job_stale_recovery
Create Date: 2026-08-11 00:00:00.000000
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "013_add_satellite_job_results"
down_revision: Union[str, Sequence[str], None] = "012_add_satellite_job_stale_recovery"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


RUNTIME_ROLE = "litoral_trace_app"
TENANT_CONTEXT_SQL = (
    "NULLIF(current_setting('app.current_organization_id', true), '')::integer"
)


def _create_rls() -> None:
    tenant_match_sql = f"organization_id = {TENANT_CONTEXT_SQL}"

    op.execute("ALTER TABLE public.satellite_job_results ENABLE ROW LEVEL SECURITY")
    op.execute("ALTER TABLE public.satellite_job_results FORCE ROW LEVEL SECURITY")
    op.execute(
        "CREATE POLICY satellite_job_results_tenant_select "
        "ON public.satellite_job_results "
        "FOR SELECT "
        f"USING ({tenant_match_sql})"
    )
    op.execute(
        "CREATE POLICY satellite_job_results_tenant_insert "
        "ON public.satellite_job_results "
        "FOR INSERT "
        f"WITH CHECK ({tenant_match_sql})"
    )


def _drop_rls() -> None:
    op.execute(
        "DROP POLICY IF EXISTS satellite_job_results_tenant_insert "
        "ON public.satellite_job_results"
    )
    op.execute(
        "DROP POLICY IF EXISTS satellite_job_results_tenant_select "
        "ON public.satellite_job_results"
    )
    op.execute("ALTER TABLE public.satellite_job_results NO FORCE ROW LEVEL SECURITY")
    op.execute("ALTER TABLE public.satellite_job_results DISABLE ROW LEVEL SECURITY")


def _grant_runtime_access() -> None:
    op.execute("REVOKE ALL ON TABLE public.satellite_job_results FROM PUBLIC")
    op.execute(
        "REVOKE ALL ON TABLE public.satellite_job_results "
        f"FROM {RUNTIME_ROLE}"
    )
    op.execute(
        "GRANT SELECT, INSERT "
        "ON TABLE public.satellite_job_results "
        f"TO {RUNTIME_ROLE}"
    )


def _revoke_runtime_access() -> None:
    op.execute(
        "REVOKE SELECT, INSERT "
        "ON TABLE public.satellite_job_results "
        f"FROM {RUNTIME_ROLE}"
    )


def upgrade() -> None:
    op.create_table(
        "satellite_job_results",
        sa.Column("satellite_job_id", sa.Integer(), nullable=False),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("lote_id", sa.Integer(), nullable=False),
        sa.Column("result_schema_version", sa.String(length=50), nullable=False),
        sa.Column("geometry_hash", sa.String(length=64), nullable=False),
        sa.Column("algorithm_version", sa.String(length=50), nullable=False),
        sa.Column(
            "result_payload",
            sa.JSON().with_variant(
                postgresql.JSONB(astext_type=sa.Text()),
                "postgresql",
            ),
            nullable=False,
        ),
        sa.Column("payload_sha256", sa.String(length=64), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.ForeignKeyConstraint(
            ["organization_id"],
            ["organizations.id"],
            name="fk_satellite_job_results_organization_id",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["satellite_job_id", "organization_id"],
            ["satellite_jobs.id", "satellite_jobs.organization_id"],
            name="fk_satellite_job_results_job_tenant",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["lote_id", "organization_id"],
            ["lotes.id", "lotes.organization_id"],
            name="fk_satellite_job_results_lote_tenant",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("satellite_job_id"),
    )

    op.create_index(
        "ix_satellite_job_results_organization_id",
        "satellite_job_results",
        ["organization_id"],
    )
    op.create_index(
        "ix_satellite_job_results_lote_id",
        "satellite_job_results",
        ["lote_id"],
    )
    op.create_index(
        "ix_satellite_job_results_payload_sha256",
        "satellite_job_results",
        ["payload_sha256"],
    )
    op.create_index(
        "ix_satellite_job_results_tenant_created_at",
        "satellite_job_results",
        ["organization_id", "created_at"],
    )

    _create_rls()
    _grant_runtime_access()


def downgrade() -> None:
    _revoke_runtime_access()
    _drop_rls()

    op.drop_index(
        "ix_satellite_job_results_tenant_created_at",
        table_name="satellite_job_results",
    )
    op.drop_index(
        "ix_satellite_job_results_payload_sha256",
        table_name="satellite_job_results",
    )
    op.drop_index(
        "ix_satellite_job_results_lote_id",
        table_name="satellite_job_results",
    )
    op.drop_index(
        "ix_satellite_job_results_organization_id",
        table_name="satellite_job_results",
    )
    op.drop_table("satellite_job_results")
