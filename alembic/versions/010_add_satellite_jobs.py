"""Add durable satellite jobs with tenant RLS.

Revision ID: 010_add_satellite_jobs
Revises: 009_harden_platform_control_plane_audit
Create Date: 2026-08-09 00:00:00.000000
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "010_add_satellite_jobs"
down_revision: Union[str, Sequence[str], None] = "009_harden_platform_control_plane_audit"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


RUNTIME_ROLE = "litoral_trace_app"
TENANT_CONTEXT_SQL = (
    "NULLIF(current_setting('app.current_organization_id', true), '')::integer"
)


def _create_satellite_job_rls() -> None:
    tenant_match_sql = f"organization_id = {TENANT_CONTEXT_SQL}"

    op.execute("ALTER TABLE public.satellite_jobs ENABLE ROW LEVEL SECURITY")
    op.execute("ALTER TABLE public.satellite_jobs FORCE ROW LEVEL SECURITY")
    op.execute(
        "CREATE POLICY satellite_jobs_tenant_select "
        "ON public.satellite_jobs "
        "FOR SELECT "
        f"USING ({tenant_match_sql})"
    )
    op.execute(
        "CREATE POLICY satellite_jobs_tenant_insert "
        "ON public.satellite_jobs "
        "FOR INSERT "
        f"WITH CHECK ({tenant_match_sql})"
    )
    op.execute(
        "CREATE POLICY satellite_jobs_tenant_update "
        "ON public.satellite_jobs "
        "FOR UPDATE "
        f"USING ({tenant_match_sql}) "
        f"WITH CHECK ({tenant_match_sql})"
    )
    op.execute(
        "CREATE POLICY satellite_jobs_tenant_delete "
        "ON public.satellite_jobs "
        "FOR DELETE "
        f"USING ({tenant_match_sql})"
    )


def _drop_satellite_job_rls() -> None:
    op.execute(
        "DROP POLICY IF EXISTS satellite_jobs_tenant_delete ON public.satellite_jobs"
    )
    op.execute(
        "DROP POLICY IF EXISTS satellite_jobs_tenant_update ON public.satellite_jobs"
    )
    op.execute(
        "DROP POLICY IF EXISTS satellite_jobs_tenant_insert ON public.satellite_jobs"
    )
    op.execute(
        "DROP POLICY IF EXISTS satellite_jobs_tenant_select ON public.satellite_jobs"
    )
    op.execute("ALTER TABLE public.satellite_jobs NO FORCE ROW LEVEL SECURITY")
    op.execute("ALTER TABLE public.satellite_jobs DISABLE ROW LEVEL SECURITY")


def _grant_runtime_table_access() -> None:
    op.execute("REVOKE ALL ON TABLE public.satellite_jobs FROM PUBLIC")
    op.execute(
        "GRANT SELECT, INSERT, UPDATE, DELETE "
        "ON TABLE public.satellite_jobs "
        f"TO {RUNTIME_ROLE}"
    )
    op.execute(
        "GRANT USAGE, SELECT "
        "ON SEQUENCE public.satellite_jobs_id_seq "
        f"TO {RUNTIME_ROLE}"
    )


def _revoke_runtime_table_access() -> None:
    op.execute(
        "REVOKE USAGE, SELECT "
        "ON SEQUENCE public.satellite_jobs_id_seq "
        f"FROM {RUNTIME_ROLE}"
    )
    op.execute(
        "REVOKE SELECT, INSERT, UPDATE, DELETE "
        "ON TABLE public.satellite_jobs "
        f"FROM {RUNTIME_ROLE}"
    )


def upgrade() -> None:
    op.create_unique_constraint(
        "uq_lotes_id_organization_id",
        "lotes",
        ["id", "organization_id"],
    )

    op.create_table(
        "satellite_jobs",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("lote_id", sa.Integer(), nullable=True),
        sa.Column(
            "job_type",
            sa.String(length=50),
            nullable=False,
            server_default="ndvi_timeseries",
        ),
        sa.Column(
            "status",
            sa.String(length=20),
            nullable=False,
            server_default="queued",
        ),
        sa.Column(
            "attempt_count",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
        sa.Column(
            "max_attempts",
            sa.Integer(),
            nullable=False,
            server_default="3",
        ),
        sa.Column(
            "next_attempt_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column("locked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("locked_by", sa.String(length=255), nullable=True),
        sa.Column("heartbeat_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_code", sa.String(length=100), nullable=True),
        sa.Column("error_message", sa.String(length=1024), nullable=True),
        sa.Column("idempotency_key", sa.String(length=255), nullable=True),
        sa.Column("request_start_date", sa.Date(), nullable=True),
        sa.Column("request_end_date", sa.Date(), nullable=True),
        sa.Column("max_cloud_pct", sa.Float(), nullable=True),
        sa.Column("geometry_hash", sa.String(length=64), nullable=True),
        sa.Column("algorithm_version", sa.String(length=50), nullable=True),
        sa.Column("polygon_wkt_snapshot", sa.Text(), nullable=True),
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
            "status IN ('queued', 'running', 'succeeded', 'failed')",
            name="ck_satellite_jobs_status",
        ),
        sa.CheckConstraint(
            "job_type IN ('ndvi_timeseries')",
            name="ck_satellite_jobs_job_type",
        ),
        sa.CheckConstraint(
            "attempt_count >= 0",
            name="ck_satellite_jobs_attempt_count_non_negative",
        ),
        sa.CheckConstraint(
            "max_attempts > 0",
            name="ck_satellite_jobs_max_attempts_positive",
        ),
        sa.CheckConstraint(
            "attempt_count <= max_attempts",
            name="ck_satellite_jobs_attempt_count_lte_max_attempts",
        ),
        sa.CheckConstraint(
            "("
            "max_cloud_pct IS NULL "
            "OR (max_cloud_pct >= 0.0 AND max_cloud_pct <= 100.0)"
            ")",
            name="ck_satellite_jobs_max_cloud_pct_range",
        ),
        sa.CheckConstraint(
            "("
            "job_type <> 'ndvi_timeseries' "
            "OR ("
            "lote_id IS NOT NULL "
            "AND request_start_date IS NOT NULL "
            "AND request_end_date IS NOT NULL "
            "AND max_cloud_pct IS NOT NULL "
            "AND geometry_hash IS NOT NULL "
            "AND algorithm_version IS NOT NULL "
            "AND polygon_wkt_snapshot IS NOT NULL"
            ")"
            ")",
            name="ck_satellite_jobs_ndvi_timeseries_payload",
        ),
        sa.CheckConstraint(
            "("
            "request_start_date IS NULL "
            "OR request_end_date IS NULL "
            "OR request_start_date <= request_end_date"
            ")",
            name="ck_satellite_jobs_date_window",
        ),
        sa.ForeignKeyConstraint(
            ["organization_id"],
            ["organizations.id"],
            name="fk_satellite_jobs_organization_id",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["lote_id", "organization_id"],
            ["lotes.id", "lotes.organization_id"],
            name="fk_satellite_jobs_lote_tenant",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "organization_id",
            "idempotency_key",
            name="uq_satellite_jobs_tenant_idempotency_key",
        ),
        sa.UniqueConstraint(
            "id",
            "organization_id",
            name="uq_satellite_jobs_id_organization_id",
        ),
    )

    op.create_index(
        "ix_satellite_jobs_organization_id",
        "satellite_jobs",
        ["organization_id"],
    )
    op.create_index(
        "ix_satellite_jobs_lote_id",
        "satellite_jobs",
        ["lote_id"],
    )
    op.create_index(
        "ix_satellite_jobs_status_next_attempt_created_at",
        "satellite_jobs",
        ["status", "next_attempt_at", "created_at"],
    )
    op.create_index(
        "ix_satellite_jobs_tenant_history",
        "satellite_jobs",
        ["organization_id", "created_at"],
    )
    op.create_index(
        "ix_satellite_jobs_tenant_lote_history",
        "satellite_jobs",
        ["organization_id", "lote_id", "created_at"],
    )

    op.add_column(
        "satellite_ndvi_observations",
        sa.Column("satellite_job_id", sa.Integer(), nullable=True),
    )
    op.create_index(
        "ix_sat_obs_satellite_job_id",
        "satellite_ndvi_observations",
        ["satellite_job_id"],
    )
    op.create_foreign_key(
        "fk_satellite_obs_job_tenant",
        "satellite_ndvi_observations",
        "satellite_jobs",
        ["satellite_job_id", "organization_id"],
        ["id", "organization_id"],
    )

    _create_satellite_job_rls()
    _grant_runtime_table_access()


def downgrade() -> None:
    _revoke_runtime_table_access()
    _drop_satellite_job_rls()

    op.drop_constraint(
        "fk_satellite_obs_job_tenant",
        "satellite_ndvi_observations",
        type_="foreignkey",
    )
    op.drop_index(
        "ix_sat_obs_satellite_job_id",
        table_name="satellite_ndvi_observations",
    )
    op.drop_column("satellite_ndvi_observations", "satellite_job_id")

    op.drop_index(
        "ix_satellite_jobs_tenant_lote_history",
        table_name="satellite_jobs",
    )
    op.drop_index(
        "ix_satellite_jobs_tenant_history",
        table_name="satellite_jobs",
    )
    op.drop_index(
        "ix_satellite_jobs_status_next_attempt_created_at",
        table_name="satellite_jobs",
    )
    op.drop_index(
        "ix_satellite_jobs_lote_id",
        table_name="satellite_jobs",
    )
    op.drop_index(
        "ix_satellite_jobs_organization_id",
        table_name="satellite_jobs",
    )
    op.drop_table("satellite_jobs")

    op.drop_constraint(
        "uq_lotes_id_organization_id",
        "lotes",
        type_="unique",
    )
