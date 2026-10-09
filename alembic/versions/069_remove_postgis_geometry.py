"""Remove PostGIS geometry and retire satellite persistence.

Revision ID: 069_remove_postgis_geometry
Revises: 068_supabase_public_api_hardening
"""

from __future__ import annotations

from alembic import op


revision = "069_remove_postgis_geometry"
down_revision = "068_supabase_public_api_hardening"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_index(
        "ix_lotes_geom_gist",
        table_name="lotes",
        schema="public",
    )
    op.drop_column(
        "lotes",
        "geom",
        schema="public",
    )

    op.drop_table(
        "satellite_ndvi_observations",
        schema="public",
    )
    op.drop_table(
        "satellite_job_results",
        schema="public",
    )
    op.drop_table(
        "satellite_jobs",
        schema="public",
    )

    op.execute(
        "DROP FUNCTION IF EXISTS "
        "public.worker_claim_next_satellite_job(text);"
    )


def downgrade() -> None:
    raise RuntimeError(
        "069_remove_postgis_geometry is intentionally forward-only; "
        "restore the pre-migration backup to recover retired satellite "
        "persistence and PostGIS geometry."
    )
