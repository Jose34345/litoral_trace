"""Add durable worker stage progress and watchdog timestamps.

Revision ID: 060_us_lacey_processing_stages
Revises: 059_us_lacey_outreach_attribution
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "060_us_lacey_processing_stages"
down_revision = "059_us_lacey_outreach_attribution"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "us_lacey_processing_jobs",
        sa.Column("current_stage", sa.String(length=64), nullable=True),
    )
    op.add_column(
        "us_lacey_processing_jobs",
        sa.Column("stage_started_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "ix_us_lacey_processing_jobs_stage_watchdog",
        "us_lacey_processing_jobs",
        ["status", "stage_started_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_us_lacey_processing_jobs_stage_watchdog",
        table_name="us_lacey_processing_jobs",
    )
    op.drop_column("us_lacey_processing_jobs", "stage_started_at")
    op.drop_column("us_lacey_processing_jobs", "current_stage")
