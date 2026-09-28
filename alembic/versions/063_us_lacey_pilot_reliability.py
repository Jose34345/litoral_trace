"""Add privacy-bounded pilot reliability snapshots and incidents.

Revision ID: 063_us_lacey_pilot_reliability
Revises: 062_requeue_pack2_after_deploy
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "063_us_lacey_pilot_reliability"
down_revision = "062_requeue_pack2_after_deploy"
branch_labels = None
depends_on = None

RUNTIME_ROLE = "litoral_trace_app"
PLATFORM_ROLE = "litoral_trace_platform_definer"
WORKER_ROLE = "litoral_trace_worker_executor"


def upgrade() -> None:
    op.create_table(
        "us_lacey_pilot_quality_snapshots",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("operation_id", sa.Integer(), nullable=False),
        sa.Column(
            "attribution_session_id",
            sa.Uuid(),
            sa.ForeignKey("us_lacey_outreach_sessions.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("trigger", sa.String(24), nullable=False),
        sa.Column("source_set_fingerprint", sa.String(64), nullable=True),
        sa.Column("engine_version", sa.String(100), nullable=True),
        sa.Column("canonical_publisher_version", sa.String(100), nullable=True),
        sa.Column("document_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("valid_document_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("logical_document_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column(
            "document_type_counts",
            sa.JSON(),
            nullable=False,
            server_default=sa.text("'{}'::json"),
        ),
        sa.Column("commercial_line_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("canonical_line_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("auto_resolved_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("action_required_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("confirmed_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("conflict_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("total_field_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("processing_duration_ms", sa.BigInteger(), nullable=True),
        sa.Column("export_ready", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(
            ["operation_id", "organization_id"],
            ["us_lacey_operations.id", "us_lacey_operations.organization_id"],
            name="fk_lacey_pilot_snapshot_operation_tenant",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint(
            "id",
            "organization_id",
            name="uq_lacey_pilot_snapshot_id_org",
        ),
        sa.CheckConstraint(
            "trigger IN ('INITIAL_PROCESS','REPROCESS','WATCHDOG')",
            name="ck_lacey_pilot_snapshot_trigger",
        ),
        sa.CheckConstraint(
            "document_count >= 0 AND valid_document_count >= 0 "
            "AND logical_document_count >= 0",
            name="ck_lacey_pilot_snapshot_document_counts",
        ),
        sa.CheckConstraint(
            "commercial_line_count >= 0 AND canonical_line_count >= 0",
            name="ck_lacey_pilot_snapshot_line_counts",
        ),
        sa.CheckConstraint(
            "auto_resolved_count >= 0 AND action_required_count >= 0 "
            "AND confirmed_count >= 0 AND conflict_count >= 0 "
            "AND total_field_count >= 0",
            name="ck_lacey_pilot_snapshot_field_counts",
        ),
        sa.CheckConstraint(
            "processing_duration_ms IS NULL OR processing_duration_ms >= 0",
            name="ck_lacey_pilot_snapshot_processing_ms",
        ),
    )
    op.create_index(
        "ix_lacey_pilot_snapshot_org_operation_created",
        "us_lacey_pilot_quality_snapshots",
        ["organization_id", "operation_id", "created_at"],
    )
    op.create_index(
        "ix_lacey_pilot_snapshot_attribution_created",
        "us_lacey_pilot_quality_snapshots",
        ["attribution_session_id", "created_at"],
    )

    op.create_table(
        "us_lacey_pilot_incidents",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("incident_public_id", sa.Uuid(), nullable=False),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("operation_id", sa.Integer(), nullable=False),
        sa.Column("quality_snapshot_id", sa.BigInteger(), nullable=False),
        sa.Column(
            "attribution_session_id",
            sa.Uuid(),
            sa.ForeignKey("us_lacey_outreach_sessions.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("detector_code", sa.String(64), nullable=False),
        sa.Column("severity", sa.String(8), nullable=False),
        sa.Column("status", sa.String(24), nullable=False, server_default="OPEN"),
        sa.Column("fingerprint", sa.String(64), nullable=False),
        sa.Column(
            "diagnostic_manifest",
            sa.JSON(),
            nullable=False,
            server_default=sa.text("'{}'::json"),
        ),
        sa.Column("acknowledged_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("fixed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("closed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(
            ["operation_id", "organization_id"],
            ["us_lacey_operations.id", "us_lacey_operations.organization_id"],
            name="fk_lacey_pilot_incident_operation_tenant",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["quality_snapshot_id", "organization_id"],
            [
                "us_lacey_pilot_quality_snapshots.id",
                "us_lacey_pilot_quality_snapshots.organization_id",
            ],
            name="fk_lacey_pilot_incident_snapshot_tenant",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint(
            "incident_public_id",
            name="uq_lacey_pilot_incident_public_id",
        ),
        sa.UniqueConstraint(
            "fingerprint",
            name="uq_lacey_pilot_incident_fingerprint",
        ),
        sa.CheckConstraint(
            "severity IN ('P0','P1')",
            name="ck_lacey_pilot_incident_severity",
        ),
        sa.CheckConstraint(
            "status IN ('OPEN','ACKNOWLEDGED','FIX_IN_PROGRESS','FIX_READY',"
            "'FIX_DEPLOYED','RETESTED','CLOSED')",
            name="ck_lacey_pilot_incident_status",
        ),
        sa.CheckConstraint(
            "char_length(fingerprint) = 64",
            name="ck_lacey_pilot_incident_fingerprint_length",
        ),
    )
    op.create_index(
        "ix_lacey_pilot_incident_org_status_created",
        "us_lacey_pilot_incidents",
        ["organization_id", "status", "created_at"],
    )
    op.create_index(
        "ix_lacey_pilot_incident_operation_created",
        "us_lacey_pilot_incidents",
        ["organization_id", "operation_id", "created_at"],
    )
    op.create_index(
        "ix_lacey_pilot_incident_attribution_created",
        "us_lacey_pilot_incidents",
        ["attribution_session_id", "created_at"],
    )

    for table_name in (
        "us_lacey_pilot_quality_snapshots",
        "us_lacey_pilot_incidents",
    ):
        op.execute(f"REVOKE ALL ON TABLE public.{table_name} FROM PUBLIC")
        op.execute(f"REVOKE ALL ON TABLE public.{table_name} FROM {RUNTIME_ROLE}")

    op.execute(
        f"GRANT SELECT, INSERT ON TABLE public.us_lacey_pilot_quality_snapshots "
        f"TO {WORKER_ROLE}"
    )
    op.execute(
        f"GRANT SELECT, INSERT, UPDATE ON TABLE public.us_lacey_pilot_incidents "
        f"TO {WORKER_ROLE}"
    )
    op.execute(
        f"GRANT USAGE, SELECT ON SEQUENCE "
        f"public.us_lacey_pilot_quality_snapshots_id_seq TO {WORKER_ROLE}"
    )
    op.execute(
        f"GRANT USAGE, SELECT ON SEQUENCE "
        f"public.us_lacey_pilot_incidents_id_seq TO {WORKER_ROLE}"
    )

    # Future Pilot Watch is a platform-admin surface. Grant read-only capability
    # now without exposing these tables to the customer web runtime.
    op.execute(
        f"GRANT SELECT ON TABLE public.us_lacey_pilot_quality_snapshots "
        f"TO {PLATFORM_ROLE}"
    )
    op.execute(
        f"GRANT SELECT ON TABLE public.us_lacey_pilot_incidents "
        f"TO {PLATFORM_ROLE}"
    )


def downgrade() -> None:
    op.drop_index(
        "ix_lacey_pilot_incident_attribution_created",
        table_name="us_lacey_pilot_incidents",
    )
    op.drop_index(
        "ix_lacey_pilot_incident_operation_created",
        table_name="us_lacey_pilot_incidents",
    )
    op.drop_index(
        "ix_lacey_pilot_incident_org_status_created",
        table_name="us_lacey_pilot_incidents",
    )
    op.drop_table("us_lacey_pilot_incidents")

    op.drop_index(
        "ix_lacey_pilot_snapshot_attribution_created",
        table_name="us_lacey_pilot_quality_snapshots",
    )
    op.drop_index(
        "ix_lacey_pilot_snapshot_org_operation_created",
        table_name="us_lacey_pilot_quality_snapshots",
    )
    op.drop_table("us_lacey_pilot_quality_snapshots")
