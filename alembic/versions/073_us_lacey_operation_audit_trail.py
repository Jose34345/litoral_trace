"""Add immutable U.S. Lacey operation audit trail.

Revision ID: 073_us_lacey_operation_audit_trail
Revises: 072_fix_sandbox_purge_fk_order
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "073_us_lacey_operation_audit_trail"
down_revision = "072_fix_sandbox_purge_fk_order"
branch_labels = None
depends_on = None

RUNTIME_ROLE = "litoral_trace_app"
TENANT_CONTEXT_SQL = (
    "NULLIF(current_setting('app.current_organization_id', true), '')::integer"
)
TABLE = "us_lacey_operation_events"


def upgrade() -> None:
    op.create_table(
        TABLE,
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("public_id", sa.Uuid(), nullable=False),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("operation_id", sa.Integer(), nullable=False),
        sa.Column(
            "timestamp",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("actor_type", sa.String(16), nullable=False),
        sa.Column("actor_identity", sa.String(255), nullable=False),
        sa.Column("event_type", sa.String(48), nullable=False),
        sa.Column("event_key", sa.String(255), nullable=True),
        sa.Column(
            "details",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.ForeignKeyConstraint(
            ["operation_id", "organization_id"],
            ["us_lacey_operations.id", "us_lacey_operations.organization_id"],
            name="fk_us_lacey_operation_events_operation_tenant",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint(
            "public_id",
            name="uq_us_lacey_operation_events_public_id",
        ),
        sa.UniqueConstraint(
            "id",
            "organization_id",
            name="uq_us_lacey_operation_events_id_org",
        ),
        sa.UniqueConstraint(
            "organization_id",
            "operation_id",
            "event_key",
            name="uq_us_lacey_operation_events_event_key",
        ),
        sa.CheckConstraint(
            "actor_type IN ('SYSTEM','USER')",
            name="ck_us_lacey_operation_events_actor_type",
        ),
        sa.CheckConstraint(
            "event_type IN ("
            "'CREATED','DOCUMENT_UPLOADED','EXTRACTED','HUMAN_REVIEW',"
            "'EVIDENCE_REUSED','PACKAGE_GENERATED'"
            ")",
            name="ck_us_lacey_operation_events_event_type",
        ),
    )
    op.create_index(
        "ix_us_lacey_operation_events_org_operation_time",
        TABLE,
        ["organization_id", "operation_id", "timestamp", "id"],
    )

    predicate = f"organization_id = {TENANT_CONTEXT_SQL}"
    op.execute(f"ALTER TABLE public.{TABLE} ENABLE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE public.{TABLE} FORCE ROW LEVEL SECURITY")
    op.execute(
        f"CREATE POLICY {TABLE}_tenant_select ON public.{TABLE} "
        f"FOR SELECT TO {RUNTIME_ROLE} USING ({predicate})"
    )
    op.execute(
        f"CREATE POLICY {TABLE}_tenant_insert ON public.{TABLE} "
        f"FOR INSERT TO {RUNTIME_ROLE} WITH CHECK ({predicate})"
    )

    # Runtime may append and read the record, never rewrite or erase history.
    op.execute(
        f"REVOKE ALL PRIVILEGES ON TABLE public.{TABLE} "
        "FROM PUBLIC, anon, authenticated"
    )
    op.execute(
        f"GRANT SELECT, INSERT ON TABLE public.{TABLE} TO {RUNTIME_ROLE}"
    )
    op.execute(
        f"REVOKE UPDATE, DELETE ON TABLE public.{TABLE} FROM {RUNTIME_ROLE}"
    )
    op.execute(
        f"REVOKE ALL PRIVILEGES ON SEQUENCE public.{TABLE}_id_seq "
        "FROM PUBLIC, anon, authenticated"
    )
    op.execute(
        f"GRANT USAGE, SELECT ON SEQUENCE public.{TABLE}_id_seq TO {RUNTIME_ROLE}"
    )


def downgrade() -> None:
    op.execute(
        f"DROP POLICY IF EXISTS {TABLE}_tenant_insert ON public.{TABLE}"
    )
    op.execute(
        f"DROP POLICY IF EXISTS {TABLE}_tenant_select ON public.{TABLE}"
    )
    op.drop_index(
        "ix_us_lacey_operation_events_org_operation_time",
        table_name=TABLE,
    )
    op.drop_table(TABLE)
