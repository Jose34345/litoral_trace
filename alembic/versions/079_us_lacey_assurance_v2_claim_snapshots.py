"""Persist exact source-linked candidate snapshots without mutating Engine runs.

Revision ID: 079_us_lacey_assurance_v2_claim_snapshots
Revises: 078_us_lacey_assurance_v2_authority
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "079_us_lacey_assurance_v2_claim_snapshots"
down_revision = "078_us_lacey_assurance_v2_authority"
branch_labels = None
depends_on = None

TABLE = "us_lacey_assurance_v2_claim_snapshots"
RUNTIME = "litoral_trace_app"
GUC = "NULLIF(current_setting('app.current_organization_id', true), '')::integer"


def upgrade() -> None:
    op.create_unique_constraint(
        "uq_lacey_source_revision_id_org_operation_v2", "us_lacey_source_set_revisions",
        ["id", "organization_id", "operation_id"],
    )
    op.create_table(
        TABLE,
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("operation_id", sa.Integer(), nullable=False),
        sa.Column("source_set_revision_id", sa.Integer(), nullable=False),
        sa.Column("source_set_fingerprint", sa.String(64), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("document_count", sa.Integer(), nullable=False),
        sa.Column("claim_count", sa.Integer(), nullable=False),
        sa.Column("snapshot_json", postgresql.JSONB(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(
            ["operation_id", "organization_id"],
            ["us_lacey_operations.id", "us_lacey_operations.organization_id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["source_set_revision_id", "organization_id", "operation_id"],
            ["us_lacey_source_set_revisions.id", "us_lacey_source_set_revisions.organization_id",
             "us_lacey_source_set_revisions.operation_id"],
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint("id", "organization_id", name="uq_assurance_v2_claim_snapshot_tenant"),
        sa.UniqueConstraint("organization_id", "operation_id", "source_set_revision_id",
                            "source_set_fingerprint", name="uq_assurance_v2_claim_snapshot_source"),
        sa.CheckConstraint("status IN ('CANDIDATE_ONLY','FAILED')",
                           name="ck_assurance_v2_claim_snapshot_status"),
        sa.CheckConstraint("document_count > 0", name="ck_assurance_v2_claim_snapshot_documents"),
        sa.CheckConstraint("claim_count >= 0", name="ck_assurance_v2_claim_snapshot_claims"),
        sa.CheckConstraint("length(source_set_fingerprint) = 64",
                           name="ck_assurance_v2_claim_snapshot_fingerprint"),
    )
    op.create_index("ix_assurance_v2_claim_snapshot_case", TABLE,
                    ["organization_id", "operation_id", "source_set_revision_id"])
    predicate = f"organization_id = {GUC}"
    op.execute(f"ALTER TABLE public.{TABLE} ENABLE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE public.{TABLE} FORCE ROW LEVEL SECURITY")
    op.execute(f"CREATE POLICY {TABLE}_tenant_select ON public.{TABLE} "
               f"FOR SELECT TO {RUNTIME} USING ({predicate})")
    op.execute(f"CREATE POLICY {TABLE}_tenant_insert ON public.{TABLE} "
               f"FOR INSERT TO {RUNTIME} WITH CHECK ({predicate})")
    op.execute(f"REVOKE ALL PRIVILEGES ON TABLE public.{TABLE} FROM PUBLIC, anon, authenticated")
    op.execute(f"GRANT SELECT, INSERT ON TABLE public.{TABLE} TO {RUNTIME}")
    op.execute(f"REVOKE UPDATE, DELETE ON TABLE public.{TABLE} FROM {RUNTIME}")
    op.execute(f"REVOKE ALL PRIVILEGES ON SEQUENCE public.{TABLE}_id_seq "
               "FROM PUBLIC, anon, authenticated")
    op.execute(f"GRANT USAGE, SELECT ON SEQUENCE public.{TABLE}_id_seq TO {RUNTIME}")


def downgrade() -> None:
    op.execute(f"DROP POLICY IF EXISTS {TABLE}_tenant_insert ON public.{TABLE}")
    op.execute(f"DROP POLICY IF EXISTS {TABLE}_tenant_select ON public.{TABLE}")
    op.drop_table(TABLE)
    op.drop_constraint("uq_lacey_source_revision_id_org_operation_v2",
                       "us_lacey_source_set_revisions", type_="unique")
