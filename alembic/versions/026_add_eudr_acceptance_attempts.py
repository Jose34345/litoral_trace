"""Add auditable ACCEPTANCE-only EUDR V3 transport attempts.

Revision ID: 026_add_eudr_acceptance_attempts
Revises: 025_add_eudr_dds_candidates
Create Date: 2026-08-23 05:15:00.000000
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "026_add_eudr_acceptance_attempts"
down_revision: Union[str, Sequence[str], None] = "025_add_eudr_dds_candidates"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

RUNTIME_ROLE = "litoral_trace_app"
WORKER_EXECUTOR_ROLE = "litoral_trace_worker_executor"
TENANT_CONTEXT_SQL = "NULLIF(current_setting('app.current_organization_id', true), '')::integer"
TABLE = "eudr_acceptance_attempts"


def upgrade() -> None:
    op.create_table(
        TABLE,
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.Uuid(), nullable=False, server_default=sa.text("gen_random_uuid()")),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("candidate_id", sa.Integer(), nullable=False),
        sa.Column("environment", sa.String(length=16), nullable=False, server_default="ACCEPTANCE"),
        sa.Column("operation", sa.String(length=24), nullable=False, server_default="SUBMIT_DDS"),
        sa.Column("state", sa.String(length=24), nullable=False, server_default="PREPARED"),
        sa.Column("operator_role", sa.String(length=32), nullable=False),
        sa.Column("country_of_activity", sa.String(length=2), nullable=False),
        sa.Column("border_cross_country", sa.String(length=2), nullable=True),
        sa.Column("internal_reference_number", sa.String(length=120), nullable=False),
        sa.Column("candidate_payload_sha256", sa.String(length=64), nullable=False),
        sa.Column("wire_contract_profile", sa.String(length=120), nullable=False),
        sa.Column("wire_contract_sha256", sa.String(length=64), nullable=False),
        sa.Column("request_body_sha256", sa.String(length=64), nullable=False),
        sa.Column("envelope_sha256", sa.String(length=64), nullable=True),
        sa.Column("response_sha256", sa.String(length=64), nullable=True),
        sa.Column("request_body_bytes", sa.Integer(), nullable=False),
        sa.Column("remote_uuid", sa.String(length=120), nullable=True),
        sa.Column("remote_reference_number", sa.String(length=160), nullable=True),
        sa.Column("remote_verification_number", sa.String(length=160), nullable=True),
        sa.Column("remote_status", sa.String(length=80), nullable=True),
        sa.Column("http_status", sa.Integer(), nullable=True),
        sa.Column("error_code", sa.String(length=120), nullable=True),
        sa.Column("error_summary", sa.Text(), nullable=True),
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_by_user_id", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.PrimaryKeyConstraint("id", name="pk_eudr_acceptance_attempts"),
        sa.UniqueConstraint("id", "organization_id", name="uq_eudr_acceptance_attempts_id_org"),
        sa.UniqueConstraint("public_id", name="uq_eudr_acceptance_attempts_public_id"),
        sa.UniqueConstraint("candidate_id", "request_body_sha256", name="uq_eudr_acceptance_attempts_candidate_body"),
        sa.ForeignKeyConstraint(
            ["organization_id"], ["organizations.id"],
            name="fk_eudr_acceptance_attempts_organization_id", ondelete="RESTRICT"
        ),
        sa.ForeignKeyConstraint(
            ["candidate_id", "organization_id"],
            ["eudr_dds_candidates.id", "eudr_dds_candidates.organization_id"],
            name="fk_eudr_acceptance_attempts_candidate_tenant", ondelete="RESTRICT"
        ),
        sa.ForeignKeyConstraint(
            ["created_by_user_id"], ["users.id"],
            name="fk_eudr_acceptance_attempts_created_by_user_id", ondelete="SET NULL"
        ),
        sa.CheckConstraint("environment = 'ACCEPTANCE'", name="ck_eudr_acceptance_attempts_environment"),
        sa.CheckConstraint("operation = 'SUBMIT_DDS'", name="ck_eudr_acceptance_attempts_operation"),
        sa.CheckConstraint(
            "state IN ('PREPARED','SENT','REMOTE_ACCEPTED','REMOTE_REJECTED','TRANSPORT_ERROR')",
            name="ck_eudr_acceptance_attempts_state",
        ),
        sa.CheckConstraint("operator_role IN ('OPERATOR')", name="ck_eudr_acceptance_attempts_operator_role"),
        sa.CheckConstraint("length(candidate_payload_sha256) = 64", name="ck_eudr_acceptance_attempts_candidate_hash"),
        sa.CheckConstraint("length(wire_contract_sha256) = 64", name="ck_eudr_acceptance_attempts_contract_hash"),
        sa.CheckConstraint("length(request_body_sha256) = 64", name="ck_eudr_acceptance_attempts_body_hash"),
        sa.CheckConstraint("envelope_sha256 IS NULL OR length(envelope_sha256) = 64", name="ck_eudr_acceptance_attempts_envelope_hash"),
        sa.CheckConstraint("response_sha256 IS NULL OR length(response_sha256) = 64", name="ck_eudr_acceptance_attempts_response_hash"),
        sa.CheckConstraint("request_body_bytes > 0", name="ck_eudr_acceptance_attempts_body_bytes"),
    )
    op.create_index(
        "ix_eudr_acceptance_attempts_tenant_candidate_created",
        TABLE,
        ["organization_id", "candidate_id", "created_at"],
    )
    op.create_index(
        "ix_eudr_acceptance_attempts_tenant_state_created",
        TABLE,
        ["organization_id", "state", "created_at"],
    )

    tenant_match = f"organization_id = {TENANT_CONTEXT_SQL}"
    op.execute(f"ALTER TABLE public.{TABLE} ENABLE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE public.{TABLE} FORCE ROW LEVEL SECURITY")
    op.execute(
        f"CREATE POLICY {TABLE}_tenant_select ON public.{TABLE} FOR SELECT USING ({tenant_match})"
    )
    op.execute(
        f"CREATE POLICY {TABLE}_tenant_insert ON public.{TABLE} FOR INSERT WITH CHECK ({tenant_match})"
    )
    op.execute(
        f"CREATE POLICY {TABLE}_tenant_update ON public.{TABLE} FOR UPDATE USING ({tenant_match}) WITH CHECK ({tenant_match})"
    )

    op.execute(f"REVOKE ALL PRIVILEGES ON TABLE public.{TABLE} FROM PUBLIC")
    op.execute(f"REVOKE ALL PRIVILEGES ON SEQUENCE public.{TABLE}_id_seq FROM PUBLIC")
    op.execute(f"GRANT SELECT, INSERT, UPDATE ON TABLE public.{TABLE} TO {RUNTIME_ROLE}")
    op.execute(f"GRANT USAGE, SELECT ON SEQUENCE public.{TABLE}_id_seq TO {RUNTIME_ROLE}")
    op.execute(f"REVOKE ALL PRIVILEGES ON TABLE public.{TABLE} FROM {WORKER_EXECUTOR_ROLE}")
    op.execute(f"REVOKE ALL PRIVILEGES ON SEQUENCE public.{TABLE}_id_seq FROM {WORKER_EXECUTOR_ROLE}")


def downgrade() -> None:
    op.execute(f"REVOKE USAGE, SELECT ON SEQUENCE public.{TABLE}_id_seq FROM {RUNTIME_ROLE}")
    op.execute(f"REVOKE SELECT, INSERT, UPDATE ON TABLE public.{TABLE} FROM {RUNTIME_ROLE}")
    op.execute(f"DROP POLICY IF EXISTS {TABLE}_tenant_update ON public.{TABLE}")
    op.execute(f"DROP POLICY IF EXISTS {TABLE}_tenant_insert ON public.{TABLE}")
    op.execute(f"DROP POLICY IF EXISTS {TABLE}_tenant_select ON public.{TABLE}")
    op.execute(f"ALTER TABLE public.{TABLE} NO FORCE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE public.{TABLE} DISABLE ROW LEVEL SECURITY")
    op.drop_index("ix_eudr_acceptance_attempts_tenant_state_created", table_name=TABLE)
    op.drop_index("ix_eudr_acceptance_attempts_tenant_candidate_created", table_name=TABLE)
    op.drop_table(TABLE)
