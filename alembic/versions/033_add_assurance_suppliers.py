"""Add minimal tenant-scoped Assurance supplier identities.

Revision ID: 033_assurance_suppliers
Revises: 032_assurance_operational_exceptions
Create Date: 2026-08-28 11:10:00.000000
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "033_assurance_suppliers"
down_revision: Union[str, Sequence[str], None] = "032_assurance_operational_exceptions"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

RUNTIME_ROLE = "litoral_trace_app"
WORKER_EXECUTOR_ROLE = "litoral_trace_worker_executor"
TENANT_CONTEXT_SQL = (
    "NULLIF(current_setting('app.current_organization_id', true), '')::integer"
)
TABLE = "assurance_suppliers"


def _enable_rls() -> None:
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
    op.execute(
        f"CREATE POLICY {TABLE}_tenant_update ON public.{TABLE} "
        f"FOR UPDATE USING ({tenant_match}) WITH CHECK ({tenant_match})"
    )


def _grant_runtime() -> None:
    op.execute(f"REVOKE ALL PRIVILEGES ON TABLE public.{TABLE} FROM PUBLIC")
    op.execute(
        f"GRANT SELECT, INSERT, UPDATE ON TABLE public.{TABLE} TO {RUNTIME_ROLE}"
    )
    op.execute(
        f"REVOKE ALL PRIVILEGES ON TABLE public.{TABLE} FROM {WORKER_EXECUTOR_ROLE}"
    )
    sequence = f"{TABLE}_id_seq"
    op.execute(f"REVOKE ALL PRIVILEGES ON SEQUENCE public.{sequence} FROM PUBLIC")
    op.execute(
        f"GRANT USAGE, SELECT ON SEQUENCE public.{sequence} TO {RUNTIME_ROLE}"
    )
    op.execute(
        f"REVOKE ALL PRIVILEGES ON SEQUENCE public.{sequence} FROM {WORKER_EXECUTOR_ROLE}"
    )


def upgrade() -> None:
    op.create_table(
        TABLE,
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column(
            "public_id",
            sa.Uuid(),
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("cuit", sa.String(length=11), nullable=False),
        sa.Column("display_name", sa.String(length=255), nullable=True),
        sa.Column("normalized_name", sa.String(length=255), nullable=True),
        sa.Column(
            "status",
            sa.String(length=24),
            nullable=False,
            server_default="AUTO_CREATED",
        ),
        sa.Column("source_assurance_document_id", sa.Integer(), nullable=True),
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
        sa.ForeignKeyConstraint(
            ["source_assurance_document_id", "organization_id"],
            ["assurance_documents.id", "assurance_documents.organization_id"],
            name="fk_assurance_suppliers_source_document_tenant",
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint(
            "length(cuit) = 11", name="ck_assurance_suppliers_cuit_length"
        ),
        sa.CheckConstraint(
            "status IN ('AUTO_CREATED','CONFIRMED','NEEDS_REVIEW')",
            name="ck_assurance_suppliers_status",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_assurance_suppliers"),
        sa.UniqueConstraint("public_id", name="uq_assurance_suppliers_public_id"),
        sa.UniqueConstraint(
            "id", "organization_id", name="uq_assurance_suppliers_id_org"
        ),
        sa.UniqueConstraint(
            "organization_id", "cuit", name="uq_assurance_suppliers_tenant_cuit"
        ),
    )
    op.create_index(
        "ix_assurance_suppliers_organization_id", TABLE, ["organization_id"]
    )
    op.create_index(
        "ix_assurance_suppliers_tenant_name",
        TABLE,
        ["organization_id", "normalized_name"],
    )
    op.create_index(
        "ix_assurance_suppliers_tenant_status",
        TABLE,
        ["organization_id", "status"],
    )
    _enable_rls()
    _grant_runtime()


def downgrade() -> None:
    sequence = f"{TABLE}_id_seq"
    op.execute(
        f"REVOKE USAGE, SELECT ON SEQUENCE public.{sequence} FROM {RUNTIME_ROLE}"
    )
    op.execute(
        f"REVOKE SELECT, INSERT, UPDATE ON TABLE public.{TABLE} FROM {RUNTIME_ROLE}"
    )
    for action in ("update", "insert", "select"):
        op.execute(
            f"DROP POLICY IF EXISTS {TABLE}_tenant_{action} ON public.{TABLE}"
        )
    op.execute(f"ALTER TABLE public.{TABLE} NO FORCE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE public.{TABLE} DISABLE ROW LEVEL SECURITY")
    op.drop_index("ix_assurance_suppliers_tenant_status", table_name=TABLE)
    op.drop_index("ix_assurance_suppliers_tenant_name", table_name=TABLE)
    op.drop_index("ix_assurance_suppliers_organization_id", table_name=TABLE)
    op.drop_table(TABLE)
