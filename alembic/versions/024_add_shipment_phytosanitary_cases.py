"""Add shipment phytosanitary cases and SENASA/ePhyto evidence types.

Revision ID: 024_add_shipment_phytosanitary_cases
Revises: 023_add_shipment_export_cases
Create Date: 2026-08-22 23:05:00.000000
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "024_add_shipment_phytosanitary_cases"
down_revision: Union[str, Sequence[str], None] = "023_add_shipment_export_cases"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

RUNTIME_ROLE = "litoral_trace_app"
WORKER_EXECUTOR_ROLE = "litoral_trace_worker_executor"
TENANT_CONTEXT_SQL = "NULLIF(current_setting('app.current_organization_id', true), '')::integer"
TABLE = "shipment_phytosanitary_cases"
EVIDENCE_TABLE = "traceability_evidence_links"
EVIDENCE_CHECK = "ck_traceability_evidence_links_type"


def _evidence_constraint(types: str) -> str:
    return f"evidence_type IN ({types})"


def upgrade() -> None:
    op.create_table(
        TABLE,
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column(
            "public_id", sa.Uuid(), nullable=False, server_default=sa.text("gen_random_uuid()")
        ),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("shipment_id", sa.Integer(), nullable=False),
        sa.Column("certification_mode", sa.String(length=24), nullable=False),
        sa.Column("requirements_reference", sa.String(length=500), nullable=True),
        sa.Column("requirements_checked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("cert_pov_reference", sa.String(length=120), nullable=True),
        sa.Column("certificate_number", sa.String(length=120), nullable=True),
        sa.Column("ephyto_reference", sa.String(length=160), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("created_by_user_id", sa.Integer(), nullable=True),
        sa.Column("updated_by_user_id", sa.Integer(), nullable=True),
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
        sa.PrimaryKeyConstraint("id", name="pk_shipment_phytosanitary_cases"),
        sa.UniqueConstraint(
            "id", "organization_id", name="uq_shipment_phytosanitary_cases_id_org"
        ),
        sa.UniqueConstraint(
            "public_id", name="uq_shipment_phytosanitary_cases_public_id"
        ),
        sa.UniqueConstraint(
            "shipment_id", name="uq_shipment_phytosanitary_cases_shipment_id"
        ),
        sa.ForeignKeyConstraint(
            ["organization_id"],
            ["organizations.id"],
            name="fk_shipment_phytosanitary_cases_organization_id",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["shipment_id", "organization_id"],
            ["shipments.id", "shipments.organization_id"],
            name="fk_shipment_phytosanitary_cases_shipment_tenant",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["created_by_user_id"],
            ["users.id"],
            name="fk_shipment_phytosanitary_cases_created_by_user_id",
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["updated_by_user_id"],
            ["users.id"],
            name="fk_shipment_phytosanitary_cases_updated_by_user_id",
            ondelete="SET NULL",
        ),
        sa.CheckConstraint(
            "certification_mode IN ('UNASSESSED','NOT_REQUIRED','PAPER','EPHYTO')",
            name="ck_shipment_phytosanitary_cases_mode",
        ),
    )
    op.create_index(
        "ix_shipment_phytosanitary_cases_tenant_shipment",
        TABLE,
        ["organization_id", "shipment_id"],
    )
    op.create_index(
        "ix_shipment_phytosanitary_cases_tenant_mode",
        TABLE,
        ["organization_id", "certification_mode"],
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
    op.execute(
        f"CREATE POLICY {TABLE}_tenant_update ON public.{TABLE} "
        f"FOR UPDATE USING ({tenant_match}) WITH CHECK ({tenant_match})"
    )

    op.execute(f"REVOKE ALL PRIVILEGES ON TABLE public.{TABLE} FROM PUBLIC")
    op.execute(f"REVOKE ALL PRIVILEGES ON SEQUENCE public.{TABLE}_id_seq FROM PUBLIC")
    op.execute(f"GRANT SELECT, INSERT, UPDATE ON TABLE public.{TABLE} TO {RUNTIME_ROLE}")
    op.execute(f"GRANT USAGE, SELECT ON SEQUENCE public.{TABLE}_id_seq TO {RUNTIME_ROLE}")
    op.execute(f"REVOKE ALL PRIVILEGES ON TABLE public.{TABLE} FROM {WORKER_EXECUTOR_ROLE}")
    op.execute(f"REVOKE ALL PRIVILEGES ON SEQUENCE public.{TABLE}_id_seq FROM {WORKER_EXECUTOR_ROLE}")

    op.drop_constraint(EVIDENCE_CHECK, EVIDENCE_TABLE, type_="check")
    op.create_check_constraint(
        EVIDENCE_CHECK,
        EVIDENCE_TABLE,
        _evidence_constraint(
            "'ORIGIN_AUTHORIZATION','FOREST_GUIDE','FRUIT_GUIDE','REMITO','INVOICE',"
            "'CERTIFICATE','PHYTOSANITARY_CERTIFICATE','EPHYTO_XML','TRANSPORT','GEOSPATIAL',"
            "'SUPPLIER_DECLARATION','OTHER'"
        ),
    )


def downgrade() -> None:
    op.execute(
        "UPDATE public.traceability_evidence_links SET evidence_type = 'CERTIFICATE' "
        "WHERE evidence_type = 'PHYTOSANITARY_CERTIFICATE'"
    )
    op.execute(
        "UPDATE public.traceability_evidence_links SET evidence_type = 'OTHER' "
        "WHERE evidence_type = 'EPHYTO_XML'"
    )
    op.drop_constraint(EVIDENCE_CHECK, EVIDENCE_TABLE, type_="check")
    op.create_check_constraint(
        EVIDENCE_CHECK,
        EVIDENCE_TABLE,
        _evidence_constraint(
            "'ORIGIN_AUTHORIZATION','FOREST_GUIDE','FRUIT_GUIDE','REMITO','INVOICE',"
            "'CERTIFICATE','TRANSPORT','GEOSPATIAL','SUPPLIER_DECLARATION','OTHER'"
        ),
    )

    op.execute(f"REVOKE USAGE, SELECT ON SEQUENCE public.{TABLE}_id_seq FROM {RUNTIME_ROLE}")
    op.execute(f"REVOKE SELECT, INSERT, UPDATE ON TABLE public.{TABLE} FROM {RUNTIME_ROLE}")
    op.execute(f"DROP POLICY IF EXISTS {TABLE}_tenant_update ON public.{TABLE}")
    op.execute(f"DROP POLICY IF EXISTS {TABLE}_tenant_insert ON public.{TABLE}")
    op.execute(f"DROP POLICY IF EXISTS {TABLE}_tenant_select ON public.{TABLE}")
    op.execute(f"ALTER TABLE public.{TABLE} NO FORCE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE public.{TABLE} DISABLE ROW LEVEL SECURITY")
    op.drop_index("ix_shipment_phytosanitary_cases_tenant_mode", table_name=TABLE)
    op.drop_index("ix_shipment_phytosanitary_cases_tenant_shipment", table_name=TABLE)
    op.drop_table(TABLE)
