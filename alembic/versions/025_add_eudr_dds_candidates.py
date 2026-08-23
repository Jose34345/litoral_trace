"""Add local EUDR API V3 DDS candidates.

Revision ID: 025_add_eudr_dds_candidates
Revises: 024_add_shipment_phytosanitary_cases
Create Date: 2026-08-23 03:20:00.000000
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "025_add_eudr_dds_candidates"
down_revision: Union[str, Sequence[str], None] = "024_add_shipment_phytosanitary_cases"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

RUNTIME_ROLE = "litoral_trace_app"
WORKER_EXECUTOR_ROLE = "litoral_trace_worker_executor"
TENANT_CONTEXT_SQL = "NULLIF(current_setting('app.current_organization_id', true), '')::integer"
TABLE = "eudr_dds_candidates"


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
        sa.Column("shipment_id", sa.Integer(), nullable=False),
        sa.Column("activity_type", sa.String(length=16), nullable=False),
        sa.Column("operator_name", sa.String(length=240), nullable=True),
        sa.Column("operator_address", sa.Text(), nullable=True),
        sa.Column("operator_country_code", sa.String(length=2), nullable=True),
        sa.Column("operator_eori", sa.String(length=32), nullable=True),
        sa.Column("hs_code", sa.String(length=16), nullable=True),
        sa.Column("trade_name", sa.String(length=240), nullable=True),
        sa.Column("product_description", sa.Text(), nullable=True),
        sa.Column("common_species_name", sa.String(length=240), nullable=True),
        sa.Column("scientific_species_name", sa.String(length=240), nullable=True),
        sa.Column("net_mass_kg", sa.Numeric(18, 3), nullable=True),
        sa.Column("production_country_code", sa.String(length=2), nullable=True),
        sa.Column("production_date_from", sa.Date(), nullable=True),
        sa.Column("production_date_to", sa.Date(), nullable=True),
        sa.Column(
            "relies_on_previous_dds",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        sa.Column("previous_dds_reference", sa.String(length=160), nullable=True),
        sa.Column("previous_dds_verification", sa.String(length=160), nullable=True),
        sa.Column(
            "risk_conclusion",
            sa.String(length=32),
            nullable=False,
            server_default=sa.text("'UNASSESSED'"),
        ),
        sa.Column("risk_assessment_reference", sa.String(length=240), nullable=True),
        sa.Column("risk_assessed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("spec_profile", sa.String(length=120), nullable=False),
        sa.Column("spec_fingerprint_sha256", sa.String(length=64), nullable=False),
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
        sa.PrimaryKeyConstraint("id", name="pk_eudr_dds_candidates"),
        sa.UniqueConstraint("id", "organization_id", name="uq_eudr_dds_candidates_id_org"),
        sa.UniqueConstraint("public_id", name="uq_eudr_dds_candidates_public_id"),
        sa.UniqueConstraint("shipment_id", name="uq_eudr_dds_candidates_shipment_id"),
        sa.ForeignKeyConstraint(
            ["organization_id"],
            ["organizations.id"],
            name="fk_eudr_dds_candidates_organization_id",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["shipment_id", "organization_id"],
            ["shipments.id", "shipments.organization_id"],
            name="fk_eudr_dds_candidates_shipment_tenant",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["created_by_user_id"],
            ["users.id"],
            name="fk_eudr_dds_candidates_created_by_user_id",
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["updated_by_user_id"],
            ["users.id"],
            name="fk_eudr_dds_candidates_updated_by_user_id",
            ondelete="SET NULL",
        ),
        sa.CheckConstraint(
            "activity_type IN ('IMPORT','DOMESTIC','EXPORT')",
            name="ck_eudr_dds_candidates_activity_type",
        ),
        sa.CheckConstraint(
            "risk_conclusion IN ('UNASSESSED','NO_OR_NEGLIGIBLE_RISK','NON_NEGLIGIBLE_RISK')",
            name="ck_eudr_dds_candidates_risk_conclusion",
        ),
        sa.CheckConstraint(
            "net_mass_kg IS NULL OR net_mass_kg > 0",
            name="ck_eudr_dds_candidates_net_mass_positive",
        ),
        sa.CheckConstraint(
            "production_date_from IS NULL OR production_date_to IS NULL OR production_date_to >= production_date_from",
            name="ck_eudr_dds_candidates_production_date_range",
        ),
    )
    op.create_index(
        "ix_eudr_dds_candidates_tenant_shipment",
        TABLE,
        ["organization_id", "shipment_id"],
    )
    op.create_index(
        "ix_eudr_dds_candidates_tenant_risk",
        TABLE,
        ["organization_id", "risk_conclusion"],
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


def downgrade() -> None:
    op.execute(f"REVOKE USAGE, SELECT ON SEQUENCE public.{TABLE}_id_seq FROM {RUNTIME_ROLE}")
    op.execute(f"REVOKE SELECT, INSERT, UPDATE ON TABLE public.{TABLE} FROM {RUNTIME_ROLE}")
    op.execute(f"DROP POLICY IF EXISTS {TABLE}_tenant_update ON public.{TABLE}")
    op.execute(f"DROP POLICY IF EXISTS {TABLE}_tenant_insert ON public.{TABLE}")
    op.execute(f"DROP POLICY IF EXISTS {TABLE}_tenant_select ON public.{TABLE}")
    op.execute(f"ALTER TABLE public.{TABLE} NO FORCE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE public.{TABLE} DISABLE ROW LEVEL SECURITY")
    op.drop_index("ix_eudr_dds_candidates_tenant_risk", table_name=TABLE)
    op.drop_index("ix_eudr_dds_candidates_tenant_shipment", table_name=TABLE)
    op.drop_table(TABLE)
