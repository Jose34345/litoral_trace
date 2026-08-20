"""Add tenant-safe industrial genealogy graph for chain of custody.

Revision ID: 019_add_traceability_genealogy
Revises: 018_add_batch_evidence_links
Create Date: 2026-08-20 20:30:00.000000
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "019_add_traceability_genealogy"
down_revision: Union[str, Sequence[str], None] = "018_add_batch_evidence_links"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

RUNTIME_ROLE = "litoral_trace_app"
WORKER_EXECUTOR_ROLE = "litoral_trace_worker_executor"
TENANT_CONTEXT_SQL = (
    "NULLIF(current_setting('app.current_organization_id', true), '')::integer"
)
TENANT_TABLES = (
    "traceability_batches",
    "traceability_events",
    "traceability_event_inputs",
    "traceability_event_outputs",
    "shipments",
    "shipment_items",
)


def _create_rls(table_name: str) -> None:
    tenant_match_sql = f"organization_id = {TENANT_CONTEXT_SQL}"
    op.execute(f"ALTER TABLE public.{table_name} ENABLE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE public.{table_name} FORCE ROW LEVEL SECURITY")
    op.execute(
        f"CREATE POLICY {table_name}_tenant_select ON public.{table_name} "
        f"FOR SELECT USING ({tenant_match_sql})"
    )
    op.execute(
        f"CREATE POLICY {table_name}_tenant_insert ON public.{table_name} "
        f"FOR INSERT WITH CHECK ({tenant_match_sql})"
    )
    op.execute(
        f"CREATE POLICY {table_name}_tenant_update ON public.{table_name} "
        f"FOR UPDATE USING ({tenant_match_sql}) WITH CHECK ({tenant_match_sql})"
    )


def _drop_rls(table_name: str) -> None:
    for action in ("update", "insert", "select"):
        op.execute(
            f"DROP POLICY IF EXISTS {table_name}_tenant_{action} ON public.{table_name}"
        )
    op.execute(f"ALTER TABLE public.{table_name} NO FORCE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE public.{table_name} DISABLE ROW LEVEL SECURITY")


def _grant_runtime_access(table_name: str) -> None:
    sequence_name = f"{table_name}_id_seq"
    op.execute(f"REVOKE ALL PRIVILEGES ON TABLE public.{table_name} FROM PUBLIC")
    op.execute(f"REVOKE ALL PRIVILEGES ON SEQUENCE public.{sequence_name} FROM PUBLIC")
    op.execute(
        f"GRANT SELECT, INSERT, UPDATE ON TABLE public.{table_name} TO {RUNTIME_ROLE}"
    )
    op.execute(
        f"GRANT USAGE, SELECT ON SEQUENCE public.{sequence_name} TO {RUNTIME_ROLE}"
    )
    op.execute(
        f"REVOKE ALL PRIVILEGES ON TABLE public.{table_name} FROM {WORKER_EXECUTOR_ROLE}"
    )
    op.execute(
        f"REVOKE ALL PRIVILEGES ON SEQUENCE public.{sequence_name} FROM {WORKER_EXECUTOR_ROLE}"
    )


def _revoke_runtime_access(table_name: str) -> None:
    sequence_name = f"{table_name}_id_seq"
    op.execute(
        f"REVOKE USAGE, SELECT ON SEQUENCE public.{sequence_name} FROM {RUNTIME_ROLE}"
    )
    op.execute(
        f"REVOKE SELECT, INSERT, UPDATE ON TABLE public.{table_name} FROM {RUNTIME_ROLE}"
    )


def upgrade() -> None:
    # Required by the source-lot composite FK and reconciles ORM/schema drift.
    op.create_unique_constraint(
        "uq_lotes_id_organization_id", "lotes", ["id", "organization_id"]
    )

    op.create_table(
        "traceability_batches",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.Uuid(), nullable=False, server_default=sa.text("gen_random_uuid()")),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("code", sa.String(length=120), nullable=False),
        sa.Column("product_name", sa.String(length=160), nullable=False),
        sa.Column("stage", sa.String(length=32), nullable=False),
        sa.Column("unit", sa.String(length=16), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False, server_default="ACTIVE"),
        sa.Column("source_lote_id", sa.Integer(), nullable=True),
        sa.Column("created_by_user_id", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.CheckConstraint(
            "stage IN ('RECEIPT','RAW_MATERIAL','INTERMEDIATE','FINISHED_GOOD')",
            name="ck_traceability_batches_stage",
        ),
        sa.CheckConstraint("unit IN ('TON','KG','M3')", name="ck_traceability_batches_unit"),
        sa.CheckConstraint("status IN ('ACTIVE','CLOSED','VOID')", name="ck_traceability_batches_status"),
        sa.ForeignKeyConstraint(
            ["organization_id"], ["organizations.id"],
            name="fk_traceability_batches_organization_id", ondelete="RESTRICT"
        ),
        sa.ForeignKeyConstraint(
            ["source_lote_id", "organization_id"],
            ["lotes.id", "lotes.organization_id"],
            name="fk_traceability_batches_source_lote_tenant", ondelete="RESTRICT"
        ),
        sa.ForeignKeyConstraint(
            ["created_by_user_id"], ["users.id"],
            name="fk_traceability_batches_created_by_user_id", ondelete="SET NULL"
        ),
        sa.PrimaryKeyConstraint("id", name="pk_traceability_batches"),
        sa.UniqueConstraint("id", "organization_id", name="uq_traceability_batches_id_org"),
        sa.UniqueConstraint("public_id", name="uq_traceability_batches_public_id"),
    )
    op.create_index(
        "uq_traceability_batches_tenant_code_ci", "traceability_batches",
        ["organization_id", sa.text("lower(code)")], unique=True
    )
    op.create_index(
        "ix_traceability_batches_tenant_stage_status", "traceability_batches",
        ["organization_id", "stage", "status"]
    )
    op.create_index(
        "ix_traceability_batches_tenant_source_lote", "traceability_batches",
        ["organization_id", "source_lote_id"]
    )

    op.create_table(
        "traceability_events",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.Uuid(), nullable=False, server_default=sa.text("gen_random_uuid()")),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("event_code", sa.String(length=120), nullable=False),
        sa.Column("event_type", sa.String(length=32), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False, server_default="DRAFT"),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("facility_reference", sa.String(length=160), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("created_by_user_id", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.CheckConstraint(
            "event_type IN ('RECEIPT','TRANSFORMATION','MIX','SPLIT','REPACK','ADJUSTMENT')",
            name="ck_traceability_events_type",
        ),
        sa.CheckConstraint("status IN ('DRAFT','POSTED','VOID')", name="ck_traceability_events_status"),
        sa.ForeignKeyConstraint(
            ["organization_id"], ["organizations.id"],
            name="fk_traceability_events_organization_id", ondelete="RESTRICT"
        ),
        sa.ForeignKeyConstraint(
            ["created_by_user_id"], ["users.id"],
            name="fk_traceability_events_created_by_user_id", ondelete="SET NULL"
        ),
        sa.PrimaryKeyConstraint("id", name="pk_traceability_events"),
        sa.UniqueConstraint("id", "organization_id", name="uq_traceability_events_id_org"),
        sa.UniqueConstraint("public_id", name="uq_traceability_events_public_id"),
    )
    op.create_index(
        "uq_traceability_events_tenant_code_ci", "traceability_events",
        ["organization_id", sa.text("lower(event_code)")], unique=True
    )
    op.create_index(
        "ix_traceability_events_tenant_occurred_at", "traceability_events",
        ["organization_id", "occurred_at"]
    )
    op.create_index(
        "ix_traceability_events_tenant_type_status", "traceability_events",
        ["organization_id", "event_type", "status"]
    )

    for table_name, direction in (
        ("traceability_event_inputs", "inputs"),
        ("traceability_event_outputs", "outputs"),
    ):
        op.create_table(
            table_name,
            sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
            sa.Column("organization_id", sa.Integer(), nullable=False),
            sa.Column("event_id", sa.Integer(), nullable=False),
            sa.Column("batch_id", sa.Integer(), nullable=False),
            sa.Column("quantity", sa.Numeric(precision=18, scale=6), nullable=False),
            sa.Column("unit", sa.String(length=16), nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
            sa.CheckConstraint("quantity > 0", name=f"ck_traceability_event_{direction}_quantity"),
            sa.CheckConstraint("unit IN ('TON','KG','M3')", name=f"ck_traceability_event_{direction}_unit"),
            sa.ForeignKeyConstraint(
                ["event_id", "organization_id"],
                ["traceability_events.id", "traceability_events.organization_id"],
                name=f"fk_traceability_event_{direction}_event_tenant", ondelete="RESTRICT"
            ),
            sa.ForeignKeyConstraint(
                ["batch_id", "organization_id"],
                ["traceability_batches.id", "traceability_batches.organization_id"],
                name=f"fk_traceability_event_{direction}_batch_tenant", ondelete="RESTRICT"
            ),
            sa.PrimaryKeyConstraint("id", name=f"pk_traceability_event_{direction}"),
            sa.UniqueConstraint("event_id", "batch_id", name=f"uq_traceability_event_{direction}_event_batch"),
        )
        op.create_index(
            f"ix_traceability_event_{direction}_tenant_batch", table_name,
            ["organization_id", "batch_id"]
        )

    op.create_table(
        "shipments",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.Uuid(), nullable=False, server_default=sa.text("gen_random_uuid()")),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("shipment_code", sa.String(length=120), nullable=False),
        sa.Column("sale_reference", sa.String(length=160), nullable=True),
        sa.Column("buyer_reference", sa.String(length=160), nullable=True),
        sa.Column("destination_country", sa.String(length=2), nullable=True),
        sa.Column("shipped_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.String(length=16), nullable=False, server_default="DRAFT"),
        sa.Column("created_by_user_id", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.CheckConstraint(
            "status IN ('DRAFT','CONFIRMED','DISPATCHED','CANCELLED')",
            name="ck_shipments_status"
        ),
        sa.CheckConstraint(
            "destination_country IS NULL OR length(destination_country) = 2",
            name="ck_shipments_destination_country"
        ),
        sa.ForeignKeyConstraint(
            ["organization_id"], ["organizations.id"],
            name="fk_shipments_organization_id", ondelete="RESTRICT"
        ),
        sa.ForeignKeyConstraint(
            ["created_by_user_id"], ["users.id"],
            name="fk_shipments_created_by_user_id", ondelete="SET NULL"
        ),
        sa.PrimaryKeyConstraint("id", name="pk_shipments"),
        sa.UniqueConstraint("id", "organization_id", name="uq_shipments_id_org"),
        sa.UniqueConstraint("public_id", name="uq_shipments_public_id"),
    )
    op.create_index(
        "uq_shipments_tenant_code_ci", "shipments",
        ["organization_id", sa.text("lower(shipment_code)")], unique=True
    )
    op.create_index(
        "ix_shipments_tenant_shipped_at", "shipments",
        ["organization_id", "shipped_at"]
    )
    op.create_index(
        "ix_shipments_tenant_status", "shipments", ["organization_id", "status"]
    )

    op.create_table(
        "shipment_items",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("shipment_id", sa.Integer(), nullable=False),
        sa.Column("batch_id", sa.Integer(), nullable=False),
        sa.Column("quantity", sa.Numeric(precision=18, scale=6), nullable=False),
        sa.Column("unit", sa.String(length=16), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.CheckConstraint("quantity > 0", name="ck_shipment_items_quantity"),
        sa.CheckConstraint("unit IN ('TON','KG','M3')", name="ck_shipment_items_unit"),
        sa.ForeignKeyConstraint(
            ["shipment_id", "organization_id"],
            ["shipments.id", "shipments.organization_id"],
            name="fk_shipment_items_shipment_tenant", ondelete="RESTRICT"
        ),
        sa.ForeignKeyConstraint(
            ["batch_id", "organization_id"],
            ["traceability_batches.id", "traceability_batches.organization_id"],
            name="fk_shipment_items_batch_tenant", ondelete="RESTRICT"
        ),
        sa.PrimaryKeyConstraint("id", name="pk_shipment_items"),
        sa.UniqueConstraint("shipment_id", "batch_id", name="uq_shipment_items_shipment_batch"),
    )
    op.create_index(
        "ix_shipment_items_tenant_batch", "shipment_items", ["organization_id", "batch_id"]
    )

    for table_name in TENANT_TABLES:
        _create_rls(table_name)
        _grant_runtime_access(table_name)


def downgrade() -> None:
    for table_name in reversed(TENANT_TABLES):
        _revoke_runtime_access(table_name)
        _drop_rls(table_name)

    op.drop_index("ix_shipment_items_tenant_batch", table_name="shipment_items")
    op.drop_table("shipment_items")
    op.drop_index("ix_shipments_tenant_status", table_name="shipments")
    op.drop_index("ix_shipments_tenant_shipped_at", table_name="shipments")
    op.drop_index("uq_shipments_tenant_code_ci", table_name="shipments")
    op.drop_table("shipments")
    op.drop_index("ix_traceability_event_outputs_tenant_batch", table_name="traceability_event_outputs")
    op.drop_table("traceability_event_outputs")
    op.drop_index("ix_traceability_event_inputs_tenant_batch", table_name="traceability_event_inputs")
    op.drop_table("traceability_event_inputs")
    op.drop_index("ix_traceability_events_tenant_type_status", table_name="traceability_events")
    op.drop_index("ix_traceability_events_tenant_occurred_at", table_name="traceability_events")
    op.drop_index("uq_traceability_events_tenant_code_ci", table_name="traceability_events")
    op.drop_table("traceability_events")
    op.drop_index("ix_traceability_batches_tenant_source_lote", table_name="traceability_batches")
    op.drop_index("ix_traceability_batches_tenant_stage_status", table_name="traceability_batches")
    op.drop_index("uq_traceability_batches_tenant_code_ci", table_name="traceability_batches")
    op.drop_table("traceability_batches")
    op.drop_constraint("uq_lotes_id_organization_id", "lotes", type_="unique")
