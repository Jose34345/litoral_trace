"""Establish the tenant-scoped PPQ 505 preparation contract.

Revision ID: 040_us_lacey_ppq505
Revises: 039_us_lacey_pilot_fix
Create Date: 2026-09-01 12:00:00.000000
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "040_us_lacey_ppq505"
down_revision: Union[str, Sequence[str], None] = "039_us_lacey_pilot_fix"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

RUNTIME_ROLE = "litoral_trace_app"
WORKER_ROLE = "litoral_trace_worker_executor"
TENANT = "NULLIF(current_setting('app.current_organization_id', true), '')::integer"
NEW_TABLES = (
    "us_lacey_ppq_shipments",
    "us_lacey_ppq_plant_lines",
    "us_lacey_field_candidates",
)


def _secure(table: str) -> None:
    match = f"organization_id = {TENANT}"
    op.execute(f"ALTER TABLE public.{table} ENABLE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE public.{table} FORCE ROW LEVEL SECURITY")
    for action, clause in (
        ("select", f"FOR SELECT USING ({match})"),
        ("insert", f"FOR INSERT WITH CHECK ({match})"),
        ("update", f"FOR UPDATE USING ({match}) WITH CHECK ({match})"),
        ("delete", f"FOR DELETE USING ({match})"),
    ):
        op.execute(f"CREATE POLICY {table}_tenant_{action} ON public.{table} {clause}")
    op.execute(f"REVOKE ALL PRIVILEGES ON TABLE public.{table} FROM PUBLIC")
    op.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.{table} TO {RUNTIME_ROLE}")
    op.execute(f"REVOKE ALL PRIVILEGES ON TABLE public.{table} FROM {WORKER_ROLE}")
    op.execute(f"REVOKE ALL PRIVILEGES ON SEQUENCE public.{table}_id_seq FROM PUBLIC")
    op.execute(f"GRANT USAGE, SELECT ON SEQUENCE public.{table}_id_seq TO {RUNTIME_ROLE}")
    op.execute(f"REVOKE ALL PRIVILEGES ON SEQUENCE public.{table}_id_seq FROM {WORKER_ROLE}")


def upgrade() -> None:
    op.create_table(
        "us_lacey_ppq_shipments",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("operation_id", sa.Integer(), nullable=False),
        sa.Column("estimated_arrival_date", sa.Date(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.ForeignKeyConstraint(["operation_id", "organization_id"], ["us_lacey_operations.id", "us_lacey_operations.organization_id"], name="fk_us_lacey_ppq_shipments_operation_tenant", ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id", name="pk_us_lacey_ppq_shipments"),
        sa.UniqueConstraint("id", "organization_id", name="uq_us_lacey_ppq_shipments_id_org"),
        sa.UniqueConstraint("organization_id", "operation_id", name="uq_us_lacey_ppq_shipments_operation"),
    )
    op.create_index("ix_us_lacey_ppq_shipments_org_operation", "us_lacey_ppq_shipments", ["organization_id", "operation_id"])

    op.create_table(
        "us_lacey_ppq_plant_lines",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("operation_id", sa.Integer(), nullable=False),
        sa.Column("line_reference", sa.String(length=100), nullable=False),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.ForeignKeyConstraint(["operation_id", "organization_id"], ["us_lacey_operations.id", "us_lacey_operations.organization_id"], name="fk_us_lacey_ppq_plant_lines_operation_tenant", ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id", name="pk_us_lacey_ppq_plant_lines"),
        sa.UniqueConstraint("id", "organization_id", name="uq_us_lacey_ppq_plant_lines_id_org"),
        sa.UniqueConstraint("public_id", name="uq_us_lacey_ppq_plant_lines_public_id"),
        sa.UniqueConstraint("organization_id", "operation_id", "line_reference", name="uq_us_lacey_ppq_plant_lines_reference"),
        sa.UniqueConstraint("organization_id", "operation_id", "ordinal", name="uq_us_lacey_ppq_plant_lines_ordinal"),
        sa.CheckConstraint("ordinal > 0", name="ck_us_lacey_ppq_plant_lines_ordinal"),
    )
    op.create_index("ix_us_lacey_ppq_plant_lines_org_operation", "us_lacey_ppq_plant_lines", ["organization_id", "operation_id"])

    op.add_column("us_lacey_operation_fields", sa.Column("field_scope", sa.String(length=16), server_default="PLANT_LINE", nullable=False))
    op.add_column("us_lacey_operation_fields", sa.Column("plant_line_id", sa.Integer(), nullable=True))
    op.add_column("us_lacey_operation_fields", sa.Column("validation_status", sa.String(length=24), server_default="MISSING", nullable=False))
    op.add_column("us_lacey_operation_fields", sa.Column("validation_error", sa.Text(), nullable=True))
    op.add_column("us_lacey_operation_fields", sa.Column("not_required_reason_code", sa.String(length=64), nullable=True))
    op.create_foreign_key("fk_us_lacey_operation_fields_plant_line_tenant", "us_lacey_operation_fields", "us_lacey_ppq_plant_lines", ["plant_line_id", "organization_id"], ["id", "organization_id"], ondelete="CASCADE")
    op.create_check_constraint("ck_us_lacey_fields_scope", "us_lacey_operation_fields", "field_scope IN ('SHIPMENT','PLANT_LINE')")
    op.create_check_constraint("ck_us_lacey_fields_validation_status", "us_lacey_operation_fields", "validation_status IN ('VALID','INVALID','MISSING','REVIEW_REQUIRED')")

    op.create_table(
        "us_lacey_field_candidates",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("operation_id", sa.Integer(), nullable=False),
        sa.Column("operation_field_id", sa.Integer(), nullable=False),
        sa.Column("source_assurance_document_id", sa.Integer(), nullable=False),
        sa.Column("original_value", sa.Text(), nullable=False),
        sa.Column("normalized_value", sa.Text(), nullable=True),
        sa.Column("validation_status", sa.String(length=24), nullable=False),
        sa.Column("validation_error", sa.Text(), nullable=True),
        sa.Column("confidence", sa.Float(), server_default="0", nullable=False),
        sa.Column("source_page", sa.Integer(), nullable=True),
        sa.Column("source_locator", sa.Text(), nullable=True),
        sa.Column("extractor", sa.String(length=100), nullable=True),
        sa.Column("extractor_version", sa.String(length=100), nullable=True),
        sa.Column("fingerprint", sa.String(length=64), nullable=False),
        sa.Column("decision", sa.String(length=16), server_default="PENDING", nullable=False),
        sa.Column("decided_by_user_id", sa.Integer(), nullable=True),
        sa.Column("decided_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.ForeignKeyConstraint(["operation_id", "organization_id"], ["us_lacey_operations.id", "us_lacey_operations.organization_id"], name="fk_us_lacey_field_candidates_operation_tenant", ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["operation_field_id", "organization_id"], ["us_lacey_operation_fields.id", "us_lacey_operation_fields.organization_id"], name="fk_us_lacey_field_candidates_field_tenant", ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["source_assurance_document_id", "organization_id"], ["assurance_documents.id", "assurance_documents.organization_id"], name="fk_us_lacey_field_candidates_document_tenant", ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["decided_by_user_id"], ["users.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id", name="pk_us_lacey_field_candidates"),
        sa.UniqueConstraint("id", "organization_id", name="uq_us_lacey_field_candidates_id_org"),
        sa.UniqueConstraint("organization_id", "fingerprint", name="uq_us_lacey_field_candidates_fingerprint"),
        sa.CheckConstraint("confidence >= 0 AND confidence <= 1", name="ck_us_lacey_field_candidates_confidence"),
        sa.CheckConstraint("validation_status IN ('VALID','INVALID','MISSING','REVIEW_REQUIRED')", name="ck_us_lacey_field_candidates_validation"),
        sa.CheckConstraint("decision IN ('PENDING','SELECTED','REJECTED')", name="ck_us_lacey_field_candidates_decision"),
    )
    op.create_index("ix_us_lacey_field_candidates_org_field", "us_lacey_field_candidates", ["organization_id", "operation_field_id"])
    op.create_index("ix_us_lacey_field_candidates_org_operation", "us_lacey_field_candidates", ["organization_id", "operation_id"])

    # Add the one-to-one header for existing operations. Existing field rows are
    # retained untouched as legacy compatibility data; new operations use the
    # canonical shipment/plant scopes immediately.
    op.execute("INSERT INTO public.us_lacey_ppq_shipments (organization_id, operation_id) SELECT organization_id, id FROM public.us_lacey_operations ON CONFLICT DO NOTHING")

    for table in NEW_TABLES:
        _secure(table)


def downgrade() -> None:
    for table in reversed(NEW_TABLES):
        op.execute(f"REVOKE USAGE, SELECT ON SEQUENCE public.{table}_id_seq FROM {RUNTIME_ROLE}")
        op.execute(f"REVOKE SELECT, INSERT, UPDATE, DELETE ON TABLE public.{table} FROM {RUNTIME_ROLE}")
        for action in ("delete", "update", "insert", "select"):
            op.execute(f"DROP POLICY IF EXISTS {table}_tenant_{action} ON public.{table}")
        op.execute(f"ALTER TABLE public.{table} NO FORCE ROW LEVEL SECURITY")
        op.execute(f"ALTER TABLE public.{table} DISABLE ROW LEVEL SECURITY")

    op.drop_index("ix_us_lacey_field_candidates_org_operation", table_name="us_lacey_field_candidates")
    op.drop_index("ix_us_lacey_field_candidates_org_field", table_name="us_lacey_field_candidates")
    op.drop_table("us_lacey_field_candidates")
    op.drop_constraint("ck_us_lacey_fields_validation_status", "us_lacey_operation_fields", type_="check")
    op.drop_constraint("ck_us_lacey_fields_scope", "us_lacey_operation_fields", type_="check")
    op.drop_constraint("fk_us_lacey_operation_fields_plant_line_tenant", "us_lacey_operation_fields", type_="foreignkey")
    op.drop_column("us_lacey_operation_fields", "not_required_reason_code")
    op.drop_column("us_lacey_operation_fields", "validation_error")
    op.drop_column("us_lacey_operation_fields", "validation_status")
    op.drop_column("us_lacey_operation_fields", "plant_line_id")
    op.drop_column("us_lacey_operation_fields", "field_scope")
    op.drop_index("ix_us_lacey_ppq_plant_lines_org_operation", table_name="us_lacey_ppq_plant_lines")
    op.drop_table("us_lacey_ppq_plant_lines")
    op.drop_index("ix_us_lacey_ppq_shipments_org_operation", table_name="us_lacey_ppq_shipments")
    op.drop_table("us_lacey_ppq_shipments")
