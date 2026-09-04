"""Persist isolated Lacey Engine 2 shadow snapshots.

Revision ID: 043_us_lacey_engine2_shadow
Revises: 042_us_lacey_owner_admin
"""
from __future__ import annotations
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

revision = "043_us_lacey_engine2_shadow"
down_revision: Union[str, Sequence[str], None] = "042_us_lacey_owner_admin"
branch_labels = None
depends_on = None
TABLES = ("us_lacey_engine_document_runs", "us_lacey_engine_shipment_runs")
RUNTIME_ROLE = "litoral_trace_app"
WORKER_EXECUTOR_ROLE = "litoral_trace_worker_executor"
TENANT_CONTEXT_SQL = "NULLIF(current_setting('app.current_organization_id', true), '')::integer"

def _rls(table: str) -> None:
    predicate = f"organization_id = {TENANT_CONTEXT_SQL}"
    op.execute(f"ALTER TABLE public.{table} ENABLE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE public.{table} FORCE ROW LEVEL SECURITY")
    for action, clause in (("select", "USING"), ("insert", "WITH CHECK"), ("update", "USING"), ("delete", "USING")):
        suffix = f"{clause} ({predicate})" + (f" WITH CHECK ({predicate})" if action == "update" else "")
        op.execute(f"CREATE POLICY {table}_tenant_{action} ON public.{table} FOR {action.upper()} {suffix}")

def upgrade() -> None:
    op.create_table("us_lacey_engine_document_runs", sa.Column("id", sa.Integer(), primary_key=True), sa.Column("organization_id", sa.Integer(), nullable=False), sa.Column("operation_id", sa.Integer(), nullable=False), sa.Column("operation_document_id", sa.Integer(), nullable=False), sa.Column("assurance_document_id", sa.Integer(), nullable=False), sa.Column("engine_version", sa.String(100), nullable=False), sa.Column("schema_version", sa.String(100), nullable=False), sa.Column("source_sha256", sa.String(64), nullable=False), sa.Column("role_hint", sa.String(64)), sa.Column("status", sa.String(16), nullable=False), sa.Column("resolution_json", sa.JSON()), sa.Column("safe_error_code", sa.String(100)), sa.Column("safe_error_message", sa.String(512)), sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")), sa.ForeignKeyConstraint(["operation_id", "organization_id"], ["us_lacey_operations.id", "us_lacey_operations.organization_id"], ondelete="CASCADE"), sa.ForeignKeyConstraint(["operation_document_id", "organization_id"], ["us_lacey_operation_documents.id", "us_lacey_operation_documents.organization_id"], ondelete="CASCADE"), sa.ForeignKeyConstraint(["assurance_document_id", "organization_id"], ["assurance_documents.id", "assurance_documents.organization_id"], ondelete="RESTRICT"), sa.UniqueConstraint("organization_id", "assurance_document_id", "source_sha256", "engine_version", "role_hint", "status", name="uq_lacey_e2_docrun_identity"), sa.CheckConstraint("status IN ('SUCCEEDED','FAILED')", name="ck_lacey_e2_docrun_status"))
    op.create_index("ix_lacey_e2_docrun_org_operation", "us_lacey_engine_document_runs", ["organization_id", "operation_id"])
    op.create_table("us_lacey_engine_shipment_runs", sa.Column("id", sa.Integer(), primary_key=True), sa.Column("organization_id", sa.Integer(), nullable=False), sa.Column("operation_id", sa.Integer(), nullable=False), sa.Column("engine_version", sa.String(100), nullable=False), sa.Column("ruleset_version", sa.String(100), nullable=False), sa.Column("schema_version", sa.String(100), nullable=False), sa.Column("source_set_fingerprint", sa.String(64), nullable=False), sa.Column("document_count", sa.Integer(), nullable=False), sa.Column("readiness", sa.String(24), nullable=False), sa.Column("resolution_json", sa.JSON(), nullable=False), sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")), sa.ForeignKeyConstraint(["operation_id", "organization_id"], ["us_lacey_operations.id", "us_lacey_operations.organization_id"], ondelete="CASCADE"), sa.UniqueConstraint("organization_id", "operation_id", "source_set_fingerprint", name="uq_lacey_e2_shiprun_fingerprint"), sa.CheckConstraint("readiness IN ('READY','REVIEW_REQUIRED','BLOCKED')", name="ck_lacey_e2_shiprun_readiness"))
    op.create_index("ix_lacey_e2_shiprun_org_operation", "us_lacey_engine_shipment_runs", ["organization_id", "operation_id"])
    for table in TABLES: _rls(table)
    for table in TABLES:
        op.execute(f"REVOKE ALL PRIVILEGES ON TABLE public.{table} FROM PUBLIC")
        op.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.{table} TO {RUNTIME_ROLE}")
        op.execute(f"REVOKE ALL PRIVILEGES ON TABLE public.{table} FROM {WORKER_EXECUTOR_ROLE}")
        op.execute(f"REVOKE ALL PRIVILEGES ON SEQUENCE public.{table}_id_seq FROM PUBLIC")
        op.execute(f"GRANT USAGE, SELECT ON SEQUENCE public.{table}_id_seq TO {RUNTIME_ROLE}")
        op.execute(f"REVOKE ALL PRIVILEGES ON SEQUENCE public.{table}_id_seq FROM {WORKER_EXECUTOR_ROLE}")

def downgrade() -> None:
    for table in reversed(TABLES):
        for action in ("delete", "update", "insert", "select"): op.execute(f"DROP POLICY IF EXISTS {table}_tenant_{action} ON public.{table}")
        op.drop_table(table)
