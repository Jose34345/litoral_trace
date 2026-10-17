"""Add multilingual source spans and translation provenance.

Revision ID: 046_lacey_multilingual_spans
Revises: 045_lacey_evidence_snapshots
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "046_lacey_multilingual_spans"
down_revision: Union[str, Sequence[str], None] = "045_lacey_evidence_snapshots"
branch_labels = None
depends_on = None

TABLES = ("document_text_spans", "document_text_translations")
RUNTIME_ROLE = "litoral_trace_app"
WORKER_EXECUTOR_ROLE = "litoral_trace_worker_executor"
TENANT_CONTEXT_SQL = "NULLIF(current_setting('app.current_organization_id', true), '')::integer"


def _rls(table: str) -> None:
    predicate = f"organization_id = {TENANT_CONTEXT_SQL}"
    op.execute(f"ALTER TABLE public.{table} ENABLE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE public.{table} FORCE ROW LEVEL SECURITY")
    for action, clause in (("select", "USING"), ("insert", "WITH CHECK"), ("update", "USING"), ("delete", "USING")):
        suffix = f"{clause} ({predicate})"
        if action == "update":
            suffix += f" WITH CHECK ({predicate})"
        op.execute(f"CREATE POLICY {table}_tenant_{action} ON public.{table} FOR {action.upper()} {suffix}")


def _grant_runtime(table: str) -> None:
    op.execute(f"REVOKE ALL PRIVILEGES ON TABLE public.{table} FROM PUBLIC")
    op.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.{table} TO {RUNTIME_ROLE}")
    op.execute(f"REVOKE ALL PRIVILEGES ON TABLE public.{table} FROM {WORKER_EXECUTOR_ROLE}")
    op.execute(f"REVOKE ALL PRIVILEGES ON SEQUENCE public.{table}_id_seq FROM PUBLIC")
    op.execute(f"GRANT USAGE, SELECT ON SEQUENCE public.{table}_id_seq TO {RUNTIME_ROLE}")
    op.execute(f"REVOKE ALL PRIVILEGES ON SEQUENCE public.{table}_id_seq FROM {WORKER_EXECUTOR_ROLE}")


def upgrade() -> None:
    op.create_table(
        "document_text_spans",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("public_id", sa.Uuid(), nullable=False),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("assurance_document_id", sa.Integer(), nullable=False),
        sa.Column("extraction_run_id", sa.Integer(), nullable=False),
        sa.Column("page", sa.Integer(), nullable=False),
        sa.Column("block_id", sa.String(255), nullable=False),
        sa.Column("table_id", sa.String(255), nullable=True),
        sa.Column("row_index", sa.Integer(), nullable=True),
        sa.Column("column_index", sa.Integer(), nullable=True),
        sa.Column("bbox_json", sa.JSON(), nullable=True),
        sa.Column("source_locator", sa.Text(), nullable=True),
        sa.Column("original_text", sa.Text(), nullable=False),
        sa.Column("original_language", sa.String(32), nullable=False),
        sa.Column("language_confidence", sa.Float(), nullable=False, server_default="0"),
        sa.Column("extraction_method", sa.String(32), nullable=False),
        sa.Column("ocr_engine", sa.String(100), nullable=True),
        sa.Column("ocr_engine_version", sa.String(100), nullable=True),
        sa.Column("ocr_confidence", sa.Float(), nullable=True),
        sa.Column("content_hash", sa.String(64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.ForeignKeyConstraint(
            ["assurance_document_id", "organization_id"],
            ["assurance_documents.id", "assurance_documents.organization_id"],
            name="fk_document_text_spans_assurance_tenant",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["extraction_run_id", "organization_id"],
            ["document_extraction_runs.id", "document_extraction_runs.organization_id"],
            name="fk_document_text_spans_extraction_run_tenant",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint("id", "organization_id", name="uq_document_text_spans_id_org"),
        sa.UniqueConstraint("public_id", name="uq_document_text_spans_public_id"),
        sa.CheckConstraint("page > 0", name="ck_document_text_spans_page"),
        sa.CheckConstraint(
            "language_confidence >= 0 AND language_confidence <= 1",
            name="ck_document_text_spans_language_confidence",
        ),
        sa.CheckConstraint(
            "ocr_confidence IS NULL OR (ocr_confidence >= 0 AND ocr_confidence <= 1)",
            name="ck_document_text_spans_ocr_confidence",
        ),
    )
    op.create_index("ix_document_text_spans_org_document", "document_text_spans", ["organization_id", "assurance_document_id"])
    op.create_index("ix_document_text_spans_org_run", "document_text_spans", ["organization_id", "extraction_run_id"])
    op.create_index("ix_document_text_spans_org_language", "document_text_spans", ["organization_id", "original_language"])

    op.create_table(
        "document_text_translations",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("public_id", sa.Uuid(), nullable=False),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("source_span_id", sa.Integer(), nullable=False),
        sa.Column("source_language", sa.String(32), nullable=False),
        sa.Column("target_language", sa.String(32), nullable=False, server_default="en"),
        sa.Column("translated_text", sa.Text(), nullable=False),
        sa.Column("provider", sa.String(64), nullable=False),
        sa.Column("model_name", sa.String(128), nullable=False),
        sa.Column("model_version", sa.String(128), nullable=False),
        sa.Column("quality_score", sa.Float(), nullable=True),
        sa.Column("input_hash", sa.String(64), nullable=False),
        sa.Column("output_hash", sa.String(64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.ForeignKeyConstraint(
            ["source_span_id", "organization_id"],
            ["document_text_spans.id", "document_text_spans.organization_id"],
            name="fk_document_text_translations_span_tenant",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint("id", "organization_id", name="uq_document_text_translations_id_org"),
        sa.UniqueConstraint("public_id", name="uq_document_text_translations_public_id"),
        sa.UniqueConstraint(
            "organization_id", "source_span_id", "target_language", "provider",
            "model_name", "model_version", "input_hash",
            name="uq_document_text_translations_identity",
        ),
        sa.CheckConstraint(
            "quality_score IS NULL OR (quality_score >= 0 AND quality_score <= 1)",
            name="ck_document_text_translations_quality",
        ),
    )
    op.create_index("ix_document_text_translations_org_span", "document_text_translations", ["organization_id", "source_span_id"])
    op.create_index("ix_document_text_translations_org_target", "document_text_translations", ["organization_id", "target_language"])

    for table in TABLES:
        _rls(table)
        _grant_runtime(table)


def downgrade() -> None:
    for table in reversed(TABLES):
        for action in ("delete", "update", "insert", "select"):
            op.execute(f"DROP POLICY IF EXISTS {table}_tenant_{action} ON public.{table}")
        op.drop_table(table)
