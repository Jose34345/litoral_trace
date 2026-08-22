"""Add tenant-safe contextual evidence links for traceability subjects.

Revision ID: 020_add_traceability_evidence_links
Revises: 019_add_traceability_genealogy
Create Date: 2026-08-21 13:00:00.000000
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "020_add_traceability_evidence_links"
down_revision: Union[str, Sequence[str], None] = "019_add_traceability_genealogy"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

RUNTIME_ROLE = "litoral_trace_app"
WORKER_EXECUTOR_ROLE = "litoral_trace_worker_executor"
TENANT_CONTEXT_SQL = (
    "NULLIF(current_setting('app.current_organization_id', true), '')::integer"
)
TABLE = "traceability_evidence_links"


def _create_rls() -> None:
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


def _drop_rls() -> None:
    for action in ("update", "insert", "select"):
        op.execute(
            f"DROP POLICY IF EXISTS {TABLE}_tenant_{action} ON public.{TABLE}"
        )
    op.execute(f"ALTER TABLE public.{TABLE} NO FORCE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE public.{TABLE} DISABLE ROW LEVEL SECURITY")


def _grant_runtime_access() -> None:
    op.execute(f"REVOKE ALL PRIVILEGES ON TABLE public.{TABLE} FROM PUBLIC")
    op.execute(f"REVOKE ALL PRIVILEGES ON SEQUENCE public.{TABLE}_id_seq FROM PUBLIC")
    op.execute(
        f"GRANT SELECT, INSERT, UPDATE ON TABLE public.{TABLE} TO {RUNTIME_ROLE}"
    )
    op.execute(
        f"GRANT USAGE, SELECT ON SEQUENCE public.{TABLE}_id_seq TO {RUNTIME_ROLE}"
    )
    op.execute(
        f"REVOKE ALL PRIVILEGES ON TABLE public.{TABLE} FROM {WORKER_EXECUTOR_ROLE}"
    )
    op.execute(
        f"REVOKE ALL PRIVILEGES ON SEQUENCE public.{TABLE}_id_seq FROM {WORKER_EXECUTOR_ROLE}"
    )


def _revoke_runtime_access() -> None:
    op.execute(
        f"REVOKE USAGE, SELECT ON SEQUENCE public.{TABLE}_id_seq FROM {RUNTIME_ROLE}"
    )
    op.execute(
        f"REVOKE SELECT, INSERT, UPDATE ON TABLE public.{TABLE} FROM {RUNTIME_ROLE}"
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
        sa.Column("vault_document_id", sa.Integer(), nullable=False),
        sa.Column("source_lote_id", sa.Integer(), nullable=True),
        sa.Column("traceability_event_id", sa.Integer(), nullable=True),
        sa.Column("traceability_batch_id", sa.Integer(), nullable=True),
        sa.Column("shipment_id", sa.Integer(), nullable=True),
        sa.Column("evidence_type", sa.String(length=40), nullable=False),
        sa.Column("reference_number", sa.String(length=160), nullable=True),
        sa.Column("issuer", sa.String(length=200), nullable=True),
        sa.Column("document_date", sa.Date(), nullable=True),
        sa.Column("valid_from", sa.Date(), nullable=True),
        sa.Column("valid_until", sa.Date(), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("created_by_user_id", sa.Integer(), nullable=True),
        sa.Column("unlinked_by_user_id", sa.Integer(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column("unlinked_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id", name=f"pk_{TABLE}"),
        sa.UniqueConstraint("public_id", name=f"uq_{TABLE}_public_id"),
        sa.ForeignKeyConstraint(
            ["vault_document_id", "organization_id"],
            ["vault_documents.id", "vault_documents.organization_id"],
            name="fk_traceability_evidence_links_vault_tenant",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["source_lote_id", "organization_id"],
            ["lotes.id", "lotes.organization_id"],
            name="fk_traceability_evidence_links_source_lote_tenant",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["traceability_event_id", "organization_id"],
            ["traceability_events.id", "traceability_events.organization_id"],
            name="fk_traceability_evidence_links_event_tenant",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["traceability_batch_id", "organization_id"],
            ["traceability_batches.id", "traceability_batches.organization_id"],
            name="fk_traceability_evidence_links_batch_tenant",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["shipment_id", "organization_id"],
            ["shipments.id", "shipments.organization_id"],
            name="fk_traceability_evidence_links_shipment_tenant",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["created_by_user_id"],
            ["users.id"],
            name="fk_traceability_evidence_links_created_by_user_id",
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["unlinked_by_user_id"],
            ["users.id"],
            name="fk_traceability_evidence_links_unlinked_by_user_id",
            ondelete="SET NULL",
        ),
        sa.CheckConstraint(
            "evidence_type IN ('ORIGIN_AUTHORIZATION','FOREST_GUIDE','REMITO','INVOICE','CERTIFICATE','TRANSPORT','GEOSPATIAL','SUPPLIER_DECLARATION','OTHER')",
            name="ck_traceability_evidence_links_type",
        ),
        sa.CheckConstraint(
            "(CASE WHEN source_lote_id IS NOT NULL THEN 1 ELSE 0 END + "
            "CASE WHEN traceability_event_id IS NOT NULL THEN 1 ELSE 0 END + "
            "CASE WHEN traceability_batch_id IS NOT NULL THEN 1 ELSE 0 END + "
            "CASE WHEN shipment_id IS NOT NULL THEN 1 ELSE 0 END) = 1",
            name="ck_traceability_evidence_links_exactly_one_subject",
        ),
        sa.CheckConstraint(
            "unlinked_by_user_id IS NULL OR unlinked_at IS NOT NULL",
            name="ck_traceability_evidence_links_unlink_state",
        ),
        sa.CheckConstraint(
            "valid_from IS NULL OR valid_until IS NULL OR valid_until >= valid_from",
            name="ck_traceability_evidence_links_validity_range",
        ),
    )
    op.create_index(
        "ix_traceability_evidence_links_tenant_created",
        TABLE,
        ["organization_id", "created_at"],
    )
    op.create_index(
        "ix_traceability_evidence_links_tenant_vault",
        TABLE,
        ["organization_id", "vault_document_id"],
    )
    for suffix, subject_column in (
        ("source", "source_lote_id"),
        ("event", "traceability_event_id"),
        ("batch", "traceability_batch_id"),
        ("shipment", "shipment_id"),
    ):
        op.create_index(
            f"uq_traceability_evidence_active_{suffix}",
            TABLE,
            ["vault_document_id", subject_column],
            unique=True,
            postgresql_where=sa.text(
                f"unlinked_at IS NULL AND {subject_column} IS NOT NULL"
            ),
        )

    _create_rls()
    _grant_runtime_access()


def downgrade() -> None:
    _revoke_runtime_access()
    _drop_rls()
    for suffix in ("shipment", "batch", "event", "source"):
        op.drop_index(f"uq_traceability_evidence_active_{suffix}", table_name=TABLE)
    op.drop_index("ix_traceability_evidence_links_tenant_vault", table_name=TABLE)
    op.drop_index("ix_traceability_evidence_links_tenant_created", table_name=TABLE)
    op.drop_table(TABLE)
