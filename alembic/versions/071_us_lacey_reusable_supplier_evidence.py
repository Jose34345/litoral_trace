"""Add reusable supplier/product evidence for U.S. Lacey workflows.

Revision ID: 071_us_lacey_reusable_supplier_evidence
Revises: 070_us_lacey_paddle_billing
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "071_us_lacey_reusable_supplier_evidence"
down_revision = "070_us_lacey_paddle_billing"
branch_labels = None
depends_on = None

RUNTIME_ROLE = "litoral_trace_app"
TENANT_CONTEXT_SQL = "NULLIF(current_setting('app.current_organization_id', true), '')::integer"

TABLES = (
    "us_lacey_supplier",
    "us_lacey_supplier_product",
    "us_lacey_supplier_evidence",
    "us_lacey_evidence_claim",
)


def _secure_table(table: str) -> None:
    predicate = f"organization_id = {TENANT_CONTEXT_SQL}"
    op.execute(f"ALTER TABLE public.{table} ENABLE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE public.{table} FORCE ROW LEVEL SECURITY")
    for action, clause in (
        ("select", "USING"),
        ("insert", "WITH CHECK"),
        ("update", "USING"),
        ("delete", "USING"),
    ):
        suffix = f"{clause} ({predicate})"
        if action == "update":
            suffix += f" WITH CHECK ({predicate})"
        op.execute(
            f"CREATE POLICY {table}_tenant_{action} ON public.{table} "
            f"FOR {action.upper()} TO {RUNTIME_ROLE} {suffix}"
        )

    op.execute(
        f"REVOKE ALL PRIVILEGES ON TABLE public.{table} "
        "FROM PUBLIC, anon, authenticated"
    )
    op.execute(
        f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.{table} TO {RUNTIME_ROLE}"
    )
    op.execute(
        f"REVOKE ALL PRIVILEGES ON SEQUENCE public.{table}_id_seq "
        "FROM PUBLIC, anon, authenticated"
    )
    op.execute(
        f"GRANT USAGE, SELECT ON SEQUENCE public.{table}_id_seq TO {RUNTIME_ROLE}"
    )


def upgrade() -> None:
    op.create_table(
        "us_lacey_supplier",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("public_id", sa.Uuid(), nullable=False),
        sa.Column(
            "organization_id",
            sa.Integer(),
            sa.ForeignKey("organizations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("supplier_key", sa.String(128), nullable=False),
        sa.Column("display_name", sa.String(255), nullable=False),
        sa.Column("normalized_name", sa.String(255), nullable=False),
        sa.Column("status", sa.String(16), nullable=False, server_default="ACTIVE"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.UniqueConstraint("public_id", name="uq_us_lacey_supplier_public_id"),
        sa.UniqueConstraint("id", "organization_id", name="uq_us_lacey_supplier_id_org"),
        sa.UniqueConstraint(
            "organization_id",
            "supplier_key",
            name="uq_us_lacey_supplier_org_key",
        ),
        sa.CheckConstraint(
            "status IN ('ACTIVE','INACTIVE','NEEDS_REVIEW')",
            name="ck_us_lacey_supplier_status",
        ),
    )
    op.create_index(
        "ix_us_lacey_supplier_org",
        "us_lacey_supplier",
        ["organization_id"],
    )
    op.create_index(
        "ix_us_lacey_supplier_org_normalized_name",
        "us_lacey_supplier",
        ["organization_id", "normalized_name"],
    )

    op.create_table(
        "us_lacey_supplier_product",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("public_id", sa.Uuid(), nullable=False),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("supplier_id", sa.Integer(), nullable=False),
        sa.Column("product_key", sa.String(128), nullable=False),
        sa.Column("sku", sa.String(128), nullable=True),
        sa.Column("display_name", sa.String(255), nullable=True),
        sa.Column("normalized_name", sa.String(255), nullable=True),
        sa.Column("status", sa.String(16), nullable=False, server_default="ACTIVE"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(
            ["supplier_id", "organization_id"],
            ["us_lacey_supplier.id", "us_lacey_supplier.organization_id"],
            name="fk_us_lacey_supplier_product_supplier_tenant",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint(
            "public_id",
            name="uq_us_lacey_supplier_product_public_id",
        ),
        sa.UniqueConstraint(
            "id",
            "organization_id",
            name="uq_us_lacey_supplier_product_id_org",
        ),
        sa.UniqueConstraint(
            "organization_id",
            "supplier_id",
            "product_key",
            name="uq_us_lacey_supplier_product_identity",
        ),
        sa.CheckConstraint(
            "status IN ('ACTIVE','INACTIVE','NEEDS_REVIEW')",
            name="ck_us_lacey_supplier_product_status",
        ),
    )
    op.create_index(
        "ix_us_lacey_supplier_product_org_supplier",
        "us_lacey_supplier_product",
        ["organization_id", "supplier_id"],
    )
    op.create_index(
        "ix_us_lacey_supplier_product_org_key",
        "us_lacey_supplier_product",
        ["organization_id", "product_key"],
    )
    op.create_index(
        "ix_us_lacey_supplier_product_org_sku",
        "us_lacey_supplier_product",
        ["organization_id", "sku"],
    )

    op.create_table(
        "us_lacey_supplier_evidence",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("public_id", sa.Uuid(), nullable=False),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("supplier_product_id", sa.Integer(), nullable=False),
        sa.Column("evidence_type", sa.String(64), nullable=False),
        sa.Column("document_hash", sa.String(64), nullable=False),
        sa.Column("source_reference", sa.String(512), nullable=True),
        sa.Column("valid_from", sa.DateTime(timezone=True), nullable=False),
        sa.Column("valid_until", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "verified_by_user_id",
            sa.Integer(),
            sa.ForeignKey("users.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("verified_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status", sa.String(16), nullable=False, server_default="VERIFIED"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(
            ["supplier_product_id", "organization_id"],
            ["us_lacey_supplier_product.id", "us_lacey_supplier_product.organization_id"],
            name="fk_us_lacey_supplier_evidence_product_tenant",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint(
            "public_id",
            name="uq_us_lacey_supplier_evidence_public_id",
        ),
        sa.UniqueConstraint(
            "id",
            "organization_id",
            name="uq_us_lacey_supplier_evidence_id_org",
        ),
        sa.UniqueConstraint(
            "organization_id",
            "supplier_product_id",
            "document_hash",
            "evidence_type",
            name="uq_us_lacey_supplier_evidence_document",
        ),
        sa.CheckConstraint(
            "length(document_hash) = 64",
            name="ck_us_lacey_supplier_evidence_hash_length",
        ),
        sa.CheckConstraint(
            "valid_until > valid_from",
            name="ck_us_lacey_supplier_evidence_valid_window",
        ),
        sa.CheckConstraint(
            "status IN ('VERIFIED','REVOKED')",
            name="ck_us_lacey_supplier_evidence_status",
        ),
    )
    op.create_index(
        "ix_us_lacey_supplier_evidence_org_product_validity",
        "us_lacey_supplier_evidence",
        ["organization_id", "supplier_product_id", "status", "valid_until"],
    )
    op.create_index(
        "ix_us_lacey_supplier_evidence_org_hash",
        "us_lacey_supplier_evidence",
        ["organization_id", "document_hash"],
    )

    op.create_table(
        "us_lacey_evidence_claim",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("evidence_id", sa.Integer(), nullable=False),
        sa.Column("field_name", sa.String(100), nullable=False),
        sa.Column("field_value", sa.Text(), nullable=False),
        sa.Column("normalized_value", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(
            ["evidence_id", "organization_id"],
            ["us_lacey_supplier_evidence.id", "us_lacey_supplier_evidence.organization_id"],
            name="fk_us_lacey_evidence_claim_evidence_tenant",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint(
            "id",
            "organization_id",
            name="uq_us_lacey_evidence_claim_id_org",
        ),
    )
    op.create_index(
        "ix_us_lacey_evidence_claim_org_evidence_field",
        "us_lacey_evidence_claim",
        ["organization_id", "evidence_id", "field_name"],
    )
    op.create_index(
        "ix_us_lacey_evidence_claim_org_field",
        "us_lacey_evidence_claim",
        ["organization_id", "field_name"],
    )

    for table in TABLES:
        _secure_table(table)


def downgrade() -> None:
    for table in reversed(TABLES):
        for action in ("delete", "update", "insert", "select"):
            op.execute(
                f"DROP POLICY IF EXISTS {table}_tenant_{action} ON public.{table}"
            )
        op.drop_table(table)
