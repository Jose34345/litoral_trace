"""Add U.S. Lacey supplier identifiers and explicit operation/product binding.

Revision ID: 075_us_lacey_identity_and_product_bridge
Revises: 074_us_lacey_audit_identity_projection
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "075_us_lacey_identity_and_product_bridge"
down_revision = "074_us_lacey_audit_identity_projection"
branch_labels = None
depends_on = None

RUNTIME_ROLE = "litoral_trace_app"
TENANT_CONTEXT_SQL = "NULLIF(current_setting('app.current_organization_id', true), '')::integer"

NEW_TABLES = (
    "us_lacey_supplier_identifier",
    "us_lacey_operation_product_link",
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
        f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.{table} "
        f"TO {RUNTIME_ROLE}"
    )
    op.execute(
        f"REVOKE ALL PRIVILEGES ON SEQUENCE public.{table}_id_seq "
        "FROM PUBLIC, anon, authenticated"
    )
    op.execute(
        f"GRANT USAGE, SELECT ON SEQUENCE public.{table}_id_seq "
        f"TO {RUNTIME_ROLE}"
    )


def upgrade() -> None:
    # Keep existing ACTIVE/NEEDS_REVIEW values valid while adding non-authoritative
    # discovery states used by the safe backfill path.
    op.drop_constraint(
        "ck_us_lacey_supplier_status",
        "us_lacey_supplier",
        type_="check",
    )
    op.alter_column(
        "us_lacey_supplier",
        "status",
        existing_type=sa.String(length=16),
        type_=sa.String(length=24),
        existing_nullable=False,
        existing_server_default="ACTIVE",
    )
    op.create_check_constraint(
        "ck_us_lacey_supplier_status",
        "us_lacey_supplier",
        "status IN ('ACTIVE','VERIFIED','DISCOVERED','NEEDS_VERIFICATION','INACTIVE','NEEDS_REVIEW')",
    )

    op.drop_constraint(
        "ck_us_lacey_supplier_product_status",
        "us_lacey_supplier_product",
        type_="check",
    )
    op.alter_column(
        "us_lacey_supplier_product",
        "status",
        existing_type=sa.String(length=16),
        type_=sa.String(length=24),
        existing_nullable=False,
        existing_server_default="ACTIVE",
    )
    op.create_check_constraint(
        "ck_us_lacey_supplier_product_status",
        "us_lacey_supplier_product",
        "status IN ('ACTIVE','VERIFIED','DISCOVERED','NEEDS_VERIFICATION','INACTIVE','NEEDS_REVIEW')",
    )

    op.create_table(
        "us_lacey_supplier_identifier",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("supplier_id", sa.Integer(), nullable=False),
        sa.Column("identifier_type", sa.String(24), nullable=False),
        sa.Column("normalized_value", sa.String(512), nullable=False),
        sa.Column("display_value", sa.String(512), nullable=True),
        sa.Column("source_assurance_document_id", sa.Integer(), nullable=True),
        sa.Column(
            "human_confirmed",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(
            ["supplier_id", "organization_id"],
            ["us_lacey_supplier.id", "us_lacey_supplier.organization_id"],
            name="fk_us_lacey_supplier_identifier_supplier_tenant",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["source_assurance_document_id", "organization_id"],
            ["assurance_documents.id", "assurance_documents.organization_id"],
            name="fk_us_lacey_supplier_identifier_document_tenant",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint(
            "id",
            "organization_id",
            name="uq_us_lacey_supplier_identifier_id_org",
        ),
        sa.UniqueConstraint(
            "organization_id",
            "identifier_type",
            "normalized_value",
            name="uq_us_lacey_supplier_identifier_value",
        ),
        sa.CheckConstraint(
            "identifier_type IN ('MID','VENDOR_CODE','NAME_ADDRESS','MANUAL')",
            name="ck_us_lacey_supplier_identifier_type",
        ),
    )
    op.create_index(
        "ix_us_lacey_supplier_identifier_org_supplier",
        "us_lacey_supplier_identifier",
        ["organization_id", "supplier_id"],
    )
    op.create_index(
        "ix_us_lacey_supplier_identifier_org_lookup",
        "us_lacey_supplier_identifier",
        ["organization_id", "identifier_type", "normalized_value"],
    )

    op.create_table(
        "us_lacey_operation_product_link",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("public_id", sa.Uuid(), nullable=False),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("operation_id", sa.Integer(), nullable=False),
        sa.Column("source_set_revision_id", sa.Integer(), nullable=False),
        sa.Column("line_reference", sa.String(100), nullable=False),
        sa.Column("supplier_product_id", sa.Integer(), nullable=False),
        sa.Column("link_method", sa.String(24), nullable=False),
        sa.Column(
            "confirmed_by_user_id",
            sa.Integer(),
            sa.ForeignKey("users.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("confirmed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(
            ["operation_id", "organization_id"],
            ["us_lacey_operations.id", "us_lacey_operations.organization_id"],
            name="fk_us_lacey_operation_product_link_operation_tenant",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["supplier_product_id", "organization_id"],
            ["us_lacey_supplier_product.id", "us_lacey_supplier_product.organization_id"],
            name="fk_us_lacey_operation_product_link_product_tenant",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["source_set_revision_id", "organization_id"],
            ["us_lacey_source_set_revisions.id", "us_lacey_source_set_revisions.organization_id"],
            name="fk_us_lacey_operation_product_link_revision_tenant",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint(
            "public_id",
            name="uq_us_lacey_operation_product_link_public_id",
        ),
        sa.UniqueConstraint(
            "id",
            "organization_id",
            name="uq_us_lacey_operation_product_link_id_org",
        ),
        sa.UniqueConstraint(
            "organization_id",
            "source_set_revision_id",
            "line_reference",
            name="uq_us_lacey_operation_product_link_revision_line",
        ),
        sa.CheckConstraint(
            "link_method IN ('EXACT_SKU','HUMAN_CONFIRMED')",
            name="ck_us_lacey_operation_product_link_method",
        ),
        sa.CheckConstraint(
            "(link_method = 'EXACT_SKU' AND confirmed_by_user_id IS NULL) OR "
            "(link_method = 'HUMAN_CONFIRMED' AND confirmed_by_user_id IS NOT NULL)",
            name="ck_us_lacey_operation_product_link_confirmation",
        ),
    )
    op.create_index(
        "ix_us_lacey_operation_product_link_org_operation",
        "us_lacey_operation_product_link",
        ["organization_id", "operation_id"],
    )
    op.create_index(
        "ix_us_lacey_operation_product_link_org_product",
        "us_lacey_operation_product_link",
        ["organization_id", "supplier_product_id"],
    )

    for table in NEW_TABLES:
        _secure_table(table)


def downgrade() -> None:
    for table in reversed(NEW_TABLES):
        for action in ("delete", "update", "insert", "select"):
            op.execute(
                f"DROP POLICY IF EXISTS {table}_tenant_{action} ON public.{table}"
            )
        op.drop_table(table)

    # Conservative downgrade mapping: historical discovered identities are not
    # upgraded to ACTIVE; they return to NEEDS_REVIEW.
    op.execute(
        "UPDATE public.us_lacey_supplier "
        "SET status = 'NEEDS_REVIEW' "
        "WHERE status IN ('DISCOVERED','NEEDS_VERIFICATION')"
    )
    op.execute(
        "UPDATE public.us_lacey_supplier "
        "SET status = 'ACTIVE' WHERE status = 'VERIFIED'"
    )
    op.execute(
        "UPDATE public.us_lacey_supplier_product "
        "SET status = 'NEEDS_REVIEW' "
        "WHERE status IN ('DISCOVERED','NEEDS_VERIFICATION')"
    )
    op.execute(
        "UPDATE public.us_lacey_supplier_product "
        "SET status = 'ACTIVE' WHERE status = 'VERIFIED'"
    )

    op.drop_constraint(
        "ck_us_lacey_supplier_status",
        "us_lacey_supplier",
        type_="check",
    )
    op.alter_column(
        "us_lacey_supplier",
        "status",
        existing_type=sa.String(length=24),
        type_=sa.String(length=16),
        existing_nullable=False,
        existing_server_default="ACTIVE",
    )
    op.create_check_constraint(
        "ck_us_lacey_supplier_status",
        "us_lacey_supplier",
        "status IN ('ACTIVE','INACTIVE','NEEDS_REVIEW')",
    )

    op.drop_constraint(
        "ck_us_lacey_supplier_product_status",
        "us_lacey_supplier_product",
        type_="check",
    )
    op.alter_column(
        "us_lacey_supplier_product",
        "status",
        existing_type=sa.String(length=24),
        type_=sa.String(length=16),
        existing_nullable=False,
        existing_server_default="ACTIVE",
    )
    op.create_check_constraint(
        "ck_us_lacey_supplier_product_status",
        "us_lacey_supplier_product",
        "status IN ('ACTIVE','INACTIVE','NEEDS_REVIEW')",
    )
