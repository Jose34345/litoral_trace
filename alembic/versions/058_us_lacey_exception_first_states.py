"""Replace legacy U.S. Lacey review states with Exception-First semantics.

Revision ID: 058_us_lacey_exception_first_states
Revises: 057_us_lacey_learning_plane
"""
from __future__ import annotations

from alembic import op


revision = "058_us_lacey_exception_first_states"
down_revision = "057_us_lacey_learning_plane"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint(
        "ck_us_lacey_fields_status",
        "us_lacey_operation_fields",
        type_="check",
    )

    # High-confidence unambiguous legacy suggestions become explicitly supported.
    op.execute(
        """
        UPDATE public.us_lacey_operation_fields
        SET field_status = 'SUPPORTED'
        WHERE field_status = 'FOUND'
        """
    )

    # Legacy REVIEW rows backed by an actual open reconciliation issue are true
    # conflicts. Everything else is insufficiently resolved evidence and remains
    # Action Required as MISSING rather than inventing a conflict.
    op.execute(
        """
        UPDATE public.us_lacey_operation_fields AS field
        SET field_status = 'CONFLICT'
        WHERE field.field_status = 'REVIEW'
          AND EXISTS (
              SELECT 1
              FROM public.reconciliation_issues AS issue
              WHERE issue.organization_id = field.organization_id
                AND issue.us_lacey_operation_field_id = field.id
                AND issue.status = 'OPEN'
          )
        """
    )
    op.execute(
        """
        UPDATE public.us_lacey_operation_fields
        SET field_status = 'MISSING'
        WHERE field_status = 'REVIEW'
        """
    )

    op.create_check_constraint(
        "ck_us_lacey_fields_status",
        "us_lacey_operation_fields",
        "field_status IN ('MISSING','CONFLICT','SUPPORTED','MATCHED','NOT_REQUIRED')",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_us_lacey_fields_status",
        "us_lacey_operation_fields",
        type_="check",
    )

    op.execute(
        """
        UPDATE public.us_lacey_operation_fields
        SET field_status = 'FOUND'
        WHERE field_status = 'SUPPORTED'
        """
    )
    op.execute(
        """
        UPDATE public.us_lacey_operation_fields
        SET field_status = 'REVIEW'
        WHERE field_status = 'CONFLICT'
        """
    )

    op.create_check_constraint(
        "ck_us_lacey_fields_status",
        "us_lacey_operation_fields",
        "field_status IN ('FOUND','MATCHED','MISSING','REVIEW','NOT_REQUIRED')",
    )
