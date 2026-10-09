"""Add explicit exception-first U.S. Lacey field states.

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
    op.create_check_constraint(
        "ck_us_lacey_fields_status",
        "us_lacey_operation_fields",
        "field_status IN ("
        "'FOUND','MATCHED','MISSING','REVIEW','NOT_REQUIRED',"
        "'SUPPORTED','CONFLICT'"
        ")",
    )


def downgrade() -> None:
    op.execute(
        """
        UPDATE public.us_lacey_operation_fields
        SET field_status = CASE
            WHEN field_status = 'SUPPORTED' THEN 'FOUND'
            WHEN field_status = 'CONFLICT' THEN 'REVIEW'
            ELSE field_status
        END
        WHERE field_status IN ('SUPPORTED', 'CONFLICT')
        """
    )
    op.drop_constraint(
        "ck_us_lacey_fields_status",
        "us_lacey_operation_fields",
        type_="check",
    )
    op.create_check_constraint(
        "ck_us_lacey_fields_status",
        "us_lacey_operation_fields",
        "field_status IN ('FOUND','MATCHED','MISSING','REVIEW','NOT_REQUIRED')",
    )
