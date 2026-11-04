"""Regression coverage for ORM constraints that migrations alone cannot import-check."""
from litoral_trace.db.models import (
    UsLaceyOperationDocument,
    UsLaceySourceSetMember,
    UsLaceySourceSetRevision,
)


def test_source_set_model_constraints_reference_only_owned_columns():
    for model in (UsLaceyOperationDocument, UsLaceySourceSetRevision, UsLaceySourceSetMember):
        columns = set(model.__table__.columns.keys())
        for constraint in model.__table__.constraints:
            assert set(constraint.columns.keys()).issubset(columns)

    assert "operation_id" not in UsLaceySourceSetMember.__table__.columns
    assert "version_number" not in UsLaceySourceSetMember.__table__.columns
