"""P0 fail-closed acceptance contract for derived-only documentary proposals."""
from types import SimpleNamespace

from litoral_trace.us_lacey.review_source_authority import (
    is_independent_review_source, reviewed_source_class,
)
from litoral_trace.us_lacey.quick_review_eligibility import quick_accept_eligible


class SourceRows:
    def __init__(self, row):
        self.row = row
        self.statement = None

    def execute(self, statement):
        self.statement = statement
        return SimpleNamespace(first=lambda: self.row)


def test_worksheet_filename_cannot_become_original_by_declared_role():
    db = SourceRows(("COMMERCIAL_INVOICE", "05_Entry_Preparation_Worksheet_Shipment2.pdf", "available", None))
    authority = reviewed_source_class(
        db, organization_id=43, operation_id=9, assurance_document_id=81,
    )
    assert authority.code == "DERIVED_PREPARATION"
    assert not is_independent_review_source(
        db, organization_id=43, operation_id=9, assurance_document_id=81,
    )
    sql = str(db.statement.compile(compile_kwargs={"literal_binds": True}))
    assert "organization_id = 43" in sql
    assert "operation_id = 9" in sql
    assert "assurance_document_id = 81" in sql
    assert "is_current IS true" in sql or "is_current = true" in sql


def test_derived_role_overrides_misleading_original_filename():
    db = SourceRows(("ENTRY_WORKSHEET", "commercial_invoice.pdf", "available", None))
    assert not is_independent_review_source(
        db, organization_id=7, operation_id=8, assurance_document_id=21,
    )


def test_absent_and_superseded_document_fail_closed():
    db = SourceRows(None)
    assert reviewed_source_class(
        db, organization_id=7, operation_id=8, assurance_document_id=None,
    ) is None
    assert not is_independent_review_source(
        db, organization_id=7, operation_id=8, assurance_document_id=21,
    )


def test_current_original_document_is_eligible_for_human_inspection():
    db = SourceRows(("COMMERCIAL_INVOICE", "invoice.pdf", "available", None))
    assert is_independent_review_source(
        db, organization_id=7, operation_id=8, assurance_document_id=21,
    )


def test_quick_accept_fails_closed_for_known_worksheet_even_if_ppq_valid():
    candidate = SimpleNamespace(
        id=12, source_document_id=21, decision="PENDING",
        original_value="288", normalized_value="288", validation_status="VALID",
    )
    field = SimpleNamespace(
        field_name="plant_quantity", status="REVIEW",
        source_assurance_document_id=21,
        source_filename="05_Entry_Preparation_Worksheet_Shipment2.pdf",
    )
    assert not quick_accept_eligible(field, candidate)
    field.source_filename = "supplier_plant_material_declaration.pdf"
    assert quick_accept_eligible(field, candidate)


def test_expired_original_cannot_authorize_a_new_human_decision():
    for record in (
        ("COMMERCIAL_INVOICE", "invoice.pdf", "delete_pending", None),
        ("COMMERCIAL_INVOICE", "invoice.pdf", "available", "deleted"),
    ):
        db = SourceRows(record)
        assert not is_independent_review_source(
            db, organization_id=7, operation_id=8, assurance_document_id=21,
        )
