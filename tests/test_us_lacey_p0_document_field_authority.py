"""P0.1/P0.2 authority and negative provenance cases (no live customer data)."""
from types import SimpleNamespace

import pytest

from litoral_trace.us_lacey.document_evidence_class import classify_document
from litoral_trace.us_lacey.document_field_authority import source_role_supports_field
from litoral_trace.us_lacey.quick_review_eligibility import quick_accept_eligible
from litoral_trace.us_lacey.review_source_authority import is_independent_review_source


class _Rows:
    def __init__(self, row):
        self.row = None if row is None else (
            *row[:4], row[4] if len(row) > 4 else None, "application/pdf", "UNKNOWN",
        )

    def execute(self, statement):
        return SimpleNamespace(first=lambda: self.row)


@pytest.mark.parametrize("role,filename", [
    ("SUPPLIER_DECLARATION", "02_Botanical_Supplier_Declaration.pdf"),
    ("PACKING_LIST", "packing_list.pdf"),
    ("COMMERCIAL_INVOICE", "commercial_invoice.pdf"),
    ("UNKNOWN", "02_Botanical_Supplier_Declaration.pdf"),
    ("BILL_OF_LADING", "02_Botanical_Supplier_Declaration.pdf"),
])
def test_supplier_or_non_carrier_document_cannot_support_a_bill_of_lading(role, filename):
    source = _Rows((role, filename, "available", None))
    assert not is_independent_review_source(
        source, organization_id=1, operation_id=2,
        assurance_document_id=3, field_name="bill_of_lading",
    )


@pytest.mark.parametrize("role,filename", [
    ("BILL_OF_LADING", "MAEU_Bill_of_Lading.pdf"),
    ("ARRIVAL_NOTICE", "arrival_notice.pdf"),
])
def test_current_carrier_document_supports_shipping_reference(role, filename):
    source = _Rows((role, filename, "available", None))
    assert is_independent_review_source(
        source, organization_id=1, operation_id=2,
        assurance_document_id=3, field_name="bill_of_lading",
    )


def test_supplier_role_remains_suitable_for_botanical_field():
    source = _Rows(("SUPPLIER_DECLARATION", "02_Botanical_Supplier_Declaration.pdf", "available", None))
    assert is_independent_review_source(
        source, organization_id=1, operation_id=2,
        assurance_document_id=3, field_name="country_of_harvest",
    )


def test_supplier_filename_is_only_a_hint_not_authority():
    source = classify_document("02_Botanical_Supplier_Declaration.pdf", "UNKNOWN")
    assert source.code == "UNVERIFIED_SUPPLIER"
    assert source.inferred
    assert classify_document(
        "02_Botanical_Supplier_Declaration.pdf", "BILL_OF_LADING"
    ).code == "ROLE_CONFLICT"
    assert classify_document(
        "05_Entry_Preparation_Worksheet.pdf", "SUPPLIER_DECLARATION"
    ).code == "DERIVED_PREPARATION"


def test_quick_review_hides_shipping_accept_when_source_is_supplier_or_wrong_document():
    field = SimpleNamespace(
        field_name="bill_of_lading", status="REVIEW",
        source_assurance_document_id=3,
        source_filename="02_Botanical_Supplier_Declaration.pdf",
    )
    candidate = SimpleNamespace(
        id=10, source_document_id=3, normalized_value="MAEU-TEST-260921-01",
        original_value="MAEU-TEST-260921-01", decision="PENDING",
        validation_status="VALID",
    )
    assert not quick_accept_eligible(field, candidate)
    field.source_filename = "BILL_OF_LADING.pdf"
    candidate.source_document_id = 8
    assert not quick_accept_eligible(field, candidate)


def test_cross_document_field_authority_never_infers_a_role_from_filename():
    assert not source_role_supports_field("bill_of_lading", "UNKNOWN", "Bill_of_Lading.pdf")
    assert source_role_supports_field("bill_of_lading", "BILL_OF_LADING", "Bill_of_Lading.pdf")
    assert not source_role_supports_field("house_bill_of_lading", "SPECIFICATION", "Botanical_certificate.pdf")


def test_content_classification_prefers_botanical_heading_over_incidental_bol_reference():
    from litoral_trace.lacey_engine.classifier import classify, classify_text
    from litoral_trace.lacey_engine.domain import DocumentType
    text = (
        "BOTANICAL SUPPLIER DECLARATION\n"
        "Species: Quercus alba\n"
        "Bill of Lading reference: MAEU-TEST-260921-01\n"
    )
    kind, score = classify_text(text)
    assert kind is DocumentType.SUPPLIER_DECLARATION
    assert score >= 0.55
    layout = SimpleNamespace(blocks=(
        SimpleNamespace(block_type="TEXT_LINE", text="Botanical Supplier Declaration"),
        SimpleNamespace(block_type="TEXT_LINE", text="B/L No: MAEU-TEST-260921-01"),
    ))
    kind, score = classify(layout)
    assert kind is DocumentType.SUPPLIER_DECLARATION
    assert score >= 0.90


def test_automatic_bol_support_requires_distinct_anchored_document_roles():
    from litoral_trace.us_lacey.candidate_reconciliation import _corroborated_shipping_reference

    field = SimpleNamespace(
        field_name="bill_of_lading", field_status="REVIEW",
        reviewed_at=None, human_value=None,
    )
    def row(doc_id, **kw):
        base = dict(
            source_assurance_document_id=doc_id,
            validation_status="VALID", decision="PENDING",
            confidence=0.97, source_page=1, source_locator="p1:block:7",
        )
        base.update(kw)
        return SimpleNamespace(**base)

    rows = [row(11), row(12)]
    roles = {11: ("BILL_OF_LADING", "a" * 64), 12: ("ARRIVAL_NOTICE", "b" * 64)}
    assert _corroborated_shipping_reference(
        field, rows, current_carrier_source=lambda doc_id: roles.get(doc_id),
    )
    assert not _corroborated_shipping_reference(
        field, rows, current_carrier_source=lambda doc_id: ("BILL_OF_LADING", "a" * 64),
    )
    assert not _corroborated_shipping_reference(
        field, rows, current_carrier_source=lambda doc_id: (roles[doc_id][0], "a" * 64),
    )
    assert not _corroborated_shipping_reference(
        field, [row(11), row(11)], current_carrier_source=lambda doc_id: roles.get(doc_id),
    )
    for invalid_rows in (
        [row(11), row(12, confidence=0.94)],
        [row(11), row(12, source_locator="")],
        [row(11), row(12, validation_status="REVIEW_REQUIRED")],
        [row(11), row(12, source_page=None)],
    ):
        assert not _corroborated_shipping_reference(
            field, invalid_rows, current_carrier_source=lambda doc_id: roles.get(doc_id),
        )
    assert not _corroborated_shipping_reference(
        SimpleNamespace(field_name="species", field_status="REVIEW", reviewed_at=None, human_value=None),
        rows, current_carrier_source=lambda doc_id: roles.get(doc_id),
    )
    assert not _corroborated_shipping_reference(
        SimpleNamespace(field_name="bill_of_lading", field_status="MATCHED", reviewed_at=None, human_value=None),
        rows, current_carrier_source=lambda doc_id: roles.get(doc_id),
    )


def test_review_source_identity_requires_current_original_bytes_and_role():
    from litoral_trace.us_lacey.review_source_authority import independent_review_source_identity
    source = _Rows(("BILL_OF_LADING", "bol.pdf", "available", None, "e" * 64))
    assert independent_review_source_identity(
        source, organization_id=1, operation_id=2,
        assurance_document_id=3, field_name="bill_of_lading",
    ) == ("BILL_OF_LADING", "e" * 64)
    stale = _Rows(("BILL_OF_LADING", "bol.pdf", "delete_pending", None, "e" * 64))
    assert independent_review_source_identity(
        stale, organization_id=1, operation_id=2,
        assurance_document_id=3, field_name="bill_of_lading",
    ) is None
    unknown_hash = _Rows(("BILL_OF_LADING", "bol.pdf", "available", None, ""))
    assert independent_review_source_identity(
        unknown_hash, organization_id=1, operation_id=2,
        assurance_document_id=3, field_name="bill_of_lading",
    ) is None


def test_explicit_human_shipping_review_allows_anchored_unknown_original_only():
    from litoral_trace.us_lacey.review_source_authority import (
        is_anchored_human_shipping_source, is_independent_review_source,
    )
    kwargs = dict(
        organization_id=1, operation_id=2, assurance_document_id=3,
        field_name="bill_of_lading", source_page=1, source_locator="p1:block:8",
    )
    original = _Rows(("OTHER", "03_House_BL.pdf", "available", None, "a" * 64))
    assert is_anchored_human_shipping_source(original, **kwargs)
    assert not is_independent_review_source(
        original, organization_id=1, operation_id=2,
        assurance_document_id=3, field_name="bill_of_lading",
    )
    assert not is_anchored_human_shipping_source(
        original, **{**kwargs, "source_page": None},
    )
    assert not is_anchored_human_shipping_source(
        original, **{**kwargs, "source_locator": ""},
    )
    for row in (
        ("OTHER", "02_Botanical_Supplier_Declaration.pdf", "available", None, "b" * 64),
        ("SUPPLIER_DECLARATION", "supplier_declaration.pdf", "available", None, "c" * 64),
        ("OTHER", "entry_preparation_worksheet.pdf", "available", None, "d" * 64),
        ("OTHER", "03_House_BL.pdf", "delete_pending", None, "e" * 64),
    ):
        assert not is_anchored_human_shipping_source(_Rows(row), **kwargs)
