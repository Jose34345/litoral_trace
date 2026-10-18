"""Exception-first pair review and strict source-type boundary regression tests."""
from copy import copy
from pathlib import Path
from types import SimpleNamespace

import pytest

from litoral_trace.us_lacey.paired_review import _validated_pair
from litoral_trace.us_lacey.review import UsLaceyReviewError
from litoral_trace.us_lacey.document_evidence_class import classify_document
from litoral_trace.us_lacey.shadow_evidence_snapshot import (
    _non_authoritative_shadow_document_type,
)


def _row(name, value, *, document_id=20, source="canonical:1:source"):
    return SimpleNamespace(
        field_name=name, field_status="REVIEW", reviewed_at=None,
        human_value=None, field_scope="PLANT_LINE",
        extractor="canonical-shipment-truth", source_locator=source,
        normalized_value=value, operation_id=39, plant_line_id=17,
        merchandise_line_reference="1", source_assurance_document_id=document_id,
        source_page=1,
    )


def test_pack2_exact_plant_mass_pair_stays_human_review_only():
    quantity, unit = _row("plant_quantity", "288"), _row("metric_unit", "kg")
    _validated_pair((quantity, unit), expected=("288", "kg"),
                    locators=(quantity.source_locator, unit.source_locator))
    assert quantity.field_status == "REVIEW"
    assert unit.field_status == "REVIEW"
    assert quantity.human_value is None and unit.human_value is None


@pytest.mark.parametrize("alteration", [
    "document", "line", "plant_line", "page", "worksheet_locator",
    "stale_value", "stale_locator", "already_reviewed", "status",
    "wrong_extractor", "missing_document",
])
def test_pair_rejects_stale_ambiguous_or_cross_document_sources(alteration):
    quantity, unit = _row("plant_quantity", "288"), _row("metric_unit", "kg")
    if alteration == "document":
        unit.source_assurance_document_id = 21
    elif alteration == "line":
        unit.merchandise_line_reference = "2"
    elif alteration == "plant_line":
        unit.plant_line_id = 18
    elif alteration == "page":
        unit.source_page = 2
    elif alteration == "worksheet_locator":
        unit.source_locator = "worksheet:1:derived"
    elif alteration == "already_reviewed":
        unit.human_value = "kg"
    elif alteration == "status":
        unit.field_status = "SUPPORTED"
    elif alteration == "wrong_extractor":
        unit.extractor = "unverified-worksheet"
    elif alteration == "missing_document":
        unit.source_assurance_document_id = None
    with pytest.raises(UsLaceyReviewError):
        _validated_pair(
            (quantity, unit), expected=("289" if alteration == "stale_value" else "288", "kg"),
            locators=("stale" if alteration == "stale_locator" else quantity.source_locator,
                      "canonical:1:source"),
        )


def test_legacy_invoice_keyword_does_not_launder_lacey_document_authority():
    assert _non_authoritative_shadow_document_type("INVOICE", "UNKNOWN") == "UNKNOWN"
    assert _non_authoritative_shadow_document_type("INVOICE", "PACKING_LIST") == "UNKNOWN"
    assert _non_authoritative_shadow_document_type("INVOICE", "BILL_OF_LADING") == "UNKNOWN"
    assert _non_authoritative_shadow_document_type("INVOICE", "COMMERCIAL_INVOICE") == "INVOICE"
    assert _non_authoritative_shadow_document_type("UNKNOWN", "UNKNOWN") == "UNKNOWN"
    assert classify_document("05_Entry_Preparation_Worksheet_Shipment2.pdf", "COMMERCIAL_INVOICE").code == "DERIVED_PREPARATION"
    assert classify_document("02_Packing_List_Shipment2.pdf", "UNKNOWN").code == "UNVERIFIED_COMMERCIAL"


def test_review_pair_uses_authenticated_single_action_and_two_field_journals():
    root = Path(__file__).resolve().parents[1]
    code = (root / "src/litoral_trace/us_lacey/paired_review.py").read_text(encoding="utf-8")
    web = (root / "src/litoral_trace/web/us_lacey_pilot_app.py").read_text(encoding="utf-8")
    html = (root / "src/litoral_trace/templates/us_lacey/fragments/operation_workspace.html").read_text(encoding="utf-8")
    js = (root / "src/litoral_trace/static/src/js/us-lacey-workspace.js").read_text(encoding="utf-8")
    assert "with_for_update()" in code and 'status == "FINALIZED"' in code
    assert "review_us_lacey_field(" in code and "_defer_commit=True" in code
    assert 'purpose=f"complete:{operation_public_id}"' in web
    assert 'data-review-group="plant-mass"' in html
    assert 'data-pre-entry-waiting' in html
    assert 'hx-post="/operations/{{ detail.public_id }}/review/actions/mass-pair"' in html
    assert "htmx:afterSettle" in js and "data-processing-terminal" in js
    assert "Review could not be saved. Check the reason shown above." in js
