"""Fail-closed V2 batch selection and authorization contracts."""
from dataclasses import replace
import pytest

from litoral_trace.us_lacey.assurance_v2_batch_review import (
    _eligible_case_choices, preview_batch_review, accept_assurance_v2_batch,
)
from test_assurance_v2_source_claim_review import (
    CLAIM_ID, SOURCE, make_case,
)


def _review_case():
    return replace(make_case(), exceptions=({
        "id": "field:42", "field_name": "article_component",
        "line_reference": "1", "blocking": True, "status": "OPEN",
        "candidates": [{
            "id": "source:" + CLAIM_ID, "value": "solid bamboo coasters",
            "document_id": "7", "page": 1,
        }],
    },))


def test_select_one_exact_pdf_claim_after_identity_confirmation():
    result = preview_batch_review(_review_case())
    assert len(result) == 1
    assert result[0]["field_id"] == 42
    assert result[0]["field_name"] == "article_component"
    assert result[0]["document_id"] == 7
    assert result[0]["value"] == "solid bamboo coasters"
    assert result[0]["claim_id"] == CLAIM_ID


@pytest.mark.parametrize("change", [
    {"identities": ()},
    {"documents": ({"id": "7", "version": "2", "sha256": "b" * 64},)},
    {"candidate_observations": ({**SOURCE, "support_status": "UNSUPPORTED"},)},
])
def test_unconfirmed_stale_and_unsupported_sources_never_selectable(change):
    case = replace(_review_case(), **change)
    assert preview_batch_review(case) == ()


def test_conflicting_values_do_not_bulk_approve():
    case = _review_case()
    mismatched = {**case.exceptions[0]}
    mismatched["candidates"] = [
        *mismatched["candidates"],
        {"id": "source:" + "b" * 64, "value": "different product"},
    ]
    assert preview_batch_review(replace(case, exceptions=(mismatched,))) == ()


def test_botanical_authority_not_taken_by_document_batch():
    case = _review_case()
    botanical = {**case.exceptions[0],
                 "field_name": "country_of_harvest"}
    assert preview_batch_review(replace(case, exceptions=(botanical,))) == ()


@pytest.mark.parametrize("selection", [
    (),
    ({"field_id": 1, "claim_id": "a" * 64},) * 2,
    ({"field_id": 1, "claim_id": "a" * 64, "value": "forged"},),
])
def test_bad_selections_fail_before_opening_database(monkeypatch, selection):
    from litoral_trace.us_lacey import assurance_v2_batch_review as module
    monkeypatch.setattr(module, "get_us_lacey_db_session", lambda: pytest.fail("DB opened"))
    with pytest.raises(ValueError):
        accept_assurance_v2_batch(
            organization_id=1,
            operation_public_id="9cb165be-6933-4a5d-86b1-f0a4209240a2",
            authenticated_user_id=1,
            selected=selection, reason="Reviewed original PDF",
            explicit_confirmation=True,
            expected_source_set_fingerprint="f" * 64,
            expected_case_fingerprint="a" * 64,
        )


def test_absent_explicit_review_consent_blocks_even_valid_selection(monkeypatch):
    from litoral_trace.us_lacey import assurance_v2_batch_review as module
    monkeypatch.setattr(module, "get_us_lacey_db_session", lambda: pytest.fail("DB opened"))
    with pytest.raises(ValueError, match="Explicit confirmation"):
        accept_assurance_v2_batch(
            organization_id=1,
            operation_public_id="9cb165be-6933-4a5d-86b1-f0a4209240a2",
            authenticated_user_id=1,
            selected=({"field_id": 42, "claim_id": CLAIM_ID},),
            reason="Reviewed original PDF",
            explicit_confirmation=False,
            expected_source_set_fingerprint="f" * 64,
            expected_case_fingerprint="a" * 64,
        )
