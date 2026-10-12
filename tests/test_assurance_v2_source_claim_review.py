"""Reviewer may select literal current source, never a guessed product or field."""
from __future__ import annotations

from dataclasses import replace

import pytest

from litoral_trace.us_lacey.exporters.assurance_v2 import AssuranceCase
from litoral_trace.us_lacey.assurance_v2_source_claim_review import (
    select_reviewable_source_claim,
)

CLAIM_ID = "a" * 64
DOC_HASH = "c" * 64
SOURCE = {
    "id": CLAIM_ID, "field_name": "article_component",
    "candidate_origin": "DOCUMENT", "state": "CANDIDATE",
    "authority_status": "CANDIDATE", "support_status": "SUPPORTED",
    "organization_id": 11, "operation_id": 99,
    "source_set_revision_id": 5, "source_set_fingerprint": "f" * 64,
    "generation": 4, "association_status": "DOCUMENT_EXACT_SINGLE_SKU",
    "line_reference": "1", "sku": "BAM-COAST-04",
    "assurance_document_id": 7, "document_version_number": 2,
    "original_sha256": DOC_HASH,
    "source_locator": {"kind": "TEXT_SPAN", "page": 1,
                       "block_id": "p1-l21", "start": 20, "end": 40},
    "raw_value": "Solid bamboo coasters",
    "normalized_value": "solid bamboo coasters",
}


def make_case(source=None):
    return AssuranceCase(
        organization_id=11, operation_id="9cb165be-6933-4a5d-86b1-f0a4209240a2",
        source_set_fingerprint="f" * 64, source_set_revision=5,
        current_source_set_fingerprint="f" * 64, current_source_set_revision=5,
        source_set_generation=4, ruleset_version="rules-test",
        documents=({"id": "7", "version": "2", "sha256": DOC_HASH},),
        lines=("1",),
        identities=({"line_reference": "1", "status": "HUMAN_CONFIRMED",
                     "sku": "BAM-COAST-04", "supplier_id": 20, "product_id": 21},),
        candidate_observations=(source or SOURCE,),
    )


def test_current_source_claim_can_be_selected_but_does_not_grant_authority():
    result = select_reviewable_source_claim(
        make_case(), claim_id=CLAIM_ID, field_name="article_component",
        line_reference="1",
    )
    assert result.assurance_document_id == 7
    assert result.normalized_value == "solid bamboo coasters"
    assert result.locator.startswith("assurance_v2_source_claim:")
    assert result.page == 1


@pytest.mark.parametrize("alteration", [
    {"sku": "DIFFERENT-SKU"}, {"support_status": "UNSUPPORTED"},
    {"authority_status": "HUMAN_CONFIRMED"}, {"field_name": "species"},
    {"source_set_revision_id": 4}, {"source_set_fingerprint": "0" * 64},
    {"original_sha256": "b" * 64},
    {"source_locator": None},
    {"source_locator": {"kind": "TEXT_SPAN", "page": 1, "block_id": "p1-l21",
                        "start": 30, "end": 10}},
    {"normalized_value": ""},
])
def test_incompatible_or_unverifiable_candidate_is_denied(alteration):
    with pytest.raises(ValueError):
        select_reviewable_source_claim(
            make_case({**SOURCE, **alteration}),
            claim_id=CLAIM_ID, field_name="article_component", line_reference="1",
        )


def test_unconfirmed_line_and_wrong_target_fail_closed():
    case = make_case()
    with pytest.raises(ValueError):
        select_reviewable_source_claim(
            case, claim_id=CLAIM_ID, field_name="article_component",
            line_reference="2",
        )
    with pytest.raises(ValueError):
        select_reviewable_source_claim(
            case, claim_id=CLAIM_ID, field_name="country_of_harvest",
            line_reference="1",
        )
    with pytest.raises(ValueError):
        select_reviewable_source_claim(
            replace(case, identities=()),
            claim_id=CLAIM_ID, field_name="article_component",
            line_reference="1",
        )
