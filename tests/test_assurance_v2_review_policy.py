"""Exception-first policy: fewer human actions without weaker claim authority."""
from __future__ import annotations

from dataclasses import replace

import pytest

from litoral_trace.us_lacey.assurance_v2_review_policy import (
    batch_candidates, batch_fingerprint, simplified_review,
)
from test_assurance_v2_source_claim_review import (
    CLAIM_ID, SOURCE, make_case,
)


def _case(*, claim=None, identities=None, field=None, reuses=(), exceptions=()):
    row = field or {
        "id": "301", "name": "article_component", "line_reference": "1",
        "value": "solid bamboo coasters", "authority": "CANDIDATE",
        "status": "REVIEW",
    }
    case = make_case(claim or SOURCE)
    return replace(
        case, fields=(row,),
        identities=case.identities if identities is None else identities,
        reuses=reuses, exceptions=exceptions,
    )


def test_eligible_current_exact_sku_span_is_one_reviewable_value():
    case = _case()
    candidates = batch_candidates(case)
    assert len(candidates) == 1
    assert candidates[0].field_id == 301
    assert candidates[0].claim_id == CLAIM_ID
    assert candidates[0].document_id == 7
    fp = batch_fingerprint(case, candidates)
    assert len(fp) == 64
    assert fp != batch_fingerprint(replace(case, source_set_revision=6), candidates)


@pytest.mark.parametrize("alteration", [
    {"sku": "OTHER"}, {"support_status": "UNSUPPORTED"},
    {"original_sha256": "0" * 64}, {"source_set_revision_id": 4},
    {"source_locator": None}, {"normalized_value": "a different product"},
    {"candidate_origin": "MODEL"}, {"association_status": "UNBOUND"},
])
def test_unsupported_or_stale_source_never_approved_by_batch(alteration):
    case = _case(claim={**SOURCE, **alteration})
    assert batch_candidates(case) == ()


def test_identity_pending_keeps_all_current_product_values_off_batch():
    case = _case(identities=({
        "status": "REVIEW_REQUIRED", "line_reference": "1",
        "sku": "BAM-COAST-04", "supplier_id": 20, "product_id": 21,
    },))
    assert batch_candidates(case) == ()
    plan = simplified_review(case)
    assert not plan["can_batch"]
    assert len(plan["identities"]) == 1
    assert plan["filing_ready"] is False


def test_historical_botanical_and_conditional_missing_not_silently_approved():
    case = _case(reuses=({
        "id": "x", "status": "ELIGIBLE", "field_name": "genus",
        "line_reference": "1", "value": "Phyllostachys",
        "origin_document_id": "19",
    },))
    plan = simplified_review(case)
    assert plan["can_batch"]  # botanical review remains separate; not a filing approval
    assert len(plan["reuse"]) == 1
    assert plan["batch_count"] == 1  # source-supported is prepared, not granted.
    botanical = _case(field={
        "id": "99", "name": "genus", "line_reference": "1",
        "value": "Phyllostachys", "authority": "CANDIDATE",
        "status": "REVIEW",
    })
    assert batch_candidates(botanical) == ()


def test_true_conflicts_and_missing_safety_abstain():
    conflict = _case(field={
        "id": "301", "name": "article_component", "line_reference": "1",
        "value": "solid bamboo coasters", "authority": "CANDIDATE",
        "status": "CONFLICT",
    })
    assert batch_candidates(conflict) == ()
    case = _case(exceptions=({
        "id": "field:301", "field_name": "article_component",
        "line_reference": "1", "status": "OPEN", "blocking": True,
        "kind": "CONFLICT", "reason": "CONFLICTING_SOURCE_VALUES",
    },))
    plan = simplified_review(case)
    assert not plan["can_batch"]
    assert len(plan["conflicts"]) == 1
