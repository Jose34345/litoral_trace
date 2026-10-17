"""P0 Pack2: keep claims visible without multiplying blockers or granting authority."""
from __future__ import annotations
import pytest

from litoral_trace.us_lacey.assurance_v2_case_reader import (
    _observation_review_state, _source_review_candidates,
)
from litoral_trace.us_lacey.assurance_v2_evidence_checklist import (
    required_evidence_checklist,
)
from litoral_trace.us_lacey.exporters.assurance_v2 import AssuranceCase

SHIPMENT = "__shipment__"
IDENTITY = ({
    "line_reference": "1", "sku": "BAM-COAST-04",
    "status": "REVIEW_REQUIRED",
},)

def _claim(*, name="plant_quantity", association="DOCUMENT_EXACT_SINGLE_SKU",
           line="1", sku="BAM-COAST-04", entity="PRODUCT", value="288.0"):
    return {
        "field_name": name, "association_status": association,
        "line_reference": line, "sku": sku, "entity_type": entity,
        "normalized_value": value, "support_status": "SUPPORTED",
        "state": "CANDIDATE", "authority_status": "CANDIDATE",
        "source_locator": {"kind": "TEXT_SPAN", "page": 1, "block_id": "p1-l25",
                           "start": 27, "end": 32},
        "id": "a" * 64, "assurance_document_id": 23,
    }


def test_shipments_can_have_unbound_source_scope_without_being_unbound_identity():
    claim = _claim(
        name="merchandise_description", entity="SHIPMENT",
        line=None, sku=None, association="UNBOUND",
        value="retail boxed bamboo coaster sets",
    )
    assert _observation_review_state(
        claim, line_reference=SHIPMENT, identities=IDENTITY,
    ) == "REVIEW_SOURCE"
    ready, waiting = _source_review_candidates(
        (claim,), field_name="merchandise_description",
        line_reference=SHIPMENT, identities=IDENTITY,
    )
    assert len(ready) == 1 and waiting == []
    assert ready[0]["id"].startswith("source:")


def test_exact_sku_source_waits_for_identity_before_it_is_selectable():
    source = _claim()
    ready, waiting = _source_review_candidates(
        (source,), field_name="plant_quantity",
        line_reference="1", identities=IDENTITY,
    )
    assert not ready and len(waiting) == 1
    assert waiting[0]["id"].startswith("pending:")
    verified = ({**IDENTITY[0], "status": "HUMAN_CONFIRMED"},)
    ready, waiting = _source_review_candidates(
        (source,), field_name="plant_quantity",
        line_reference="1", identities=verified,
    )
    assert len(ready) == 1 and not waiting
    assert ready[0]["id"] == "source:" + source["id"]


@pytest.mark.parametrize("bad", [
    {"sku": "UNRELATED-SKU"},
    {"line_reference": "2"},
    {"association_status": "UNBOUND"},
    {"support_status": "UNSUPPORTED"},
    {"source_locator": None},
])
def test_unbound_stale_or_wrong_line_never_becomes_selectable(bad):
    claim = {**_claim(), **bad}
    ready, waiting = _source_review_candidates(
        (claim,), field_name="plant_quantity",
        line_reference="1", identities=({**IDENTITY[0], "status": "HUMAN_CONFIRMED"},),
    )
    assert ready == [] and waiting == []
