"""Controlled reuse must be blocked by unknown or competing current evidence."""
from litoral_trace.us_lacey.assurance_v2_reuse_guard import current_claim_values_for_reuse


def test_missing_snapshot_is_not_empty_evidence():
    assert current_claim_values_for_reuse(None, field_name="species", line_reference="1") is None
    assert current_claim_values_for_reuse((), field_name="species", line_reference="1") == ()


def test_current_same_line_and_unbound_claims_take_precedence():
    claim = {
        "field_name": "species", "line_reference": "1",
        "normalized_value": "Acer rubrum", "original_value": "Acer rubrum",
    }
    assert current_claim_values_for_reuse((claim,), field_name="species", line_reference="1") == ("Acer rubrum",)
    claim["line_reference"] = None
    assert current_claim_values_for_reuse((claim,), field_name="species", line_reference="1") == ("Acer rubrum",)
    claim["line_reference"] = "2"
    assert current_claim_values_for_reuse((claim,), field_name="species", line_reference="1") == ()


def test_placeholders_are_not_botanical_facts_but_unknowns_fail_closed():
    placeholder = {
        "field_name": "species", "line_reference": "1",
        "original_value": "TO BE FILLED FROM REVIEWED SUPPLIER MEMORY",
        "normalized_value": None, "reason_codes": ["NONFACTUAL_PLACEHOLDER"],
    }
    assert current_claim_values_for_reuse((placeholder,), field_name="species", line_reference="1") == ()
    placeholder["reason_codes"] = []
    assert current_claim_values_for_reuse((placeholder,), field_name="species", line_reference="1")
    placeholder["original_value"] = ""
    assert current_claim_values_for_reuse((placeholder,), field_name="species", line_reference="1") is None
