from __future__ import annotations

from litoral_trace.us_lacey.ppq505 import (
    PPQ505_ALLOWED_UNITS,
    PPQ505_PLANT_FIELDS,
    PPQ505_SHIPMENT_FIELDS,
    PpqValidationStatus,
    is_paper_or_paperboard,
    not_required_allowed,
    validate_ppq_value,
)


def test_ppq_units_are_explicit_lowercase_metric_values():
    assert PPQ505_ALLOWED_UNITS == {
        "kg", "g", "cg", "mg", "kl", "l", "ml", "mm", "mm2", "mm3",
        "cm", "cm2", "cm3", "m", "m2", "m3", "km",
    }
    assert validate_ppq_value("metric_unit", "KG").normalized_value == "kg"
    assert validate_ppq_value("metric_unit", "cm²").normalized_value == "cm2"
    assert validate_ppq_value("metric_unit", "NO").status is PpqValidationStatus.INVALID


def test_entry_number_requires_cbp_shape_and_preserves_controlled_format():
    valid = validate_ppq_value("filing_entry_reference", "123-4567890-1")
    assert valid.status is PpqValidationStatus.VALID
    assert valid.normalized_value == "123-4567890-1"
    assert validate_ppq_value("filing_entry_reference", "12345678901").normalized_value == "123-4567890-1"
    assert validate_ppq_value("filing_entry_reference", "arbitrary-reference").status is PpqValidationStatus.INVALID


def test_recycled_is_zero_for_paper_and_contextual_not_required_only_for_nonpaper():
    assert validate_ppq_value("percent_recycled", "0").normalized_value == "0"
    assert validate_ppq_value("percent_recycled", "101").status is PpqValidationStatus.INVALID
    assert is_paper_or_paperboard("Paperboard carton") is True
    assert not not_required_allowed("percent_recycled", "NOT_PAPER_OR_PAPERBOARD", article_or_product="paper carton")
    assert not_required_allowed("percent_recycled", "NOT_PAPER_OR_PAPERBOARD", article_or_product="oak furniture")


def test_ppq_scope_is_exactly_shipment_1_to_10_and_plant_11_to_18():
    assert [field.number for field in PPQ505_SHIPMENT_FIELDS] == list(range(1, 11))
    assert {field.number for field in PPQ505_PLANT_FIELDS} == set(range(11, 19))
