from decimal import Decimal

import pytest

from litoral_trace.product_intelligence.units import MassNormalizationError, normalize_mass


@pytest.mark.parametrize(
    ("value", "unit", "expected"),
    [
        ("1", "kg", Decimal("1")),
        ("500", "g", Decimal("0.5")),
        ("2", "lb", Decimal("0.90718474")),
        ("4", "oz", Decimal("0.113398092500")),
    ],
)
def test_normalize_mass_preserves_raw_and_converts_to_kg(value, unit, expected):
    result = normalize_mass(value, unit)
    assert result.raw_value == value
    assert result.raw_unit == unit
    assert result.kilograms == expected


def test_normalize_mass_rejects_unknown_unit():
    with pytest.raises(MassNormalizationError):
        normalize_mass("1", "ton")


def test_normalize_mass_rejects_missing_or_negative_values():
    with pytest.raises(MassNormalizationError):
        normalize_mass("", "kg")
    with pytest.raises(MassNormalizationError):
        normalize_mass("-1", "kg")
