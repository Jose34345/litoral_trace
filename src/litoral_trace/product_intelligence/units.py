"""Deterministic unit normalization for Product Intelligence."""
from __future__ import annotations

from decimal import Decimal, InvalidOperation

from litoral_trace.product_intelligence.domain import MassValue


class MassNormalizationError(ValueError):
    """Raised when a mass value/unit cannot be normalized safely."""


_MASS_FACTORS_KG: dict[str, Decimal] = {
    "kg": Decimal("1"),
    "kilogram": Decimal("1"),
    "kilograms": Decimal("1"),
    "g": Decimal("0.001"),
    "gram": Decimal("0.001"),
    "grams": Decimal("0.001"),
    "lb": Decimal("0.45359237"),
    "lbs": Decimal("0.45359237"),
    "pound": Decimal("0.45359237"),
    "pounds": Decimal("0.45359237"),
    "oz": Decimal("0.028349523125"),
    "ounce": Decimal("0.028349523125"),
    "ounces": Decimal("0.028349523125"),
}


def normalize_mass(raw_value: object, raw_unit: object) -> MassValue:
    """Normalize an explicit mass to kilograms without inferring missing units."""
    value_text = str(raw_value).strip() if raw_value is not None else ""
    unit_text = str(raw_unit).strip() if raw_unit is not None else ""
    if not value_text:
        raise MassNormalizationError("Mass value is missing.")
    if not unit_text:
        raise MassNormalizationError("Mass unit is missing.")

    factor = _MASS_FACTORS_KG.get(unit_text.casefold())
    if factor is None:
        raise MassNormalizationError(f"Unsupported mass unit: {unit_text}")

    try:
        value = Decimal(value_text)
    except (InvalidOperation, ValueError) as exc:
        raise MassNormalizationError(f"Invalid mass value: {value_text}") from exc

    if not value.is_finite() or value < 0:
        raise MassNormalizationError(f"Invalid mass value: {value_text}")

    return MassValue(
        raw_value=value_text,
        raw_unit=unit_text,
        kilograms=value * factor,
    )
