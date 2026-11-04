"""Deterministic header binding for explicit BOM tables."""
from __future__ import annotations

from dataclasses import dataclass
import re


class BomSchemaError(ValueError):
    """Raised when an explicit BOM table cannot be bound safely."""


@dataclass(frozen=True, slots=True)
class BomColumnBinding:
    sku: str
    component: str
    material: str
    product_name: str | None = None
    quantity: str | None = None
    mass_value: str | None = None
    mass_unit: str | None = None


def _normalize_header(value: str) -> str:
    return re.sub(r"\s+", " ", str(value).strip()).casefold()


_ALIASES: dict[str, frozenset[str]] = {
    "sku": frozenset({"sku", "item number", "item no", "item", "product sku"}),
    "product_name": frozenset({"product", "product name", "item description", "product description"}),
    "component": frozenset({"component", "part", "part name", "component description"}),
    "material": frozenset({"material", "material description", "material name"}),
    "quantity": frozenset({"quantity", "qty", "component quantity"}),
    "mass_value": frozenset({"weight", "mass", "weight value", "mass value"}),
    "mass_unit": frozenset({"uom", "unit", "weight unit", "mass unit", "unit of measure"}),
}

_REQUIRED = ("sku", "component", "material")


def bind_bom_headers(headers: tuple[str, ...]) -> BomColumnBinding:
    matches: dict[str, list[str]] = {key: [] for key in _ALIASES}
    for header in headers:
        normalized = _normalize_header(header)
        for canonical, aliases in _ALIASES.items():
            if normalized in aliases:
                matches[canonical].append(header)

    for canonical, physical in matches.items():
        if len(physical) > 1:
            raise BomSchemaError(
                f"Ambiguous BOM header mapping for {canonical}: {', '.join(physical)}"
            )
    for canonical in _REQUIRED:
        if not matches[canonical]:
            raise BomSchemaError(f"Missing required BOM field: {canonical}")

    return BomColumnBinding(
        sku=matches["sku"][0],
        component=matches["component"][0],
        material=matches["material"][0],
        product_name=matches["product_name"][0] if matches["product_name"] else None,
        quantity=matches["quantity"][0] if matches["quantity"] else None,
        mass_value=matches["mass_value"][0] if matches["mass_value"] else None,
        mass_unit=matches["mass_unit"][0] if matches["mass_unit"] else None,
    )
