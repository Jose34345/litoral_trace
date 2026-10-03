"""Pure domain contracts for reusable product/BOM intelligence.

These objects describe source-backed product composition. They are deliberately
non-persistent and carry no regulatory/canonical authority by themselves.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum


@dataclass(frozen=True, slots=True)
class SourceAnchor:
    """Transport-level pointer back to the parsed source location."""

    table_name: str
    document_id: str | None = None
    sheet: str | None = None
    row: int | None = None
    column: int | None = None
    locator: str | None = None


@dataclass(frozen=True, slots=True)
class MassValue:
    """Mass preserving the literal source value/unit plus deterministic kg value."""

    raw_value: str
    raw_unit: str
    kilograms: Decimal


@dataclass(frozen=True, slots=True)
class Material:
    name_raw: str
    name_normalized: str
    mass: MassValue | None
    source: SourceAnchor


@dataclass(frozen=True, slots=True)
class Component:
    component_key: str
    description_raw: str
    material: Material
    quantity: Decimal | None
    source: SourceAnchor


@dataclass(frozen=True, slots=True)
class SkuComposition:
    sku: str
    product_name: str | None
    components: tuple[Component, ...]


class BomIssueSeverity(StrEnum):
    WARNING = "WARNING"
    ERROR = "ERROR"


@dataclass(frozen=True, slots=True)
class BomIssue:
    code: str
    message: str
    severity: BomIssueSeverity
    source: SourceAnchor


@dataclass(frozen=True, slots=True)
class BomIngestionResult:
    compositions: tuple[SkuComposition, ...]
    issues: tuple[BomIssue, ...]
