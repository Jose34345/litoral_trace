"""Transform an explicit parsed BOM table into source-backed compositions."""
from __future__ import annotations

from collections import OrderedDict
from decimal import Decimal, InvalidOperation
import re

from litoral_trace.assurance.parsers import ParsedTable
from litoral_trace.product_intelligence.bom_schema import bind_bom_headers
from litoral_trace.product_intelligence.domain import (
    BomIngestionResult,
    BomIssue,
    BomIssueSeverity,
    Component,
    Material,
    SkuComposition,
    SourceAnchor,
)
from litoral_trace.product_intelligence.units import MassNormalizationError, normalize_mass


def _clean_text(value: object) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def _parse_decimal(value: object) -> Decimal:
    text = _clean_text(value)
    if not text:
        raise InvalidOperation
    result = Decimal(text)
    if not result.is_finite() or result < 0:
        raise InvalidOperation
    return result


def ingest_bom_table(
    table: ParsedTable,
    *,
    document_id: str | None = None,
) -> BomIngestionResult:
    """Ingest explicit BOM rows without inventing missing business facts."""
    binding = bind_bom_headers(table.headers)
    grouped: "OrderedDict[str, dict[str, object]]" = OrderedDict()
    issues: list[BomIssue] = []
    header_row = table.source.row or 0

    for ordinal, row in enumerate(table.rows, start=1):
        source = SourceAnchor(
            document_id=document_id,
            table_name=table.name,
            sheet=table.source.sheet,
            row=(header_row + ordinal) if header_row else ordinal,
            column=None,
            locator=table.source.locator,
        )

        sku = _clean_text(row.get(binding.sku))
        component_text = _clean_text(row.get(binding.component))
        material_text = _clean_text(row.get(binding.material))

        if not sku:
            issues.append(BomIssue("MISSING_SKU", "BOM row is missing SKU.", BomIssueSeverity.ERROR, source))
            continue
        if not component_text:
            issues.append(BomIssue("MISSING_COMPONENT", "BOM row is missing component.", BomIssueSeverity.ERROR, source))
            continue
        if not material_text:
            issues.append(BomIssue("MISSING_MATERIAL", "BOM row is missing material.", BomIssueSeverity.ERROR, source))
            continue

        quantity: Decimal | None = None
        if binding.quantity is not None:
            raw_quantity = row.get(binding.quantity)
            if _clean_text(raw_quantity):
                try:
                    quantity = _parse_decimal(raw_quantity)
                except (InvalidOperation, ValueError):
                    issues.append(BomIssue("INVALID_QUANTITY", "BOM row has an invalid quantity.", BomIssueSeverity.ERROR, source))
                    continue

        mass = None
        raw_mass = row.get(binding.mass_value) if binding.mass_value is not None else None
        raw_mass_unit = row.get(binding.mass_unit) if binding.mass_unit is not None else None
        if _clean_text(raw_mass) or _clean_text(raw_mass_unit):
            try:
                mass = normalize_mass(raw_mass, raw_mass_unit)
            except MassNormalizationError:
                issues.append(BomIssue("INVALID_MASS", "BOM row has invalid or incomplete mass information.", BomIssueSeverity.ERROR, source))
                continue

        product_name = _clean_text(row.get(binding.product_name)) if binding.product_name else ""
        component = Component(
            component_key=f"{sku}:row:{source.row}",
            description_raw=component_text,
            material=Material(
                name_raw=material_text,
                name_normalized=material_text.casefold(),
                mass=mass,
                source=source,
            ),
            quantity=quantity,
            source=source,
        )

        bucket = grouped.setdefault(
            sku,
            {"product_name": product_name or None, "components": []},
        )
        if bucket["product_name"] is None and product_name:
            bucket["product_name"] = product_name
        components = bucket["components"]
        assert isinstance(components, list)
        components.append(component)

    compositions = tuple(
        SkuComposition(
            sku=sku,
            product_name=bucket["product_name"] if isinstance(bucket["product_name"], str) else None,
            components=tuple(bucket["components"]),
        )
        for sku, bucket in grouped.items()
    )
    return BomIngestionResult(compositions=compositions, issues=tuple(issues))
