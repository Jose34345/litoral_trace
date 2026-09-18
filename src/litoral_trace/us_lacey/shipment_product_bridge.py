"""Deterministic bridge between canonical shipment lines and Product Intelligence.

The bridge is read-only and non-authoritative. It never changes PPQ review state
and never binds by description, HTS, taxonomy, or fuzzy similarity. A binding
exists only when a canonical line reference explicitly names the SKU or equals
the deterministic line reference already used for specialized SKU materialization.
"""
from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
import hashlib
from typing import Any, Iterable, Mapping


BRIDGE_SCHEMA_VERSION = "shipment-product-bridge-v1"


@dataclass(frozen=True, slots=True)
class CanonicalLineFieldInput:
    field_key: str
    value: str
    status: str
    source_assurance_document_id: int | None = None
    source_page: int | None = None
    source_locator: str | None = None


@dataclass(frozen=True, slots=True)
class CanonicalPlantLineInput:
    line_reference: str
    ordinal: int
    fields: tuple[CanonicalLineFieldInput, ...] = ()


def materialized_line_reference(line_item_key: str) -> str:
    """Return the stable reference used when a proven line identity creates a line."""
    raw = str(line_item_key or "").strip()
    if not raw:
        raise ValueError("line_item_key is required")
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20].upper()
    return f"LT-{digest}"


def _sku_key(value: object) -> str:
    return str(value or "").strip().casefold()


def _line_key(value: object) -> str:
    return str(value or "").strip().casefold()


def _product_index(payload: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    """Collect explicit BOM compositions by SKU while preserving provenance."""
    result: dict[str, dict[str, Any]] = {}
    for source in payload.get("sources", ()):
        if not isinstance(source, Mapping):
            continue
        source_context = {
            "document_id": source.get("document_id"),
            "filename": source.get("filename"),
            "operation_document_id": source.get("operation_document_id"),
            "assurance_document_id": source.get("assurance_document_id"),
        }
        for table in source.get("tables", ()):
            if not isinstance(table, Mapping):
                continue
            table_context = {
                "name": table.get("name"),
                "source": deepcopy(table.get("source")),
            }
            for composition in table.get("compositions", ()):
                if not isinstance(composition, Mapping):
                    continue
                sku = str(composition.get("sku") or "").strip()
                key = _sku_key(sku)
                if not key:
                    continue
                product = result.setdefault(
                    key,
                    {"sku": sku, "product_names": [], "compositions": []},
                )
                product_name = str(composition.get("product_name") or "").strip()
                if product_name and product_name not in product["product_names"]:
                    product["product_names"].append(product_name)
                product["compositions"].append(
                    {
                        "source": deepcopy(source_context),
                        "table": deepcopy(table_context),
                        "product_name": composition.get("product_name"),
                        "components": deepcopy(list(composition.get("components") or ())),
                    }
                )
    return result


def _shipment_facts(line: CanonicalPlantLineInput) -> dict[str, dict[str, Any]]:
    facts: dict[str, dict[str, Any]] = {}
    for field in line.fields:
        key = str(field.field_key or "").strip()
        value = str(field.value or "").strip()
        if not key or not value:
            continue
        facts[key] = {
            "value": value,
            "status": str(field.status or ""),
            "source": {
                "assurance_document_id": field.source_assurance_document_id,
                "page": field.source_page,
                "locator": field.source_locator,
            },
        }
    return facts


def _binding_candidates(
    line_reference: str,
    products: Mapping[str, Mapping[str, Any]],
) -> tuple[tuple[str, str], ...]:
    line = _line_key(line_reference)
    matches: list[tuple[str, str]] = []
    for sku_key, product in products.items():
        sku = str(product.get("sku") or "").strip()
        if line and line == _line_key(sku):
            matches.append((sku_key, "DIRECT_SKU_LINE_REFERENCE"))
            continue
        expected = materialized_line_reference(f"SKU:{sku}")
        if line and line == _line_key(expected):
            matches.append((sku_key, "SPECIALIZED_SKU_MATERIALIZATION"))
    return tuple(matches)


def build_shipment_product_bridge(
    *,
    canonical_lines: Iterable[CanonicalPlantLineInput],
    product_intelligence_payload: Mapping[str, Any],
) -> dict[str, Any]:
    """Build a fail-closed common representation for shipment/product evidence."""
    lines = tuple(
        sorted(
            canonical_lines,
            key=lambda item: (int(item.ordinal), str(item.line_reference)),
        )
    )
    products = _product_index(product_intelligence_payload)

    candidates_by_index = {
        index: _binding_candidates(line.line_reference, products)
        for index, line in enumerate(lines)
    }
    sku_claimants: dict[str, set[int]] = {}
    for index, candidates in candidates_by_index.items():
        for sku_key, _method in candidates:
            sku_claimants.setdefault(sku_key, set()).add(index)

    bound_skus: set[str] = set()
    rendered_lines: list[dict[str, Any]] = []
    ambiguous_count = 0

    for index, line in enumerate(lines):
        candidates = candidates_by_index[index]
        binding: dict[str, Any]
        product_payload: dict[str, Any] | None = None

        if len(candidates) == 1:
            sku_key, method = candidates[0]
            if len(sku_claimants.get(sku_key, ())) == 1:
                product_payload = deepcopy(products[sku_key])
                bound_skus.add(sku_key)
                binding = {
                    "status": "BOUND",
                    "method": method,
                    "sku": product_payload["sku"],
                }
            else:
                ambiguous_count += 1
                binding = {
                    "status": "AMBIGUOUS",
                    "method": "MULTIPLE_CANONICAL_LINES_FOR_SKU",
                    "sku": products[sku_key]["sku"],
                }
        elif len(candidates) > 1:
            ambiguous_count += 1
            binding = {
                "status": "AMBIGUOUS",
                "method": "MULTIPLE_PRODUCT_IDENTITIES",
                "candidate_skus": [products[key]["sku"] for key, _ in candidates],
            }
        else:
            binding = {"status": "UNBOUND", "method": None, "sku": None}

        rendered_lines.append(
            {
                "line_reference": line.line_reference,
                "ordinal": int(line.ordinal),
                "binding": binding,
                "shipment_facts": _shipment_facts(line),
                "product": product_payload,
            }
        )

    unbound_products = [
        deepcopy(product)
        for key, product in products.items()
        if key not in bound_skus
    ]
    bound_count = sum(
        item["binding"]["status"] == "BOUND" for item in rendered_lines
    )
    unbound_line_count = sum(
        item["binding"]["status"] == "UNBOUND" for item in rendered_lines
    )

    if not products:
        status = "NOT_APPLICABLE"
    elif (
        bound_count == len(lines)
        and bound_count == len(products)
        and not unbound_products
        and ambiguous_count == 0
    ):
        status = "READY"
    else:
        status = "PARTIAL"

    return {
        "schema_version": BRIDGE_SCHEMA_VERSION,
        "status": status,
        "summary": {
            "canonical_line_count": len(lines),
            "product_sku_count": len(products),
            "bound_line_count": int(bound_count),
            "unbound_line_count": int(unbound_line_count),
            "unbound_product_count": len(unbound_products),
            "ambiguous_binding_count": int(ambiguous_count),
        },
        "lines": rendered_lines,
        "unbound_products": unbound_products,
    }


__all__ = [
    "BRIDGE_SCHEMA_VERSION",
    "CanonicalLineFieldInput",
    "CanonicalPlantLineInput",
    "build_shipment_product_bridge",
    "materialized_line_reference",
]
