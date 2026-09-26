"""Non-canonical bridge between shipment lines and Product Intelligence compositions."""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Iterable

from litoral_trace.us_lacey.specialized_projection import (
    derived_line_reference_for_identity,
)


BRIDGE_SCHEMA_VERSION = "shipment-product-bridge-v1"


def _normalized_reference(value: object) -> str:
    return str(value or "").strip().casefold()


def _candidate_references_for_sku(sku: str) -> tuple[str, ...]:
    raw = str(sku or "").strip()
    if not raw:
        return ()
    line_item_key = f"SKU:{raw}"
    return (
        raw,
        line_item_key,
        derived_line_reference_for_identity(line_item_key),
    )


def build_shipment_product_bridge(
    product_payload: dict[str, Any],
    *,
    line_references: Iterable[str],
) -> dict[str, Any]:
    """Build a deterministic review-safe Shipment Line ↔ Product graph.

    The bridge never creates shipment lines and never guesses by description,
    material or taxonomy. A product is LINKED only when exactly one existing
    line reference matches the explicit SKU identity.
    """
    existing = tuple(str(value).strip() for value in line_references if str(value).strip())
    by_normalized: dict[str, list[str]] = {}
    for reference in existing:
        by_normalized.setdefault(_normalized_reference(reference), []).append(reference)

    links: list[dict[str, Any]] = []
    linked_count = 0
    review_count = 0

    for source in product_payload.get("sources", ()):
        if not isinstance(source, dict):
            continue
        for table in source.get("tables", ()):
            if not isinstance(table, dict):
                continue
            for composition in table.get("compositions", ()):
                if not isinstance(composition, dict):
                    continue
                sku = str(composition.get("sku") or "").strip()
                candidates: list[str] = []
                seen: set[str] = set()
                for candidate in _candidate_references_for_sku(sku):
                    for match in by_normalized.get(_normalized_reference(candidate), ()):
                        if match not in seen:
                            seen.add(match)
                            candidates.append(match)

                if len(candidates) == 1:
                    status = "LINKED"
                    shipment_line_reference = candidates[0]
                    linked_count += 1
                elif len(candidates) > 1:
                    status = "AMBIGUOUS_REVIEW"
                    shipment_line_reference = None
                    review_count += 1
                else:
                    status = "UNLINKED_REVIEW"
                    shipment_line_reference = None
                    review_count += 1

                links.append(
                    {
                        "status": status,
                        "line_item_key": f"SKU:{sku}" if sku else None,
                        "shipment_line_reference": shipment_line_reference,
                        "candidate_line_references": candidates,
                        "source": {
                            "document_id": source.get("document_id"),
                            "filename": source.get("filename"),
                            "assurance_document_id": source.get("assurance_document_id"),
                            "table_name": table.get("name"),
                            "table_source": deepcopy(table.get("source") or {}),
                        },
                        "product": deepcopy(composition),
                    }
                )

    return {
        "schema_version": BRIDGE_SCHEMA_VERSION,
        "shipment_line_count": len(existing),
        "composition_count": len(links),
        "linked_count": linked_count,
        "review_count": review_count,
        "links": links,
    }
