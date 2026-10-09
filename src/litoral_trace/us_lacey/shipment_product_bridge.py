"""Non-canonical bridge between shipment lines and Product Intelligence compositions."""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from copy import deepcopy
from typing import Any


BRIDGE_SCHEMA_VERSION = "shipment-product-bridge-v2"


def _normalized_reference(value: object) -> str:
    return str(value or "").strip().casefold()


def _normalized_sku(value: object) -> str:
    return str(value or "").strip().upper()


def _explicit_links_by_sku(
    explicit_links: Iterable[Mapping[str, object]],
    *,
    existing_references: tuple[str, ...],
) -> dict[str, list[dict[str, object]]]:
    existing = {
        _normalized_reference(reference): reference
        for reference in existing_references
    }
    grouped: dict[str, list[dict[str, object]]] = {}
    for raw in explicit_links:
        sku = _normalized_sku(raw.get("sku"))
        line_reference = str(raw.get("line_reference") or "").strip()
        if not sku or not line_reference:
            continue
        canonical_line = existing.get(
            _normalized_reference(line_reference)
        )
        if canonical_line is None:
            continue
        item = dict(raw)
        item["line_reference"] = canonical_line
        grouped.setdefault(sku, []).append(item)
    return grouped


def build_shipment_product_bridge(
    product_payload: dict[str, Any],
    *,
    line_references: Iterable[str],
    explicit_links: Iterable[Mapping[str, object]] | None = None,
) -> dict[str, Any]:
    """Build a deterministic review-safe Shipment Line to Product graph.

    A shipment line and a commercial SKU are separate identities. LINKED status
    is possible only through one explicit exact/human-confirmed relationship.
    Literal line-reference/SKU equality, description, taxonomy, and ordinal
    proximity are never accepted as binding evidence.
    """
    existing = tuple(
        str(value).strip()
        for value in line_references
        if str(value).strip()
    )
    explicit_by_sku = _explicit_links_by_sku(
        explicit_links or (),
        existing_references=existing,
    )

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
                persisted = list(
                    explicit_by_sku.get(_normalized_sku(sku), ())
                )
                candidates = list(
                    dict.fromkeys(
                        str(item["line_reference"])
                        for item in persisted
                    )
                )

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

                selected_metadata: dict[str, object] = {}
                if status == "LINKED" and len(persisted) == 1:
                    selected_metadata = {
                        "link_method": persisted[0].get("link_method"),
                        "supplier_public_id": persisted[0].get(
                            "supplier_public_id"
                        ),
                        "supplier_product_public_id": persisted[0].get(
                            "supplier_product_public_id"
                        ),
                    }

                links.append(
                    {
                        "status": status,
                        "line_item_key": f"SKU:{sku}" if sku else None,
                        "shipment_line_reference": shipment_line_reference,
                        "candidate_line_references": candidates,
                        **selected_metadata,
                        "source": {
                            "document_id": source.get("document_id"),
                            "filename": source.get("filename"),
                            "assurance_document_id": source.get(
                                "assurance_document_id"
                            ),
                            "table_name": table.get("name"),
                            "table_source": deepcopy(
                                table.get("source") or {}
                            ),
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
