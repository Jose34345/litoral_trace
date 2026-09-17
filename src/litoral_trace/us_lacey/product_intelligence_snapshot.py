"""Source-set-scoped Product Intelligence integration for U.S. Lacey.

The module deliberately produces non-canonical product-composition evidence.
Nothing here may promote BOM observations into PPQ 505, LAWGS, canonical
shipment truth, or a regulatory decision without a separate reviewed boundary.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from pathlib import PurePath
from typing import Any, Iterable

from sqlalchemy import update
from sqlalchemy.orm import Session

from litoral_trace.assurance.parsers import DocumentParseError, SourceLocation, parse_document
from litoral_trace.db.models import UsLaceyProductIntelligenceSnapshot
from litoral_trace.product_intelligence.bom_ingestion import ingest_bom_table
from litoral_trace.product_intelligence.bom_schema import BomSchemaError
from litoral_trace.product_intelligence.domain import (
    BomIssue,
    BomIssueSeverity,
    Component,
    MassValue,
    Material,
    SkuComposition,
    SourceAnchor,
)

SNAPSHOT_SCHEMA_VERSION = "product-intelligence-snapshot-v1"
_ELIGIBLE_EXTENSIONS = frozenset({".csv", ".xls", ".xlsx"})


@dataclass(frozen=True, slots=True)
class ProductIntelligenceDocumentInput:
    operation_document_id: int
    assurance_document_id: int
    document_id: str
    filename: str
    source_sha256: str
    content: bytes


@dataclass(frozen=True, slots=True)
class ProductIntelligenceAnalysis:
    status: str
    document_count: int
    eligible_document_count: int
    recognized_bom_table_count: int
    unique_sku_count: int
    component_count: int
    material_count: int
    issue_count: int
    payload: dict[str, Any]


def _decimal(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def _source_anchor(source: SourceAnchor) -> dict[str, Any]:
    return {
        "document_id": source.document_id,
        "table_name": source.table_name,
        "sheet": source.sheet,
        "row": source.row,
        "column": source.column,
        "locator": source.locator,
    }


def _table_source(source: SourceLocation) -> dict[str, Any]:
    return {
        "page": source.page,
        "sheet": source.sheet,
        "row": source.row,
        "column": source.column,
        "locator": source.locator,
    }


def _mass(value: MassValue | None) -> dict[str, Any] | None:
    if value is None:
        return None
    return {
        "raw_value": value.raw_value,
        "raw_unit": value.raw_unit,
        "kilograms": str(value.kilograms),
    }


def _material(value: Material) -> dict[str, Any]:
    return {
        "name_raw": value.name_raw,
        "name_normalized": value.name_normalized,
        "mass": _mass(value.mass),
        "source": _source_anchor(value.source),
    }


def _component(value: Component) -> dict[str, Any]:
    return {
        "component_key": value.component_key,
        "description_raw": value.description_raw,
        "quantity": _decimal(value.quantity),
        "material": _material(value.material),
        "source": _source_anchor(value.source),
    }


def _composition(value: SkuComposition) -> dict[str, Any]:
    return {
        "sku": value.sku,
        "product_name": value.product_name,
        "components": [_component(component) for component in value.components],
    }


def _issue(value: BomIssue) -> dict[str, Any]:
    return {
        "code": value.code,
        "message": value.message,
        "severity": value.severity.value,
        "source": _source_anchor(value.source),
    }


def _document_parse_issue(document: ProductIntelligenceDocumentInput) -> dict[str, Any]:
    # Deliberately do not persist arbitrary parser exception text.  Filenames and
    # stable identifiers are sufficient for customer-safe diagnostics.
    return {
        "code": "DOCUMENT_PARSE_FAILED",
        "message": "The tabular document could not be parsed safely.",
        "severity": BomIssueSeverity.ERROR.value,
        "source": {
            "document_id": document.document_id,
            "filename": document.filename,
            "operation_document_id": document.operation_document_id,
            "assurance_document_id": document.assurance_document_id,
        },
    }


def analyze_product_intelligence_documents(
    documents: Iterable[ProductIntelligenceDocumentInput],
) -> ProductIntelligenceAnalysis:
    """Analyze explicit tabular BOMs deterministically and fail closed on ambiguity."""
    items = tuple(documents)
    source_payloads: list[dict[str, Any]] = []
    global_issues: list[dict[str, Any]] = []
    eligible_count = 0
    recognized_tables = 0
    all_skus: set[str] = set()
    all_materials: set[str] = set()
    component_count = 0
    bom_issue_count = 0

    for document in items:
        extension = PurePath(document.filename).suffix.casefold()
        if extension not in _ELIGIBLE_EXTENSIONS:
            continue
        eligible_count += 1
        source_payload: dict[str, Any] = {
            "operation_document_id": int(document.operation_document_id),
            "assurance_document_id": int(document.assurance_document_id),
            "document_id": document.document_id,
            "filename": document.filename,
            "source_sha256": document.source_sha256,
            "tables": [],
        }
        try:
            parsed = parse_document(document.filename, document.content)
        except (DocumentParseError, ValueError, TypeError):
            global_issues.append(_document_parse_issue(document))
            source_payloads.append(source_payload)
            continue

        for table in parsed.tables:
            try:
                ingested = ingest_bom_table(table, document_id=document.document_id)
            except BomSchemaError:
                # A normal spreadsheet that is not an explicit BOM is not an error.
                continue
            recognized_tables += 1
            serialized_issues = [_issue(issue) for issue in ingested.issues]
            serialized_compositions = [_composition(value) for value in ingested.compositions]
            source_payload["tables"].append(
                {
                    "name": table.name,
                    "source": _table_source(table.source),
                    "compositions": serialized_compositions,
                    "issues": serialized_issues,
                }
            )
            bom_issue_count += len(ingested.issues)
            for composition in ingested.compositions:
                if composition.components:
                    all_skus.add(composition.sku)
                for component in composition.components:
                    component_count += 1
                    all_materials.add(component.material.name_normalized)

        source_payloads.append(source_payload)

    total_issue_count = bom_issue_count + len(global_issues)
    has_error = bool(global_issues)
    if not has_error:
        has_error = any(
            issue["severity"] == BomIssueSeverity.ERROR.value
            for source in source_payloads
            for table in source["tables"]
            for issue in table["issues"]
        )

    if component_count > 0:
        status = "PARTIAL" if has_error else "READY"
    elif recognized_tables > 0 or global_issues:
        status = "FAILED"
    else:
        status = "NOT_APPLICABLE"

    summary = {
        "document_count": len(items),
        "eligible_document_count": eligible_count,
        "recognized_bom_table_count": recognized_tables,
        "unique_sku_count": len(all_skus),
        "component_count": component_count,
        "material_count": len(all_materials),
        "issue_count": total_issue_count,
    }
    payload = {
        "schema_version": SNAPSHOT_SCHEMA_VERSION,
        "summary": summary,
        "sources": source_payloads,
        "issues": global_issues,
    }
    return ProductIntelligenceAnalysis(
        status=status,
        document_count=summary["document_count"],
        eligible_document_count=eligible_count,
        recognized_bom_table_count=recognized_tables,
        unique_sku_count=len(all_skus),
        component_count=component_count,
        material_count=len(all_materials),
        issue_count=total_issue_count,
        payload=payload,
    )


def snapshot_matches_claim(revision: Any, claim: Any) -> bool:
    """Fence Product Intelligence publication to the exact FINALIZING source set."""
    return bool(
        getattr(claim, "claimed", False)
        and getattr(revision, "is_current", False)
        and str(getattr(revision, "status", "")) == "FINALIZING"
        and int(getattr(revision, "id", -1)) == int(getattr(claim, "revision_id", -2))
        and int(getattr(revision, "generation", -1)) == int(getattr(claim, "generation", -2))
        and str(getattr(revision, "source_set_fingerprint", ""))
        == str(getattr(claim, "fingerprint", ""))
        and getattr(revision, "claimed_at", None) == getattr(claim, "claimed_at", None)
    )


def mark_product_intelligence_snapshots_stale(
    session: Session,
    *,
    organization_id: int,
    operation_id: int,
) -> int:
    """Mark superseded snapshot metadata stale without rewriting immutable payloads."""
    result = session.execute(
        update(UsLaceyProductIntelligenceSnapshot)
        .where(
            UsLaceyProductIntelligenceSnapshot.organization_id == int(organization_id),
            UsLaceyProductIntelligenceSnapshot.operation_id == int(operation_id),
            UsLaceyProductIntelligenceSnapshot.status != "STALE",
        )
        .values(status="STALE")
    )
    return int(result.rowcount or 0)
