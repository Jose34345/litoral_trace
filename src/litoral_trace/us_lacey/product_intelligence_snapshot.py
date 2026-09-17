"""Source-set-scoped Product Intelligence integration for U.S. Lacey.

The module deliberately produces non-canonical product-composition evidence.
Nothing here may promote BOM observations into PPQ 505, LAWGS, canonical
shipment truth, or a regulatory decision without a separate reviewed boundary.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import PurePath
from typing import Any, Callable, Iterable
from uuid import UUID

from sqlalchemy import and_, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from litoral_trace.assurance.parsers import DocumentParseError, SourceLocation, parse_document
from litoral_trace.db.models import (
    AssuranceDocument,
    UsLaceyOperation,
    UsLaceyProductIntelligenceSnapshot,
    UsLaceySourceSetMember,
    UsLaceySourceSetRevision,
    VaultDocument,
)
from litoral_trace.db.tenant import set_tenant_db_context
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
from litoral_trace.services.vault import VaultError, VaultService
from litoral_trace.us_lacey.db import get_us_lacey_db_session
from litoral_trace.us_lacey.storage import build_us_lacey_storage_settings, get_us_lacey_storage_client

SNAPSHOT_SCHEMA_VERSION = "product-intelligence-snapshot-v1"
_ELIGIBLE_EXTENSIONS = frozenset({".csv", ".xls", ".xlsx"})
SessionFactory = Callable[[], Session]


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


@dataclass(frozen=True, slots=True)
class ProductIntelligenceView:
    status: str
    generation: int
    source_set_fingerprint: str
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
    # Deliberately do not persist arbitrary parser exception text. Filenames and
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


def _default_vault_service(*, session_factory: SessionFactory) -> VaultService:
    return VaultService(
        storage_settings=build_us_lacey_storage_settings(),
        storage=get_us_lacey_storage_client(),
        session_factory=session_factory,
    )


def _load_revision_and_members(
    session: Session,
    *,
    organization_id: int,
    operation_id: int,
    revision_id: int,
) -> tuple[UsLaceySourceSetRevision | None, list[tuple[Any, ...]]]:
    revision = session.scalar(
        select(UsLaceySourceSetRevision).where(
            UsLaceySourceSetRevision.organization_id == organization_id,
            UsLaceySourceSetRevision.operation_id == operation_id,
            UsLaceySourceSetRevision.id == revision_id,
        )
    )
    if revision is None:
        return None, []
    rows = session.execute(
        select(
            UsLaceySourceSetMember.operation_document_id,
            AssuranceDocument.id,
            AssuranceDocument.public_id,
            VaultDocument.public_id,
            VaultDocument.original_filename,
            VaultDocument.sha256,
        )
        .join(
            AssuranceDocument,
            and_(
                AssuranceDocument.id == UsLaceySourceSetMember.assurance_document_id,
                AssuranceDocument.organization_id == UsLaceySourceSetMember.organization_id,
            ),
        )
        .join(
            VaultDocument,
            and_(
                VaultDocument.id == AssuranceDocument.vault_document_id,
                VaultDocument.organization_id == AssuranceDocument.organization_id,
            ),
        )
        .where(
            UsLaceySourceSetMember.organization_id == organization_id,
            UsLaceySourceSetMember.source_set_revision_id == revision_id,
        )
        .order_by(UsLaceySourceSetMember.id)
    ).all()
    return revision, list(rows)


def build_product_intelligence_snapshot(
    *,
    organization_id: int,
    operation_id: int,
    claim: Any,
    session_factory: SessionFactory | None = None,
    vault_service: VaultService | None = None,
) -> UsLaceyProductIntelligenceSnapshot | None:
    """Build one idempotent non-canonical snapshot for an exact claimed source set."""
    if not getattr(claim, "claimed", False) or getattr(claim, "revision_id", None) is None:
        return None
    factory = session_factory or get_us_lacey_db_session
    organization_id = int(organization_id)
    operation_id = int(operation_id)
    revision_id = int(claim.revision_id)

    session = factory()
    try:
        set_tenant_db_context(session, organization_id)
        revision, rows = _load_revision_and_members(
            session,
            organization_id=organization_id,
            operation_id=operation_id,
            revision_id=revision_id,
        )
        if revision is None or not snapshot_matches_claim(revision, claim):
            return None
        existing = session.scalar(
            select(UsLaceyProductIntelligenceSnapshot).where(
                UsLaceyProductIntelligenceSnapshot.organization_id == organization_id,
                UsLaceyProductIntelligenceSnapshot.source_set_revision_id == revision_id,
            )
        )
        if existing is not None:
            return existing
        # Copy identifiers while the tenant-scoped session is live; object bytes are
        # materialized outside this database transaction.
        descriptors = [tuple(row) for row in rows]
    finally:
        session.close()

    vault = vault_service or _default_vault_service(session_factory=factory)
    documents: list[ProductIntelligenceDocumentInput] = []
    for (
        operation_document_id,
        assurance_document_id,
        assurance_public_id,
        vault_public_id,
        filename,
        source_sha256,
    ) in descriptors:
        content = b""
        if PurePath(str(filename)).suffix.casefold() in _ELIGIBLE_EXTENSIONS:
            try:
                with vault.materialize_verified_download(
                    organization_id=organization_id,
                    document_id=vault_public_id,
                ) as download:
                    content = b"".join(download.iter_chunks())
            except VaultError:
                # Empty eligible content becomes a sanitized parser failure in the
                # analysis payload. Other source-set documents remain usable.
                content = b""
        documents.append(
            ProductIntelligenceDocumentInput(
                operation_document_id=int(operation_document_id),
                assurance_document_id=int(assurance_document_id),
                document_id=str(assurance_public_id),
                filename=str(filename),
                source_sha256=str(source_sha256),
                content=content,
            )
        )

    analysis = analyze_product_intelligence_documents(documents)
    payload = dict(analysis.payload)
    payload["operation_id"] = operation_id
    payload["source_set"] = {
        "revision_id": revision_id,
        "generation": int(claim.generation),
        "fingerprint": str(claim.fingerprint),
    }

    session = factory()
    try:
        set_tenant_db_context(session, organization_id)
        revision = session.scalar(
            select(UsLaceySourceSetRevision).where(
                UsLaceySourceSetRevision.organization_id == organization_id,
                UsLaceySourceSetRevision.operation_id == operation_id,
                UsLaceySourceSetRevision.id == revision_id,
            )
        )
        if revision is None or not snapshot_matches_claim(revision, claim):
            session.rollback()
            return None
        existing = session.scalar(
            select(UsLaceyProductIntelligenceSnapshot).where(
                UsLaceyProductIntelligenceSnapshot.organization_id == organization_id,
                UsLaceyProductIntelligenceSnapshot.source_set_revision_id == revision_id,
            )
        )
        if existing is not None:
            return existing
        snapshot = UsLaceyProductIntelligenceSnapshot(
            organization_id=organization_id,
            operation_id=operation_id,
            source_set_revision_id=revision_id,
            generation=int(claim.generation),
            source_set_fingerprint=str(claim.fingerprint),
            status=analysis.status,
            document_count=analysis.document_count,
            eligible_document_count=analysis.eligible_document_count,
            recognized_bom_table_count=analysis.recognized_bom_table_count,
            unique_sku_count=analysis.unique_sku_count,
            component_count=analysis.component_count,
            material_count=analysis.material_count,
            issue_count=analysis.issue_count,
            payload_json=payload,
            finalized_at=datetime.now(timezone.utc),
        )
        session.add(snapshot)
        try:
            session.commit()
        except IntegrityError:
            session.rollback()
            existing = session.scalar(
                select(UsLaceyProductIntelligenceSnapshot).where(
                    UsLaceyProductIntelligenceSnapshot.organization_id == organization_id,
                    UsLaceyProductIntelligenceSnapshot.source_set_revision_id == revision_id,
                )
            )
            if existing is None:
                raise
            return existing
        session.refresh(snapshot)
        return snapshot
    finally:
        session.close()


def get_current_product_intelligence_view(
    *,
    organization_id: int,
    operation_public_id: UUID | str,
    session_factory: SessionFactory | None = None,
) -> ProductIntelligenceView | None:
    """Return only the snapshot attached to the operation's current source revision."""
    factory = session_factory or get_us_lacey_db_session
    organization_id = int(organization_id)
    try:
        public_id = operation_public_id if isinstance(operation_public_id, UUID) else UUID(str(operation_public_id))
    except (TypeError, ValueError, AttributeError):
        return None

    session = factory()
    try:
        set_tenant_db_context(session, organization_id)
        row = session.execute(
            select(UsLaceyProductIntelligenceSnapshot)
            .join(
                UsLaceyOperation,
                and_(
                    UsLaceyOperation.id == UsLaceyProductIntelligenceSnapshot.operation_id,
                    UsLaceyOperation.organization_id == UsLaceyProductIntelligenceSnapshot.organization_id,
                ),
            )
            .join(
                UsLaceySourceSetRevision,
                and_(
                    UsLaceySourceSetRevision.id == UsLaceyProductIntelligenceSnapshot.source_set_revision_id,
                    UsLaceySourceSetRevision.organization_id == UsLaceyProductIntelligenceSnapshot.organization_id,
                ),
            )
            .where(
                UsLaceyProductIntelligenceSnapshot.organization_id == organization_id,
                UsLaceyOperation.public_id == public_id,
                UsLaceySourceSetRevision.is_current.is_(True),
                UsLaceyProductIntelligenceSnapshot.status != "STALE",
            )
            .order_by(UsLaceyProductIntelligenceSnapshot.id.desc())
            .limit(1)
        ).scalar_one_or_none()
        if row is None:
            return None
        return ProductIntelligenceView(
            status=row.status,
            generation=row.generation,
            source_set_fingerprint=row.source_set_fingerprint,
            document_count=row.document_count,
            eligible_document_count=row.eligible_document_count,
            recognized_bom_table_count=row.recognized_bom_table_count,
            unique_sku_count=row.unique_sku_count,
            component_count=row.component_count,
            material_count=row.material_count,
            issue_count=row.issue_count,
            payload=dict(row.payload_json or {}),
        )
    finally:
        session.close()
