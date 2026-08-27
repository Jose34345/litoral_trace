"""Persistence bridge for Assurance deterministic reconciliation.

This service turns already accepted extraction output into operation snapshots,
runs deterministic reconciliation and upserts tenant-scoped issues. Only fields
that passed automatic acceptance are eligible for automatic reconciliation; data
still marked for human review cannot silently create blocking conclusions.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Callable, Iterable
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from litoral_trace.assurance.domain import ReconciliationIssueStatus
from litoral_trace.assurance.reconciliation import (
    DocumentSnapshot,
    OperationSnapshot,
    ReconciliationFinding,
    reconcile_operation,
)
from litoral_trace.db.engine import get_db_session
from litoral_trace.db.models import (
    AssuranceDocument,
    DocumentEntityLink,
    DocumentExtractionRun,
    ExtractedDocumentField,
    Lote,
    ReconciliationIssue,
    Shipment,
    ShipmentItem,
    TraceabilityBatch,
    VaultDocument,
)
from litoral_trace.db.tenant import set_tenant_db_context


SessionFactory = Callable[[], Session | None]
_OPERATION_LINK_TYPES = frozenset({"OPERATION", "SHIPMENT", "ORDER"})


class AssuranceReconciliationError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class ReconciliationPersistenceResult:
    operation_count: int
    finding_count: int
    created_count: int
    refreshed_count: int
    reopened_count: int
    auto_resolved_count: int


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _canonical_operation_target(
    entity_type: str,
    entity_reference: str,
) -> tuple[str, tuple[tuple[str, str], ...]] | None:
    """Canonicalize order/shipment links that point to the same Shipment UUID."""
    kind = str(entity_type or "").strip().upper()
    reference = str(entity_reference or "").strip()
    if kind not in _OPERATION_LINK_TYPES or not reference:
        return None

    if kind in {"SHIPMENT", "ORDER"} and ":" in reference:
        _, suffix = reference.split(":", 1)
        suffix = suffix.strip()
        if suffix:
            canonical = f"shipment:{suffix}"
            return canonical, (
                ("SHIPMENT", f"shipment:{suffix}"),
                ("ORDER", f"order:{suffix}"),
            )

    return reference, ((kind, reference),)


def _latest_run_ids(
    session: Session,
    *,
    organization_id: int,
    assurance_document_ids: Iterable[int],
) -> dict[int, int]:
    document_ids = tuple(sorted({int(value) for value in assurance_document_ids}))
    if not document_ids:
        return {}
    runs = session.scalars(
        select(DocumentExtractionRun)
        .where(
            DocumentExtractionRun.organization_id == organization_id,
            DocumentExtractionRun.assurance_document_id.in_(document_ids),
        )
        .order_by(
            DocumentExtractionRun.assurance_document_id.asc(),
            DocumentExtractionRun.id.desc(),
        )
    ).all()
    latest: dict[int, int] = {}
    for run in runs:
        latest.setdefault(run.assurance_document_id, run.id)
    return latest


def _accepted_fields_by_document(
    session: Session,
    *,
    organization_id: int,
    assurance_document_ids: Iterable[int],
) -> tuple[dict[int, dict[str, object]], dict[int, dict[str, str]]]:
    latest_runs = _latest_run_ids(
        session,
        organization_id=organization_id,
        assurance_document_ids=assurance_document_ids,
    )
    if not latest_runs:
        return {}, {}

    rows = session.scalars(
        select(ExtractedDocumentField)
        .where(
            ExtractedDocumentField.organization_id == organization_id,
            ExtractedDocumentField.extraction_run_id.in_(tuple(latest_runs.values())),
            ExtractedDocumentField.auto_accepted.is_(True),
        )
        .order_by(ExtractedDocumentField.id.asc())
    ).all()

    values: dict[int, dict[str, object]] = {}
    locators: dict[int, dict[str, str]] = {}
    for row in rows:
        expected_run = latest_runs.get(row.assurance_document_id)
        if expected_run != row.extraction_run_id:
            continue
        if row.field_name.startswith("raw."):
            continue
        value = row.normalized_value if row.normalized_value is not None else row.original_value
        if value is None:
            continue
        values.setdefault(row.assurance_document_id, {})[row.field_name] = value
        if row.source_locator:
            locators.setdefault(row.assurance_document_id, {})[row.field_name] = row.source_locator
    return values, locators


def _shipment_context(
    session: Session,
    *,
    organization_id: int,
    operation_reference: str,
) -> tuple[dict[str, object], tuple[str, ...], datetime | None]:
    if not operation_reference.startswith("shipment:"):
        return {}, (), None
    suffix = operation_reference.split(":", 1)[1]
    try:
        public_id = UUID(suffix)
    except (TypeError, ValueError):
        return {}, (), None

    shipment = session.scalar(
        select(Shipment).where(
            Shipment.organization_id == organization_id,
            Shipment.public_id == public_id,
        )
    )
    if shipment is None:
        return {}, (), None

    system_values: dict[str, object] = {}
    if shipment.destination_country:
        system_values["destination_country"] = shipment.destination_country
    if shipment.shipped_at is not None:
        system_values["shipment_date"] = shipment.shipped_at

    items = session.scalars(
        select(ShipmentItem).where(
            ShipmentItem.organization_id == organization_id,
            ShipmentItem.shipment_id == shipment.id,
        )
    ).all()
    units = {str(item.unit or "").strip().upper() for item in items if str(item.unit or "").strip()}
    if items and len(units) == 1:
        system_values["quantity"] = sum((Decimal(item.quantity) for item in items), Decimal("0"))
        system_values["unit"] = next(iter(units))

    batch_ids = tuple(sorted({item.batch_id for item in items}))
    batches = []
    if batch_ids:
        batches = session.scalars(
            select(TraceabilityBatch).where(
                TraceabilityBatch.organization_id == organization_id,
                TraceabilityBatch.id.in_(batch_ids),
            )
        ).all()

    lot_identifiers: set[str] = {
        str(batch.code).strip()
        for batch in batches
        if str(batch.code or "").strip()
    }
    lote_ids = tuple(sorted({batch.source_lote_id for batch in batches if batch.source_lote_id is not None}))
    if lote_ids:
        lotes = session.scalars(
            select(Lote).where(
                Lote.organization_id == organization_id,
                Lote.id.in_(lote_ids),
            )
        ).all()
        lot_identifiers.update(
            str(lote.identificador).strip()
            for lote in lotes
            if str(lote.identificador or "").strip()
        )

    return system_values, tuple(sorted(lot_identifiers)), shipment.shipped_at


def _persist_findings(
    session: Session,
    *,
    organization_id: int,
    operation: OperationSnapshot,
    findings: tuple[ReconciliationFinding, ...],
) -> tuple[int, int, int, int]:
    existing = session.scalars(
        select(ReconciliationIssue).where(
            ReconciliationIssue.organization_id == organization_id,
            ReconciliationIssue.operation_reference == operation.operation_reference,
        )
    ).all()
    by_fingerprint = {issue.fingerprint: issue for issue in existing}

    created = 0
    refreshed = 0
    reopened = 0
    active_fingerprints: set[str] = set()
    for finding in findings:
        active_fingerprints.add(finding.fingerprint)
        issue = by_fingerprint.get(finding.fingerprint)
        if issue is None:
            issue = ReconciliationIssue(
                organization_id=organization_id,
                operation_reference=operation.operation_reference,
                fingerprint=finding.fingerprint,
                rule_code=finding.rule_code,
                severity=finding.severity.value,
                status=ReconciliationIssueStatus.OPEN.value,
                field_name=finding.field_name,
                left_document_id=finding.left_document_id,
                right_document_id=finding.right_document_id,
                left_source=finding.left_source,
                right_source=finding.right_source,
                left_value=finding.left_value,
                right_value=finding.right_value,
                delta_numeric=finding.delta_numeric,
                explanation=finding.explanation,
                evidence_json={"sources": list(finding.evidence)},
            )
            session.add(issue)
            by_fingerprint[finding.fingerprint] = issue
            created += 1
            continue

        issue.rule_code = finding.rule_code
        issue.severity = finding.severity.value
        issue.field_name = finding.field_name
        issue.left_document_id = finding.left_document_id
        issue.right_document_id = finding.right_document_id
        issue.left_source = finding.left_source
        issue.right_source = finding.right_source
        issue.left_value = finding.left_value
        issue.right_value = finding.right_value
        issue.delta_numeric = finding.delta_numeric
        issue.explanation = finding.explanation
        issue.evidence_json = {"sources": list(finding.evidence)}
        if issue.status == ReconciliationIssueStatus.RESOLVED.value:
            issue.status = ReconciliationIssueStatus.OPEN.value
            issue.resolved_at = None
            issue.resolution_justification = None
            reopened += 1
        else:
            refreshed += 1

    current_document_ids = {
        document.assurance_document_id
        for document in operation.documents
        if document.assurance_document_id is not None
    }
    auto_resolved = 0
    now = _utc_now()
    for issue in existing:
        if issue.fingerprint in active_fingerprints:
            continue
        if issue.status != ReconciliationIssueStatus.OPEN.value:
            continue
        source_ids = {
            value
            for value in (issue.left_document_id, issue.right_document_id)
            if value is not None
        }
        # Resolve automatically only when the evidence documents that created the
        # old issue are still part of the recomputed snapshot. This avoids
        # declaring a discrepancy solved merely because evidence disappeared.
        if not source_ids or not source_ids.issubset(current_document_ids):
            continue
        issue.status = ReconciliationIssueStatus.RESOLVED.value
        issue.resolved_at = now
        issue.resolution_justification = (
            "Resuelta automaticamente al volver a conciliar las mismas fuentes vigentes."
        )
        auto_resolved += 1

    return created, refreshed, reopened, auto_resolved


class AssuranceReconciliationService:
    def __init__(self, *, session_factory: SessionFactory | None = None) -> None:
        self._session_factory = session_factory or get_db_session

    def _new_session(self, organization_id: int) -> Session:
        session = self._session_factory()
        if session is None:
            raise AssuranceReconciliationError("No se pudo abrir una sesion de conciliacion.")
        set_tenant_db_context(session, organization_id)
        return session

    def _operation_targets_for_document(
        self,
        session: Session,
        *,
        organization_id: int,
        assurance_document_id: int,
    ) -> dict[str, tuple[tuple[str, str], ...]]:
        links = session.scalars(
            select(DocumentEntityLink).where(
                DocumentEntityLink.organization_id == organization_id,
                DocumentEntityLink.assurance_document_id == assurance_document_id,
                DocumentEntityLink.entity_type.in_(tuple(_OPERATION_LINK_TYPES)),
            )
        ).all()
        targets: dict[str, tuple[tuple[str, str], ...]] = {}
        for link in links:
            target = _canonical_operation_target(link.entity_type, link.entity_reference)
            if target is not None:
                canonical, related = target
                targets[canonical] = related
        return targets

    def _document_ids_for_target(
        self,
        session: Session,
        *,
        organization_id: int,
        related_targets: tuple[tuple[str, str], ...],
    ) -> tuple[int, ...]:
        document_ids: set[int] = set()
        for entity_type, entity_reference in related_targets:
            links = session.scalars(
                select(DocumentEntityLink).where(
                    DocumentEntityLink.organization_id == organization_id,
                    DocumentEntityLink.entity_type == entity_type,
                    DocumentEntityLink.entity_reference == entity_reference,
                )
            ).all()
            document_ids.update(link.assurance_document_id for link in links)
        return tuple(sorted(document_ids))

    def _build_operation_snapshot(
        self,
        session: Session,
        *,
        organization_id: int,
        operation_reference: str,
        related_targets: tuple[tuple[str, str], ...],
    ) -> OperationSnapshot:
        document_ids = self._document_ids_for_target(
            session,
            organization_id=organization_id,
            related_targets=related_targets,
        )
        if not document_ids:
            return OperationSnapshot(operation_reference=operation_reference)

        documents = session.scalars(
            select(AssuranceDocument)
            .where(
                AssuranceDocument.organization_id == organization_id,
                AssuranceDocument.id.in_(document_ids),
            )
            .order_by(AssuranceDocument.id.asc())
        ).all()
        vault_ids = tuple(sorted({document.vault_document_id for document in documents}))
        vaults = session.scalars(
            select(VaultDocument).where(
                VaultDocument.organization_id == organization_id,
                VaultDocument.id.in_(vault_ids),
            )
        ).all()
        vault_by_id = {vault.id: vault for vault in vaults}
        values, locators = _accepted_fields_by_document(
            session,
            organization_id=organization_id,
            assurance_document_ids=document_ids,
        )

        snapshots: list[DocumentSnapshot] = []
        for document in documents:
            vault = vault_by_id.get(document.vault_document_id)
            reference = (
                vault.original_filename
                if vault is not None and vault.original_filename
                else f"assurance:{document.public_id}"
            )
            snapshots.append(
                DocumentSnapshot(
                    reference=reference,
                    document_type=document.semantic_document_type,
                    fields=values.get(document.id, {}),
                    source_locators=locators.get(document.id, {}),
                    assurance_document_id=document.id,
                    valid_from=document.valid_from,
                    valid_until=document.valid_until,
                )
            )

        system_values, allocated_lots, shipment_date = _shipment_context(
            session,
            organization_id=organization_id,
            operation_reference=operation_reference,
        )
        return OperationSnapshot(
            operation_reference=operation_reference,
            documents=tuple(snapshots),
            system_values=system_values,
            shipment_date=shipment_date,
            allocated_lots=allocated_lots,
        )

    def reconcile_document(
        self,
        *,
        organization_id: int,
        assurance_public_id: UUID | str,
    ) -> ReconciliationPersistenceResult:
        org_id = int(organization_id)
        public_id = (
            assurance_public_id
            if isinstance(assurance_public_id, UUID)
            else UUID(str(assurance_public_id))
        )
        session = self._new_session(org_id)
        try:
            document = session.scalar(
                select(AssuranceDocument).where(
                    AssuranceDocument.organization_id == org_id,
                    AssuranceDocument.public_id == public_id,
                )
            )
            if document is None:
                raise AssuranceReconciliationError("Documento Assurance no encontrado.")

            targets = self._operation_targets_for_document(
                session,
                organization_id=org_id,
                assurance_document_id=document.id,
            )
            finding_count = 0
            created_count = 0
            refreshed_count = 0
            reopened_count = 0
            auto_resolved_count = 0

            for operation_reference, related_targets in sorted(targets.items()):
                operation = self._build_operation_snapshot(
                    session,
                    organization_id=org_id,
                    operation_reference=operation_reference,
                    related_targets=related_targets,
                )
                findings = reconcile_operation(operation)
                created, refreshed, reopened, resolved = _persist_findings(
                    session,
                    organization_id=org_id,
                    operation=operation,
                    findings=findings,
                )
                finding_count += len(findings)
                created_count += created
                refreshed_count += refreshed
                reopened_count += reopened
                auto_resolved_count += resolved

            session.commit()
            return ReconciliationPersistenceResult(
                operation_count=len(targets),
                finding_count=finding_count,
                created_count=created_count,
                refreshed_count=refreshed_count,
                reopened_count=reopened_count,
                auto_resolved_count=auto_resolved_count,
            )
        except (ValueError, AssuranceReconciliationError):
            session.rollback()
            raise
        except Exception as exc:
            session.rollback()
            raise AssuranceReconciliationError("No se pudo conciliar el documento Assurance.") from exc
        finally:
            session.close()
