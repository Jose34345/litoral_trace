"""Tenant-scoped Preflight 2.0 service backed by persisted LT evidence."""
from __future__ import annotations

from dataclasses import dataclass, replace
import hashlib
from typing import Callable

from sqlalchemy import and_, or_, select
from sqlalchemy.orm import Session

from litoral_trace.assurance.domain import (
    DocumentProcessingStatus,
    ReconciliationIssueStatus,
    ReconciliationSeverity,
)
from litoral_trace.assurance.preflight import (
    PreflightInput,
    PreflightResult,
    evaluate_preflight,
)
from litoral_trace.assurance.reconciliation import ReconciliationFinding
from litoral_trace.db.engine import get_db_session
from litoral_trace.db.models import AssuranceDocument, DocumentEntityLink, ReconciliationIssue
from litoral_trace.db.tenant import set_tenant_db_context


SessionFactory = Callable[[], Session | None]


class AssurancePreflightError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class AssurancePreflightView:
    operation_reference: str
    result: PreflightResult
    open_reconciliation_issue_count: int


def _finding_from_issue(issue: ReconciliationIssue) -> ReconciliationFinding:
    raw_sources = []
    if isinstance(issue.evidence_json, dict):
        candidate = issue.evidence_json.get("sources")
        if isinstance(candidate, list):
            raw_sources = [item for item in candidate if isinstance(item, dict)]

    return ReconciliationFinding(
        rule_code=issue.rule_code,
        severity=ReconciliationSeverity(issue.severity),
        field_name=issue.field_name,
        left_source=issue.left_source,
        left_value=issue.left_value,
        right_source=issue.right_source,
        right_value=issue.right_value,
        explanation=issue.explanation,
        evidence=tuple(raw_sources),
        fingerprint=issue.fingerprint,
        left_document_id=issue.left_document_id,
        right_document_id=issue.right_document_id,
        delta_numeric=issue.delta_numeric,
    )


def _operation_link_targets(reference: str) -> tuple[tuple[str, str], ...]:
    """Return deterministic aliases accepted by document matching for one operation."""
    normalized = str(reference or "").strip()
    targets: set[tuple[str, str]] = {("OPERATION", normalized)}
    if ":" in normalized:
        prefix, suffix = normalized.split(":", 1)
        suffix = suffix.strip()
        if suffix and prefix.strip().lower() in {"shipment", "order"}:
            targets.update(
                {
                    ("SHIPMENT", f"shipment:{suffix}"),
                    ("ORDER", f"order:{suffix}"),
                }
            )
    else:
        targets.update({("SHIPMENT", normalized), ("ORDER", normalized)})
    return tuple(sorted(targets))


def _document_state_finding(document: AssuranceDocument) -> ReconciliationFinding | None:
    status = str(document.processing_status or "").strip().upper()
    if status == DocumentProcessingStatus.EXTRACTED.value:
        return None

    public_reference = str(document.public_id or document.id)
    source = f"assurance_document:{public_reference}"
    error_code = str(document.last_error_code or "").strip() or None

    if status == DocumentProcessingStatus.FAILED.value:
        rule_code = "DOCUMENT_PROCESSING_FAILED"
        severity = ReconciliationSeverity.BLOCKING
        explanation = "Un documento vinculado falló durante su procesamiento y no puede sustentar un READY."
    elif status == DocumentProcessingStatus.NEEDS_REVIEW.value:
        rule_code = "DOCUMENT_REVIEW_REQUIRED"
        severity = ReconciliationSeverity.BLOCKING
        explanation = "Un documento vinculado requiere revisión humana antes de liberar la operación."
    else:
        rule_code = "DOCUMENT_PROCESSING_PENDING"
        severity = ReconciliationSeverity.WARNING
        explanation = "Un documento vinculado todavía no terminó su procesamiento."

    if error_code:
        explanation = f"{explanation} Código controlado: {error_code}."

    fingerprint = hashlib.sha256(
        f"{rule_code}|{document.organization_id}|{document.id}|{status}|{error_code or ''}".encode("utf-8")
    ).hexdigest()
    return ReconciliationFinding(
        rule_code=rule_code,
        severity=severity,
        field_name="processing_status",
        left_source=source,
        left_value=status or None,
        right_source="preflight.required_document_state",
        right_value=DocumentProcessingStatus.EXTRACTED.value,
        explanation=explanation,
        evidence=(
            {
                "source": source,
                "processing_status": status,
                "error_code": error_code,
            },
        ),
        left_document_id=document.id,
        fingerprint=fingerprint,
    )


class AssurancePreflightService:
    """Evaluate readiness using caller inputs plus LT-owned persisted facts."""

    def __init__(self, *, session_factory: SessionFactory | None = None) -> None:
        self._session_factory = session_factory or get_db_session

    def evaluate(
        self,
        *,
        organization_id: int,
        operation_reference: str,
        payload: PreflightInput,
    ) -> AssurancePreflightView:
        org_id = int(organization_id)
        reference = str(operation_reference or "").strip()
        if org_id <= 0:
            raise ValueError("organization_id debe ser mayor que cero.")
        if not reference:
            raise ValueError("operation_reference es obligatorio.")

        session = self._session_factory()
        if session is None:
            raise AssurancePreflightError("No se pudo abrir una sesión para Preflight 2.0.")
        set_tenant_db_context(session, org_id)
        try:
            issues = session.scalars(
                select(ReconciliationIssue)
                .where(
                    ReconciliationIssue.organization_id == org_id,
                    ReconciliationIssue.operation_reference == reference,
                    ReconciliationIssue.status == ReconciliationIssueStatus.OPEN.value,
                )
                .order_by(ReconciliationIssue.id.asc())
            ).all()
            findings = [_finding_from_issue(issue) for issue in issues]

            targets = _operation_link_targets(reference)
            predicates = [
                and_(
                    DocumentEntityLink.entity_type == entity_type,
                    DocumentEntityLink.entity_reference == entity_reference,
                )
                for entity_type, entity_reference in targets
            ]
            links = []
            if predicates:
                links = session.scalars(
                    select(DocumentEntityLink).where(
                        DocumentEntityLink.organization_id == org_id,
                        or_(*predicates),
                    )
                ).all()
            linked_document_ids = tuple(sorted({row.assurance_document_id for row in links}))
            if linked_document_ids:
                documents = session.scalars(
                    select(AssuranceDocument)
                    .where(
                        AssuranceDocument.organization_id == org_id,
                        AssuranceDocument.id.in_(linked_document_ids),
                    )
                    .order_by(AssuranceDocument.id.asc())
                ).all()
                findings.extend(
                    finding
                    for document in documents
                    if (finding := _document_state_finding(document)) is not None
                )

            evaluated = evaluate_preflight(
                replace(payload, reconciliation_findings=tuple(findings))
            )
            return AssurancePreflightView(
                operation_reference=reference,
                result=evaluated,
                open_reconciliation_issue_count=len(issues),
            )
        except (ValueError, AssurancePreflightError):
            raise
        except Exception as exc:
            raise AssurancePreflightError("No se pudo evaluar Preflight 2.0.") from exc
        finally:
            session.close()
