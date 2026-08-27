"""Tenant-scoped Preflight 2.0 service backed by persisted reconciliation."""
from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Callable

from sqlalchemy import select
from sqlalchemy.orm import Session

from litoral_trace.assurance.domain import (
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
from litoral_trace.db.models import ReconciliationIssue
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


class AssurancePreflightService:
    """Evaluate readiness using caller inputs plus LT-owned open discrepancies."""

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
            findings = tuple(_finding_from_issue(issue) for issue in issues)
            evaluated = evaluate_preflight(
                replace(payload, reconciliation_findings=findings)
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
