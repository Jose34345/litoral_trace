"""Tenant-scoped actionable exception lifecycle for Assurance v1.

Exceptions are derived views over LT-owned reconciliation/preflight facts. They
must never hide an unresolved source condition: reconciliation exceptions can be
closed only with an explicit justification on the source discrepancy, and a
subsequent preflight recomputation decides whether the operation is actually
ready.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
from typing import Callable, Iterable
from uuid import UUID

from sqlalchemy import case, select
from sqlalchemy.orm import Session

from litoral_trace.assurance.domain import (
    OperationalExceptionPriority,
    OperationalExceptionSource,
    OperationalExceptionStatus,
    ReconciliationIssueStatus,
    ReconciliationSeverity,
)
from litoral_trace.assurance.preflight import PreflightInput, PreflightStatus
from litoral_trace.assurance.preflight_service import (
    AssurancePreflightService,
    AssurancePreflightView,
)
from litoral_trace.db.engine import get_db_session
from litoral_trace.db.models import OperationalException, ReconciliationIssue
from litoral_trace.db.tenant import set_tenant_db_context


SessionFactory = Callable[[], Session | None]


class AssuranceOperationalExceptionError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class OperationalExceptionSyncResult:
    created_count: int
    refreshed_count: int
    reopened_count: int
    auto_resolved_count: int


@dataclass(frozen=True, slots=True)
class OperationalExceptionResolutionResult:
    exception_public_id: UUID
    source_status: str | None
    preflight: AssurancePreflightView | None


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _fingerprint(*parts: object) -> str:
    canonical = "|".join(str(value or "").strip() for value in parts)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _priority_for_impact(impact: str) -> OperationalExceptionPriority:
    normalized = str(impact or "").strip().upper()
    if normalized == ReconciliationSeverity.BLOCKING.value:
        return OperationalExceptionPriority.CRITICAL
    if normalized == ReconciliationSeverity.WARNING.value:
        return OperationalExceptionPriority.HIGH
    return OperationalExceptionPriority.LOW


def _impact_for_preflight(status: PreflightStatus) -> ReconciliationSeverity:
    if status == PreflightStatus.BLOCKED:
        return ReconciliationSeverity.BLOCKING
    if status == PreflightStatus.CONDITIONAL:
        return ReconciliationSeverity.WARNING
    return ReconciliationSeverity.INFO


def _source_ref_for_issue(issue: ReconciliationIssue) -> str:
    if issue.public_id is not None:
        return str(issue.public_id)
    return f"fingerprint:{issue.fingerprint}"


class AssuranceOperationalExceptionService:
    """Create, list and resolve actionable Assurance exceptions."""

    def __init__(self, *, session_factory: SessionFactory | None = None) -> None:
        self._session_factory = session_factory or get_db_session

    def _new_session(self, organization_id: int) -> Session:
        session = self._session_factory()
        if session is None:
            raise AssuranceOperationalExceptionError(
                "No se pudo abrir una sesión para excepciones operativas."
            )
        set_tenant_db_context(session, organization_id)
        return session

    @staticmethod
    def _upsert(
        session: Session,
        *,
        existing_by_fingerprint: dict[str, OperationalException],
        organization_id: int,
        fingerprint: str,
        source_type: str,
        source_reference: str | None,
        operation_reference: str,
        cause_code: str,
        entity_type: str,
        entity_reference: str,
        title: str,
        description: str,
        impact: str,
        priority: str,
        recommended_action: str,
        source_snapshot: dict[str, object] | None,
    ) -> tuple[str, OperationalException]:
        row = existing_by_fingerprint.get(fingerprint)
        if row is None:
            row = OperationalException(
                organization_id=organization_id,
                fingerprint=fingerprint,
                source_type=source_type,
                source_reference=source_reference,
                operation_reference=operation_reference,
                cause_code=cause_code,
                entity_type=entity_type,
                entity_reference=entity_reference,
                title=title,
                description=description,
                impact=impact,
                priority=priority,
                status=OperationalExceptionStatus.OPEN.value,
                recommended_action=recommended_action,
                source_snapshot=source_snapshot,
            )
            session.add(row)
            existing_by_fingerprint[fingerprint] = row
            return "created", row

        row.source_type = source_type
        row.source_reference = source_reference
        row.operation_reference = operation_reference
        row.cause_code = cause_code
        row.entity_type = entity_type
        row.entity_reference = entity_reference
        row.title = title
        row.description = description
        row.impact = impact
        row.priority = priority
        row.recommended_action = recommended_action
        row.source_snapshot = source_snapshot
        if row.status in {
            OperationalExceptionStatus.RESOLVED.value,
            OperationalExceptionStatus.DISMISSED.value,
        }:
            row.status = OperationalExceptionStatus.OPEN.value
            row.resolution_note = None
            row.resolved_by_user_id = None
            row.resolved_at = None
            return "reopened", row
        return "refreshed", row

    def sync_reconciliation(self, *, organization_id: int) -> OperationalExceptionSyncResult:
        """Materialize all currently open tenant reconciliation issues."""
        org_id = int(organization_id)
        if org_id <= 0:
            raise ValueError("organization_id debe ser mayor que cero.")
        session = self._new_session(org_id)
        try:
            issues = session.scalars(
                select(ReconciliationIssue).where(
                    ReconciliationIssue.organization_id == org_id,
                    ReconciliationIssue.status == ReconciliationIssueStatus.OPEN.value,
                )
            ).all()
            existing = session.scalars(
                select(OperationalException).where(
                    OperationalException.organization_id == org_id,
                    OperationalException.source_type
                    == OperationalExceptionSource.RECONCILIATION.value,
                )
            ).all()
            by_fingerprint = {row.fingerprint: row for row in existing}
            active: set[str] = set()
            counters = {"created": 0, "refreshed": 0, "reopened": 0}

            for issue in issues:
                source_reference = _source_ref_for_issue(issue)
                fingerprint = _fingerprint(
                    OperationalExceptionSource.RECONCILIATION.value,
                    issue.operation_reference,
                    issue.fingerprint,
                )
                active.add(fingerprint)
                outcome, _ = self._upsert(
                    session,
                    existing_by_fingerprint=by_fingerprint,
                    organization_id=org_id,
                    fingerprint=fingerprint,
                    source_type=OperationalExceptionSource.RECONCILIATION.value,
                    source_reference=source_reference,
                    operation_reference=issue.operation_reference,
                    cause_code=issue.rule_code,
                    entity_type="OPERATION",
                    entity_reference=issue.operation_reference,
                    title=f"Conciliación: {issue.rule_code}",
                    description=issue.explanation,
                    impact=issue.severity,
                    priority=_priority_for_impact(issue.severity).value,
                    recommended_action=(
                        "Revisar las fuentes indicadas, corregir el dato o aceptar "
                        "la diferencia con una justificación explícita."
                    ),
                    source_snapshot={
                        "field_name": issue.field_name,
                        "left_source": issue.left_source,
                        "right_source": issue.right_source,
                        "left_value": issue.left_value,
                        "right_value": issue.right_value,
                        "evidence": issue.evidence_json,
                    },
                )
                counters[outcome] += 1

            auto_resolved = 0
            now = _utc_now()
            for row in existing:
                if row.fingerprint in active:
                    continue
                if row.status not in {
                    OperationalExceptionStatus.OPEN.value,
                    OperationalExceptionStatus.IN_PROGRESS.value,
                }:
                    continue
                row.status = OperationalExceptionStatus.RESOLVED.value
                row.resolved_at = now
                row.resolution_note = (
                    "Cerrada automáticamente porque la discrepancia fuente ya no está abierta."
                )
                auto_resolved += 1

            session.commit()
            return OperationalExceptionSyncResult(
                created_count=counters["created"],
                refreshed_count=counters["refreshed"],
                reopened_count=counters["reopened"],
                auto_resolved_count=auto_resolved,
            )
        except (ValueError, AssuranceOperationalExceptionError):
            session.rollback()
            raise
        except Exception as exc:
            session.rollback()
            raise AssuranceOperationalExceptionError(
                "No se pudieron sincronizar excepciones de conciliación."
            ) from exc
        finally:
            session.close()

    def sync_preflight(
        self,
        *,
        organization_id: int,
        view: AssurancePreflightView,
    ) -> OperationalExceptionSyncResult:
        """Materialize actionable reasons from one freshly evaluated preflight."""
        org_id = int(organization_id)
        reference = str(view.operation_reference or "").strip()
        if org_id <= 0 or not reference:
            raise ValueError("Tenant y operación son obligatorios.")
        session = self._new_session(org_id)
        try:
            existing = session.scalars(
                select(OperationalException).where(
                    OperationalException.organization_id == org_id,
                    OperationalException.operation_reference == reference,
                    OperationalException.source_type
                    == OperationalExceptionSource.PREFLIGHT.value,
                )
            ).all()
            by_fingerprint = {row.fingerprint: row for row in existing}
            active: set[str] = set()
            counters = {"created": 0, "refreshed": 0, "reopened": 0}

            for reason in view.result.reasons:
                if reason.status == PreflightStatus.READY:
                    continue
                source_reference = f"{reason.code}:{reason.source or 'operation'}"[:255]
                fingerprint = _fingerprint(
                    OperationalExceptionSource.PREFLIGHT.value,
                    reference,
                    reason.code,
                    reason.source,
                )
                active.add(fingerprint)
                impact = _impact_for_preflight(reason.status)
                outcome, _ = self._upsert(
                    session,
                    existing_by_fingerprint=by_fingerprint,
                    organization_id=org_id,
                    fingerprint=fingerprint,
                    source_type=OperationalExceptionSource.PREFLIGHT.value,
                    source_reference=source_reference,
                    operation_reference=reference,
                    cause_code=reason.code,
                    entity_type="OPERATION",
                    entity_reference=reference,
                    title=f"Preflight: {reason.code}",
                    description=reason.explanation,
                    impact=impact.value,
                    priority=_priority_for_impact(impact.value).value,
                    recommended_action=reason.action,
                    source_snapshot={
                        "category": reason.category,
                        "preflight_status": reason.status.value,
                        "source": reason.source,
                    },
                )
                counters[outcome] += 1

            auto_resolved = 0
            now = _utc_now()
            for row in existing:
                if row.fingerprint in active:
                    continue
                if row.status not in {
                    OperationalExceptionStatus.OPEN.value,
                    OperationalExceptionStatus.IN_PROGRESS.value,
                }:
                    continue
                row.status = OperationalExceptionStatus.RESOLVED.value
                row.resolved_at = now
                row.resolution_note = (
                    "Cerrada automáticamente porque el nuevo Preflight ya no reporta esta causa."
                )
                auto_resolved += 1

            session.commit()
            return OperationalExceptionSyncResult(
                created_count=counters["created"],
                refreshed_count=counters["refreshed"],
                reopened_count=counters["reopened"],
                auto_resolved_count=auto_resolved,
            )
        except (ValueError, AssuranceOperationalExceptionError):
            session.rollback()
            raise
        except Exception as exc:
            session.rollback()
            raise AssuranceOperationalExceptionError(
                "No se pudieron sincronizar excepciones de Preflight."
            ) from exc
        finally:
            session.close()

    def list_attention(
        self,
        *,
        organization_id: int,
        operation_reference: str | None = None,
    ) -> tuple[OperationalException, ...]:
        """Return open work ordered by business severity and deadline."""
        org_id = int(organization_id)
        session = self._new_session(org_id)
        priority_rank = case(
            (OperationalException.priority == OperationalExceptionPriority.CRITICAL.value, 0),
            (OperationalException.priority == OperationalExceptionPriority.HIGH.value, 1),
            (OperationalException.priority == OperationalExceptionPriority.MEDIUM.value, 2),
            else_=3,
        )
        try:
            query = select(OperationalException).where(
                OperationalException.organization_id == org_id,
                OperationalException.status.in_(
                    (
                        OperationalExceptionStatus.OPEN.value,
                        OperationalExceptionStatus.IN_PROGRESS.value,
                    )
                ),
            )
            if operation_reference:
                query = query.where(
                    OperationalException.operation_reference
                    == str(operation_reference).strip()
                )
            rows = session.scalars(
                query.order_by(
                    priority_rank.asc(),
                    OperationalException.due_at.is_(None).asc(),
                    OperationalException.due_at.asc(),
                    OperationalException.id.asc(),
                )
            ).all()
            # Return detached rows so API/UI callers cannot mutate the session.
            for row in rows:
                session.expunge(row)
            return tuple(rows)
        except Exception as exc:
            raise AssuranceOperationalExceptionError(
                "No se pudo consultar la bandeja de excepciones."
            ) from exc
        finally:
            session.close()

    def resolve(
        self,
        *,
        organization_id: int,
        exception_public_id: UUID | str,
        resolved_by_user_id: int | None,
        resolution_note: str,
        preflight_payload: PreflightInput | None = None,
    ) -> OperationalExceptionResolutionResult:
        """Resolve an exception and optionally recompute the operation preflight.

        Reconciliation-derived exceptions update their source issue to
        ACCEPTED_WITH_JUSTIFICATION. Preflight-derived exceptions are not allowed
        to suppress their source reason; a recomputation will reopen the exception
        if the underlying condition still exists.
        """
        org_id = int(organization_id)
        public_id = (
            exception_public_id
            if isinstance(exception_public_id, UUID)
            else UUID(str(exception_public_id))
        )
        note = str(resolution_note or "").strip()
        if not note:
            raise ValueError("resolution_note es obligatorio.")

        session = self._new_session(org_id)
        source_status: str | None = None
        operation_reference: str
        try:
            row = session.scalar(
                select(OperationalException).where(
                    OperationalException.organization_id == org_id,
                    OperationalException.public_id == public_id,
                )
            )
            if row is None:
                raise AssuranceOperationalExceptionError("Excepción operativa no encontrada.")
            operation_reference = row.operation_reference
            now = _utc_now()

            if row.source_type == OperationalExceptionSource.RECONCILIATION.value:
                source_issue = None
                if row.source_reference:
                    try:
                        source_uuid = UUID(row.source_reference)
                    except (TypeError, ValueError):
                        source_uuid = None
                    if source_uuid is not None:
                        source_issue = session.scalar(
                            select(ReconciliationIssue).where(
                                ReconciliationIssue.organization_id == org_id,
                                ReconciliationIssue.public_id == source_uuid,
                            )
                        )
                if source_issue is None:
                    raise AssuranceOperationalExceptionError(
                        "La discrepancia fuente ya no puede identificarse; no se cerró la excepción."
                    )
                source_issue.status = ReconciliationIssueStatus.ACCEPTED_WITH_JUSTIFICATION.value
                source_issue.resolution_justification = note
                source_issue.resolved_at = now
                source_status = source_issue.status

            row.status = OperationalExceptionStatus.RESOLVED.value
            row.resolution_note = note
            row.resolved_by_user_id = resolved_by_user_id
            row.resolved_at = now
            session.commit()
        except (ValueError, AssuranceOperationalExceptionError):
            session.rollback()
            raise
        except Exception as exc:
            session.rollback()
            raise AssuranceOperationalExceptionError(
                "No se pudo resolver la excepción operativa."
            ) from exc
        finally:
            session.close()

        preflight_view: AssurancePreflightView | None = None
        if preflight_payload is not None:
            preflight_view = AssurancePreflightService(
                session_factory=self._session_factory
            ).evaluate(
                organization_id=org_id,
                operation_reference=operation_reference,
                payload=preflight_payload,
            )
            self.sync_preflight(organization_id=org_id, view=preflight_view)

        return OperationalExceptionResolutionResult(
            exception_public_id=public_id,
            source_status=source_status,
            preflight=preflight_view,
        )
