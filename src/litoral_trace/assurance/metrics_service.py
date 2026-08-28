"""Persisted pilot metrics for the Assurance v1 workflow."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from statistics import mean
from typing import Callable

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from litoral_trace.assurance.metrics import (
    AssurancePilotMetrics,
    REVIEW_TIME_REDUCTION_TARGET_PERCENTAGE,
    ZERO_FRICTION_TARGET_PERCENTAGE,
    review_time_reduction_percentage,
)
from litoral_trace.db.engine import get_db_session
from litoral_trace.db.models import (
    AssuranceDocument,
    AuditLog,
    DocumentExtractionRun,
    ExtractedDocumentField,
    OperationalException,
    ReconciliationIssue,
    Shipment,
)
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.services.audit import (
    AuditAction,
    AuditActor,
    AuditOutcome,
    AuditRequestContext,
    record_audit_event,
)


SessionFactory = Callable[[], Session | None]
_COMPARISON_SCOPE = "DOCUMENT_INTAKE_TO_EXTRACTION"


class AssuranceMetricsError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class AssuranceManualVsLtReport:
    available: bool
    comparison_scope: str
    manual_baseline_seconds: float | None
    lt_average_seconds: float
    time_reduction_percentage: float | None
    target_reduction_percentage: float
    target_met: bool | None
    baseline_label: str | None
    baseline_recorded_at: str | None

    def as_dict(self) -> dict[str, object]:
        return {
            "available": self.available,
            "comparison_scope": self.comparison_scope,
            "manual_baseline_seconds": self.manual_baseline_seconds,
            "lt_average_seconds": self.lt_average_seconds,
            "time_reduction_percentage": self.time_reduction_percentage,
            "target_reduction_percentage": self.target_reduction_percentage,
            "target_met": self.target_met,
            "baseline_label": self.baseline_label,
            "baseline_recorded_at": self.baseline_recorded_at,
            "caveat": (
                "Compara ingreso/revisión documental manual contra upload→extracción de LT; "
                "no representa todavía el tiempo end-to-end de una exportación."
            ),
        }


@dataclass(frozen=True, slots=True)
class AssuranceMetricsSnapshot:
    metrics: AssurancePilotMetrics
    completed_documents: int
    average_upload_to_extraction_seconds: float
    average_exception_resolution_seconds: float
    resolved_exception_count: int
    open_reconciliation_issue_count: int
    blocking_exceptions_before_dispatch: int
    zero_friction_target_percentage: float
    zero_friction_target_met: bool
    manual_vs_lt: AssuranceManualVsLtReport

    def as_dict(self) -> dict[str, object]:
        metric_payload = self.metrics.as_dict()
        automatic = float(metric_payload["automatic_data_percentage"])
        return {
            **metric_payload,
            "auto_acceptance_percentage": automatic,
            "discrepancies_detected": self.metrics.reconciliation_issues,
            "completed_documents": self.completed_documents,
            "upload_to_extraction_seconds": self.average_upload_to_extraction_seconds,
            "average_upload_to_extraction_seconds": self.average_upload_to_extraction_seconds,
            "average_exception_resolution_seconds": self.average_exception_resolution_seconds,
            "resolved_exception_count": self.resolved_exception_count,
            "open_reconciliation_issue_count": self.open_reconciliation_issue_count,
            "blocking_exceptions_before_dispatch": self.blocking_exceptions_before_dispatch,
            "zero_friction_target_percentage": self.zero_friction_target_percentage,
            "zero_friction_target_met": self.zero_friction_target_met,
            "manual_vs_lt": self.manual_vs_lt.as_dict(),
        }


def _aware(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def _state_after(row: AuditLog) -> dict[str, object]:
    payload = row.after_data if isinstance(row.after_data, dict) else {}
    state = payload.get("state_after")
    return state if isinstance(state, dict) else {}


def _metadata(row: AuditLog) -> dict[str, object]:
    payload = row.after_data if isinstance(row.after_data, dict) else {}
    metadata = payload.get("metadata")
    return metadata if isinstance(metadata, dict) else {}


def _unique_manually_changed_fields(rows: list[AuditLog]) -> int:
    changed: set[tuple[int | None, int]] = set()
    for row in rows:
        if row.action != AuditAction.ASSURANCE_REVIEW_CORRECT.value:
            continue
        before_payload = row.before_data if isinstance(row.before_data, dict) else {}
        before_fields = before_payload.get("fields")
        before_by_id: dict[int, object] = {}
        if isinstance(before_fields, list):
            for item in before_fields:
                if not isinstance(item, dict):
                    continue
                try:
                    field_id = int(item.get("field_id"))
                except (TypeError, ValueError):
                    continue
                before_by_id[field_id] = item.get("effective_value")

        after_fields = _state_after(row).get("fields")
        if not isinstance(after_fields, list):
            continue
        for item in after_fields:
            if not isinstance(item, dict):
                continue
            try:
                field_id = int(item.get("field_id"))
            except (TypeError, ValueError):
                continue
            if str(before_by_id.get(field_id) or "") != str(item.get("effective_value") or ""):
                changed.add((row.entity_id, field_id))
    return len(changed)


def _blocking_before_dispatch(
    *,
    exceptions: list[OperationalException],
    shipments: list[Shipment],
) -> int:
    by_public_id = {str(row.public_id): row for row in shipments}
    detected = 0
    for row in exceptions:
        if str(row.impact or "").upper() != "BLOCKING":
            continue
        reference = str(row.operation_reference or "").strip()
        if not reference.startswith("shipment:"):
            continue
        shipment = by_public_id.get(reference.split(":", 1)[1])
        if shipment is None:
            continue
        created_at = _aware(row.created_at)
        shipped_at = _aware(shipment.shipped_at)
        if created_at is None:
            continue
        if shipped_at is not None:
            if created_at <= shipped_at:
                detected += 1
            continue
        if str(shipment.status or "").upper() != "DISPATCHED":
            detected += 1
    return detected


class AssuranceMetricsService:
    def __init__(self, *, session_factory: SessionFactory | None = None) -> None:
        self._session_factory = session_factory or get_db_session

    def _new_session(self, organization_id: int) -> Session:
        session = self._session_factory()
        if session is None:
            raise AssuranceMetricsError("No se pudo abrir una sesión para métricas Assurance.")
        set_tenant_db_context(session, int(organization_id))
        return session

    def set_manual_baseline(
        self,
        *,
        organization_id: int,
        manual_baseline_seconds: float,
        actor: AuditActor,
        request_context: AuditRequestContext | None = None,
        label: str | None = None,
    ) -> dict[str, object]:
        """Persist the pilot's manual document-intake baseline in the immutable audit log."""
        org_id = int(organization_id)
        baseline = float(manual_baseline_seconds)
        if org_id <= 0:
            raise ValueError("organization_id debe ser mayor que cero.")
        if actor.organization_id != org_id:
            raise ValueError("El actor no pertenece al tenant de las métricas.")
        if baseline <= 0 or baseline > 604800:
            raise ValueError("El baseline manual debe estar entre 0 y 604800 segundos.")
        normalized_label = str(label or "").strip()[:120] or None

        session = self._new_session(org_id)
        try:
            record_audit_event(
                session,
                actor=actor,
                action=AuditAction.ASSURANCE_PILOT_BASELINE_SET,
                entity_type="assurance_pilot_metrics",
                outcome=AuditOutcome.SUCCESS,
                request_context=request_context,
                metadata={"comparison_scope": _COMPARISON_SCOPE},
                after_data={
                    "manual_baseline_seconds": round(baseline, 3),
                    "comparison_scope": _COMPARISON_SCOPE,
                    "label": normalized_label,
                },
            )
            session.commit()
            return {
                "manual_baseline_seconds": round(baseline, 3),
                "comparison_scope": _COMPARISON_SCOPE,
                "label": normalized_label,
            }
        except (ValueError, AssuranceMetricsError):
            session.rollback()
            raise
        except Exception as exc:
            session.rollback()
            raise AssuranceMetricsError("No se pudo registrar el baseline manual.") from exc
        finally:
            session.close()

    def snapshot(self, *, organization_id: int) -> AssuranceMetricsSnapshot:
        org_id = int(organization_id)
        session = self._new_session(org_id)
        try:
            latest_run_ids = select(
                func.max(DocumentExtractionRun.id).label("run_id")
            ).where(
                DocumentExtractionRun.organization_id == org_id
            ).group_by(
                DocumentExtractionRun.assurance_document_id
            )
            latest_ids = tuple(
                value for value in session.scalars(latest_run_ids).all() if value is not None
            )

            runs: list[DocumentExtractionRun] = []
            fields: list[ExtractedDocumentField] = []
            if latest_ids:
                runs = list(
                    session.scalars(
                        select(DocumentExtractionRun).where(
                            DocumentExtractionRun.organization_id == org_id,
                            DocumentExtractionRun.id.in_(latest_ids),
                        )
                    ).all()
                )
                fields = list(
                    session.scalars(
                        select(ExtractedDocumentField).where(
                            ExtractedDocumentField.organization_id == org_id,
                            ExtractedDocumentField.extraction_run_id.in_(latest_ids),
                            ~ExtractedDocumentField.field_name.startswith("raw."),
                        )
                    ).all()
                )

            documents = {
                row.id: row
                for row in session.scalars(
                    select(AssuranceDocument).where(
                        AssuranceDocument.organization_id == org_id
                    )
                ).all()
            }
            upload_to_extraction: list[float] = []
            processing_durations: list[float] = []
            for run in runs:
                started_at = _aware(run.started_at)
                completed_at = _aware(run.completed_at)
                if started_at is not None and completed_at is not None:
                    processing_durations.append(
                        max(0.0, (completed_at - started_at).total_seconds())
                    )
                document = documents.get(run.assurance_document_id)
                document_created = _aware(document.created_at) if document is not None else None
                if document_created is not None and completed_at is not None:
                    upload_to_extraction.append(
                        max(0.0, (completed_at - document_created).total_seconds())
                    )

            reconciliation = list(
                session.scalars(
                    select(ReconciliationIssue).where(
                        ReconciliationIssue.organization_id == org_id
                    )
                ).all()
            )
            exceptions = list(
                session.scalars(
                    select(OperationalException).where(
                        OperationalException.organization_id == org_id
                    )
                ).all()
            )
            shipments = list(
                session.scalars(
                    select(Shipment).where(Shipment.organization_id == org_id)
                ).all()
            )
            audit_rows = list(
                session.scalars(
                    select(AuditLog).where(
                        AuditLog.organization_id == org_id,
                        AuditLog.action.in_(
                            (
                                AuditAction.ASSURANCE_REVIEW_CORRECT.value,
                                AuditAction.ASSURANCE_PILOT_BASELINE_SET.value,
                            )
                        ),
                    )
                ).all()
            )

            resolution_durations: list[float] = []
            for row in exceptions:
                created_at = _aware(row.created_at)
                resolved_at = _aware(row.resolved_at)
                if created_at is not None and resolved_at is not None:
                    resolution_durations.append(
                        max(0.0, (resolved_at - created_at).total_seconds())
                    )

            metrics = AssurancePilotMetrics(
                processing_seconds=round(sum(processing_durations), 3),
                fields_detected=len(fields),
                fields_auto_accepted=sum(1 for row in fields if row.auto_accepted),
                fields_manually_reviewed=sum(
                    1 for row in fields if not row.auto_accepted and not row.needs_review
                ),
                fields_manually_changed=_unique_manually_changed_fields(audit_rows),
                reconciliation_issues=len(reconciliation),
                blocking_issues=sum(
                    1 for row in reconciliation if str(row.severity).upper() == "BLOCKING"
                ),
            )
            average_upload = round(mean(upload_to_extraction), 3) if upload_to_extraction else 0.0

            baseline_rows = [
                row
                for row in audit_rows
                if row.action == AuditAction.ASSURANCE_PILOT_BASELINE_SET.value
            ]
            baseline_rows.sort(
                key=lambda row: (_aware(row.timestamp) or datetime.min.replace(tzinfo=timezone.utc), row.id),
                reverse=True,
            )
            baseline_seconds: float | None = None
            baseline_label: str | None = None
            baseline_recorded_at: str | None = None
            if baseline_rows:
                latest_baseline = baseline_rows[0]
                state = _state_after(latest_baseline)
                try:
                    baseline_seconds = float(state.get("manual_baseline_seconds"))
                except (TypeError, ValueError):
                    baseline_seconds = None
                raw_label = state.get("label")
                baseline_label = str(raw_label).strip() if raw_label else None
                recorded = _aware(latest_baseline.timestamp)
                baseline_recorded_at = recorded.isoformat() if recorded is not None else None

            comparison_available = bool(
                baseline_seconds is not None and baseline_seconds > 0 and upload_to_extraction
            )
            reduction = (
                review_time_reduction_percentage(
                    baseline_seconds=baseline_seconds or 0.0,
                    assurance_seconds=average_upload,
                )
                if comparison_available
                else None
            )
            manual_vs_lt = AssuranceManualVsLtReport(
                available=comparison_available,
                comparison_scope=_COMPARISON_SCOPE,
                manual_baseline_seconds=baseline_seconds,
                lt_average_seconds=average_upload,
                time_reduction_percentage=reduction,
                target_reduction_percentage=REVIEW_TIME_REDUCTION_TARGET_PERCENTAGE,
                target_met=(
                    reduction >= REVIEW_TIME_REDUCTION_TARGET_PERCENTAGE
                    if reduction is not None
                    else None
                ),
                baseline_label=baseline_label,
                baseline_recorded_at=baseline_recorded_at,
            )

            return AssuranceMetricsSnapshot(
                metrics=metrics,
                completed_documents=len(upload_to_extraction),
                average_upload_to_extraction_seconds=average_upload,
                average_exception_resolution_seconds=(
                    round(mean(resolution_durations), 3) if resolution_durations else 0.0
                ),
                resolved_exception_count=len(resolution_durations),
                open_reconciliation_issue_count=sum(
                    1 for row in reconciliation if str(row.status).upper() == "OPEN"
                ),
                blocking_exceptions_before_dispatch=_blocking_before_dispatch(
                    exceptions=exceptions,
                    shipments=shipments,
                ),
                zero_friction_target_percentage=ZERO_FRICTION_TARGET_PERCENTAGE,
                zero_friction_target_met=metrics.meets_zero_friction_target(),
                manual_vs_lt=manual_vs_lt,
            )
        except AssuranceMetricsError:
            raise
        except Exception as exc:
            raise AssuranceMetricsError("No se pudieron calcular las métricas Assurance.") from exc
        finally:
            session.close()
