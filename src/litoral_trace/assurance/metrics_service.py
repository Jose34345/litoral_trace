"""Persisted pilot metrics for the Assurance v1 workflow."""
from __future__ import annotations

from dataclasses import dataclass
from statistics import mean
from typing import Callable

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from litoral_trace.assurance.metrics import AssurancePilotMetrics, ZERO_FRICTION_TARGET_PERCENTAGE
from litoral_trace.db.engine import get_db_session
from litoral_trace.db.models import (
    AssuranceDocument,
    DocumentExtractionRun,
    ExtractedDocumentField,
    OperationalException,
    ReconciliationIssue,
)
from litoral_trace.db.tenant import set_tenant_db_context


SessionFactory = Callable[[], Session | None]


class AssuranceMetricsError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class AssuranceMetricsSnapshot:
    metrics: AssurancePilotMetrics
    completed_documents: int
    average_upload_to_extraction_seconds: float
    average_exception_resolution_seconds: float
    resolved_exception_count: int
    zero_friction_target_percentage: float
    zero_friction_target_met: bool

    def as_dict(self) -> dict[str, object]:
        return {
            **self.metrics.as_dict(),
            "completed_documents": self.completed_documents,
            "average_upload_to_extraction_seconds": self.average_upload_to_extraction_seconds,
            "average_exception_resolution_seconds": self.average_exception_resolution_seconds,
            "resolved_exception_count": self.resolved_exception_count,
            "zero_friction_target_percentage": self.zero_friction_target_percentage,
            "zero_friction_target_met": self.zero_friction_target_met,
        }


class AssuranceMetricsService:
    def __init__(self, *, session_factory: SessionFactory | None = None) -> None:
        self._session_factory = session_factory or get_db_session

    def _new_session(self, organization_id: int) -> Session:
        session = self._session_factory()
        if session is None:
            raise AssuranceMetricsError("No se pudo abrir una sesión para métricas Assurance.")
        set_tenant_db_context(session, int(organization_id))
        return session

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

            runs = []
            fields = []
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
                if run.started_at is not None and run.completed_at is not None:
                    processing_durations.append(
                        max(0.0, (run.completed_at - run.started_at).total_seconds())
                    )
                document = documents.get(run.assurance_document_id)
                if (
                    document is not None
                    and document.created_at is not None
                    and run.completed_at is not None
                ):
                    upload_to_extraction.append(
                        max(0.0, (run.completed_at - document.created_at).total_seconds())
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
            resolution_durations = [
                max(0.0, (row.resolved_at - row.created_at).total_seconds())
                for row in exceptions
                if row.created_at is not None and row.resolved_at is not None
            ]

            metrics = AssurancePilotMetrics(
                processing_seconds=round(sum(processing_durations), 3),
                fields_detected=len(fields),
                fields_auto_accepted=sum(1 for row in fields if row.auto_accepted),
                fields_manually_reviewed=sum(
                    1 for row in fields if not row.auto_accepted and not row.needs_review
                ),
                # Human value correction is intentionally not inferred from
                # approval. It stays zero until a correction event exists.
                fields_manually_changed=0,
                reconciliation_issues=len(reconciliation),
                blocking_issues=sum(
                    1 for row in reconciliation if str(row.severity).upper() == "BLOCKING"
                ),
            )
            return AssuranceMetricsSnapshot(
                metrics=metrics,
                completed_documents=len(upload_to_extraction),
                average_upload_to_extraction_seconds=(
                    round(mean(upload_to_extraction), 3) if upload_to_extraction else 0.0
                ),
                average_exception_resolution_seconds=(
                    round(mean(resolution_durations), 3) if resolution_durations else 0.0
                ),
                resolved_exception_count=len(resolution_durations),
                zero_friction_target_percentage=ZERO_FRICTION_TARGET_PERCENTAGE,
                zero_friction_target_met=metrics.meets_zero_friction_target(),
            )
        except AssuranceMetricsError:
            raise
        except Exception as exc:
            raise AssuranceMetricsError("No se pudieron calcular las métricas Assurance.") from exc
        finally:
            session.close()
