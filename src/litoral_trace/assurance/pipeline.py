"""Persist completion metadata for the Assurance document pipeline.

Processing and reconciliation run sequentially in a background task.  The
processing status can become terminal before reconciliation has finished, so
the workspace needs an explicit pipeline-completed marker before it renders
final discrepancies or launches Preflight.
"""
from __future__ import annotations

from typing import Callable, Mapping
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from litoral_trace.db.engine import get_db_session
from litoral_trace.db.models import AssuranceDocument, DocumentExtractionRun
from litoral_trace.db.tenant import set_tenant_db_context


SessionFactory = Callable[[], Session | None]


class AssurancePipelineError(RuntimeError):
    pass


def mark_pipeline_completed(
    *,
    organization_id: int,
    assurance_public_id: UUID | str,
    metadata: Mapping[str, object] | None = None,
    session_factory: SessionFactory | None = None,
) -> None:
    """Mark the latest extraction run as fully reconciled and ready for UX use."""
    org_id = int(organization_id)
    public_id = (
        assurance_public_id
        if isinstance(assurance_public_id, UUID)
        else UUID(str(assurance_public_id))
    )
    factory = session_factory or get_db_session
    session = factory()
    if session is None:
        raise AssurancePipelineError("No se pudo abrir una sesión para cerrar el pipeline Assurance.")
    set_tenant_db_context(session, org_id)
    try:
        document = session.scalar(
            select(AssuranceDocument).where(
                AssuranceDocument.organization_id == org_id,
                AssuranceDocument.public_id == public_id,
            )
        )
        if document is None:
            raise AssurancePipelineError("Documento Assurance no encontrado.")
        latest_run = session.scalar(
            select(DocumentExtractionRun)
            .where(
                DocumentExtractionRun.organization_id == org_id,
                DocumentExtractionRun.assurance_document_id == document.id,
            )
            .order_by(DocumentExtractionRun.id.desc())
        )
        if latest_run is None:
            raise AssurancePipelineError("El documento no tiene una corrida de extracción.")
        merged = dict(latest_run.extraction_metadata or {})
        merged.update(dict(metadata or {}))
        merged["pipeline_completed"] = True
        latest_run.extraction_metadata = merged
        session.commit()
    except (ValueError, AssurancePipelineError):
        session.rollback()
        raise
    except Exception as exc:
        session.rollback()
        raise AssurancePipelineError("No se pudo cerrar el pipeline Assurance.") from exc
    finally:
        session.close()
