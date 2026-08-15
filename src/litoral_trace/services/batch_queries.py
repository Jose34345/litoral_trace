"""Read-only tenant-scoped queries for persistent batch imports."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from litoral_trace.db.engine import get_db_session
from litoral_trace.db.models import BatchImport
from litoral_trace.db.tenant import set_tenant_db_context


class BatchImportQueryError(RuntimeError):
    """Sanitized read-side failure for batch import metadata."""


@dataclass(frozen=True)
class BatchImportSnapshot:
    """Safe public projection of one persistent batch import."""

    organization_id: int
    public_id: UUID
    status: str
    source_filename: str
    source_sha256: str
    total_rows: int
    inserted_rows: int
    lote_ids: tuple[int, ...]
    identifiers: tuple[str, ...]
    created_at: datetime
    completed_at: datetime | None


SessionFactory = Callable[[], Session | None]


def _normalize_organization_id(
    organization_id: int | str,
) -> int:
    try:
        normalized = int(
            organization_id
        )
    except (TypeError, ValueError) as exc:
        raise BatchImportQueryError(
            "Tenant de consulta inválido."
        ) from exc

    if normalized <= 0:
        raise BatchImportQueryError(
            "Tenant de consulta inválido."
        )

    return normalized


def _snapshot_from_record(
    record: BatchImport,
) -> BatchImportSnapshot:
    if not isinstance(
        record.lote_ids,
        list,
    ) or not isinstance(
        record.identifiers,
        list,
    ):
        raise BatchImportQueryError(
            "Resultado persistido inválido."
        )

    if (
        len(record.lote_ids)
        != record.inserted_rows
        or len(record.identifiers)
        != record.inserted_rows
    ):
        raise BatchImportQueryError(
            "Resultado persistido inválido."
        )

    try:
        lote_ids = tuple(
            int(value)
            for value in record.lote_ids
        )
    except (TypeError, ValueError) as exc:
        raise BatchImportQueryError(
            "Resultado persistido inválido."
        ) from exc

    identifiers = tuple(
        str(value)
        for value in record.identifiers
    )

    return BatchImportSnapshot(
        organization_id=int(
            record.organization_id
        ),
        public_id=record.public_id,
        status=str(
            record.status
        ),
        source_filename=str(
            record.source_filename
        ),
        source_sha256=str(
            record.source_sha256
        ),
        total_rows=int(
            record.total_rows
        ),
        inserted_rows=int(
            record.inserted_rows
        ),
        lote_ids=lote_ids,
        identifiers=identifiers,
        created_at=record.created_at,
        completed_at=record.completed_at,
    )


class BatchImportQueryService:
    """Tenant-safe read model for batch import status/result."""

    def __init__(
        self,
        *,
        session_factory: SessionFactory | None = None,
    ) -> None:
        self._session_factory = (
            session_factory
            or get_db_session
        )

    def get_by_public_id(
        self,
        *,
        organization_id: int | str,
        public_id: UUID,
    ) -> BatchImportSnapshot | None:
        normalized_org_id = (
            _normalize_organization_id(
                organization_id
            )
        )

        session = self._session_factory()

        if session is None:
            raise BatchImportQueryError(
                "Servicio de base de datos no disponible."
            )

        try:
            set_tenant_db_context(
                session,
                normalized_org_id,
            )

            record = session.execute(
                select(BatchImport).where(
                    BatchImport.organization_id
                    == normalized_org_id,
                    BatchImport.public_id
                    == public_id,
                )
            ).scalar_one_or_none()

            if record is None:
                return None

            return _snapshot_from_record(
                record
            )

        except BatchImportQueryError:
            raise

        except SQLAlchemyError as exc:
            raise BatchImportQueryError(
                "No fue posible consultar la importación."
            ) from exc

        finally:
            session.close()