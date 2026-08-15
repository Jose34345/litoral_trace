"""Tenant-safe atomic PostgreSQL batch import for validated lotes."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from litoral_trace.db.engine import get_db_session
from litoral_trace.db.models import Lote
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.services.audit import (
    AuditAction,
    AuditActor,
    AuditOutcome,
    AuditRequestContext,
    record_audit_event,
)
from litoral_trace.services.batch import (
    BatchCanonicalRow,
    BatchSemanticValidationError,
    BatchValidationResult,
    BatchWorkbook,
    validar_filas_lotes,
)


class BatchImportError(RuntimeError):
    """Base class for safe batch persistence errors."""


class BatchImportAuthorizationError(BatchImportError):
    """Raised when the trusted actor and requested tenant do not match."""


class BatchImportConflictError(BatchImportError):
    """Raised when one or more lot identifiers already exist in the tenant."""

    def __init__(
        self,
        identifiers: tuple[str, ...],
    ) -> None:
        self.identifiers = identifiers
        super().__init__(
            "La importación contiene identificadores de lote ya existentes."
        )


class BatchImportPersistenceError(BatchImportError):
    """Sanitized persistence failure; never exposes raw DB exception details."""


@dataclass(frozen=True)
class BatchImportResult:
    """Committed import result."""

    organization_id: int
    total_rows: int
    inserted_rows: int
    lote_ids: tuple[int, ...]
    identifiers: tuple[str, ...]


SessionFactory = Callable[[], Session | None]


def _normalize_organization_id(
    organization_id: int | str,
) -> int:
    try:
        normalized = int(organization_id)
    except (TypeError, ValueError) as exc:
        raise BatchImportAuthorizationError(
            "El tenant de importación no es válido."
        ) from exc

    if normalized <= 0:
        raise BatchImportAuthorizationError(
            "El tenant de importación no es válido."
        )

    return normalized


def _default_polygon_wkt(
    *,
    latitud: float,
    longitud: float,
) -> str:
    """
    Build the deterministic compatibility polygon from validated coordinates.

    Coordinates are never defaulted here. The caller receives only canonical
    P2.4B rows whose latitude/longitude already passed semantic validation.
    """

    delta = 0.01

    return (
        "POLYGON(("
        f"{longitud - delta} {latitud - delta}, "
        f"{longitud + delta} {latitud - delta}, "
        f"{longitud + delta} {latitud + delta}, "
        f"{longitud - delta} {latitud + delta}, "
        f"{longitud - delta} {latitud - delta}"
        "))"
    )


def _lote_audit_state(
    lote: Lote,
) -> dict[str, Any]:
    return {
        "identificador": lote.identificador,
        "estatus": lote.estatus,
        "producto_forestal": lote.producto_forestal,
        "volumen_ingresado_ton": lote.volumen_ingresado_ton,
        "volumen_exportar_ton": lote.volumen_exportar_ton,
    }


def _build_lote(
    *,
    organization_id: int,
    row: BatchCanonicalRow,
) -> Lote:
    return Lote(
        organization_id=organization_id,
        identificador=row.identificador,
        productor_id=row.productor_id,
        producto_forestal=row.producto_forestal,
        hectareas=row.hectareas,
        latitud=row.latitud,
        longitud=row.longitud,
        polygon_wkt=_default_polygon_wkt(
            latitud=row.latitud,
            longitud=row.longitud,
        ),
        estatus="Pendiente",
        volumen_ingresado_ton=row.volumen_ingresado_ton,
        volumen_exportar_ton=row.volumen_exportar_ton,
    )


class BatchImportService:
    """
    Persist a fully validated workbook atomically inside one tenant transaction.

    P2.4C intentionally does not implement persistent idempotency. P2.4D owns
    that contract. This service performs no generic DB retries.
    """

    def __init__(
        self,
        *,
        session_factory: SessionFactory | None = None,
    ) -> None:
        self._session_factory = session_factory or get_db_session

    def _validate_actor_scope(
        self,
        *,
        actor: AuditActor,
        organization_id: int,
    ) -> None:
        if int(actor.organization_id) != organization_id:
            raise BatchImportAuthorizationError(
                "El actor autenticado no pertenece al tenant de importación."
            )

    def _find_existing_identifiers(
        self,
        session: Session,
        *,
        organization_id: int,
        rows: tuple[BatchCanonicalRow, ...],
    ) -> tuple[str, ...]:
        """
        Detect existing identifiers inside the active tenant before inserts.

        Case-insensitive lookup matches P2.4B's in-file duplicate policy. A
        database-level uniqueness constraint is deliberately deferred to P2.4D,
        where it will be introduced together with persistent import identity.
        """

        if not rows:
            return ()

        lookup_values = sorted(
            {
                row.identificador.lower()
                for row in rows
            }
        )

        result = session.execute(
            select(Lote.identificador).where(
                Lote.organization_id == organization_id,
                func.lower(Lote.identificador).in_(lookup_values),
            )
        )

        existing = sorted(
            {
                str(value)
                for value in result.scalars().all()
            },
            key=str.casefold,
        )

        return tuple(existing)

    def import_workbook(
        self,
        workbook: BatchWorkbook,
        *,
        organization_id: int | str,
        actor: AuditActor,
        request_context: AuditRequestContext | None = None,
    ) -> BatchImportResult:
        """
        Validate and atomically persist every row of a safe workbook.

        Invariants:
        - semantic validation occurs before any DB session/write;
        - organization_id never comes from workbook data;
        - one transaction contains duplicate preflight, all INSERTs and audit;
        - any DB/audit failure rolls the whole transaction back;
        - no partial successful import is returned.
        """

        normalized_org_id = _normalize_organization_id(
            organization_id
        )
        self._validate_actor_scope(
            actor=actor,
            organization_id=normalized_org_id,
        )

        validation = validar_filas_lotes(
            workbook
        )

        if not validation.valid:
            raise BatchSemanticValidationError(
                validation
            )

        return self.import_validated(
            validation,
            organization_id=normalized_org_id,
            actor=actor,
            request_context=request_context,
            source_filename=workbook.filename,
            source_sha256=workbook.sha256,
        )

    def import_validated(
        self,
        validation: BatchValidationResult,
        *,
        organization_id: int | str,
        actor: AuditActor,
        request_context: AuditRequestContext | None = None,
        source_filename: str | None = None,
        source_sha256: str | None = None,
    ) -> BatchImportResult:
        """
        Persist an already validated result atomically.

        This method is useful for the later preview/import API, but still fails
        closed if the supplied validation result is not globally valid.
        """

        normalized_org_id = _normalize_organization_id(
            organization_id
        )
        self._validate_actor_scope(
            actor=actor,
            organization_id=normalized_org_id,
        )

        if (
            not validation.valid
            or validation.invalid_rows != 0
            or validation.valid_rows != validation.total_rows
        ):
            raise BatchSemanticValidationError(
                validation
            )

        canonical_rows = validation.canonical_rows

        if len(canonical_rows) != validation.total_rows:
            raise BatchImportPersistenceError(
                "La importación validada no contiene todas sus filas canónicas."
            )

        session = self._session_factory()
        if session is None:
            raise BatchImportPersistenceError(
                "Servicio de base de datos no disponible."
            )

        try:
            set_tenant_db_context(
                session,
                normalized_org_id,
            )

            existing = self._find_existing_identifiers(
                session,
                organization_id=normalized_org_id,
                rows=canonical_rows,
            )

            if existing:
                session.rollback()
                raise BatchImportConflictError(
                    existing
                )

            lotes = [
                _build_lote(
                    organization_id=normalized_org_id,
                    row=row,
                )
                for row in canonical_rows
            ]

            session.add_all(
                lotes
            )
            session.flush()

            for row, lote in zip(
                canonical_rows,
                lotes,
                strict=True,
            ):
                record_audit_event(
                    session,
                    actor=actor,
                    action=AuditAction.LOTE_CREATE,
                    entity_type="lote",
                    entity_id=lote.id,
                    outcome=AuditOutcome.SUCCESS,
                    request_context=request_context,
                    metadata={
                        "source": "batch_import",
                        "source_row": row.source_row,
                    },
                    after_data=_lote_audit_state(
                        lote
                    ),
                )

            record_audit_event(
                session,
                actor=actor,
                action=AuditAction.LOTE_BATCH_UPLOAD,
                entity_type="lote_batch_import",
                outcome=AuditOutcome.SUCCESS,
                request_context=request_context,
                metadata={
                    "source_filename": source_filename,
                    "source_sha256": source_sha256,
                    "row_count": validation.total_rows,
                    "inserted_rows": len(lotes),
                },
            )

            session.commit()

            lote_ids = tuple(
                int(lote.id)
                for lote in lotes
            )
            identifiers = tuple(
                lote.identificador
                for lote in lotes
            )

            return BatchImportResult(
                organization_id=normalized_org_id,
                total_rows=validation.total_rows,
                inserted_rows=len(lotes),
                lote_ids=lote_ids,
                identifiers=identifiers,
            )

        except (
            BatchImportConflictError,
            BatchSemanticValidationError,
        ):
            session.rollback()
            raise

        except SQLAlchemyError as exc:
            session.rollback()
            raise BatchImportPersistenceError(
                "No fue posible persistir la importación de lotes."
            ) from exc

        except Exception as exc:
            session.rollback()
            raise BatchImportPersistenceError(
                "No fue posible completar la importación de lotes."
            ) from exc

        finally:
            session.close()