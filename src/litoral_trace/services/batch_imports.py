"""Tenant-safe atomic PostgreSQL batch import with persistent idempotency."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
import re
import unicodedata
from typing import Any
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session

from litoral_trace.db.engine import get_db_session
from litoral_trace.db.models import BatchImport, Lote
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
    normalizar_nombre_archivo_batch,
    validar_filas_lotes,
)


IDEMPOTENCY_KEY_MAX_LENGTH = 255
LOTE_TENANT_IDENTIFIER_CONSTRAINT = "uq_lotes_tenant_identificador_ci"
BATCH_IMPORT_IDEMPOTENCY_CONSTRAINT = (
    "uq_batch_imports_tenant_idempotency_key"
)
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_CONTROL_CHARACTER_RE = re.compile(r"[\x00-\x1f\x7f]")


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


class BatchImportIdempotencyConflictError(BatchImportError):
    """Raised when one tenant reuses a key for a different source workbook."""

    def __init__(
        self,
        import_public_id: UUID | None = None,
    ) -> None:
        self.import_public_id = import_public_id
        super().__init__(
            "La clave de idempotencia ya fue utilizada para otro archivo."
        )


class BatchImportPersistenceError(BatchImportError):
    """Sanitized persistence failure; never exposes raw DB exception details."""


@dataclass(frozen=True)
class BatchImportResult:
    """Committed or replayed import result."""

    organization_id: int
    total_rows: int
    inserted_rows: int
    lote_ids: tuple[int, ...]
    identifiers: tuple[str, ...]
    import_public_id: UUID | None = None
    replayed: bool = False


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


def normalize_idempotency_key(
    idempotency_key: str | None,
) -> str | None:
    """Normalize an optional idempotency key without ever logging it."""

    if idempotency_key is None:
        return None

    if not isinstance(idempotency_key, str):
        raise BatchImportIdempotencyConflictError()

    normalized = unicodedata.normalize(
        "NFKC",
        idempotency_key,
    ).strip()

    if (
        not normalized
        or len(normalized) > IDEMPOTENCY_KEY_MAX_LENGTH
        or _CONTROL_CHARACTER_RE.search(normalized)
    ):
        raise BatchImportIdempotencyConflictError()

    return normalized


def _normalize_source_identity(
    *,
    source_filename: str | None,
    source_sha256: str | None,
) -> tuple[str, str]:
    if not source_filename or not source_sha256:
        raise BatchImportPersistenceError(
            "La importación idempotente requiere identidad de archivo."
        )

    safe_filename = normalizar_nombre_archivo_batch(
        source_filename
    )
    normalized_sha = str(source_sha256).strip().lower()

    if not _SHA256_RE.fullmatch(normalized_sha):
        raise BatchImportPersistenceError(
            "La huella SHA-256 de la importación no es válida."
        )

    return safe_filename, normalized_sha


def _default_polygon_wkt(
    *,
    latitud: float,
    longitud: float,
) -> str:
    """Build the deterministic compatibility polygon from validated coordinates."""

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


def _constraint_name(
    exc: IntegrityError,
) -> str | None:
    original = getattr(exc, "orig", None)
    diagnostic = getattr(original, "diag", None)
    name = getattr(diagnostic, "constraint_name", None)

    if not name:
        return None

    return str(name)


class BatchImportService:
    """
    Persist validated XLSX rows atomically with optional persistent idempotency.

    A keyed request first claims `(organization_id, idempotency_key)` inside the
    same transaction as all lote inserts and audit rows. Competing requests do
    not use generic retries: a unique-key collision performs one targeted read
    of the winner and either replays that committed result or returns conflict.
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

        return tuple(
            sorted(
                {
                    str(value)
                    for value in result.scalars().all()
                },
                key=str.casefold,
            )
        )

    def _get_existing_import(
        self,
        session: Session,
        *,
        organization_id: int,
        idempotency_key: str,
    ) -> BatchImport | None:
        return session.execute(
            select(BatchImport).where(
                BatchImport.organization_id == organization_id,
                BatchImport.idempotency_key == idempotency_key,
            )
        ).scalar_one_or_none()

    def _result_from_existing(
        self,
        record: BatchImport,
        *,
        source_sha256: str,
    ) -> BatchImportResult:
        if record.source_sha256 != source_sha256:
            raise BatchImportIdempotencyConflictError(
                record.public_id
            )

        if (
            record.status != "completed"
            or record.completed_at is None
            or record.inserted_rows != record.total_rows
        ):
            raise BatchImportPersistenceError(
                "La importación idempotente no está en un estado recuperable."
            )

        raw_lote_ids = record.lote_ids
        raw_identifiers = record.identifiers

        if (
            not isinstance(raw_lote_ids, list)
            or not isinstance(raw_identifiers, list)
            or len(raw_lote_ids) != record.inserted_rows
            or len(raw_identifiers) != record.inserted_rows
        ):
            raise BatchImportPersistenceError(
                "El resultado persistido de la importación no es válido."
            )

        try:
            lote_ids = tuple(
                int(value)
                for value in raw_lote_ids
            )
        except (TypeError, ValueError) as exc:
            raise BatchImportPersistenceError(
                "El resultado persistido de la importación no es válido."
            ) from exc

        identifiers = tuple(
            str(value)
            for value in raw_identifiers
        )

        return BatchImportResult(
            organization_id=int(record.organization_id),
            total_rows=int(record.total_rows),
            inserted_rows=int(record.inserted_rows),
            lote_ids=lote_ids,
            identifiers=identifiers,
            import_public_id=record.public_id,
            replayed=True,
        )

    def _recover_claim_collision(
        self,
        session: Session,
        *,
        organization_id: int,
        idempotency_key: str,
        source_sha256: str,
    ) -> BatchImportResult:
        """
        Targeted concurrency recovery after one unique idempotency collision.

        PostgreSQL only reports the unique collision after the competing
        transaction resolves, so the committed winner is visible in this new
        transaction under the normal READ COMMITTED isolation level.
        """

        set_tenant_db_context(
            session,
            organization_id,
        )

        existing = self._get_existing_import(
            session,
            organization_id=organization_id,
            idempotency_key=idempotency_key,
        )

        if existing is None:
            raise BatchImportPersistenceError(
                "No fue posible recuperar la importación idempotente."
            )

        return self._result_from_existing(
            existing,
            source_sha256=source_sha256,
        )

    def import_workbook(
        self,
        workbook: BatchWorkbook,
        *,
        organization_id: int | str,
        actor: AuditActor,
        request_context: AuditRequestContext | None = None,
        idempotency_key: str | None = None,
    ) -> BatchImportResult:
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
            idempotency_key=idempotency_key,
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
        idempotency_key: str | None = None,
    ) -> BatchImportResult:
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

        normalized_key = normalize_idempotency_key(
            idempotency_key
        )

        safe_filename: str | None = None
        normalized_sha: str | None = None

        if normalized_key is not None:
            safe_filename, normalized_sha = _normalize_source_identity(
                source_filename=source_filename,
                source_sha256=source_sha256,
            )

        session = self._session_factory()
        if session is None:
            raise BatchImportPersistenceError(
                "Servicio de base de datos no disponible."
            )

        import_record: BatchImport | None = None

        try:
            set_tenant_db_context(
                session,
                normalized_org_id,
            )

            if normalized_key is not None:
                assert safe_filename is not None
                assert normalized_sha is not None

                existing_import = self._get_existing_import(
                    session,
                    organization_id=normalized_org_id,
                    idempotency_key=normalized_key,
                )

                if existing_import is not None:
                    return self._result_from_existing(
                        existing_import,
                        source_sha256=normalized_sha,
                    )

                import_record = BatchImport(
                    organization_id=normalized_org_id,
                    created_by_user_id=actor.user_id,
                    idempotency_key=normalized_key,
                    source_sha256=normalized_sha,
                    source_filename=safe_filename,
                    status="processing",
                    total_rows=validation.total_rows,
                    inserted_rows=0,
                    lote_ids=[],
                    identifiers=[
                        row.identificador
                        for row in canonical_rows
                    ],
                )
                session.add(
                    import_record
                )

                try:
                    session.flush(
                        [import_record]
                    )
                except IntegrityError as exc:
                    constraint = _constraint_name(
                        exc
                    )
                    session.rollback()

                    if (
                        constraint
                        not in {
                            None,
                            BATCH_IMPORT_IDEMPOTENCY_CONSTRAINT,
                        }
                    ):
                        raise BatchImportPersistenceError(
                            "No fue posible reservar la importación idempotente."
                        ) from exc

                    return self._recover_claim_collision(
                        session,
                        organization_id=normalized_org_id,
                        idempotency_key=normalized_key,
                        source_sha256=normalized_sha,
                    )

            existing = self._find_existing_identifiers(
                session,
                organization_id=normalized_org_id,
                rows=canonical_rows,
            )

            if existing:
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

            try:
                session.flush()
            except IntegrityError as exc:
                constraint = _constraint_name(
                    exc
                )
                session.rollback()

                if constraint == LOTE_TENANT_IDENTIFIER_CONSTRAINT:
                    raise BatchImportConflictError(
                        tuple(
                            row.identificador
                            for row in canonical_rows
                        )
                    ) from exc

                raise BatchImportPersistenceError(
                    "No fue posible persistir la importación de lotes."
                ) from exc

            lote_ids = tuple(
                int(lote.id)
                for lote in lotes
            )
            identifiers = tuple(
                lote.identificador
                for lote in lotes
            )
            import_public_id: UUID | None = None

            if import_record is not None:
                import_record.status = "completed"
                import_record.inserted_rows = len(lotes)
                import_record.lote_ids = list(
                    lote_ids
                )
                import_record.identifiers = list(
                    identifiers
                )
                import_record.completed_at = datetime.now(
                    timezone.utc
                )
                import_public_id = import_record.public_id

            for row, lote in zip(
                canonical_rows,
                lotes,
                strict=True,
            ):
                metadata: dict[str, Any] = {
                    "source": "batch_import",
                    "source_row": row.source_row,
                }

                if import_public_id is not None:
                    metadata[
                        "batch_import_public_id"
                    ] = str(import_public_id)

                record_audit_event(
                    session,
                    actor=actor,
                    action=AuditAction.LOTE_CREATE,
                    entity_type="lote",
                    entity_id=lote.id,
                    outcome=AuditOutcome.SUCCESS,
                    request_context=request_context,
                    metadata=metadata,
                    after_data=_lote_audit_state(
                        lote
                    ),
                )

            summary_metadata: dict[str, Any] = {
                "source_filename": source_filename,
                "source_sha256": source_sha256,
                "row_count": validation.total_rows,
                "inserted_rows": len(lotes),
            }

            if import_public_id is not None:
                summary_metadata[
                    "batch_import_public_id"
                ] = str(import_public_id)

            record_audit_event(
                session,
                actor=actor,
                action=AuditAction.LOTE_BATCH_UPLOAD,
                entity_type="lote_batch_import",
                entity_id=(
                    import_record.id
                    if import_record is not None
                    else None
                ),
                outcome=AuditOutcome.SUCCESS,
                request_context=request_context,
                metadata=summary_metadata,
            )

            session.commit()

            return BatchImportResult(
                organization_id=normalized_org_id,
                total_rows=validation.total_rows,
                inserted_rows=len(lotes),
                lote_ids=lote_ids,
                identifiers=identifiers,
                import_public_id=import_public_id,
                replayed=False,
            )

        except BatchImportError:
            session.rollback()
            raise

        except BatchSemanticValidationError:
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
