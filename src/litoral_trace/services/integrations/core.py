"""Tenant-safe integration core and first vendor-neutral ERP bridge.

External systems never mutate the traceability ledger from this service. They
only stage source records and create explicit reconciliation links after a
human/operator decision or a future reviewed mapping policy.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import re
from typing import Any, Iterable
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session

from litoral_trace.db.models import (
    ExternalEntity,
    ExternalEntityVersion,
    ExternalReference,
    IntegrationConnection,
    IntegrationEvent,
    IntegrationSyncRun,
    Lote,
    Shipment,
    TraceabilityBatch,
    TraceabilityEvent,
)
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.services.integrations.canonical import GenericErpPayload


_SECRET_REF_RE = re.compile(r"^[A-Za-z0-9_.:/-]{1,255}$")
_FORBIDDEN_CONFIG_FRAGMENTS = (
    "password",
    "secret",
    "token",
    "credential",
    "private_key",
    "api_key",
    "database_url",
)


class IntegrationError(RuntimeError):
    """Safe integration-domain error."""


class IntegrationValidationError(IntegrationError):
    def __init__(self, code: str, detail: str) -> None:
        self.code = code
        self.detail = detail
        super().__init__(detail)


class IntegrationNotFoundError(IntegrationError):
    pass


class IntegrationConflictError(IntegrationError):
    pass


class IntegrationPersistenceError(IntegrationError):
    pass


@dataclass(frozen=True)
class SyncResult:
    public_id: UUID
    status: str
    records_seen: int
    records_created: int
    records_updated: int
    records_unchanged: int
    records_conflict: int
    replayed: bool = False


@dataclass(frozen=True)
class IntegrationSnapshot:
    connections: tuple[IntegrationConnection, ...]
    sync_runs: tuple[IntegrationSyncRun, ...]
    entities: tuple[ExternalEntity, ...]
    references: tuple[ExternalReference, ...]


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _clean_text(value: Any, *, field: str, maximum: int, required: bool = True) -> str | None:
    normalized = str(value or "").strip()
    if required and not normalized:
        raise IntegrationValidationError(
            f"MISSING_{field.upper()}", f"El campo {field} es obligatorio."
        )
    if len(normalized) > maximum:
        raise IntegrationValidationError(
            f"{field.upper()}_TOO_LONG",
            f"El campo {field} supera el máximo de {maximum} caracteres.",
        )
    return normalized or None


def _validate_secret_ref(value: Any) -> str | None:
    normalized = _clean_text(value, field="secret_ref", maximum=255, required=False)
    if normalized is None:
        return None
    if (
        not _SECRET_REF_RE.fullmatch(normalized)
        or "://" in normalized
        or "BEGIN" in normalized.upper()
        or "=" in normalized
    ):
        raise IntegrationValidationError(
            "INVALID_SECRET_REF",
            "secret_ref debe ser sólo una referencia al gestor de secretos, nunca la credencial.",
        )
    return normalized


def _validate_config(config: dict[str, Any] | None) -> dict[str, Any] | None:
    if not config:
        return None
    serialized_keys = " ".join(str(key).lower() for key in config.keys())
    if any(fragment in serialized_keys for fragment in _FORBIDDEN_CONFIG_FRAGMENTS):
        raise IntegrationValidationError(
            "SENSITIVE_CONFIG_REJECTED",
            "La configuración no puede contener secretos; use secret_ref.",
        )
    encoded = json.dumps(config, sort_keys=True, default=str)
    if len(encoded.encode("utf-8")) > 32_768:
        raise IntegrationValidationError(
            "CONFIG_TOO_LARGE",
            "La configuración de integración supera el máximo permitido.",
        )
    return config


def _canonical_json(value: dict[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _payload_hash(value: dict[str, Any]) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _idempotency_hash(value: str) -> str:
    normalized = _clean_text(value, field="idempotency_key", maximum=200)
    assert normalized is not None
    if len(normalized) < 16:
        raise IntegrationValidationError(
            "WEAK_IDEMPOTENCY_KEY",
            "La clave de idempotencia debe tener al menos 16 caracteres.",
        )
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _event(
    session: Session,
    *,
    organization_id: int,
    event_type: str,
    connection_id: int | None = None,
    sync_run_id: int | None = None,
    external_entity_id: int | None = None,
    actor_user_id: int | None = None,
    metadata: dict[str, Any] | None = None,
) -> IntegrationEvent:
    row = IntegrationEvent(
        organization_id=organization_id,
        connection_id=connection_id,
        sync_run_id=sync_run_id,
        external_entity_id=external_entity_id,
        actor_user_id=actor_user_id,
        event_type=event_type[:64],
        metadata_json=metadata,
    )
    session.add(row)
    return row


def _version(
    session: Session,
    *,
    organization_id: int,
    external_entity_id: int,
    sync_run_id: int,
    payload_hash: str,
    payload_json: dict[str, Any],
    normalized_json: dict[str, Any],
    source_updated_at: datetime | None,
) -> ExternalEntityVersion:
    row = ExternalEntityVersion(
        organization_id=organization_id,
        external_entity_id=external_entity_id,
        sync_run_id=sync_run_id,
        payload_hash=payload_hash,
        payload_json=payload_json,
        normalized_json=normalized_json,
        source_updated_at=source_updated_at,
    )
    session.add(row)
    return row


def _entity_rows(payload: GenericErpPayload) -> Iterable[tuple[str, Any]]:
    for item in payload.suppliers:
        yield "SUPPLIER", item
    for item in payload.products:
        yield "PRODUCT", item
    for item in payload.receipts:
        yield "RECEIPT", item
    for item in payload.shipments:
        yield "SHIPMENT", item


class IntegrationCoreService:
    """Core service used by API and browser integration workspaces."""

    def __init__(self, *, session: Session, organization_id: int) -> None:
        self.session = session
        self.organization_id = int(organization_id)
        set_tenant_db_context(self.session, self.organization_id)

    def create_connection(
        self,
        *,
        name: str,
        connector_type: str = "GENERIC_ERP",
        secret_ref: str | None = None,
        config_json: dict[str, Any] | None = None,
        actor_user_id: int | None = None,
    ) -> IntegrationConnection:
        normalized_name = _clean_text(name, field="name", maximum=160)
        normalized_type = str(connector_type or "").strip().upper()
        if normalized_type != "GENERIC_ERP":
            raise IntegrationValidationError(
                "UNSUPPORTED_CONNECTOR",
                "P1-A sólo habilita el conector GENERIC_ERP.",
            )
        normalized_secret_ref = _validate_secret_ref(secret_ref)
        normalized_config = _validate_config(config_json)

        connection = IntegrationConnection(
            organization_id=self.organization_id,
            name=normalized_name,
            connector_type=normalized_type,
            status="ACTIVE",
            secret_ref=normalized_secret_ref,
            config_json=normalized_config,
        )
        self.session.add(connection)
        try:
            self.session.flush()
            _event(
                self.session,
                organization_id=self.organization_id,
                event_type="CONNECTION_CREATED",
                connection_id=connection.id,
                actor_user_id=actor_user_id,
                metadata={"connector_type": normalized_type},
            )
            self.session.commit()
            return connection
        except IntegrityError as exc:
            self.session.rollback()
            raise IntegrationConflictError(
                "Ya existe una conexión con ese nombre para la organización."
            ) from exc
        except SQLAlchemyError as exc:
            self.session.rollback()
            raise IntegrationPersistenceError(
                "No fue posible crear la conexión de integración."
            ) from exc

    def set_connection_status(
        self,
        public_id: UUID,
        *,
        status: str,
        actor_user_id: int | None = None,
    ) -> IntegrationConnection:
        normalized = str(status or "").strip().upper()
        if normalized not in {"ACTIVE", "DISABLED"}:
            raise IntegrationValidationError(
                "INVALID_CONNECTION_STATUS", "El estado debe ser ACTIVE o DISABLED."
            )
        connection = self._connection(public_id, allow_disabled=True)
        connection.status = normalized
        _event(
            self.session,
            organization_id=self.organization_id,
            event_type="CONNECTION_STATUS_CHANGED",
            connection_id=connection.id,
            actor_user_id=actor_user_id,
            metadata={"status": normalized},
        )
        try:
            self.session.commit()
            return connection
        except SQLAlchemyError as exc:
            self.session.rollback()
            raise IntegrationPersistenceError(
                "No fue posible actualizar la conexión."
            ) from exc

    def stage_generic_erp(
        self,
        *,
        connection_public_id: UUID,
        payload: GenericErpPayload,
        idempotency_key: str,
        actor_user_id: int | None = None,
    ) -> SyncResult:
        """Stage external facts only; never post or dispatch the LT ledger."""
        connection = self._connection(connection_public_id)
        if connection.connector_type != "GENERIC_ERP":
            raise IntegrationValidationError(
                "INVALID_CONNECTOR_FOR_PAYLOAD",
                "La conexión seleccionada no acepta payload GENERIC_ERP.",
            )

        idempotency_key_hash = _idempotency_hash(idempotency_key)
        existing_run = self.session.scalar(
            select(IntegrationSyncRun).where(
                IntegrationSyncRun.organization_id == self.organization_id,
                IntegrationSyncRun.connection_id == connection.id,
                IntegrationSyncRun.idempotency_key_hash == idempotency_key_hash,
            )
        )
        if existing_run is not None:
            return SyncResult(
                public_id=existing_run.public_id,
                status=existing_run.status,
                records_seen=existing_run.records_seen,
                records_created=existing_run.records_created,
                records_updated=existing_run.records_updated,
                records_unchanged=existing_run.records_unchanged,
                records_conflict=existing_run.records_conflict,
                replayed=True,
            )

        run = IntegrationSyncRun(
            organization_id=self.organization_id,
            connection_id=connection.id,
            idempotency_key_hash=idempotency_key_hash,
            status="RUNNING",
            records_seen=payload.entity_count(),
        )
        self.session.add(run)
        created = updated = unchanged = conflict = 0

        try:
            self.session.flush()
            _event(
                self.session,
                organization_id=self.organization_id,
                connection_id=connection.id,
                sync_run_id=run.id,
                actor_user_id=actor_user_id,
                event_type="SYNC_STARTED",
                metadata={
                    "source_system": payload.source_system,
                    "records_seen": payload.entity_count(),
                },
            )

            for entity_type, item in _entity_rows(payload):
                raw = item.model_dump(mode="json")
                normalized = dict(raw)
                digest = _payload_hash(raw)
                external_id = str(raw["external_id"])
                existing = self.session.scalar(
                    select(ExternalEntity).where(
                        ExternalEntity.organization_id == self.organization_id,
                        ExternalEntity.connection_id == connection.id,
                        ExternalEntity.entity_type == entity_type,
                        ExternalEntity.external_id == external_id,
                    )
                )

                if existing is None:
                    entity = ExternalEntity(
                        organization_id=self.organization_id,
                        connection_id=connection.id,
                        last_sync_run_id=run.id,
                        entity_type=entity_type,
                        external_id=external_id,
                        payload_hash=digest,
                        payload_json=raw,
                        normalized_json=normalized,
                        status="STAGED",
                        source_updated_at=item.source_updated_at,
                    )
                    self.session.add(entity)
                    self.session.flush()
                    _version(
                        self.session,
                        organization_id=self.organization_id,
                        external_entity_id=entity.id,
                        sync_run_id=run.id,
                        payload_hash=digest,
                        payload_json=raw,
                        normalized_json=normalized,
                        source_updated_at=item.source_updated_at,
                    )
                    _event(
                        self.session,
                        organization_id=self.organization_id,
                        connection_id=connection.id,
                        sync_run_id=run.id,
                        external_entity_id=entity.id,
                        actor_user_id=actor_user_id,
                        event_type="ENTITY_STAGED",
                        metadata={"entity_type": entity_type, "external_id": external_id},
                    )
                    created += 1
                    continue

                existing.last_sync_run_id = run.id
                existing.source_updated_at = item.source_updated_at
                if existing.payload_hash == digest:
                    unchanged += 1
                    continue

                previous_hash = existing.payload_hash
                was_reconciled = existing.status == "RECONCILED"
                _version(
                    self.session,
                    organization_id=self.organization_id,
                    external_entity_id=existing.id,
                    sync_run_id=run.id,
                    payload_hash=digest,
                    payload_json=raw,
                    normalized_json=normalized,
                    source_updated_at=item.source_updated_at,
                )
                existing.payload_hash = digest
                existing.payload_json = raw
                existing.normalized_json = normalized
                if was_reconciled:
                    existing.status = "CONFLICT"
                    existing.conflict_reason = "SOURCE_CHANGED_AFTER_RECONCILIATION"
                    conflict += 1
                    event_type = "ENTITY_CONFLICT"
                else:
                    existing.status = "STAGED"
                    existing.conflict_reason = None
                    updated += 1
                    event_type = "ENTITY_UPDATED"
                _event(
                    self.session,
                    organization_id=self.organization_id,
                    connection_id=connection.id,
                    sync_run_id=run.id,
                    external_entity_id=existing.id,
                    actor_user_id=actor_user_id,
                    event_type=event_type,
                    metadata={
                        "entity_type": entity_type,
                        "external_id": external_id,
                        "previous_payload_hash": previous_hash,
                        "new_payload_hash": digest,
                    },
                )

            run.records_created = created
            run.records_updated = updated
            run.records_unchanged = unchanged
            run.records_conflict = conflict
            run.status = "PARTIAL" if conflict else "SUCCEEDED"
            run.finished_at = _now()
            _event(
                self.session,
                organization_id=self.organization_id,
                connection_id=connection.id,
                sync_run_id=run.id,
                actor_user_id=actor_user_id,
                event_type="SYNC_COMPLETED",
                metadata={
                    "status": run.status,
                    "created": created,
                    "updated": updated,
                    "unchanged": unchanged,
                    "conflict": conflict,
                },
            )
            self.session.commit()
        except IntegrityError as exc:
            self.session.rollback()
            raise IntegrationConflictError(
                "La sincronización ERP produjo una colisión de identidad externa."
            ) from exc
        except SQLAlchemyError as exc:
            self.session.rollback()
            raise IntegrationPersistenceError(
                "No fue posible persistir la sincronización ERP."
            ) from exc

        return SyncResult(
            public_id=run.public_id,
            status=run.status,
            records_seen=run.records_seen,
            records_created=created,
            records_updated=updated,
            records_unchanged=unchanged,
            records_conflict=conflict,
        )

    def reconcile_entity(
        self,
        *,
        entity_public_id: UUID,
        target_type: str,
        target_reference: str,
        user_id: int | None,
    ) -> ExternalReference:
        normalized_type = str(target_type or "").strip().upper()
        if normalized_type not in {
            "LOTE", "TRACEABILITY_BATCH", "TRACEABILITY_EVENT", "SHIPMENT"
        }:
            raise IntegrationValidationError(
                "INVALID_TARGET_TYPE",
                "El destino debe ser LOTE, TRACEABILITY_BATCH, TRACEABILITY_EVENT o SHIPMENT.",
            )
        normalized_reference = _clean_text(
            target_reference, field="target_reference", maximum=200
        )
        assert normalized_reference is not None

        entity = self.session.scalar(
            select(ExternalEntity).where(
                ExternalEntity.organization_id == self.organization_id,
                ExternalEntity.public_id == entity_public_id,
            )
        )
        if entity is None:
            raise IntegrationNotFoundError("La entidad externa no existe en este tenant.")

        self._assert_target_exists(normalized_type, normalized_reference)
        reference = self.session.scalar(
            select(ExternalReference).where(
                ExternalReference.organization_id == self.organization_id,
                ExternalReference.external_entity_id == entity.id,
            )
        )
        if reference is None:
            reference = ExternalReference(
                organization_id=self.organization_id,
                external_entity_id=entity.id,
                target_type=normalized_type,
                target_reference=normalized_reference,
                reconciled_by_user_id=user_id,
            )
            self.session.add(reference)
        else:
            reference.target_type = normalized_type
            reference.target_reference = normalized_reference
            reference.reconciled_by_user_id = user_id

        entity.status = "RECONCILED"
        entity.reconciled_at = _now()
        entity.conflict_reason = None
        _event(
            self.session,
            organization_id=self.organization_id,
            connection_id=entity.connection_id,
            sync_run_id=entity.last_sync_run_id,
            external_entity_id=entity.id,
            actor_user_id=user_id,
            event_type="ENTITY_RECONCILED",
            metadata={
                "target_type": normalized_type,
                "target_reference": normalized_reference,
            },
        )
        try:
            self.session.commit()
            return reference
        except SQLAlchemyError as exc:
            self.session.rollback()
            raise IntegrationPersistenceError(
                "No fue posible guardar la reconciliación."
            ) from exc

    def snapshot(self, *, entity_limit: int = 100, sync_limit: int = 30) -> IntegrationSnapshot:
        connections = tuple(
            self.session.scalars(
                select(IntegrationConnection)
                .where(IntegrationConnection.organization_id == self.organization_id)
                .order_by(IntegrationConnection.name.asc())
            ).all()
        )
        sync_runs = tuple(
            self.session.scalars(
                select(IntegrationSyncRun)
                .where(IntegrationSyncRun.organization_id == self.organization_id)
                .order_by(IntegrationSyncRun.started_at.desc())
                .limit(max(1, min(sync_limit, 100)))
            ).all()
        )
        entities = tuple(
            self.session.scalars(
                select(ExternalEntity)
                .where(ExternalEntity.organization_id == self.organization_id)
                .order_by(ExternalEntity.updated_at.desc())
                .limit(max(1, min(entity_limit, 500)))
            ).all()
        )
        entity_ids = [row.id for row in entities]
        references = (
            tuple(
                self.session.scalars(
                    select(ExternalReference).where(
                        ExternalReference.organization_id == self.organization_id,
                        ExternalReference.external_entity_id.in_(entity_ids),
                    )
                ).all()
            )
            if entity_ids
            else ()
        )
        return IntegrationSnapshot(
            connections=connections,
            sync_runs=sync_runs,
            entities=entities,
            references=references,
        )

    def _connection(self, public_id: UUID, *, allow_disabled: bool = False) -> IntegrationConnection:
        connection = self.session.scalar(
            select(IntegrationConnection).where(
                IntegrationConnection.organization_id == self.organization_id,
                IntegrationConnection.public_id == public_id,
            )
        )
        if connection is None:
            raise IntegrationNotFoundError("La conexión no existe en este tenant.")
        if not allow_disabled and connection.status != "ACTIVE":
            raise IntegrationConflictError("La conexión está deshabilitada.")
        return connection

    def _assert_target_exists(self, target_type: str, reference: str) -> None:
        if target_type == "LOTE":
            found = self.session.scalar(
                select(Lote.id).where(
                    Lote.organization_id == self.organization_id,
                    func.lower(Lote.identificador) == reference.lower(),
                )
            )
        elif target_type == "TRACEABILITY_BATCH":
            found = self.session.scalar(
                select(TraceabilityBatch.id).where(
                    TraceabilityBatch.organization_id == self.organization_id,
                    func.lower(TraceabilityBatch.code) == reference.lower(),
                )
            )
        elif target_type == "TRACEABILITY_EVENT":
            found = self.session.scalar(
                select(TraceabilityEvent.id).where(
                    TraceabilityEvent.organization_id == self.organization_id,
                    func.lower(TraceabilityEvent.event_code) == reference.lower(),
                )
            )
        else:
            found = self.session.scalar(
                select(Shipment.id).where(
                    Shipment.organization_id == self.organization_id,
                    func.lower(Shipment.shipment_code) == reference.lower(),
                )
            )
        if found is None:
            raise IntegrationNotFoundError(
                "El destino de reconciliación no existe en el tenant activo."
            )
