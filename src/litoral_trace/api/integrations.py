"""Tenant-scoped Integration Core and generic ERP staging API."""
from __future__ import annotations

from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, status
from pydantic import BaseModel, Field

from litoral_trace.api.auth import UserTenantContext
from litoral_trace.auth.rbac import Permission, require_permission
from litoral_trace.db.tenant import get_tenant_scoped_db_session
from litoral_trace.services.integrations.canonical import GenericErpPayload
from litoral_trace.services.integrations.core import (
    IntegrationConflictError,
    IntegrationCoreService,
    IntegrationNotFoundError,
    IntegrationPersistenceError,
    IntegrationValidationError,
)


router = APIRouter(prefix="/api/v1/integrations", tags=["Integraciones"])


class ConnectionCreateRequest(BaseModel):
    name: str = Field(min_length=1, max_length=160)
    connector_type: str = "GENERIC_ERP"
    secret_ref: str | None = Field(default=None, max_length=255)
    config_json: dict[str, Any] | None = None


class ConnectionStatusRequest(BaseModel):
    status: str


class ReconcileRequest(BaseModel):
    target_type: str
    target_reference: str = Field(min_length=1, max_length=200)


def _raise_service_error(exc: Exception) -> None:
    if isinstance(exc, IntegrationValidationError):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail={"code": exc.code, "message": exc.detail},
        ) from None
    if isinstance(exc, IntegrationNotFoundError):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"code": "INTEGRATION_NOT_FOUND", "message": str(exc)},
        ) from None
    if isinstance(exc, IntegrationConflictError):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"code": "INTEGRATION_CONFLICT", "message": str(exc)},
        ) from None
    if isinstance(exc, IntegrationPersistenceError):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"code": "INTEGRATION_UNAVAILABLE", "message": str(exc)},
        ) from None
    raise exc


def _service(user: UserTenantContext):
    session = get_tenant_scoped_db_session(user.organization_id)
    if session is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"code": "INTEGRATION_UNAVAILABLE", "message": "Base de datos no disponible."},
        )
    return session, IntegrationCoreService(
        session=session,
        organization_id=user.organization_id,
    )


@router.get("/connections")
def list_connections(
    user: UserTenantContext = Depends(require_permission(Permission.INTEGRATION_READ)),
) -> list[dict[str, Any]]:
    session, service = _service(user)
    try:
        return [
            {
                "public_id": str(row.public_id),
                "name": row.name,
                "connector_type": row.connector_type,
                "status": row.status,
                "secret_ref": row.secret_ref,
                "config_json": row.config_json,
            }
            for row in service.snapshot(entity_limit=1, sync_limit=1).connections
        ]
    finally:
        session.close()


@router.post("/connections", status_code=status.HTTP_201_CREATED)
def create_connection(
    body: ConnectionCreateRequest,
    user: UserTenantContext = Depends(require_permission(Permission.INTEGRATION_MANAGE)),
) -> dict[str, Any]:
    session, service = _service(user)
    try:
        try:
            row = service.create_connection(
                name=body.name,
                connector_type=body.connector_type,
                secret_ref=body.secret_ref,
                config_json=body.config_json,
                actor_user_id=user.user_id,
            )
        except Exception as exc:
            _raise_service_error(exc)
            raise
        return {
            "public_id": str(row.public_id),
            "name": row.name,
            "connector_type": row.connector_type,
            "status": row.status,
        }
    finally:
        session.close()


@router.patch("/connections/{connection_public_id}/status")
def update_connection_status(
    connection_public_id: UUID,
    body: ConnectionStatusRequest,
    user: UserTenantContext = Depends(require_permission(Permission.INTEGRATION_MANAGE)),
) -> dict[str, Any]:
    session, service = _service(user)
    try:
        try:
            row = service.set_connection_status(
                connection_public_id,
                status=body.status,
                actor_user_id=user.user_id,
            )
        except Exception as exc:
            _raise_service_error(exc)
            raise
        return {"public_id": str(row.public_id), "status": row.status}
    finally:
        session.close()


@router.post("/connections/{connection_public_id}/sync/generic-erp")
def stage_generic_erp(
    connection_public_id: UUID,
    body: GenericErpPayload,
    idempotency_key: str = Header(..., alias="Idempotency-Key"),
    user: UserTenantContext = Depends(require_permission(Permission.INTEGRATION_MANAGE)),
) -> dict[str, Any]:
    """Stage a vendor-neutral ERP payload without touching ledger/stock."""
    session, service = _service(user)
    try:
        try:
            result = service.stage_generic_erp(
                connection_public_id=connection_public_id,
                payload=body,
                idempotency_key=idempotency_key,
                actor_user_id=user.user_id,
            )
        except Exception as exc:
            _raise_service_error(exc)
            raise
        return {
            "sync_run_public_id": str(result.public_id),
            "status": result.status,
            "records_seen": result.records_seen,
            "records_created": result.records_created,
            "records_updated": result.records_updated,
            "records_unchanged": result.records_unchanged,
            "records_conflict": result.records_conflict,
            "replayed": result.replayed,
            "ledger_mutated": False,
        }
    finally:
        session.close()


@router.get("/entities")
def list_entities(
    user: UserTenantContext = Depends(require_permission(Permission.INTEGRATION_READ)),
) -> list[dict[str, Any]]:
    session, service = _service(user)
    try:
        snapshot = service.snapshot(entity_limit=200, sync_limit=1)
        refs = {row.external_entity_id: row for row in snapshot.references}
        return [
            {
                "public_id": str(row.public_id),
                "entity_type": row.entity_type,
                "external_id": row.external_id,
                "status": row.status,
                "payload_hash": row.payload_hash,
                "source_updated_at": row.source_updated_at.isoformat() if row.source_updated_at else None,
                "normalized": row.normalized_json,
                "reconciliation": (
                    {
                        "target_type": refs[row.id].target_type,
                        "target_reference": refs[row.id].target_reference,
                    }
                    if row.id in refs
                    else None
                ),
            }
            for row in snapshot.entities
        ]
    finally:
        session.close()


@router.post("/entities/{entity_public_id}/reconcile")
def reconcile_entity(
    entity_public_id: UUID,
    body: ReconcileRequest,
    user: UserTenantContext = Depends(require_permission(Permission.INTEGRATION_MANAGE)),
) -> dict[str, Any]:
    session, service = _service(user)
    try:
        try:
            row = service.reconcile_entity(
                entity_public_id=entity_public_id,
                target_type=body.target_type,
                target_reference=body.target_reference,
                user_id=user.user_id,
            )
        except Exception as exc:
            _raise_service_error(exc)
            raise
        return {
            "public_id": str(row.public_id),
            "target_type": row.target_type,
            "target_reference": row.target_reference,
            "status": "RECONCILED",
        }
    finally:
        session.close()


@router.get("/sync-runs")
def list_sync_runs(
    user: UserTenantContext = Depends(require_permission(Permission.INTEGRATION_READ)),
) -> list[dict[str, Any]]:
    session, service = _service(user)
    try:
        return [
            {
                "public_id": str(row.public_id),
                "status": row.status,
                "started_at": row.started_at.isoformat(),
                "finished_at": row.finished_at.isoformat() if row.finished_at else None,
                "records_seen": row.records_seen,
                "records_created": row.records_created,
                "records_updated": row.records_updated,
                "records_unchanged": row.records_unchanged,
                "records_conflict": row.records_conflict,
            }
            for row in service.snapshot(entity_limit=1, sync_limit=100).sync_runs
        ]
    finally:
        session.close()
