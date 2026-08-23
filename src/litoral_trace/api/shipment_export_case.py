"""Tenant-scoped Corrientes + ARCA shipment export-case API."""
from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field

from litoral_trace.api.auth import UserTenantContext
from litoral_trace.auth.rbac import Permission, require_permission
from litoral_trace.db.tenant import get_tenant_scoped_db_session
from litoral_trace.services.shipment_export_case import (
    ShipmentExportCaseNotFoundError,
    ShipmentExportCasePersistenceError,
    ShipmentExportCaseService,
    ShipmentExportCaseValidationError,
)


router = APIRouter(prefix="/api/v1/export-cases", tags=["Expediente Exportador"])


class ShipmentExportCaseRequest(BaseModel):
    origin_profile: str
    export_invoice_number: str | None = Field(default=None, max_length=80)
    export_invoice_cae: str | None = Field(default=None, max_length=32)
    customs_destination_id: str | None = Field(default=None, max_length=64)
    customs_subregime: str | None = Field(default=None, max_length=16)
    customs_officialized_at: datetime | None = None
    notes: str | None = Field(default=None, max_length=2000)


def _service(user: UserTenantContext):
    session = get_tenant_scoped_db_session(user.organization_id)
    if session is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "code": "EXPORT_CASE_UNAVAILABLE",
                "message": "La base de datos no está disponible temporalmente.",
            },
        )
    return session, ShipmentExportCaseService(
        session=session,
        organization_id=user.organization_id,
    )


def _raise_service_error(exc: Exception) -> None:
    if isinstance(exc, ShipmentExportCaseValidationError):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail={"code": exc.code, "message": exc.detail},
        ) from None
    if isinstance(exc, ShipmentExportCaseNotFoundError):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"code": "SHIPMENT_NOT_FOUND", "message": str(exc)},
        ) from None
    if isinstance(exc, ShipmentExportCasePersistenceError):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"code": "EXPORT_CASE_UNAVAILABLE", "message": str(exc)},
        ) from None
    raise exc


def _case_payload(row) -> dict[str, Any]:
    return {
        "public_id": str(row.public_id),
        "shipment_public_id": str(row.shipment_public_id),
        "shipment_code": row.shipment_code,
        "origin_profile": row.origin_profile,
        "export_invoice_number": row.export_invoice_number,
        "export_invoice_cae": row.export_invoice_cae,
        "customs_destination_id": row.customs_destination_id,
        "customs_subregime": row.customs_subregime,
        "customs_officialized_at": (
            row.customs_officialized_at.isoformat()
            if row.customs_officialized_at
            else None
        ),
        "notes": row.notes,
        "created_at": row.created_at.isoformat(),
        "updated_at": row.updated_at.isoformat(),
    }


def _readiness_payload(row) -> dict[str, Any]:
    return {
        "shipment_public_id": str(row.shipment_public_id),
        "shipment_code": row.shipment_code,
        "state": row.state,
        "ready": row.ready,
        "origin_profile": row.origin_profile,
        "missing": list(row.missing),
        "evidence_types": list(row.evidence_types),
        "requirements": [
            {
                "key": item.key,
                "label": item.label,
                "satisfied": item.satisfied,
                "source": item.source,
            }
            for item in row.requirements
        ],
        "export_case": _case_payload(row.export_case) if row.export_case else None,
        "ledger_mutated": False,
    }


@router.get("/{shipment_code}")
def get_export_case_readiness(
    shipment_code: str,
    user: UserTenantContext = Depends(require_permission(Permission.LOTE_READ)),
) -> dict[str, Any]:
    session, service = _service(user)
    try:
        try:
            return _readiness_payload(service.readiness(shipment_code))
        except Exception as exc:
            _raise_service_error(exc)
            raise
    finally:
        session.close()


@router.put("/{shipment_code}")
def upsert_export_case(
    shipment_code: str,
    body: ShipmentExportCaseRequest,
    user: UserTenantContext = Depends(
        require_permission(Permission.TRACEABILITY_EVIDENCE)
    ),
) -> dict[str, Any]:
    session, service = _service(user)
    try:
        try:
            row = service.upsert_case(
                shipment_code=shipment_code,
                origin_profile=body.origin_profile,
                export_invoice_number=body.export_invoice_number,
                export_invoice_cae=body.export_invoice_cae,
                customs_destination_id=body.customs_destination_id,
                customs_subregime=body.customs_subregime,
                customs_officialized_at=body.customs_officialized_at,
                notes=body.notes,
                actor_user_id=user.user_id,
            )
            # Re-apply tenant context because commit closes the transaction-local
            # GUC used by FORCE RLS before the readiness query starts.
            from litoral_trace.db.tenant import set_tenant_db_context

            set_tenant_db_context(session, user.organization_id)
            readiness = service.readiness(shipment_code)
            return {
                "export_case": _case_payload(row),
                "readiness": _readiness_payload(readiness),
                "ledger_mutated": False,
            }
        except Exception as exc:
            _raise_service_error(exc)
            raise
    finally:
        session.close()
