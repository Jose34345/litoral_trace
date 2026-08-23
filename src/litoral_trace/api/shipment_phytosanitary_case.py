"""Tenant-scoped SENASA/CERT-POV/ePhyto shipment API."""
from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field

from litoral_trace.api.auth import UserTenantContext
from litoral_trace.auth.rbac import Permission, require_permission
from litoral_trace.db.tenant import get_tenant_scoped_db_session, set_tenant_db_context
from litoral_trace.services.shipment_phytosanitary_case import (
    ShipmentPhytosanitaryCaseNotFoundError,
    ShipmentPhytosanitaryCasePersistenceError,
    ShipmentPhytosanitaryCaseService,
    ShipmentPhytosanitaryCaseValidationError,
)


router = APIRouter(
    prefix="/api/v1/phytosanitary-cases", tags=["Expediente Fitosanitario"]
)


class ShipmentPhytosanitaryCaseRequest(BaseModel):
    certification_mode: str
    requirements_reference: str | None = Field(default=None, max_length=500)
    requirements_checked_at: datetime | None = None
    cert_pov_reference: str | None = Field(default=None, max_length=120)
    certificate_number: str | None = Field(default=None, max_length=120)
    ephyto_reference: str | None = Field(default=None, max_length=160)
    notes: str | None = Field(default=None, max_length=2000)


def _service(user: UserTenantContext):
    session = get_tenant_scoped_db_session(user.organization_id)
    if session is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "code": "PHYTOSANITARY_CASE_UNAVAILABLE",
                "message": "La base de datos no está disponible temporalmente.",
            },
        )
    return session, ShipmentPhytosanitaryCaseService(
        session=session, organization_id=user.organization_id
    )


def _raise_service_error(exc: Exception) -> None:
    if isinstance(exc, ShipmentPhytosanitaryCaseValidationError):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail={"code": exc.code, "message": exc.detail},
        ) from None
    if isinstance(exc, ShipmentPhytosanitaryCaseNotFoundError):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"code": "SHIPMENT_NOT_FOUND", "message": str(exc)},
        ) from None
    if isinstance(exc, ShipmentPhytosanitaryCasePersistenceError):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"code": "PHYTOSANITARY_CASE_UNAVAILABLE", "message": str(exc)},
        ) from None
    raise exc


def _case_payload(row) -> dict[str, Any]:
    return {
        "public_id": str(row.public_id),
        "shipment_public_id": str(row.shipment_public_id),
        "shipment_code": row.shipment_code,
        "certification_mode": row.certification_mode,
        "requirements_reference": row.requirements_reference,
        "requirements_checked_at": (
            row.requirements_checked_at.isoformat() if row.requirements_checked_at else None
        ),
        "cert_pov_reference": row.cert_pov_reference,
        "certificate_number": row.certificate_number,
        "ephyto_reference": row.ephyto_reference,
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
        "certification_mode": row.certification_mode,
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
        "phytosanitary_case": (
            _case_payload(row.phytosanitary_case) if row.phytosanitary_case else None
        ),
        "ledger_mutated": False,
        "certificate_issued_by_litoral_trace": False,
    }


@router.get("/{shipment_code}")
def get_phytosanitary_case_readiness(
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
def upsert_phytosanitary_case(
    shipment_code: str,
    body: ShipmentPhytosanitaryCaseRequest,
    user: UserTenantContext = Depends(
        require_permission(Permission.TRACEABILITY_EVIDENCE)
    ),
) -> dict[str, Any]:
    session, service = _service(user)
    try:
        try:
            row = service.upsert_case(
                shipment_code=shipment_code,
                certification_mode=body.certification_mode,
                requirements_reference=body.requirements_reference,
                requirements_checked_at=body.requirements_checked_at,
                cert_pov_reference=body.cert_pov_reference,
                certificate_number=body.certificate_number,
                ephyto_reference=body.ephyto_reference,
                notes=body.notes,
                actor_user_id=user.user_id,
            )
            set_tenant_db_context(session, user.organization_id)
            readiness = service.readiness(shipment_code)
            return {
                "phytosanitary_case": _case_payload(row),
                "readiness": _readiness_payload(readiness),
                "ledger_mutated": False,
                "certificate_issued_by_litoral_trace": False,
            }
        except Exception as exc:
            _raise_service_error(exc)
            raise
    finally:
        session.close()
