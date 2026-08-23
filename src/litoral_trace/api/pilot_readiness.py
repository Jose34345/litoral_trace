"""Read-only first-customer pilot readiness API."""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status

from litoral_trace.api.auth import UserTenantContext
from litoral_trace.auth.rbac import Permission, require_permission
from litoral_trace.db.tenant import get_tenant_scoped_db_session
from litoral_trace.services.pilot_readiness import (
    PilotReadinessPersistenceError,
    PilotReadinessService,
)


router = APIRouter(prefix="/api/v1/pilot-readiness", tags=["Pilot Readiness"])


def _payload(view) -> dict[str, Any]:
    return {
        "organization_id": view.organization_id,
        "organization_name": view.organization_name,
        "state": view.state,
        "ready": view.ready,
        "completed_steps": view.completed_steps,
        "total_steps": view.total_steps,
        "shipment_code": view.shipment_code,
        "counts": dict(view.counts),
        "steps": [
            {
                "key": step.key,
                "label": step.label,
                "completed": step.completed,
                "detail": step.detail,
                "action_label": step.action_label,
                "action_href": step.action_href,
            }
            for step in view.steps
        ],
        "acceptance_smoke_required_for_pilot": False,
        "live_eudr_enabled": False,
    }


@router.get("")
def get_pilot_readiness(
    user: UserTenantContext = Depends(require_permission(Permission.LOTE_READ)),
) -> dict[str, Any]:
    session = get_tenant_scoped_db_session(user.organization_id)
    if session is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "code": "PILOT_READINESS_UNAVAILABLE",
                "message": "El estado del piloto no está disponible temporalmente.",
            },
        )
    try:
        try:
            view = PilotReadinessService(
                session=session,
                organization_id=user.organization_id,
                organization_name=user.organization_name,
            ).evaluate()
            return _payload(view)
        except PilotReadinessPersistenceError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail={
                    "code": "PILOT_READINESS_UNAVAILABLE",
                    "message": str(exc),
                },
            ) from None
    finally:
        session.close()
