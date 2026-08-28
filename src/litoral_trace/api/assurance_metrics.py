"""Read-only product metrics surface for Assurance v1."""
from __future__ import annotations

from fastapi import Depends, HTTPException, status
from fastapi.responses import JSONResponse

from litoral_trace.api.auth import UserTenantContext
from litoral_trace.assurance.feature_flags import get_assurance_feature_flags
from litoral_trace.assurance.metrics_service import AssuranceMetricsError, AssuranceMetricsService
from litoral_trace.auth.rbac import Permission, require_permission


def _require_metrics_enabled() -> None:
    flags = get_assurance_feature_flags()
    if not flags.assurance_v1:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Las métricas Assurance no están habilitadas.",
        )


async def assurance_metrics(
    user: UserTenantContext = Depends(require_permission(Permission.VAULT_READ)),
) -> JSONResponse:
    _require_metrics_enabled()
    try:
        snapshot = AssuranceMetricsService().snapshot(
            organization_id=user.organization_id
        )
    except AssuranceMetricsError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"code": "ASSURANCE_METRICS_UNAVAILABLE", "message": str(exc)},
        ) from None
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={"organization_id": user.organization_id, **snapshot.as_dict()},
    )
