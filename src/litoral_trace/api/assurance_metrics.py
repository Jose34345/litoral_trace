"""Product metrics and pilot comparison surface for Assurance v1."""
from __future__ import annotations

from fastapi import Depends, HTTPException, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from litoral_trace.api.auth import UserTenantContext
from litoral_trace.assurance.feature_flags import get_assurance_feature_flags
from litoral_trace.assurance.metrics_service import AssuranceMetricsError, AssuranceMetricsService
from litoral_trace.auth.rbac import Permission, require_permission
from litoral_trace.services.audit import (
    build_audit_actor_from_user,
    build_request_audit_context,
)


class AssuranceMetricsBaselineRequest(BaseModel):
    manual_baseline_seconds: float = Field(gt=0, le=604800)
    label: str | None = Field(default=None, max_length=120)


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


async def set_assurance_metrics_baseline(
    payload: AssuranceMetricsBaselineRequest,
    request: Request,
    user: UserTenantContext = Depends(
        require_permission(Permission.TRACEABILITY_OPERATE)
    ),
) -> JSONResponse:
    """Register the manual document-intake baseline used for the pilot comparison."""
    _require_metrics_enabled()
    try:
        baseline = AssuranceMetricsService().set_manual_baseline(
            organization_id=user.organization_id,
            manual_baseline_seconds=payload.manual_baseline_seconds,
            label=payload.label,
            actor=build_audit_actor_from_user(user),
            request_context=build_request_audit_context(request),
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail={"code": "ASSURANCE_METRICS_BASELINE_INVALID", "message": str(exc)},
        ) from None
    except AssuranceMetricsError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"code": "ASSURANCE_METRICS_BASELINE_UNAVAILABLE", "message": str(exc)},
        ) from None
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={"organization_id": user.organization_id, **baseline},
    )


async def assurance_metrics_report(
    user: UserTenantContext = Depends(require_permission(Permission.VAULT_READ)),
) -> JSONResponse:
    """Return the explicit manual-vs-LT pilot report with its measurement scope."""
    _require_metrics_enabled()
    try:
        snapshot = AssuranceMetricsService().snapshot(
            organization_id=user.organization_id
        )
    except AssuranceMetricsError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"code": "ASSURANCE_METRICS_REPORT_UNAVAILABLE", "message": str(exc)},
        ) from None
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={
            "organization_id": user.organization_id,
            "manual_vs_lt": snapshot.manual_vs_lt.as_dict(),
            "automatic_data_percentage": snapshot.metrics.automatic_data_percentage,
            "zero_friction_target_percentage": snapshot.zero_friction_target_percentage,
            "zero_friction_target_met": snapshot.zero_friction_target_met,
            "fields_manually_changed": snapshot.metrics.fields_manually_changed,
            "discrepancies_detected": snapshot.metrics.reconciliation_issues,
            "blocking_exceptions_before_dispatch": snapshot.blocking_exceptions_before_dispatch,
            "average_exception_resolution_seconds": snapshot.average_exception_resolution_seconds,
        },
    )
