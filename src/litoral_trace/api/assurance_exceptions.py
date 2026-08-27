"""Tenant-scoped API for the Assurance operational attention queue."""
from __future__ import annotations

from fastapi import Depends, HTTPException, Query, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from litoral_trace.api.assurance_preflight import (
    AssurancePreflightRequest,
    build_preflight_input,
)
from litoral_trace.api.auth import UserTenantContext
from litoral_trace.assurance.feature_flags import get_assurance_feature_flags
from litoral_trace.assurance.operational_exceptions import (
    AssuranceOperationalExceptionError,
    AssuranceOperationalExceptionService,
)
from litoral_trace.auth.rbac import Permission, require_permission


class AssuranceExceptionResolveRequest(BaseModel):
    resolution_note: str = Field(min_length=3, max_length=4000)
    preflight: AssurancePreflightRequest | None = None


def _require_operational_exceptions_enabled() -> None:
    flags = get_assurance_feature_flags()
    if not flags.assurance_v1 or not flags.operational_exceptions:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="La bandeja de excepciones Assurance no está habilitada.",
        )


def _serialize_exception(row) -> dict[str, object]:
    return {
        "public_id": str(row.public_id),
        "operation_reference": row.operation_reference,
        "source_type": row.source_type,
        "source_reference": row.source_reference,
        "cause_code": row.cause_code,
        "entity_type": row.entity_type,
        "entity_reference": row.entity_reference,
        "title": row.title,
        "description": row.description,
        "impact": row.impact,
        "priority": row.priority,
        "status": row.status,
        "assigned_to_user_id": row.assigned_to_user_id,
        "assigned_to_name": row.assigned_to_name,
        "due_at": row.due_at.isoformat() if row.due_at is not None else None,
        "recommended_action": row.recommended_action,
        "source_snapshot": row.source_snapshot,
        "created_at": row.created_at.isoformat() if row.created_at is not None else None,
        "updated_at": row.updated_at.isoformat() if row.updated_at is not None else None,
    }


def _serialize_preflight(view) -> dict[str, object] | None:
    if view is None:
        return None
    return {
        "operation_reference": view.operation_reference,
        "status": view.result.status.value,
        "reason_codes": list(view.result.reason_codes),
        "open_reconciliation_issue_count": view.open_reconciliation_issue_count,
        "reasons": [
            {
                "code": reason.code,
                "category": reason.category,
                "status": reason.status.value,
                "explanation": reason.explanation,
                "action": reason.action,
                "source": reason.source,
            }
            for reason in view.result.reasons
        ],
    }


async def assurance_attention_queue(
    operation_reference: str | None = Query(default=None, max_length=255),
    user: UserTenantContext = Depends(require_permission(Permission.VAULT_READ)),
) -> JSONResponse:
    _require_operational_exceptions_enabled()
    try:
        rows = AssuranceOperationalExceptionService().list_attention(
            organization_id=user.organization_id,
            operation_reference=operation_reference,
        )
    except AssuranceOperationalExceptionError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"code": "ASSURANCE_EXCEPTIONS_UNAVAILABLE", "message": str(exc)},
        ) from None
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={
            "organization_id": user.organization_id,
            "count": len(rows),
            "exceptions": [_serialize_exception(row) for row in rows],
        },
    )


async def resolve_assurance_exception(
    exception_id: str,
    payload: AssuranceExceptionResolveRequest,
    user: UserTenantContext = Depends(
        require_permission(Permission.TRACEABILITY_OPERATE)
    ),
) -> JSONResponse:
    _require_operational_exceptions_enabled()
    domain_preflight = (
        build_preflight_input(payload.preflight) if payload.preflight is not None else None
    )
    try:
        result = AssuranceOperationalExceptionService().resolve(
            organization_id=user.organization_id,
            exception_public_id=exception_id,
            resolved_by_user_id=user.user_id,
            resolution_note=payload.resolution_note,
            preflight_payload=domain_preflight,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail={"code": "ASSURANCE_EXCEPTION_INVALID", "message": str(exc)},
        ) from None
    except AssuranceOperationalExceptionError as exc:
        message = str(exc)
        if "no encontrada" in message.lower():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={"code": "ASSURANCE_EXCEPTION_NOT_FOUND", "message": message},
            ) from None
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"code": "ASSURANCE_EXCEPTION_UNAVAILABLE", "message": message},
        ) from None

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={
            "organization_id": user.organization_id,
            "exception_id": str(result.exception_public_id),
            "status": "RESOLVED",
            "source_status": result.source_status,
            "preflight": _serialize_preflight(result.preflight),
        },
    )
