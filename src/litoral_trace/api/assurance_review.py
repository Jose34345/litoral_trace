"""Tenant-scoped document review API for Assurance v1."""
from __future__ import annotations

from fastapi import Depends, HTTPException, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from litoral_trace.api.auth import UserTenantContext
from litoral_trace.assurance.document_review import (
    AssuranceDocumentReviewError,
    AssuranceDocumentReviewService,
)
from litoral_trace.assurance.feature_flags import get_assurance_feature_flags
from litoral_trace.auth.rbac import Permission, require_permission
from litoral_trace.services.audit import (
    build_audit_actor_from_user,
    build_request_audit_context,
)


class AssuranceReviewApproveRequest(BaseModel):
    field_ids: list[int] = Field(min_length=1, max_length=200)


def _require_review_enabled() -> None:
    flags = get_assurance_feature_flags()
    if not flags.assurance_v1 or not flags.document_intelligence:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="La revisión documental Assurance no está habilitada.",
        )


def _serialize_view(view) -> dict[str, object]:
    return {
        "assurance_document_id": str(view.assurance_document_id),
        "filename": view.filename,
        "semantic_document_type": view.semantic_document_type,
        "type_confidence": view.type_confidence,
        "processing_status": view.processing_status,
        "last_error_code": view.last_error_code,
        "last_error_message": view.last_error_message,
        "structured_field_count": view.structured_field_count,
        "auto_accepted_count": view.auto_accepted_count,
        "review_count": view.review_count,
        "fields": [
            {
                "id": field.id,
                "field_name": field.field_name,
                "original_value": field.original_value,
                "normalized_value": field.normalized_value,
                "value_type": field.value_type,
                "confidence": field.confidence,
                "confidence_level": field.confidence_level,
                "source_page": field.source_page,
                "source_locator": field.source_locator,
                "auto_accepted": field.auto_accepted,
                "needs_review": field.needs_review,
            }
            for field in view.fields
        ],
        "links": [
            {
                "entity_type": link.entity_type,
                "entity_reference": link.entity_reference,
                "confidence": link.confidence,
                "method": link.method,
                "human_confirmed": link.human_confirmed,
            }
            for link in view.links
        ],
    }


async def assurance_document_review(
    assurance_document_id: str,
    user: UserTenantContext = Depends(require_permission(Permission.VAULT_READ)),
) -> JSONResponse:
    _require_review_enabled()
    try:
        view = AssuranceDocumentReviewService().get(
            organization_id=user.organization_id,
            assurance_public_id=assurance_document_id,
        )
    except (ValueError, AssuranceDocumentReviewError) as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"code": "ASSURANCE_REVIEW_NOT_FOUND", "message": str(exc)},
        ) from None
    return JSONResponse(status_code=status.HTTP_200_OK, content=_serialize_view(view))


async def approve_assurance_review_fields(
    assurance_document_id: str,
    payload: AssuranceReviewApproveRequest,
    request: Request,
    user: UserTenantContext = Depends(
        require_permission(Permission.TRACEABILITY_OPERATE)
    ),
) -> JSONResponse:
    _require_review_enabled()
    try:
        result = AssuranceDocumentReviewService().approve_fields(
            organization_id=user.organization_id,
            assurance_public_id=assurance_document_id,
            field_ids=payload.field_ids,
            actor=build_audit_actor_from_user(user),
            request_context=build_request_audit_context(request),
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail={"code": "ASSURANCE_REVIEW_INVALID", "message": str(exc)},
        ) from None
    except AssuranceDocumentReviewError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"code": "ASSURANCE_REVIEW_CONFLICT", "message": str(exc)},
        ) from None
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={
            "assurance_document_id": assurance_document_id,
            "approved_count": result.approved_count,
            "remaining_review_count": result.remaining_review_count,
            "processing_status": result.processing_status,
        },
    )
