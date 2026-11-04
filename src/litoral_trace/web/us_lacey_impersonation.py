"""Read-only support UI for zero-trust U.S. Lacey impersonation."""
from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Cookie, Depends, HTTPException, Request, status
from fastapi.responses import HTMLResponse
from sqlalchemy import select
from sqlalchemy.engine import Connection

from litoral_trace.db.models import (
    AssuranceDocument,
    UsLaceyOperation,
    UsLaceyOperationDocument,
    UsLaceyOperationField,
    UsLaceyPlantDeclaration,
    UsLaceyPpqPlantLine,
    VaultDocument,
)
from litoral_trace.us_lacey.csrf import us_lacey_csrf_token
from litoral_trace.us_lacey.impersonation_db import (
    ReadOnlyImpersonationContext,
    readonly_impersonation_db,
    resolve_readonly_impersonation_context,
)
from litoral_trace.us_lacey.portal_auth import US_LACEY_SESSION_COOKIE
from litoral_trace.web.templates import templates


router = APIRouter(
    prefix="/admin/impersonation",
    tags=["Platform Admin Read-Only Impersonation"],
)


def _base_context(
    *,
    request: Request,
    context: ReadOnlyImpersonationContext,
    us_session: str | None,
) -> dict:
    if not us_session:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Platform session required.",
        )

    return {
        "request": request,
        "authenticated": True,
        "impersonation": context,
        "target_organization_id": context.target_organization_id,
        "impersonation_session_id": context.session_id,
        "impersonation_end_csrf": us_lacey_csrf_token(
            session_token=us_session,
            purpose="platform-admin-impersonation-end",
        ),
    }


@router.get("/operations", response_class=HTMLResponse)
def impersonated_operations(
    request: Request,
    context: ReadOnlyImpersonationContext = Depends(
        resolve_readonly_impersonation_context
    ),
    db: Connection = Depends(readonly_impersonation_db),
    us_session: str | None = Cookie(
        None,
        alias=US_LACEY_SESSION_COOKIE,
    ),
):
    rows = (
        db.execute(
            select(
                UsLaceyOperation.public_id,
                UsLaceyOperation.client_reference,
                UsLaceyOperation.status,
                UsLaceyOperation.document_count,
                UsLaceyOperation.merchandise_line_count,
                UsLaceyOperation.review_result,
                UsLaceyOperation.operation_date,
                UsLaceyOperation.created_at,
                UsLaceyOperation.updated_at,
            )
            .where(
                UsLaceyOperation.organization_id
                == context.target_organization_id
            )
            .order_by(
                UsLaceyOperation.created_at.desc(),
                UsLaceyOperation.id.desc(),
            )
            .limit(250)
        )
        .mappings()
        .all()
    )

    template_context = _base_context(
        request=request,
        context=context,
        us_session=us_session,
    )
    template_context["operations"] = [dict(row) for row in rows]

    content = templates.get_template(
        "us_lacey/impersonation_operations.html"
    ).render(**template_context)

    return HTMLResponse(
        content=content,
        headers={"Cache-Control": "no-store, max-age=0"},
    )


@router.get(
    "/operations/{public_id}",
    response_class=HTMLResponse,
)
def impersonated_operation_detail(
    public_id: UUID,
    request: Request,
    context: ReadOnlyImpersonationContext = Depends(
        resolve_readonly_impersonation_context
    ),
    db: Connection = Depends(readonly_impersonation_db),
    us_session: str | None = Cookie(
        None,
        alias=US_LACEY_SESSION_COOKIE,
    ),
):
    operation = (
        db.execute(
            select(
                UsLaceyOperation.id,
                UsLaceyOperation.public_id,
                UsLaceyOperation.client_reference,
                UsLaceyOperation.importer_name,
                UsLaceyOperation.consignee_name,
                UsLaceyOperation.broker_name,
                UsLaceyOperation.supplier_name,
                UsLaceyOperation.operation_date,
                UsLaceyOperation.status,
                UsLaceyOperation.document_count,
                UsLaceyOperation.merchandise_line_count,
                UsLaceyOperation.review_result,
                UsLaceyOperation.created_at,
                UsLaceyOperation.updated_at,
            ).where(
                UsLaceyOperation.organization_id
                == context.target_organization_id,
                UsLaceyOperation.public_id == public_id,
            )
        )
        .mappings()
        .one_or_none()
    )

    if operation is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Operation not found.",
        )

    operation_id = int(operation["id"])

    fields = (
        db.execute(
            select(
                UsLaceyOperationField.id,
                UsLaceyOperationField.merchandise_line_reference,
                UsLaceyOperationField.field_name,
                UsLaceyOperationField.field_scope,
                UsLaceyOperationField.original_value,
                UsLaceyOperationField.normalized_value,
                UsLaceyOperationField.human_value,
                UsLaceyOperationField.field_status,
                UsLaceyOperationField.validation_status,
                UsLaceyOperationField.validation_error,
                UsLaceyOperationField.confidence,
                UsLaceyOperationField.source_page,
                UsLaceyOperationField.source_locator,
                UsLaceyOperationField.reviewed_at,
            )
            .where(
                UsLaceyOperationField.organization_id
                == context.target_organization_id,
                UsLaceyOperationField.operation_id == operation_id,
            )
            .order_by(
                UsLaceyOperationField.merchandise_line_reference,
                UsLaceyOperationField.id,
            )
        )
        .mappings()
        .all()
    )

    declarations = (
        db.execute(
            select(
                UsLaceyPpqPlantLine.line_reference,
                UsLaceyPpqPlantLine.ordinal.label("line_ordinal"),
                UsLaceyPlantDeclaration.ordinal,
                UsLaceyPlantDeclaration.genus,
                UsLaceyPlantDeclaration.species,
                UsLaceyPlantDeclaration.country_of_harvest,
                UsLaceyPlantDeclaration.quantity,
                UsLaceyPlantDeclaration.unit,
                UsLaceyPlantDeclaration.confidence,
                UsLaceyPlantDeclaration.source_page,
                UsLaceyPlantDeclaration.source_locator,
            )
            .join(
                UsLaceyPlantDeclaration,
                UsLaceyPlantDeclaration.plant_line_id
                == UsLaceyPpqPlantLine.id,
            )
            .where(
                UsLaceyPpqPlantLine.organization_id
                == context.target_organization_id,
                UsLaceyPlantDeclaration.organization_id
                == context.target_organization_id,
                UsLaceyPpqPlantLine.operation_id == operation_id,
            )
            .order_by(
                UsLaceyPpqPlantLine.ordinal,
                UsLaceyPlantDeclaration.ordinal,
            )
        )
        .mappings()
        .all()
    )

    documents = (
        db.execute(
            select(
                UsLaceyOperationDocument.document_role,
                UsLaceyOperationDocument.version_number,
                UsLaceyOperationDocument.created_at.label("linked_at"),
                AssuranceDocument.processing_status,
                AssuranceDocument.last_error_code,
                VaultDocument.public_id.label("vault_public_id"),
                VaultDocument.original_filename,
                VaultDocument.content_type,
                VaultDocument.size_bytes,
                VaultDocument.sha256,
                VaultDocument.document_type,
                VaultDocument.status.label("vault_status"),
                VaultDocument.created_at,
            )
            .join(
                AssuranceDocument,
                AssuranceDocument.id
                == UsLaceyOperationDocument.assurance_document_id,
            )
            .join(
                VaultDocument,
                VaultDocument.id
                == AssuranceDocument.vault_document_id,
            )
            .where(
                UsLaceyOperationDocument.organization_id
                == context.target_organization_id,
                AssuranceDocument.organization_id
                == context.target_organization_id,
                VaultDocument.organization_id
                == context.target_organization_id,
                UsLaceyOperationDocument.operation_id == operation_id,
                UsLaceyOperationDocument.is_current.is_(True),
            )
            .order_by(UsLaceyOperationDocument.id)
        )
        .mappings()
        .all()
    )

    template_context = _base_context(
        request=request,
        context=context,
        us_session=us_session,
    )
    template_context.update(
        {
            "operation": dict(operation),
            "fields": [dict(row) for row in fields],
            "declarations": [dict(row) for row in declarations],
            "documents": [dict(row) for row in documents],
        }
    )

    content = templates.get_template(
        "us_lacey/impersonation_operation_detail.html"
    ).render(**template_context)

    return HTMLResponse(
        content=content,
        headers={"Cache-Control": "no-store, max-age=0"},
    )
