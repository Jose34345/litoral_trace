"""Upload-first customer intake and safe bulk review actions for U.S. Lacey.

This router deliberately reuses the certified operation, Vault, queue and human-review
services. It does not make AI output authoritative. Intake creates a technical operation
reference, stores the source files, and lets the extraction/reconciliation pipeline fill
customer-visible fields afterwards.
"""
from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from fastapi import APIRouter, Cookie, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse

from litoral_trace.assurance.ingestion import (
    AssuranceIngestionValidationError,
    validate_incoming_file,
)
from litoral_trace.us_lacey.access import (
    UsLaceyOperationalAccessError,
    require_us_lacey_operational_access,
)
from litoral_trace.us_lacey.bulk_review import accept_supported_us_lacey_fields
from litoral_trace.us_lacey.csrf import UsLaceyCsrfError, verify_us_lacey_csrf
from litoral_trace.us_lacey.operations import (
    UsLaceyOperationError,
    UsLaceyOperationNotFound,
    UsLaceyOperationService,
)
from litoral_trace.us_lacey.portal_auth import (
    US_LACEY_SESSION_COOKIE,
    UsLaceyPortalAuthError,
    resolve_us_lacey_session,
)
from litoral_trace.us_lacey.review import UsLaceyReviewError, review_us_lacey_field
from litoral_trace.us_lacey.storage import build_us_lacey_storage_settings
from litoral_trace.us_lacey.workflow import (
    UsLaceyWorkflowError,
    create_us_lacey_customer_operation,
    upload_and_enqueue_us_lacey_document,
)
from litoral_trace.web.us_lacey_portal_views import render_message_page


router = APIRouter()
MAX_INTAKE_DOCUMENTS = 20


def _login_redirect(*, clear_cookie: bool = False) -> RedirectResponse:
    response = RedirectResponse("/login", status_code=303)
    if clear_cookie:
        response.delete_cookie(US_LACEY_SESSION_COOKIE, path="/")
    return response


def _operation_reference() -> str:
    """Return a collision-resistant technical reference until evidence supplies one."""
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    return f"INTAKE-{stamp}-{uuid4().hex[:12].upper()}"


def _error_page(
    request: Request,
    message: str,
    *,
    action_href: str = "/operations",
    status_code: int = 400,
) -> HTMLResponse:
    return HTMLResponse(
        render_message_page(
            request=request,
            title="Shipment intake needs attention.",
            message=message,
            authenticated=True,
            action_href=action_href,
            action_label="Return to operations",
        ),
        status_code=status_code,
        headers={"Cache-Control": "no-store, max-age=0"},
    )


def _identity_and_entitlement(session_token: str | None, *, require_slot: bool = False):
    if not session_token:
        raise UsLaceyPortalAuthError("Sign in to continue.", code="session_invalid")
    identity = resolve_us_lacey_session(session_token)
    entitlement = require_us_lacey_operational_access(
        organization_id=identity.organization_id,
        require_operation_slot=require_slot,
    )
    return identity, entitlement


@router.post("/operations/intake", response_class=HTMLResponse)
async def upload_first_operation_intake(
    request: Request,
    documents: list[UploadFile] = File(...),
    csrf_token: str = Form(...),
    us_session: str | None = Cookie(None, alias=US_LACEY_SESSION_COOKIE),
):
    """Create one operation from source documents with zero required metadata entry."""
    created = None
    try:
        identity, _entitlement = _identity_and_entitlement(us_session, require_slot=True)
        verify_us_lacey_csrf(
            session_token=us_session or "",
            purpose="operation:create",
            submitted_token=csrf_token,
        )
        if not documents:
            raise UsLaceyWorkflowError("Choose at least one shipment or supplier document.")
        if len(documents) > MAX_INTAKE_DOCUMENTS:
            raise UsLaceyWorkflowError(
                f"Upload at most {MAX_INTAKE_DOCUMENTS} documents in one intake. You can add more afterwards."
            )

        # Validate every selected file before consuming an operation slot. The same
        # mature Vault-first ingestion layer validates again when persisting each file;
        # this preflight prevents a bad second/third file from creating a mostly-empty
        # customer operation before its format problem is discovered.
        payloads: list[tuple[str, str, bytes]] = []
        storage_settings = build_us_lacey_storage_settings()
        for document in documents:
            content = await document.read()
            validated = validate_incoming_file(
                filename=document.filename or "document",
                content_type=document.content_type or "application/octet-stream",
                content=content,
                storage_settings=storage_settings,
            )
            payloads.append(
                (
                    validated.filename,
                    validated.content_type,
                    validated.content,
                )
            )

        created = create_us_lacey_customer_operation(
            organization_id=identity.organization_id,
            user_id=identity.user_id,
            client_reference=_operation_reference(),
            # A default declaration line removes the old requirement for customers to
            # know PPQ line structure before document analysis. Later reconciliation may
            # split or add component lines; no regulatory fact is inferred here.
            line_references=("1",),
        )
        for filename, content_type, content in payloads:
            upload_and_enqueue_us_lacey_document(
                organization_id=identity.organization_id,
                user_id=identity.user_id,
                operation_public_id=created.public_id,
                filename=filename,
                content_type=content_type,
                content=content,
                document_role="UNKNOWN",
            )
        return RedirectResponse(f"/operations/{created.public_id}", status_code=303)
    except UsLaceyPortalAuthError:
        return _login_redirect(clear_cookie=bool(us_session))
    except UsLaceyOperationalAccessError:
        return RedirectResponse("/billing", status_code=303)
    except (
        AssuranceIngestionValidationError,
        UsLaceyCsrfError,
        UsLaceyWorkflowError,
        UsLaceyOperationError,
        ValueError,
    ) as exc:
        action = f"/operations/{created.public_id}" if created is not None else "/operations"
        return _error_page(request, str(exc), action_href=action, status_code=400)


def _find_supported_field(detail, field_id: int):
    return next((field for field in detail.fields if field.id == int(field_id)), None)


# Compatibility-only route for links/forms rendered before the canonical review
# contract was introduced.  The integer path converter is deliberate: it prevents
# this dynamic endpoint from ever consuming a literal action slug.
@router.post(
    "/operations/{operation_public_id}/review-supported/{field_id:int}",
    response_class=HTMLResponse,
    include_in_schema=False,
)
def review_supported_field(
    operation_public_id: str,
    field_id: int,
    request: Request,
    action: str = Form(...),
    value: str = Form(""),
    csrf_token: str = Form(...),
    us_session: str | None = Cookie(None, alias=US_LACEY_SESSION_COOKIE),
):
    """Confirm/edit a legacy high-confidence FOUND proposal explicitly."""
    try:
        identity, _entitlement = _identity_and_entitlement(us_session)
        verify_us_lacey_csrf(
            session_token=us_session or "",
            purpose=f"complete:{operation_public_id}",
            submitted_token=csrf_token,
        )
        detail = UsLaceyOperationService().get_detail(
            organization_id=identity.organization_id,
            operation_public_id=operation_public_id,
        )
        field = _find_supported_field(detail, field_id)
        if field is None or field.status != "FOUND":
            raise UsLaceyReviewError("This suggestion is no longer awaiting confirmation.")
        normalized_action = str(action or "").strip().lower()
        if normalized_action not in {"accept", "edit"}:
            raise UsLaceyReviewError("Supported-value review action is invalid.")
        review_us_lacey_field(
            organization_id=identity.organization_id,
            operation_public_id=operation_public_id,
            field_id=field.id,
            user_id=identity.user_id,
            user_email=identity.email,
            action=normalized_action,
            value=value or None,
        )
        return RedirectResponse(f"/operations/{operation_public_id}", status_code=303)
    except UsLaceyPortalAuthError:
        return _login_redirect(clear_cookie=bool(us_session))
    except UsLaceyOperationalAccessError:
        return RedirectResponse("/billing", status_code=303)
    except (UsLaceyCsrfError, UsLaceyReviewError, UsLaceyOperationNotFound) as exc:
        return _error_page(
            request,
            str(exc),
            action_href=f"/operations/{operation_public_id}",
            status_code=400,
        )


# Canonical action namespace.  The legacy alias is safe because the old per-field
# route is now constrained with Starlette's :int converter rather than a catch-all
# string parameter.
@router.post(
    "/operations/{operation_public_id}/review/accept-supported",
    response_class=HTMLResponse,
    include_in_schema=False,
)
@router.post(
    "/operations/{operation_public_id}/review/actions/accept-supported",
    response_class=HTMLResponse,
)
def accept_all_supported_fields(
    operation_public_id: str,
    request: Request,
    csrf_token: str = Form(...),
    us_session: str | None = Cookie(None, alias=US_LACEY_SESSION_COOKIE),
):
    """Atomically confirm only unambiguous FOUND proposals.

    REVIEW/MISSING values, multi-candidate values and fields with an open
    reconciliation issue remain untouched.  A repeated POST is idempotent because
    already-confirmed rows are no longer FOUND.
    """
    try:
        identity, _entitlement = _identity_and_entitlement(us_session)
        verify_us_lacey_csrf(
            session_token=us_session or "",
            purpose=f"complete:{operation_public_id}",
            submitted_token=csrf_token,
        )
        accept_supported_us_lacey_fields(
            organization_id=identity.organization_id,
            operation_public_id=operation_public_id,
            user_id=identity.user_id,
            user_email=identity.email,
        )
        return RedirectResponse(f"/operations/{operation_public_id}", status_code=303)
    except UsLaceyPortalAuthError:
        return _login_redirect(clear_cookie=bool(us_session))
    except UsLaceyOperationalAccessError:
        return RedirectResponse("/billing", status_code=303)
    except (UsLaceyCsrfError, UsLaceyReviewError, UsLaceyOperationNotFound) as exc:
        return _error_page(
            request,
            str(exc),
            action_href=f"/operations/{operation_public_id}",
            status_code=400,
        )
