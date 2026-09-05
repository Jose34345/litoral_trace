"""Standalone ASGI entrypoint for the private U.S. Lacey customer portal.

The process is isolated from the Argentina application. Browser authentication
uses U.S.-database opaque sessions rather than the generic Litoral Trace JWT.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
import logging
import re

from fastapi import Cookie, FastAPI, File, Form, HTTPException, Request, UploadFile, status
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles

from litoral_trace.us_lacey.access import (
    UsLaceyOperationalAccessError,
    require_us_lacey_operational_access,
)
from litoral_trace.us_lacey.commercial import (
    UsLaceyCommercialConfigurationError,
    load_us_lacey_commercial_config,
)
from litoral_trace.us_lacey.config import (
    UsLaceyConfigurationError,
    load_us_lacey_runtime_config,
)
from litoral_trace.us_lacey.csrf import (
    UsLaceyCsrfError,
    us_lacey_csrf_token,
    verify_us_lacey_csrf,
)
from litoral_trace.us_lacey.email_delivery import (
    UsLaceyEmailConfigurationError,
    load_us_lacey_email_config,
    send_us_lacey_verification_email,
)
from litoral_trace.us_lacey.operations import (
    UsLaceyOperationError,
    UsLaceyOperationNotFound,
    UsLaceyOperationService,
)
from litoral_trace.us_lacey.lacey_engine_dossier import Engine2DossierAvailability, Engine2DossierView, UsLaceyEngineDossierService
from litoral_trace.us_lacey.portal_auth import (
    US_LACEY_SESSION_COOKIE,
    UsLaceyPortalAuthError,
    login_us_lacey_user,
    logout_us_lacey_user,
    resolve_us_lacey_session,
)
from litoral_trace.us_lacey.portal_config import (
    UsLaceyPortalConfigurationError,
    load_us_lacey_portal_config,
)
from litoral_trace.us_lacey.review import (
    UsLaceyReviewError,
    export_us_lacey_csv,
    export_us_lacey_xlsx,
    finalize_us_lacey_review,
    review_us_lacey_field,
)
from litoral_trace.us_lacey.self_service import (
    UsLaceySelfServiceError,
    get_us_lacey_billing_summary,
    register_us_lacey_company,
    verify_us_lacey_email,
)
from litoral_trace.us_lacey.workflow import (
    UsLaceyWorkflowError,
    create_us_lacey_customer_operation,
    upload_and_enqueue_us_lacey_document,
)
from litoral_trace.web.us_lacey_operational_views import (
    render_new_operation,
    render_operation_detail,
    render_operations,
)
from litoral_trace.web.us_lacey_portal_views import (
    render_billing,
    render_check_email,
    render_login,
    render_signup,
    render_verification_error,
    render_message_page,
)
from litoral_trace.web.templates import STATIC_DIR

LOGGER = logging.getLogger(__name__)


app = FastAPI(
    title="Litoral Trace U.S. Lacey Pilot",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


def _html(content: str, *, status_code: int = 200) -> HTMLResponse:
    response = HTMLResponse(content=content, status_code=status_code)
    response.headers["Cache-Control"] = "no-store, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "same-origin"
    return response


def _configuration_error_page(request: Request) -> HTMLResponse:
    return _html(render_message_page(request=request, title="Portal configuration incomplete.", message="The private U.S. portal is not ready for customer signup."), status_code=503)


def _request_metadata(request: Request) -> tuple[str | None, str | None]:
    client_ip = request.client.host if request.client is not None else None
    user_agent = request.headers.get("user-agent")
    return client_ip, user_agent


def _load_customer_configuration():
    return load_us_lacey_commercial_config(), load_us_lacey_portal_config()


def _login_redirect(*, clear_cookie: bool = False):
    response = RedirectResponse("/login", status_code=303)
    if clear_cookie:
        response.delete_cookie(US_LACEY_SESSION_COOKIE, path="/")
    return response


def _operational_context(us_session: str | None):
    if not us_session:
        raise UsLaceyPortalAuthError("Sign in to continue.", code="session_invalid")
    identity = resolve_us_lacey_session(us_session)
    entitlement = require_us_lacey_operational_access(
        organization_id=identity.organization_id
    )
    return identity, entitlement


def _operation_error_page(request: Request, message: str, *, status_code: int = 400) -> HTMLResponse:
    return _html(render_message_page(request=request, title="Operation unavailable.", message=message, authenticated=True, action_href="/operations", action_label="Return to operations"), status_code=status_code)


def _detail_page(*, request: Request, identity, operation_public_id: str, us_session: str, error: str | None = None, notice: str | None = None, status_code: int = 200):
    service = UsLaceyOperationService()
    detail = service.get_detail(
        organization_id=identity.organization_id,
        operation_public_id=operation_public_id,
    )
    try:
        engine2_dossier = UsLaceyEngineDossierService().get_dossier(
            organization_id=identity.organization_id, operation_public_id=detail.public_id
        )
    except Exception:
        LOGGER.exception("Engine 2 dossier preview failed", extra={"organization_id": identity.organization_id})
        engine2_dossier = Engine2DossierView(Engine2DossierAvailability.INVALID, safe_status_message="The stored dossier could not be safely read.")
    review_tokens = {
        field.id: us_lacey_csrf_token(
            session_token=us_session,
            purpose=f"review:{detail.public_id}:{field.id}",
        )
        for field in detail.fields
        if field.status in {"MISSING", "REVIEW"}
    }
    return _html(
        render_operation_detail(
            request=request,
            identity=identity,
            detail=detail,
            engine2_dossier=engine2_dossier,
            upload_csrf=us_lacey_csrf_token(
                session_token=us_session,
                purpose=f"upload:{detail.public_id}",
            ),
            complete_csrf=us_lacey_csrf_token(
                session_token=us_session,
                purpose=f"complete:{detail.public_id}",
            ),
            review_csrf=review_tokens,
            error=error,
            notice=notice,
        ),
        status_code=status_code,
    )


@app.get("/health")
def health() -> dict[str, str]:
    """Process liveness only; does not claim infrastructure readiness."""
    return {"status": "healthy", "service": "us-lacey-pilot"}


@app.get("/ready")
def ready() -> dict[str, str]:
    """Fail closed until isolated runtime, commercial, legal and email config exists."""
    try:
        config = load_us_lacey_runtime_config()
        load_us_lacey_commercial_config()
        load_us_lacey_portal_config()
        load_us_lacey_email_config()
    except (
        UsLaceyConfigurationError,
        UsLaceyCommercialConfigurationError,
        UsLaceyPortalConfigurationError,
        UsLaceyEmailConfigurationError,
    ) as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="U.S. Lacey pilot runtime is not safely configured.",
        ) from exc

    return {
        "status": "ready",
        "service": "us-lacey-pilot",
        "environment": config.environment,
        "hostname": config.app_hostname,
    }


@app.get("/", response_class=HTMLResponse)
def portal_home(
    us_session: str | None = Cookie(None, alias=US_LACEY_SESSION_COOKIE),
):
    if us_session:
        try:
            identity = resolve_us_lacey_session(us_session)
            target = "/operations" if identity.account_status in {"ACTIVE", "PILOT"} else "/billing"
            return RedirectResponse(target, status_code=303)
        except UsLaceyPortalAuthError:
            pass
    return RedirectResponse("/login", status_code=303)


@app.get("/signup", response_class=HTMLResponse)
def signup_page(request: Request):
    try:
        commercial, portal = _load_customer_configuration()
    except (
        UsLaceyConfigurationError,
        UsLaceyCommercialConfigurationError,
        UsLaceyPortalConfigurationError,
    ):
        return _configuration_error_page(request)
    return _html(render_signup(request=request, commercial=commercial, portal=portal))


@app.post("/signup", response_class=HTMLResponse)
def signup_submit(
    request: Request,
    legal_name: str = Form(...),
    business_type: str = Form(...),
    admin_name: str = Form(...),
    admin_email: str = Form(...),
    password: str = Form(...),
    accept_terms: str | None = Form(None),
    accept_privacy: str | None = Form(None),
    accept_beta: str | None = Form(None),
):
    try:
        commercial, portal = _load_customer_configuration()
    except (
        UsLaceyConfigurationError,
        UsLaceyCommercialConfigurationError,
        UsLaceyPortalConfigurationError,
    ):
        return _configuration_error_page(request)

    if {accept_terms, accept_privacy, accept_beta} != {"yes"}:
        return _html(
            render_signup(
                request=request,
                commercial=commercial,
                portal=portal,
                error="You must accept all three legal documents to create an account.",
            ),
            status_code=400,
        )

    def deliver(recipient: str, company_name: str, token: str) -> None:
        send_us_lacey_verification_email(
            recipient=recipient,
            company_name=company_name,
            verification_token=token,
            public_origin=portal.public_origin,
        )

    try:
        register_us_lacey_company(
            legal_name=legal_name,
            business_type=business_type,
            admin_name=admin_name,
            admin_email=admin_email,
            password=password,
            commercial_config=commercial,
            verification_delivery=deliver,
        )
    except (UsLaceySelfServiceError, UsLaceyEmailConfigurationError) as exc:
        return _html(
            render_signup(request=request, commercial=commercial, portal=portal, error=str(exc)),
            status_code=400,
        )

    return _html(render_check_email(request=request, email=admin_email.strip().lower()), status_code=201)


@app.get("/verify-email", response_class=HTMLResponse)
def verify_email_page(request: Request, token: str = ""):
    if not token.strip():
        return _html(
            render_verification_error(request=request, message="Verification token is missing."), status_code=400
        )
    try:
        result = verify_us_lacey_email(token)
    except UsLaceySelfServiceError as exc:
        return _html(render_verification_error(request=request, message=str(exc)), status_code=400)
    if result.account_status != "PAYMENT_PENDING":
        return _html(
            render_verification_error(request=request, message="Unexpected account state after verification."),
            status_code=409,
        )
    return RedirectResponse("/login?verified=1", status_code=303)


@app.get("/login", response_class=HTMLResponse)
def login_page(
    request: Request,
    verified: str | None = None,
    us_session: str | None = Cookie(None, alias=US_LACEY_SESSION_COOKIE),
):
    if us_session:
        try:
            identity = resolve_us_lacey_session(us_session)
            target = "/operations" if identity.account_status in {"ACTIVE", "PILOT"} else "/billing"
            return RedirectResponse(target, status_code=303)
        except UsLaceyPortalAuthError:
            pass
    return _html(render_login(request=request, verified=verified == "1"))


@app.post("/login", response_class=HTMLResponse)
def login_submit(
    request: Request,
    email: str = Form(...),
    password: str = Form(...),
):
    try:
        portal = load_us_lacey_portal_config()
    except (UsLaceyConfigurationError, UsLaceyPortalConfigurationError):
        return _configuration_error_page(request)

    client_ip, user_agent = _request_metadata(request)
    try:
        login_result = login_us_lacey_user(
            email=email,
            password=password,
            client_ip=client_ip,
            user_agent=user_agent,
        )
    except UsLaceyPortalAuthError as exc:
        response_code = 403 if exc.code in {
            "email_unverified",
            "account_suspended",
            "account_disabled",
            "account_unavailable",
        } else 401
        return _html(render_login(request=request, error=str(exc)), status_code=response_code)

    target = "/operations" if login_result.identity.account_status in {"ACTIVE", "PILOT"} else "/billing"
    response = RedirectResponse(target, status_code=303)
    max_age = max(
        1,
        int((login_result.expires_at - datetime.now(timezone.utc)).total_seconds()),
    )
    response.set_cookie(
        key=US_LACEY_SESSION_COOKIE,
        value=login_result.session_token,
        max_age=max_age,
        httponly=True,
        secure=portal.session_cookie_secure,
        samesite="lax",
        path="/",
    )
    response.headers["Cache-Control"] = "no-store"
    return response


@app.get("/billing", response_class=HTMLResponse)
def billing_page(
    request: Request,
    us_session: str | None = Cookie(None, alias=US_LACEY_SESSION_COOKIE),
):
    if not us_session:
        return RedirectResponse("/login", status_code=303)
    try:
        identity = resolve_us_lacey_session(us_session)
        billing = get_us_lacey_billing_summary(organization_id=identity.organization_id)
        commercial = load_us_lacey_commercial_config()
    except UsLaceyPortalAuthError:
        return _login_redirect(clear_cookie=True)
    except (UsLaceySelfServiceError, UsLaceyCommercialConfigurationError):
        return _html(render_message_page(request=request, title="Billing temporarily unavailable.", message="We could not load this account's billing state.", authenticated=True, action_href="/billing", action_label="Try billing again"), status_code=503)

    return _html(render_billing(request=request, identity=identity, billing=billing, commercial=commercial))


@app.get("/operations", response_class=HTMLResponse)
def operations_page(
    request: Request,
    us_session: str | None = Cookie(None, alias=US_LACEY_SESSION_COOKIE),
):
    try:
        identity, entitlement = _operational_context(us_session)
        items = UsLaceyOperationService().list_operations(
            organization_id=identity.organization_id,
            limit=100,
        )
    except UsLaceyPortalAuthError:
        return _login_redirect(clear_cookie=bool(us_session))
    except UsLaceyOperationalAccessError:
        return RedirectResponse("/billing", status_code=303)
    return _html(render_operations(request=request, identity=identity, operations=items, entitlement=entitlement))


@app.get("/operations/new", response_class=HTMLResponse)
def new_operation_page(
    request: Request,
    us_session: str | None = Cookie(None, alias=US_LACEY_SESSION_COOKIE),
):
    try:
        identity, entitlement = _operational_context(us_session)
    except UsLaceyPortalAuthError:
        return _login_redirect(clear_cookie=bool(us_session))
    except UsLaceyOperationalAccessError:
        return RedirectResponse("/billing", status_code=303)
    if entitlement.remaining_operations <= 0:
        return _operation_error_page(request, "This workspace has reached its current operation limit.", status_code=409)
    return _html(
        render_new_operation(
            request=request,
            identity=identity,
            entitlement=entitlement,
            csrf_token=us_lacey_csrf_token(session_token=us_session or "", purpose="operation:create"),
        )
    )


@app.post("/operations/new", response_class=HTMLResponse)
def new_operation_submit(
    request: Request,
    client_reference: str = Form(...),
    importer_name: str = Form(""),
    consignee_name: str = Form(""),
    broker_name: str = Form(""),
    supplier_name: str = Form(""),
    operation_date: str = Form(""),
    line_references: str = Form(""),
    csrf_token: str = Form(...),
    us_session: str | None = Cookie(None, alias=US_LACEY_SESSION_COOKIE),
):
    try:
        identity, entitlement = _operational_context(us_session)
        verify_us_lacey_csrf(
            session_token=us_session or "",
            purpose="operation:create",
            submitted_token=csrf_token,
        )
        parsed_date = date.fromisoformat(operation_date) if operation_date.strip() else None
        parsed_lines = tuple(
            value.strip()
            for value in re.split(r"[,;\n]+", line_references)
            if value.strip()
        ) or None
        created = create_us_lacey_customer_operation(
            organization_id=identity.organization_id,
            user_id=identity.user_id,
            client_reference=client_reference,
            importer_name=importer_name or None,
            consignee_name=consignee_name or None,
            broker_name=broker_name or None,
            supplier_name=supplier_name or None,
            operation_date=parsed_date,
            line_references=parsed_lines,
        )
        return RedirectResponse(f"/operations/{created.public_id}", status_code=303)
    except UsLaceyPortalAuthError:
        return _login_redirect(clear_cookie=bool(us_session))
    except UsLaceyOperationalAccessError:
        return RedirectResponse("/billing", status_code=303)
    except (UsLaceyCsrfError, UsLaceyOperationError, ValueError) as exc:
        return _html(
            render_new_operation(
                request=request,
                identity=identity,
                entitlement=entitlement,
                csrf_token=us_lacey_csrf_token(session_token=us_session or "", purpose="operation:create"),
                error=str(exc),
            ),
            status_code=400,
        )


@app.get("/operations/{operation_public_id}", response_class=HTMLResponse)
def operation_detail_page(
    operation_public_id: str,
    request: Request,
    us_session: str | None = Cookie(None, alias=US_LACEY_SESSION_COOKIE),
):
    try:
        identity, _entitlement = _operational_context(us_session)
        return _detail_page(
            request=request,
            identity=identity,
            operation_public_id=operation_public_id,
            us_session=us_session or "",
        )
    except UsLaceyPortalAuthError:
        return _login_redirect(clear_cookie=bool(us_session))
    except UsLaceyOperationalAccessError:
        return RedirectResponse("/billing", status_code=303)
    except UsLaceyOperationNotFound:
        return _operation_error_page(request, "Operation not found.", status_code=404)


@app.post("/operations/{operation_public_id}/upload", response_class=HTMLResponse)
async def operation_upload_submit(
    operation_public_id: str,
    request: Request,
    document: UploadFile = File(...),
    document_role: str = Form("UNKNOWN"),
    csrf_token: str = Form(...),
    us_session: str | None = Cookie(None, alias=US_LACEY_SESSION_COOKIE),
):
    try:
        identity, _entitlement = _operational_context(us_session)
        verify_us_lacey_csrf(
            session_token=us_session or "",
            purpose=f"upload:{operation_public_id}",
            submitted_token=csrf_token,
        )
        content = await document.read()
        if not content:
            raise UsLaceyWorkflowError("The uploaded document is empty.")
        upload_and_enqueue_us_lacey_document(
            organization_id=identity.organization_id,
            user_id=identity.user_id,
            operation_public_id=operation_public_id,
            filename=document.filename or "document",
            content_type=document.content_type or "application/octet-stream",
            content=content,
            document_role=document_role,
        )
        return RedirectResponse(
            f"/operations/{operation_public_id}?uploaded=1",
            status_code=303,
        )
    except UsLaceyPortalAuthError:
        return _login_redirect(clear_cookie=bool(us_session))
    except UsLaceyOperationalAccessError:
        return RedirectResponse("/billing", status_code=303)
    except (UsLaceyCsrfError, UsLaceyWorkflowError, UsLaceyOperationError, ValueError) as exc:
        try:
            return _detail_page(
                request=request,
                identity=identity,
                operation_public_id=operation_public_id,
                us_session=us_session or "",
                error=str(exc),
                status_code=400,
            )
        except UsLaceyOperationNotFound:
            return _operation_error_page(request, "Operation not found.", status_code=404)


@app.post("/operations/{operation_public_id}/review/{field_id}", response_class=HTMLResponse)
def operation_review_submit(
    operation_public_id: str,
    field_id: int,
    request: Request,
    action: str = Form(...),
    value: str = Form(""),
    candidate_id: int | None = Form(None),
    reason_code: str = Form(""),
    csrf_token: str = Form(...),
    us_session: str | None = Cookie(None, alias=US_LACEY_SESSION_COOKIE),
):
    try:
        identity, _entitlement = _operational_context(us_session)
        verify_us_lacey_csrf(
            session_token=us_session or "",
            purpose=f"review:{operation_public_id}:{field_id}",
            submitted_token=csrf_token,
        )
        review_us_lacey_field(
            organization_id=identity.organization_id,
            operation_public_id=operation_public_id,
            field_id=field_id,
            user_id=identity.user_id,
            user_email=identity.email,
            action=action,
            value=value or None,
            candidate_id=candidate_id,
            reason_code=reason_code or None,
        )
        return RedirectResponse(f"/operations/{operation_public_id}", status_code=303)
    except UsLaceyPortalAuthError:
        return _login_redirect(clear_cookie=bool(us_session))
    except UsLaceyOperationalAccessError:
        return RedirectResponse("/billing", status_code=303)
    except (UsLaceyCsrfError, UsLaceyReviewError, UsLaceyOperationNotFound) as exc:
        try:
            return _detail_page(
                request=request,
                identity=identity,
                operation_public_id=operation_public_id,
                us_session=us_session or "",
                error=str(exc),
                status_code=400,
            )
        except UsLaceyOperationNotFound:
            return _operation_error_page(request, "Operation not found.", status_code=404)


@app.post("/operations/{operation_public_id}/complete", response_class=HTMLResponse)
def operation_complete_submit(
    operation_public_id: str,
    request: Request,
    csrf_token: str = Form(...),
    us_session: str | None = Cookie(None, alias=US_LACEY_SESSION_COOKIE),
):
    try:
        identity, _entitlement = _operational_context(us_session)
        verify_us_lacey_csrf(
            session_token=us_session or "",
            purpose=f"complete:{operation_public_id}",
            submitted_token=csrf_token,
        )
        finalize_us_lacey_review(
            organization_id=identity.organization_id,
            operation_public_id=operation_public_id,
            user_id=identity.user_id,
            user_email=identity.email,
        )
        return RedirectResponse(f"/operations/{operation_public_id}?completed=1", status_code=303)
    except UsLaceyPortalAuthError:
        return _login_redirect(clear_cookie=bool(us_session))
    except UsLaceyOperationalAccessError:
        return RedirectResponse("/billing", status_code=303)
    except (UsLaceyCsrfError, UsLaceyReviewError, UsLaceyOperationNotFound) as exc:
        try:
            return _detail_page(
                request=request,
                identity=identity,
                operation_public_id=operation_public_id,
                us_session=us_session or "",
                error=str(exc),
                status_code=409,
            )
        except UsLaceyOperationNotFound:
            return _operation_error_page(request, "Operation not found.", status_code=404)


def _export_response(*, request: Request, operation_public_id: str, us_session: str | None, kind: str):
    try:
        identity, _entitlement = _operational_context(us_session)
        detail = UsLaceyOperationService().get_detail(
            organization_id=identity.organization_id,
            operation_public_id=operation_public_id,
        )
        if detail.status != "COMPLETED":
            return _operation_error_page(
                request,
                "Complete human review before exporting the preparation package.",
                status_code=409,
            )
        if kind == "xlsx":
            payload = export_us_lacey_xlsx(
                organization_id=identity.organization_id,
                operation_public_id=operation_public_id,
            )
            media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        else:
            payload = export_us_lacey_csv(
                organization_id=identity.organization_id,
                operation_public_id=operation_public_id,
            )
            media_type = "text/csv; charset=utf-8"
        response = Response(content=payload, media_type=media_type)
        response.headers["Content-Disposition"] = f'attachment; filename="lacey-preparation-{detail.public_id}.{kind}"'
        response.headers["Cache-Control"] = "no-store, max-age=0"
        response.headers["X-Content-Type-Options"] = "nosniff"
        return response
    except UsLaceyPortalAuthError:
        return _login_redirect(clear_cookie=bool(us_session))
    except UsLaceyOperationalAccessError:
        return RedirectResponse("/billing", status_code=303)
    except UsLaceyOperationNotFound:
        return _operation_error_page(request, "Operation not found.", status_code=404)
    except UsLaceyReviewError as exc:
        return _operation_error_page(request, str(exc), status_code=400)


@app.get("/operations/{operation_public_id}/export.xlsx")
def operation_export_xlsx(
    operation_public_id: str,
    request: Request,
    us_session: str | None = Cookie(None, alias=US_LACEY_SESSION_COOKIE),
):
    return _export_response(request=request, operation_public_id=operation_public_id, us_session=us_session, kind="xlsx")


@app.get("/operations/{operation_public_id}/export.csv")
def operation_export_csv(
    operation_public_id: str,
    request: Request,
    us_session: str | None = Cookie(None, alias=US_LACEY_SESSION_COOKIE),
):
    return _export_response(request=request, operation_public_id=operation_public_id, us_session=us_session, kind="csv")


@app.post("/logout")
def logout_submit(
    us_session: str | None = Cookie(None, alias=US_LACEY_SESSION_COOKIE),
):
    if us_session:
        logout_us_lacey_user(us_session)
    response = RedirectResponse("/login", status_code=303)
    try:
        secure = load_us_lacey_portal_config().session_cookie_secure
    except Exception:
        secure = True
    response.delete_cookie(
        US_LACEY_SESSION_COOKIE,
        httponly=True,
        secure=secure,
        samesite="lax",
        path="/",
    )
    response.headers["Cache-Control"] = "no-store"
    return response
