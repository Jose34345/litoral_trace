"""Standalone ASGI entrypoint for the private U.S. Lacey customer portal.

The process is isolated from the Argentina application. Browser authentication
uses U.S.-database opaque sessions rather than the generic Litoral Trace JWT.
"""
from __future__ import annotations

from datetime import datetime, timezone

from fastapi import Cookie, FastAPI, Form, HTTPException, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse

from litoral_trace.us_lacey.commercial import (
    UsLaceyCommercialConfigurationError,
    load_us_lacey_commercial_config,
)
from litoral_trace.us_lacey.config import (
    UsLaceyConfigurationError,
    load_us_lacey_runtime_config,
)
from litoral_trace.us_lacey.email_delivery import (
    UsLaceyEmailConfigurationError,
    load_us_lacey_email_config,
    send_us_lacey_verification_email,
)
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
from litoral_trace.us_lacey.self_service import (
    UsLaceySelfServiceError,
    get_us_lacey_billing_summary,
    register_us_lacey_company,
    verify_us_lacey_email,
)
from litoral_trace.web.us_lacey_portal_views import (
    render_billing,
    render_check_email,
    render_login,
    render_signup,
    render_verification_error,
    shell,
)


app = FastAPI(
    title="Litoral Trace U.S. Lacey Pilot",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)


def _html(content: str, *, status_code: int = 200) -> HTMLResponse:
    response = HTMLResponse(content=content, status_code=status_code)
    response.headers["Cache-Control"] = "no-store, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "same-origin"
    return response


def _configuration_error_page() -> HTMLResponse:
    body = (
        "<h1>Portal configuration incomplete.</h1>"
        "<section class='card'><div class='error'>The private U.S. portal is not ready for customer signup.</div></section>"
    )
    return _html(shell("Configuration unavailable", body), status_code=503)


def _request_metadata(request: Request) -> tuple[str | None, str | None]:
    client_ip = request.client.host if request.client is not None else None
    user_agent = request.headers.get("user-agent")
    return client_ip, user_agent


def _load_customer_configuration():
    return load_us_lacey_commercial_config(), load_us_lacey_portal_config()


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
            resolve_us_lacey_session(us_session)
            return RedirectResponse("/billing", status_code=303)
        except UsLaceyPortalAuthError:
            pass
    return RedirectResponse("/login", status_code=303)


@app.get("/signup", response_class=HTMLResponse)
def signup_page():
    try:
        commercial, portal = _load_customer_configuration()
    except (
        UsLaceyConfigurationError,
        UsLaceyCommercialConfigurationError,
        UsLaceyPortalConfigurationError,
    ):
        return _configuration_error_page()
    return _html(render_signup(commercial=commercial, portal=portal))


@app.post("/signup", response_class=HTMLResponse)
def signup_submit(
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
        return _configuration_error_page()

    if {accept_terms, accept_privacy, accept_beta} != {"yes"}:
        return _html(
            render_signup(
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
            render_signup(commercial=commercial, portal=portal, error=str(exc)),
            status_code=400,
        )

    return _html(render_check_email(admin_email.strip().lower()), status_code=201)


@app.get("/verify-email", response_class=HTMLResponse)
def verify_email_page(token: str = ""):
    if not token.strip():
        return _html(
            render_verification_error("Verification token is missing."), status_code=400
        )
    try:
        result = verify_us_lacey_email(token)
    except UsLaceySelfServiceError as exc:
        return _html(render_verification_error(str(exc)), status_code=400)
    if result.account_status != "PAYMENT_PENDING":
        return _html(
            render_verification_error("Unexpected account state after verification."),
            status_code=409,
        )
    return RedirectResponse("/login?verified=1", status_code=303)


@app.get("/login", response_class=HTMLResponse)
def login_page(
    verified: str | None = None,
    us_session: str | None = Cookie(None, alias=US_LACEY_SESSION_COOKIE),
):
    if us_session:
        try:
            resolve_us_lacey_session(us_session)
            return RedirectResponse("/billing", status_code=303)
        except UsLaceyPortalAuthError:
            pass
    return _html(render_login(verified=verified == "1"))


@app.post("/login", response_class=HTMLResponse)
def login_submit(
    request: Request,
    email: str = Form(...),
    password: str = Form(...),
):
    try:
        portal = load_us_lacey_portal_config()
    except (UsLaceyConfigurationError, UsLaceyPortalConfigurationError):
        return _configuration_error_page()

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
        return _html(render_login(error=str(exc)), status_code=response_code)

    response = RedirectResponse("/billing", status_code=303)
    max_age = max(
        1,
        int(
            (
                login_result.expires_at
                - datetime.now(timezone.utc)
            ).total_seconds()
        ),
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
    us_session: str | None = Cookie(None, alias=US_LACEY_SESSION_COOKIE),
):
    if not us_session:
        return RedirectResponse("/login", status_code=303)
    try:
        identity = resolve_us_lacey_session(us_session)
        billing = get_us_lacey_billing_summary(
            organization_id=identity.organization_id
        )
        commercial = load_us_lacey_commercial_config()
    except UsLaceyPortalAuthError:
        response = RedirectResponse("/login", status_code=303)
        response.delete_cookie(US_LACEY_SESSION_COOKIE, path="/")
        return response
    except (UsLaceySelfServiceError, UsLaceyCommercialConfigurationError):
        body = (
            "<h1>Billing temporarily unavailable.</h1>"
            "<section class='card'><div class='error'>We could not load this account's billing state.</div></section>"
        )
        return _html(shell("Billing unavailable", body, authenticated=True), status_code=503)

    return _html(
        render_billing(identity=identity, billing=billing, commercial=commercial)
    )


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
