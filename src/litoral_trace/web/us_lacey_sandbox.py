"""Public zero-touch sandbox provisioning for the U.S. Lacey product."""
from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Cookie, Request
from fastapi.responses import PlainTextResponse, RedirectResponse

from litoral_trace.us_lacey.access import (
    UsLaceyOperationalAccessError,
    require_us_lacey_operational_access,
)
from litoral_trace.us_lacey.portal_auth import (
    US_LACEY_SESSION_COOKIE,
    UsLaceyPortalAuthError,
    resolve_us_lacey_session,
    start_us_lacey_sandbox_session,
)
from litoral_trace.us_lacey.portal_config import (
    UsLaceyPortalConfigurationError,
    load_us_lacey_portal_config,
)


router = APIRouter()


def _redirect(path: str) -> RedirectResponse:
    response = RedirectResponse(path, status_code=303)
    response.headers["Cache-Control"] = "no-store, max-age=0"
    response.headers["X-Robots-Tag"] = "noindex, nofollow"
    return response


@router.get("/sandbox/start", include_in_schema=False)
def start_sandbox(
    request: Request,
    us_session: str | None = Cookie(None, alias=US_LACEY_SESSION_COOKIE),
):
    """Create an isolated four-hour tenant and enter the normal operations UI.

    Existing authenticated customers are never replaced by an anonymous sandbox
    cookie. A live sandbox cookie is idempotent: refreshing this URL reuses the
    current tenant instead of provisioning another one.
    """

    if us_session:
        try:
            identity = resolve_us_lacey_session(us_session)
            try:
                entitlement = require_us_lacey_operational_access(
                    organization_id=identity.organization_id
                )
            except UsLaceyOperationalAccessError:
                return _redirect(
                    "/billing"
                    if identity.account_status == "PAYMENT_PENDING"
                    else "/operations"
                )

            if entitlement.is_sandbox:
                target = (
                    "/operations/new"
                    if entitlement.remaining_operations > 0
                    else "/operations"
                )
                return _redirect(target)

            # Never overwrite a paid/pilot customer's authenticated browser.
            return _redirect("/operations")
        except UsLaceyPortalAuthError:
            # Expired/invalid cookies are replaced only after provisioning a fresh
            # isolated sandbox session below.
            pass

    client_ip = request.client.host if request.client is not None else None
    user_agent = request.headers.get("user-agent")

    try:
        portal = load_us_lacey_portal_config()
        sandbox = start_us_lacey_sandbox_session(
            client_ip=client_ip,
            user_agent=user_agent,
        )
    except UsLaceyPortalAuthError as exc:
        rate_limited = exc.code == "sandbox_rate_limited"
        response = PlainTextResponse(
            (
                "Sandbox trial limit reached. Try again later."
                if rate_limited
                else "The sandbox is temporarily unavailable."
            ),
            status_code=429 if rate_limited else 503,
        )
        response.headers["Cache-Control"] = "no-store, max-age=0"
        response.headers["X-Robots-Tag"] = "noindex, nofollow"
        if rate_limited:
            response.headers["Retry-After"] = "3600"
        return response
    except UsLaceyPortalConfigurationError:
        response = PlainTextResponse(
            "The sandbox is temporarily unavailable.",
            status_code=503,
        )
        response.headers["Cache-Control"] = "no-store, max-age=0"
        response.headers["X-Robots-Tag"] = "noindex, nofollow"
        return response

    response = _redirect("/operations/new")
    max_age = max(
        1,
        int((sandbox.expires_at - datetime.now(timezone.utc)).total_seconds()),
    )
    response.set_cookie(
        key=US_LACEY_SESSION_COOKIE,
        value=sandbox.session_token,
        max_age=max_age,
        httponly=True,
        secure=portal.session_cookie_secure,
        samesite="lax",
        path="/",
    )
    return response
