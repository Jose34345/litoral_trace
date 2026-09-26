"""Public zero-touch sandbox provisioning for the U.S. Lacey product."""
from __future__ import annotations

from datetime import datetime, timezone
import logging

from fastapi import APIRouter, Cookie, Form, Request, status
from fastapi.responses import PlainTextResponse, RedirectResponse

from litoral_trace.us_lacey.growth_attribution import (
    OUTREACH_ATTRIBUTION_COOKIE,
    OUTREACH_ATTRIBUTION_COOKIE_MAX_AGE,
    UsLaceyOutreachError,
    bind_outreach_to_sandbox,
    open_outreach_link,
)
from litoral_trace.us_lacey.portal_auth import (
    US_LACEY_SESSION_COOKIE,
    UsLaceyPortalAuthError,
    resolve_us_lacey_session,
)
from litoral_trace.us_lacey.portal_config import (
    UsLaceyPortalConfigurationError,
    load_us_lacey_portal_config,
)
from litoral_trace.us_lacey.sandbox import (
    UsLaceySandboxError,
    provision_us_lacey_sandbox,
)
from litoral_trace.web.templates import render_template


router = APIRouter(tags=["U.S. Lacey Sandbox"])
LOGGER = logging.getLogger(__name__)


def _harden_public_response(response):
    response.headers["Cache-Control"] = "no-store, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["X-Robots-Tag"] = "noindex, nofollow, noarchive"
    response.headers["Referrer-Policy"] = "no-referrer"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Content-Security-Policy"] = (
        "default-src 'none'; "
        "style-src 'self'; "
        "form-action 'self'; "
        "base-uri 'none'; "
        "frame-ancestors 'none'"
    )
    return response


@router.get("/sandbox/ref/{slug}", include_in_schema=False)
def sandbox_outreach_referral(slug: str):
    """Record a first-party outreach click, then enter the normal sandbox."""

    try:
        portal = load_us_lacey_portal_config()
        attribution = open_outreach_link(slug)
    except UsLaceyPortalConfigurationError:
        return _harden_public_response(
            PlainTextResponse(
                "Sandbox is temporarily unavailable.",
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            )
        )
    except UsLaceyOutreachError:
        return _harden_public_response(
            PlainTextResponse(
                "Sandbox link is unavailable.",
                status_code=status.HTTP_404_NOT_FOUND,
            )
        )

    response = RedirectResponse(
        "/sandbox/start",
        status_code=status.HTTP_303_SEE_OTHER,
    )
    response.set_cookie(
        key=OUTREACH_ATTRIBUTION_COOKIE,
        value=str(attribution.attribution_session_id),
        max_age=OUTREACH_ATTRIBUTION_COOKIE_MAX_AGE,
        httponly=True,
        secure=portal.session_cookie_secure,
        samesite="lax",
        path="/",
    )
    return _harden_public_response(response)


@router.get("/sandbox/start", include_in_schema=False)
def sandbox_start_view(request: Request):
    """Render the consent screen without creating any tenant or browser state."""

    response = render_template(
        request,
        "us_lacey/sandbox_start.html",
        {},
        status_code=status.HTTP_200_OK,
    )
    return _harden_public_response(response)


@router.post("/sandbox/start", include_in_schema=False)
def sandbox_start_provision(
    request: Request,
    consent: str | None = Form(default=None),
    learning_consent: str | None = Form(default=None),
    us_session: str | None = Cookie(None, alias=US_LACEY_SESSION_COOKIE),
    outreach_attribution: str | None = Cookie(
        None,
        alias=OUTREACH_ATTRIBUTION_COOKIE,
    ),
):
    """Provision one isolated four-hour tenant after explicit browser consent."""

    if consent != "accepted":
        return _harden_public_response(
            PlainTextResponse(
                "Sandbox consent is required.",
                status_code=status.HTTP_400_BAD_REQUEST,
            )
        )

    # Existing authenticated visitors keep their current tenant instead of
    # silently replacing it with an anonymous sandbox.
    if us_session:
        try:
            resolve_us_lacey_session(us_session)
            response = RedirectResponse(
                "/operations/new",
                status_code=status.HTTP_303_SEE_OTHER,
            )
            if outreach_attribution:
                response.delete_cookie(
                    OUTREACH_ATTRIBUTION_COOKIE,
                    path="/",
                )
            return _harden_public_response(response)
        except UsLaceyPortalAuthError:
            # Invalid/expired browser state is replaced by a fresh sandbox token.
            pass

    try:
        portal = load_us_lacey_portal_config()
    except UsLaceyPortalConfigurationError:
        return _harden_public_response(
            PlainTextResponse(
                "Sandbox is temporarily unavailable.",
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            )
        )

    client_ip = request.client.host if request.client is not None else None
    user_agent = request.headers.get("user-agent")

    try:
        sandbox = provision_us_lacey_sandbox(
            client_ip=client_ip,
            user_agent=user_agent,
            learning_opt_in=learning_consent == "accepted",
        )
    except UsLaceySandboxError as exc:
        response_status = (
            status.HTTP_429_TOO_MANY_REQUESTS
            if exc.code == "rate_limited"
            else status.HTTP_503_SERVICE_UNAVAILABLE
        )
        response = PlainTextResponse(str(exc), status_code=response_status)
        if response_status == status.HTTP_429_TOO_MANY_REQUESTS:
            response.headers["Retry-After"] = "3600"
        return _harden_public_response(response)

    if outreach_attribution:
        try:
            bind_outreach_to_sandbox(
                attribution_session_id=outreach_attribution,
                session_token=sandbox.session_token,
                organization_id=sandbox.organization_id,
            )
        except UsLaceyOutreachError:
            LOGGER.exception(
                "us_lacey_outreach_bind_failed",
                extra={"organization_id": sandbox.organization_id},
            )

    response = RedirectResponse(
        "/operations/new",
        status_code=status.HTTP_303_SEE_OTHER,
    )
    max_age = max(
        1,
        int(
            (
                sandbox.expires_at
                - datetime.now(timezone.utc)
            ).total_seconds()
        ),
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
    if outreach_attribution:
        response.delete_cookie(
            OUTREACH_ATTRIBUTION_COOKIE,
            path="/",
        )
    return _harden_public_response(response)
