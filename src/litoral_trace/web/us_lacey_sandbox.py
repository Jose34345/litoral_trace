"""Public zero-touch sandbox provisioning for the U.S. Lacey product."""
from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Cookie, Form, Request, status
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse

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


router = APIRouter(tags=["U.S. Lacey Sandbox"])


_SANDBOX_START_HTML = """<!doctype html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="robots" content="noindex,nofollow,noarchive">
    <meta name="viewport" content="width=device-width,initial-scale=1">
    <title>Litoral Trace Sandbox</title>
</head>
<body>
    <main>
        <h1>Try Litoral Trace</h1>
        <p>
            This creates a temporary private workspace for testing
            Litoral Trace with your shipment documents.
        </p>
        <p><strong>Files are automatically deleted after 4 hours</strong></p>
        <form method="post" action="/sandbox/start">
            <button type="submit" name="consent" value="accepted">
                Start sandbox
            </button>
        </form>
    </main>
</body>
</html>
"""


def _harden_public_response(response):
    response.headers["Cache-Control"] = "no-store, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["X-Robots-Tag"] = "noindex, nofollow, noarchive"
    response.headers["Referrer-Policy"] = "no-referrer"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Content-Security-Policy"] = (
        "default-src 'none'; "
        "form-action 'self'; "
        "base-uri 'none'; "
        "frame-ancestors 'none'"
    )
    return response


@router.get("/sandbox/start", include_in_schema=False)
def sandbox_start_view():
    """Render the consent screen without creating any tenant or browser state."""

    return _harden_public_response(
        HTMLResponse(
            _SANDBOX_START_HTML,
            status_code=status.HTTP_200_OK,
        )
    )


@router.post("/sandbox/start", include_in_schema=False)
def sandbox_start_provision(
    request: Request,
    consent: str | None = Form(default=None),
    us_session: str | None = Cookie(None, alias=US_LACEY_SESSION_COOKIE),
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
            return _harden_public_response(
                RedirectResponse(
                    "/operations/new",
                    status_code=status.HTTP_303_SEE_OTHER,
                )
            )
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
    return _harden_public_response(response)
