"""Pure-ASGI CSRF enforcement for unsafe API requests authenticated by cookies."""
from __future__ import annotations

from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.types import (
    ASGIApp,
    Receive,
    Scope,
    Send,
)

from litoral_trace.auth.sessions import (
    ACCESS_TOKEN_COOKIE_KEY,
    REFRESH_TOKEN_COOKIE_KEY,
)
from litoral_trace.auth.tokens import verify_jwt_token
from litoral_trace.web.csrf import (
    CSRF_HEADER_NAME,
    csrf_subject_from_access_payload,
    get_csrf_browser_nonce,
    refresh_csrf_max_age_seconds,
    verify_csrf_browser_binding,
    verify_csrf_token,
)


SAFE_METHODS = frozenset(
    {"GET", "HEAD", "OPTIONS", "TRACE"}
)

COOKIE_CSRF_EXEMPT_PATHS = frozenset(
    {
        "/api/v1/auth/login",
    }
)

REFRESH_COOKIE_MUTATION_PATHS = frozenset(
    {
        "/api/v1/auth/refresh",
        "/api/v1/auth/logout",
    }
)


def _has_bearer_authorization(
    request: Request,
) -> bool:
    authorization = (
        request.headers.get("authorization")
        or ""
    ).strip()

    if not authorization.lower().startswith(
        "bearer "
    ):
        return False

    return bool(
        authorization.split(" ", 1)[1].strip()
    )


def validate_cookie_csrf_request(
    request: Request,
    *,
    secret_key: str | None = None,
) -> str | None:
    """Return an error code when an unsafe cookie-auth API request lacks CSRF."""

    if request.method.upper() in SAFE_METHODS:
        return None

    path = request.url.path

    if not path.startswith("/api/v1/"):
        return None

    if path in COOKIE_CSRF_EXEMPT_PATHS:
        return None

    if _has_bearer_authorization(request):
        return None

    access_token = request.cookies.get(
        ACCESS_TOKEN_COOKIE_KEY
    )
    refresh_token = request.cookies.get(
        REFRESH_TOKEN_COOKIE_KEY
    )

    if not access_token and not refresh_token:
        return None

    csrf_token = (
        request.headers.get(CSRF_HEADER_NAME)
        or ""
    ).strip()

    browser_nonce = get_csrf_browser_nonce(
        request
    )

    if not csrf_token or browser_nonce is None:
        return "csrf_missing"

    # Session renewal accepts only the dedicated anonymous-subject CSRF token,
    # signed and bound to the HttpOnly browser nonce. Its verification window
    # matches the refresh-session TTL so a legitimately suspended tab can renew
    # after the one-hour regular form-CSRF window. A normal session-bound CSRF
    # token is deliberately not interchangeable with this refresh capability.
    if refresh_token and path == "/api/v1/auth/refresh":
        if verify_csrf_token(
            csrf_token,
            subject=None,
            browser_nonce=browser_nonce,
            max_age_seconds=refresh_csrf_max_age_seconds(),
            secret_key=secret_key,
        ):
            return None
        return "csrf_invalid"

    if access_token:
        payload = verify_jwt_token(
            access_token,
            secret_key=secret_key,
            expected_token_type="access",
        )

        if payload:
            try:
                subject = (
                    csrf_subject_from_access_payload(
                        payload
                    )
                )
            except ValueError:
                return "csrf_subject_invalid"

            if verify_csrf_token(
                csrf_token,
                subject=subject,
                browser_nonce=browser_nonce,
                secret_key=secret_key,
            ):
                return None

            return "csrf_invalid"

    if (
        refresh_token
        and path in REFRESH_COOKIE_MUTATION_PATHS
        and verify_csrf_browser_binding(
            csrf_token,
            browser_nonce=browser_nonce,
            secret_key=secret_key,
        )
    ):
        return None

    return "csrf_invalid"


class CookieApiCsrfMiddleware:
    """Require CSRF for unsafe `/api/v1` requests that rely on auth cookies."""

    def __init__(
        self,
        app: ASGIApp,
    ) -> None:
        self.app = app

    async def __call__(
        self,
        scope: Scope,
        receive: Receive,
        send: Send,
    ) -> None:
        if scope["type"] != "http":
            await self.app(
                scope,
                receive,
                send,
            )
            return

        request = Request(scope)

        error_code = validate_cookie_csrf_request(
            request
        )

        if error_code is None:
            await self.app(
                scope,
                receive,
                send,
            )
            return

        response = JSONResponse(
            status_code=403,
            content={
                "detail": "CSRF validation failed.",
                "code": error_code,
            },
            headers={
                "Cache-Control": "no-store",
            },
        )

        await response(
            scope,
            receive,
            send,
        )
