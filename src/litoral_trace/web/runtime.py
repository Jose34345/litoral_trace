"""Shared authentication, rendering and response helpers for HTML routes."""
from __future__ import annotations

from typing import Any

from fastapi import HTTPException, Request, Response, status
from fastapi.responses import HTMLResponse, RedirectResponse

from litoral_trace.api.auth import (
    clear_auth_cookies,
    get_current_tenant_user,
)
from litoral_trace.auth.rbac import (
    Permission,
    ensure_permission,
)
from litoral_trace.auth.sessions import (
    ACCESS_TOKEN_COOKIE_KEY,
)
from litoral_trace.web.context import (
    build_template_context,
)
from litoral_trace.web.csrf import (
    clear_csrf_browser_cookie,
    create_csrf_browser_nonce,
    get_or_create_csrf_browser_nonce,
    set_csrf_browser_cookie,
)
from litoral_trace.web.templates import (
    render_template,
)


def copy_response_cookies(
    *,
    source: Response,
    target: Response,
) -> None:
    """Copy Set-Cookie headers emitted by service-level auth handlers."""

    for set_cookie_header in source.headers.getlist(
        "set-cookie"
    ):
        target.headers.append(
            "set-cookie",
            set_cookie_header,
        )


def redirect_to_login(
    *,
    clear_cookies: bool,
) -> RedirectResponse:
    response = RedirectResponse(
        url="/login",
        status_code=status.HTTP_303_SEE_OTHER,
    )

    if clear_cookies:
        clear_auth_cookies(response)

    return response


def render_access_denied() -> HTMLResponse:
    return HTMLResponse(
        status_code=status.HTTP_403_FORBIDDEN,
        content=(
            "<!DOCTYPE html>"
            "<html lang='es'><head><meta charset='UTF-8'>"
            "<meta name='viewport' "
            "content='width=device-width, initial-scale=1.0'>"
            "<title>Acceso denegado</title></head>"
            "<body style='font-family: sans-serif; padding: 2rem;'>"
            "<h2>Acceso denegado</h2>"
            "<p>La cuenta autenticada no posee permisos "
            "para esta vista.</p>"
            "<p><a href='/'>Volver al inicio</a></p>"
            "</body></html>"
        ),
    )


def render_csrf_failure() -> HTMLResponse:
    return HTMLResponse(
        status_code=status.HTTP_403_FORBIDDEN,
        content=(
            "<!DOCTYPE html>"
            "<html lang='es'><head><meta charset='UTF-8'>"
            "<meta name='viewport' "
            "content='width=device-width, initial-scale=1.0'>"
            "<title>Solicitud expirada</title></head>"
            "<body style='font-family: sans-serif; padding: 2rem;'>"
            "<h2>Solicitud expirada</h2>"
            "<p>El formulario de seguridad vencio o no corresponde "
            "a esta sesion.</p>"
            "<p><a href='/dashboard'>Volver al dashboard</a></p>"
            "</body></html>"
        ),
    )


def get_authenticated_html_user(
    request: Request,
):
    """Hydrate the current browser session without inventing tenant context."""

    session_jwt = request.cookies.get(
        ACCESS_TOKEN_COOKIE_KEY
    )

    if not session_jwt:
        return None, redirect_to_login(
            clear_cookies=False
        )

    try:
        user = get_current_tenant_user(
            session_jwt=session_jwt
        )

        if user.session_id is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Sesion HTML invalida o incompleta.",
            )

        return user, None

    except HTTPException as exc:
        if (
            exc.status_code
            == status.HTTP_401_UNAUTHORIZED
        ):
            return None, redirect_to_login(
                clear_cookies=True
            )

        if (
            exc.status_code
            == status.HTTP_403_FORBIDDEN
        ):
            return None, render_access_denied()

        raise


def get_html_route_user(
    request: Request,
    *,
    required_permission: Permission,
):
    user, denied_response = (
        get_authenticated_html_user(request)
    )

    if denied_response is not None:
        return None, denied_response

    try:
        ensure_permission(
            user,
            required_permission,
        )
    except HTTPException as exc:
        if (
            exc.status_code
            == status.HTTP_403_FORBIDDEN
        ):
            return None, render_access_denied()
        raise

    return user, None


def render_web_template(
    request: Request,
    name: str,
    *,
    user: Any | None,
    context: dict[str, Any] | None = None,
    status_code: int = 200,
) -> HTMLResponse:
    """Render a no-store HTML response with a browser-bound CSRF token."""

    browser_nonce, created = (
        get_or_create_csrf_browser_nonce(
            request
        )
    )

    template_context = build_template_context(
        request,
        user=user,
        browser_nonce=browser_nonce,
        context=context,
    )

    response = render_template(
        request,
        name,
        template_context,
        status_code=status_code,
    )

    response.headers["Cache-Control"] = (
        "no-store, max-age=0"
    )
    response.headers["Pragma"] = "no-cache"

    # Authenticated renders are also the synchronization heartbeat used by the
    # browser session-renewal layer. Re-issuing the same HttpOnly nonce only
    # slides its Max-Age; it does not expose or rotate the binding. This keeps
    # the 8-hour browser binding alive while an authenticated application tab is
    # actively renewing, without extending anonymous cookies unnecessarily.
    if created or user is not None:
        set_csrf_browser_cookie(
            response,
            browser_nonce,
        )

    return response


def rotate_csrf_browser_cookie(
    response: Response,
) -> None:
    """Rotate browser CSRF binding after authentication boundary changes."""

    set_csrf_browser_cookie(
        response,
        create_csrf_browser_nonce(),
    )


def clear_browser_security_cookies(
    response: Response,
) -> None:
    clear_csrf_browser_cookie(response)
