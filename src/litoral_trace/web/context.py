"""Secure base context for every server-rendered Litoral Trace template."""
from __future__ import annotations

import time
from typing import Any

from fastapi import Request

from litoral_trace.auth.sessions import ACCESS_TOKEN_COOKIE_KEY
from litoral_trace.auth.tokens import verify_jwt_token
from litoral_trace.config import get_settings
from litoral_trace.web.csrf import (
    CSRF_FORM_FIELD,
    CSRF_HEADER_NAME,
    create_csrf_token,
    csrf_subject_from_user,
)
from litoral_trace.web.navigation import build_navigation


def _session_refresh_after_seconds() -> int:
    """Refresh well before access expiry without creating excessive rotations."""

    access_seconds = get_settings().jwt.access_token_expire_seconds
    return max(15, min(10 * 60, access_seconds // 2))


def _session_access_expires_at_epoch(
    request: Request,
    *,
    user: Any | None,
) -> int | None:
    """Return the verified current access-token expiry for this exact session."""

    if user is None:
        return None

    token = request.cookies.get(ACCESS_TOKEN_COOKIE_KEY)
    if not token:
        return None

    payload = verify_jwt_token(
        token,
        expected_token_type="access",
    )
    if not payload:
        return None

    try:
        expires_at = int(payload.get("exp"))
        organization_id = int(payload.get("org_id"))
        session_id = int(payload.get("sid"))
        expected_organization_id = int(getattr(user, "organization_id", None))
        expected_session_id = int(getattr(user, "session_id", None))
    except (TypeError, ValueError):
        return None

    username = str(payload.get("sub", "")).strip()
    expected_username = str(getattr(user, "username", "")).strip()

    if (
        expires_at <= 0
        or organization_id != expected_organization_id
        or session_id != expected_session_id
        or not username
        or username != expected_username
    ):
        return None

    return expires_at


def build_template_context(
    request: Request,
    *,
    user: Any | None,
    browser_nonce: str,
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build security-owned template variables that callers cannot override."""

    resolved = dict(context or {})

    subject = (
        csrf_subject_from_user(user)
        if user is not None
        else None
    )
    access_expires_at_epoch = _session_access_expires_at_epoch(
        request,
        user=user,
    )

    security_context = {
        "user": user,
        "csrf_token": create_csrf_token(
            subject=subject,
            browser_nonce=browser_nonce,
        ),
        # Refresh uses a separate browser-bound CSRF token so a suspended tab
        # can renew safely even after the short-lived access cookie expired.
        # It carries no tenant/user identity and is still signed + bound to the
        # HttpOnly browser nonce. The refresh token itself remains HttpOnly.
        "refresh_csrf_token": (
            create_csrf_token(
                subject=None,
                browser_nonce=browser_nonce,
            )
            if user is not None
            else ""
        ),
        "session_refresh_after_seconds": (
            _session_refresh_after_seconds()
            if user is not None
            else None
        ),
        # Absolute expiry is derived from the verified HttpOnly access JWT and
        # matched back to the hydrated tenant/session. Pair it with server time
        # from the same rendered response so browser clock skew never changes
        # the renewal delay.
        "session_access_expires_at_epoch": access_expires_at_epoch,
        "session_server_now_epoch": (
            int(time.time())
            if access_expires_at_epoch is not None
            else None
        ),
        "csrf_header_name": CSRF_HEADER_NAME,
        "csrf_form_field": CSRF_FORM_FIELD,
        "navigation": (
            build_navigation(
                user,
                current_path=request.url.path,
            )
            if user is not None
            else ()
        ),
    }

    resolved.update(security_context)
    return resolved
