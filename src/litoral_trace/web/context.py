"""Secure base context for every server-rendered Litoral Trace template."""
from __future__ import annotations

from typing import Any

from fastapi import Request

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
