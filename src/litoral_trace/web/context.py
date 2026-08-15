"""Secure base context for every server-rendered Litoral Trace template."""
from __future__ import annotations

from typing import Any

from fastapi import Request

from litoral_trace.web.csrf import (
    CSRF_FORM_FIELD,
    CSRF_HEADER_NAME,
    create_csrf_token,
    csrf_subject_from_user,
)
from litoral_trace.web.navigation import build_navigation


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