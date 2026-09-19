"""Jinja-backed render helpers for the U.S. Lacey customer portal."""
from __future__ import annotations

from litoral_trace.web.templates import templates


def money(cents: int, currency: str = "USD") -> str:
    return f"{currency} {cents / 100:,.2f}"


def _render(request, name: str, **context: object) -> str:
    """Render Lacey content through the shared Litoral Trace template stack."""
    return templates.get_template(f"us_lacey/{name}.html").render(request=request, **context)


def render_login(*, request, error: str | None = None, verified: bool = False) -> str:
    return _render(request, "login", error=error, verified=verified)


def render_signup(*, request, commercial, portal, error: str | None = None) -> str:
    return _render(request, "signup", commercial=commercial, portal=portal, error=error, money=money)


def render_check_email(*, request, email: str) -> str:
    return _render(request, "check_email", email=email)


def render_verification_error(*, request, message: str) -> str:
    return _render(request, "verification_error", message=message)


def render_billing(*, request, identity, billing, commercial) -> str:
    return _render(request, "billing", identity=identity, billing=billing, commercial=commercial, money=money)


def render_message_page(*, request, title: str, message: str, authenticated: bool = False, action_href: str = "/login", action_label: str = "Return to sign in") -> str:
    return _render(request, "message", title=title, message=message, authenticated=authenticated, action_href=action_href, action_label=action_label)
