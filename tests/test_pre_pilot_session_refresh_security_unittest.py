from __future__ import annotations

import time

from fastapi import HTTPException, status
from starlette.requests import Request

from litoral_trace.auth.sessions import (
    ACCESS_TOKEN_COOKIE_KEY,
    REFRESH_TOKEN_COOKIE_KEY,
)
from litoral_trace.web import middleware as middleware_web
from litoral_trace.web import runtime as runtime_web
from litoral_trace.web.csrf import (
    CSRF_BROWSER_COOKIE_KEY,
    CSRF_HEADER_NAME,
    CsrfSubject,
    create_csrf_browser_nonce,
    create_csrf_token,
)


SECRET = "pre-pilot-refresh-security-secret-at-least-32-chars"


def _refresh_request(*, browser_nonce: str, csrf_token: str) -> Request:
    cookies = "; ".join(
        (
            f"{ACCESS_TOKEN_COOKIE_KEY}=expired-access-token",
            f"{REFRESH_TOKEN_COOKIE_KEY}=refresh-cookie-value",
            f"{CSRF_BROWSER_COOKIE_KEY}={browser_nonce}",
        )
    )
    scope = {
        "type": "http",
        "http_version": "1.1",
        "method": "POST",
        "scheme": "https",
        "path": "/api/v1/auth/refresh",
        "raw_path": b"/api/v1/auth/refresh",
        "query_string": b"",
        "headers": [
            (b"cookie", cookies.encode("utf-8")),
            (CSRF_HEADER_NAME.lower().encode("ascii"), csrf_token.encode("utf-8")),
        ],
        "client": ("127.0.0.1", 12345),
        "server": ("testserver", 443),
        "root_path": "",
    }
    return Request(scope)


def _html_request() -> Request:
    cookies = "; ".join(
        (
            f"{ACCESS_TOKEN_COOKIE_KEY}=expired-access-token",
            f"{REFRESH_TOKEN_COOKIE_KEY}=still-valid-refresh-token",
        )
    )
    scope = {
        "type": "http",
        "http_version": "1.1",
        "method": "GET",
        "scheme": "https",
        "path": "/dashboard",
        "raw_path": b"/dashboard",
        "query_string": b"",
        "headers": [(b"cookie", cookies.encode("utf-8"))],
        "client": ("127.0.0.1", 12345),
        "server": ("testserver", 443),
        "root_path": "",
    }
    return Request(scope)


def test_suspended_tab_refresh_csrf_can_outlive_regular_one_hour_window(monkeypatch) -> None:
    browser_nonce = create_csrf_browser_nonce()
    issued_at = int(time.time()) - (2 * 60 * 60)
    refresh_csrf = create_csrf_token(
        subject=None,
        browser_nonce=browser_nonce,
        now_epoch=issued_at,
        secret_key=SECRET,
    )
    monkeypatch.setattr(
        middleware_web,
        "refresh_csrf_max_age_seconds",
        lambda: 30 * 24 * 60 * 60,
    )

    assert middleware_web.validate_cookie_csrf_request(
        _refresh_request(
            browser_nonce=browser_nonce,
            csrf_token=refresh_csrf,
        ),
        secret_key=SECRET,
    ) is None


def test_regular_session_csrf_cannot_be_reused_as_refresh_capability(monkeypatch) -> None:
    browser_nonce = create_csrf_browser_nonce()
    regular_session_csrf = create_csrf_token(
        subject=CsrfSubject(
            username="operator",
            organization_id=17,
            session_id=42,
        ),
        browser_nonce=browser_nonce,
        secret_key=SECRET,
    )
    monkeypatch.setattr(
        middleware_web,
        "refresh_csrf_max_age_seconds",
        lambda: 30 * 24 * 60 * 60,
    )

    assert middleware_web.validate_cookie_csrf_request(
        _refresh_request(
            browser_nonce=browser_nonce,
            csrf_token=regular_session_csrf,
        ),
        secret_key=SECRET,
    ) == "csrf_invalid"


def test_expired_html_access_does_not_erase_recoverable_refresh_cookie(monkeypatch) -> None:
    def reject_access(*_args, **_kwargs):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="expired",
        )

    monkeypatch.setattr(
        runtime_web,
        "get_current_tenant_user",
        reject_access,
    )

    user, response = runtime_web.get_authenticated_html_user(
        _html_request()
    )

    assert user is None
    assert response is not None
    assert response.status_code == status.HTTP_303_SEE_OTHER
    assert response.headers.get("location") == "/login"
    assert response.headers.getlist("set-cookie") == []
