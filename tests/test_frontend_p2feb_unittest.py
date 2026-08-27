from __future__ import annotations

from pathlib import Path

from fastapi.routing import iter_route_contexts
from starlette.requests import Request

from litoral_trace.auth.sessions import (
    ACCESS_TOKEN_COOKIE_KEY,
    REFRESH_TOKEN_COOKIE_KEY,
)
from litoral_trace.auth.tokens import (
    create_jwt_token,
)
from litoral_trace.web.csrf import (
    CSRF_BROWSER_COOKIE_KEY,
    CSRF_HEADER_NAME,
    CsrfSubject,
    create_csrf_browser_nonce,
    create_csrf_token,
    is_valid_csrf_browser_nonce,
    verify_csrf_browser_binding,
    verify_csrf_token,
)
from litoral_trace.web.middleware import (
    validate_cookie_csrf_request,
)
from litoral_trace.web.router import router as web_router


_TEST_SECRET = (
    "p2feb-test-secret-key-"
    + ("x" * 32)
)


def _request(
    *,
    method: str,
    path: str,
    headers: dict[str, str] | None = None,
    cookies: dict[str, str] | None = None,
) -> Request:
    raw_headers: list[tuple[bytes, bytes]] = []

    for key, value in (
        headers
        or {}
    ).items():
        raw_headers.append(
            (
                key.lower().encode("latin-1"),
                value.encode("latin-1"),
            )
        )

    if cookies:
        cookie_value = "; ".join(
            f"{key}={value}"
            for key, value in cookies.items()
        )
        raw_headers.append(
            (
                b"cookie",
                cookie_value.encode("latin-1"),
            )
        )

    scope = {
        "type": "http",
        "asgi": {
            "version": "3.0",
        },
        "http_version": "1.1",
        "method": method.upper(),
        "scheme": "https",
        "path": path,
        "raw_path": path.encode("ascii"),
        "query_string": b"",
        "headers": raw_headers,
        "client": ("127.0.0.1", 55000),
        "server": ("testserver", 443),
        "root_path": "",
    }

    return Request(scope)


def _access_token(
    *,
    subject: CsrfSubject,
) -> str:
    return create_jwt_token(
        {
            "sub": subject.username,
            "org_id": subject.organization_id,
            "org_name": "Tenant Test",
            "role": "admin",
            "email": "user@example.test",
            "sid": subject.session_id,
        },
        expires_in_seconds=3600,
        secret_key=_TEST_SECRET,
        algorithm="HS256",
        issuer="",
        audience="",
    )


def test_browser_nonce_is_high_entropy_urlsafe_value():
    nonce = create_csrf_browser_nonce()

    assert is_valid_csrf_browser_nonce(
        nonce
    )
    assert len(nonce) >= 32


def test_csrf_token_rejects_cross_browser_replay():
    subject = CsrfSubject(
        username="user",
        organization_id=7,
        session_id=11,
    )

    browser_a = (
        "a" * 43
    )
    browser_b = (
        "b" * 43
    )

    token = create_csrf_token(
        subject=subject,
        browser_nonce=browser_a,
        now_epoch=1_000,
        nonce="n" * 24,
        secret_key=_TEST_SECRET,
    )

    assert verify_csrf_token(
        token,
        subject=subject,
        browser_nonce=browser_a,
        now_epoch=1_100,
        secret_key=_TEST_SECRET,
    )

    assert not verify_csrf_token(
        token,
        subject=subject,
        browser_nonce=browser_b,
        now_epoch=1_100,
        secret_key=_TEST_SECRET,
    )


def test_browser_binding_can_secure_refresh_when_access_jwt_is_unavailable():
    subject = CsrfSubject(
        username="user",
        organization_id=7,
        session_id=11,
    )

    browser_nonce = (
        "c" * 43
    )

    token = create_csrf_token(
        subject=subject,
        browser_nonce=browser_nonce,
        now_epoch=1_000,
        nonce="n" * 24,
        secret_key=_TEST_SECRET,
    )

    assert verify_csrf_browser_binding(
        token,
        browser_nonce=browser_nonce,
        now_epoch=1_100,
        secret_key=_TEST_SECRET,
    )


def test_cookie_auth_mutation_requires_valid_session_and_browser_csrf(monkeypatch):
    import litoral_trace.web.middleware as middleware_module

    subject = CsrfSubject(
        username="user",
        organization_id=7,
        session_id=11,
    )

    browser_nonce = (
        "d" * 43
    )

    access_token = _access_token(
        subject=subject
    )

    csrf_token = create_csrf_token(
        subject=subject,
        browser_nonce=browser_nonce,
        nonce="n" * 24,
        secret_key=_TEST_SECRET,
    )

    original_verify = (
        middleware_module.verify_jwt_token
    )

    def verify_with_test_claim_config(
        token,
        **kwargs,
    ):
        kwargs["issuer"] = ""
        kwargs["audience"] = ""
        return original_verify(
            token,
            **kwargs,
        )

    monkeypatch.setattr(
        middleware_module,
        "verify_jwt_token",
        verify_with_test_claim_config,
    )

    request = _request(
        method="POST",
        path="/api/v1/vault/documents",
        headers={
            CSRF_HEADER_NAME: csrf_token,
        },
        cookies={
            ACCESS_TOKEN_COOKIE_KEY: access_token,
            CSRF_BROWSER_COOKIE_KEY: browser_nonce,
        },
    )

    assert (
        validate_cookie_csrf_request(
            request,
            secret_key=_TEST_SECRET,
        )
        is None
    )

    missing_header_request = _request(
        method="POST",
        path="/api/v1/vault/documents",
        cookies={
            ACCESS_TOKEN_COOKIE_KEY: access_token,
            CSRF_BROWSER_COOKIE_KEY: browser_nonce,
        },
    )

    assert (
        validate_cookie_csrf_request(
            missing_header_request,
            secret_key=_TEST_SECRET,
        )
        == "csrf_missing"
    )


def test_bearer_api_mutation_does_not_gain_browser_csrf_requirement():
    request = _request(
        method="POST",
        path="/api/v1/lotes",
        headers={
            "Authorization": (
                "Bearer external-api-token"
            ),
        },
        cookies={
            ACCESS_TOKEN_COOKIE_KEY: "stale-cookie",
        },
    )

    assert (
        validate_cookie_csrf_request(
            request,
            secret_key=_TEST_SECRET,
        )
        is None
    )


def test_refresh_cookie_mutation_accepts_dedicated_browser_binding_without_access_cookie():
    browser_nonce = (
        "e" * 43
    )

    # Refresh has its own anonymous-subject capability. A normal session-bound
    # CSRF token is intentionally not interchangeable with this token.
    csrf_token = create_csrf_token(
        subject=None,
        browser_nonce=browser_nonce,
        nonce="n" * 24,
        secret_key=_TEST_SECRET,
    )

    request = _request(
        method="POST",
        path="/api/v1/auth/refresh",
        headers={
            CSRF_HEADER_NAME: csrf_token,
        },
        cookies={
            REFRESH_TOKEN_COOKIE_KEY: "opaque-refresh-cookie",
            CSRF_BROWSER_COOKIE_KEY: browser_nonce,
        },
    )

    assert (
        validate_cookie_csrf_request(
            request,
            secret_key=_TEST_SECRET,
        )
        is None
    )


def test_html_routes_live_in_dedicated_web_router():
    def has_route(
        path: str,
        method: str,
    ) -> bool:
        return any(
            route.path == path
            and method in (
                route.methods
                or set()
            )
            for route in web_router.routes
        )

    assert has_route("/", "GET")
    assert has_route("/login", "GET")
    assert has_route("/login", "POST")
    assert has_route("/dashboard", "GET")
    assert has_route("/vault", "GET")
    assert has_route("/settings", "GET")
    assert has_route("/admin", "GET")
    assert has_route("/logout", "GET")
    assert has_route("/logout", "POST")


def test_main_mounts_static_and_delegates_html_routes():
    import main

    static_mounts = [
        route
        for route in main.app.routes
        if getattr(route, "path", None)
        == "/static"
    ]

    assert len(static_mounts) == 1

    effective_paths = {
        route_context.path
        for route_context in iter_route_contexts(
            main.app.routes
        )
    }

    expected_web_paths = {
        "/",
        "/login",
        "/dashboard",
        "/vault",
        "/settings",
        "/admin",
        "/logout",
    }

    assert expected_web_paths.issubset(
        effective_paths
    ), (
        "The application did not expose every "
        "server-rendered web path through the "
        "FastAPI router tree. "
        f"Effective paths: {sorted(effective_paths)}"
    )

    assert "/api/v1/auth/login" in effective_paths
    assert "/health" in effective_paths
    assert "/ready" in effective_paths
    assert "/api/v1/info" in effective_paths


def test_templates_wire_browser_csrf_without_changing_visual_phase():
    root = Path(__file__).resolve().parents[1]

    base = (
        root
        / "src/litoral_trace/templates/base.html"
    ).read_text(
        encoding="utf-8"
    )

    app_base = (
        root
        / "src/litoral_trace/templates/app/base_app.html"
    ).read_text(
        encoding="utf-8"
    )

    login = (
        root
        / "src/litoral_trace/templates/login.html"
    ).read_text(
        encoding="utf-8"
    )

    logout = (
        root
        / "src/litoral_trace/templates/logout.html"
    ).read_text(
        encoding="utf-8"
    )

    assert 'name="csrf-token"' in base
    assert (
        "url_for('static', path='/src/js/app.js')"
        in base
    )

    assert 'action="/logout"' in app_base
    assert (
        'name="{{ csrf_form_field }}"'
        in app_base
    )

    assert 'action="/login"' in login
    assert 'name="{{ csrf_form_field }}"' in login

    assert 'action="/logout"' in logout
    assert 'name="{{ csrf_form_field }}"' in logout


def test_base_javascript_covers_htmx_and_same_origin_fetch_mutations():
    root = Path(__file__).resolve().parents[1]

    app_js = (
        root
        / "src/litoral_trace/static/src/js/app.js"
    ).read_text(
        encoding="utf-8"
    )

    assert "htmx:validateUrl" in app_js
    assert "htmx:configRequest" in app_js
    assert "installFetchCsrfBridge" in app_js
    assert "window.location.origin" in app_js
    assert "X-CSRF-Token" in app_js
