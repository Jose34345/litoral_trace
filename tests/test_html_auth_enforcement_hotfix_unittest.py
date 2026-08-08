from __future__ import annotations

import asyncio
import sys
from datetime import timedelta
from http.cookies import SimpleCookie
from pathlib import Path
from urllib.parse import urlencode
from uuid import uuid4

import pytest
from fastapi import Response
from sqlalchemy import delete, select
from starlette.requests import Request

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import main
from litoral_trace.api.auth import LoginRequest, login_b2b
from litoral_trace.auth.passwords import hash_password
from litoral_trace.auth.sessions import (
    ACCESS_TOKEN_COOKIE_KEY,
    REFRESH_TOKEN_COOKIE_KEY,
    utc_now,
)
from litoral_trace.auth.tokens import create_jwt_token, verify_jwt_token
from litoral_trace.db.engine import get_db_session
from litoral_trace.db.init_db import get_non_production_superadmin_seed
from litoral_trace.db.models import Organization, User, UserSession


@pytest.fixture(autouse=True)
def cleanup_user_sessions():
    db_session = get_db_session()
    db_session.execute(delete(UserSession))
    admin_user = db_session.execute(
        select(User).where(User.username == "admin")
    ).scalar_one()
    admin_user.last_login_at = None
    db_session.commit()
    db_session.close()

    yield

    db_session = get_db_session()
    db_session.execute(delete(UserSession))
    admin_user = db_session.execute(
        select(User).where(User.username == "admin")
    ).scalar_one()
    admin_user.last_login_at = None
    db_session.commit()
    db_session.close()


def _build_request(
    *,
    method: str,
    path: str,
    cookies: dict[str, str] | None = None,
    form_data: dict[str, str] | None = None,
) -> Request:
    body = b""
    headers: list[tuple[bytes, bytes]] = []

    if cookies:
        cookie_header = "; ".join(
            f"{cookie_name}={cookie_value}"
            for cookie_name, cookie_value in cookies.items()
        )
        headers.append((b"cookie", cookie_header.encode("utf-8")))

    if form_data:
        body = urlencode(form_data).encode("utf-8")
        headers.append(
            (
                b"content-type",
                b"application/x-www-form-urlencoded",
            )
        )
        headers.append((b"content-length", str(len(body)).encode("utf-8")))

    scope = {
        "type": "http",
        "http_version": "1.1",
        "method": method,
        "scheme": "http",
        "path": path,
        "raw_path": path.encode("utf-8"),
        "query_string": b"",
        "headers": headers,
        "client": ("testclient", 50000),
        "server": ("testserver", 80),
        "root_path": "",
    }

    delivered = False

    async def receive():
        nonlocal delivered
        if delivered:
            return {"type": "http.request", "body": b"", "more_body": False}
        delivered = True
        return {"type": "http.request", "body": body, "more_body": False}

    return Request(scope, receive)


def _extract_cookies_from_response(response) -> dict[str, str]:
    parsed_cookie = SimpleCookie()
    for set_cookie_header in response.headers.getlist("set-cookie"):
        parsed_cookie.load(set_cookie_header)
    return {
        cookie_name: morsel.value
        for cookie_name, morsel in parsed_cookie.items()
    }


def _create_org_admin_account() -> dict[str, str | int]:
    suffix = uuid4().hex[:8]
    password = f"Tenant-{suffix}-Password!"
    db_session = get_db_session()

    try:
        organization = Organization(
            name=f"Tenant Admin Org {suffix}",
            slug=f"tenant-admin-org-{suffix}",
            tax_id=f"30-66{suffix[:6]}",
            tier="pro",
            is_active=True,
        )
        db_session.add(organization)
        db_session.commit()
        db_session.refresh(organization)

        user = User(
            organization_id=organization.id,
            email=f"tenant-admin-{suffix}@example.com",
            username=f"tenant_admin_{suffix}",
            password_hash=hash_password(password),
            role="admin",
            full_name="Tenant Admin",
            is_active=True,
        )
        db_session.add(user)
        db_session.commit()
        db_session.refresh(user)

        return {
            "organization_id": organization.id,
            "organization_name": organization.name,
            "username": user.username,
            "password": password,
            "email": user.email,
        }
    finally:
        db_session.close()


def _issue_real_access_cookie(*, username: str, password: str) -> str:
    response = Response()
    token_response = asyncio.run(
        login_b2b(
            LoginRequest(username=username, password=password),
            response,
        )
    )
    assert token_response.access_token
    return token_response.access_token


@pytest.mark.parametrize(
    ("path", "handler"),
    (
        ("/dashboard", main.render_dashboard_view),
        ("/vault", main.render_vault_view),
        ("/settings", main.render_settings_view),
        ("/admin", main.render_admin_view),
    ),
)
def test_direct_private_html_routes_require_authentication(path: str, handler):
    request = _build_request(method="GET", path=path)
    response = asyncio.run(handler(request))

    assert response.status_code == 303
    assert response.headers["location"] == "/"


def test_login_form_posts_to_real_auth_and_sets_cookies_only_on_success():
    login_page = asyncio.run(
        main.render_login_view(
            _build_request(method="GET", path="/")
        )
    )
    assert login_page.status_code == 200

    failed_login = asyncio.run(
        main.submit_login_view(
            _build_request(
                method="POST",
                path="/login",
                form_data={"username": "admin", "password": "wrong"},
            )
        )
    )
    assert failed_login.status_code == 401
    assert failed_login.headers.get("location") is None
    assert ACCESS_TOKEN_COOKIE_KEY not in _extract_cookies_from_response(failed_login)
    assert REFRESH_TOKEN_COOKIE_KEY not in _extract_cookies_from_response(failed_login)

    denied_dashboard = asyncio.run(
        main.render_dashboard_view(
            _build_request(method="GET", path="/dashboard")
        )
    )
    assert denied_dashboard.status_code == 303
    assert denied_dashboard.headers["location"] == "/"

    successful_login = asyncio.run(
        main.submit_login_view(
            _build_request(
                method="POST",
                path="/login",
                form_data={
                    "username": "admin",
                    "password": get_non_production_superadmin_seed()[1],
                },
            )
        )
    )
    issued_cookies = _extract_cookies_from_response(successful_login)
    assert successful_login.status_code == 303
    assert successful_login.headers["location"] == "/dashboard"
    assert ACCESS_TOKEN_COOKIE_KEY in issued_cookies
    assert REFRESH_TOKEN_COOKIE_KEY in issued_cookies

    dashboard = asyncio.run(
        main.render_dashboard_view(
            _build_request(
                method="GET",
                path="/dashboard",
                cookies=issued_cookies,
            )
        )
    )
    assert dashboard.status_code == 200
    assert "Trazabilidad Forestal" in dashboard.body.decode("utf-8")


def test_multiple_password_candidates_only_allow_the_real_test_password():
    real_password = get_non_production_superadmin_seed()[1]
    candidates = [real_password, "admin123", "wrong", uuid4().hex]

    for candidate in candidates:
        response = asyncio.run(
            main.submit_login_view(
                _build_request(
                    method="POST",
                    path="/login",
                    form_data={"username": "admin", "password": candidate},
                )
            )
        )

        if candidate == real_password:
            assert response.status_code == 303
            assert ACCESS_TOKEN_COOKIE_KEY in _extract_cookies_from_response(response)
        else:
            assert response.status_code == 401
            assert ACCESS_TOKEN_COOKIE_KEY not in _extract_cookies_from_response(response)


def test_logout_revokes_html_session_and_blocks_dashboard_afterwards():
    login_response = asyncio.run(
        main.submit_login_view(
            _build_request(
                method="POST",
                path="/login",
                form_data={
                    "username": "admin",
                    "password": get_non_production_superadmin_seed()[1],
                },
            )
        )
    )
    cookies = _extract_cookies_from_response(login_response)
    assert login_response.status_code == 303

    logout_response = asyncio.run(
        main.logout_submit_view(
            _build_request(
                method="POST",
                path="/logout",
                cookies=cookies,
            )
        )
    )
    assert logout_response.status_code == 303
    assert logout_response.headers["location"] == "/"

    dashboard = asyncio.run(
        main.render_dashboard_view(
            _build_request(
                method="GET",
                path="/dashboard",
                cookies=cookies,
            )
        )
    )
    assert dashboard.status_code == 303
    assert dashboard.headers["location"] == "/"


def test_invalid_access_cookie_variants_do_not_render_protected_html():
    valid_access_token = _issue_real_access_cookie(
        username="admin",
        password=get_non_production_superadmin_seed()[1],
    )
    valid_payload = verify_jwt_token(valid_access_token)
    assert valid_payload is not None

    tampered_last_char = "a" if valid_access_token[-1] != "a" else "b"
    tampered_token = valid_access_token[:-1] + tampered_last_char
    expired_token = create_jwt_token(
        {
            "sub": valid_payload["sub"],
            "org_id": valid_payload["org_id"],
            "org_name": valid_payload["org_name"],
            "role": valid_payload["role"],
            "email": valid_payload["email"],
            "sid": valid_payload["sid"],
        },
        expires_in_seconds=-1,
        token_type="access",
    )
    nonexistent_session_token = create_jwt_token(
        {
            "sub": valid_payload["sub"],
            "org_id": valid_payload["org_id"],
            "org_name": valid_payload["org_name"],
            "role": valid_payload["role"],
            "email": valid_payload["email"],
            "sid": 999999,
        },
        expires_in_seconds=3600,
        token_type="access",
    )

    for candidate in (
        "garbage-token",
        tampered_token,
        expired_token,
        nonexistent_session_token,
    ):
        response = asyncio.run(
            main.render_dashboard_view(
                _build_request(
                    method="GET",
                    path="/dashboard",
                    cookies={ACCESS_TOKEN_COOKIE_KEY: candidate},
                )
            )
        )

        assert response.status_code == 303
        assert response.headers["location"] == "/"


def test_revoked_session_cookie_is_rejected_for_dashboard():
    login_response = asyncio.run(
        main.submit_login_view(
            _build_request(
                method="POST",
                path="/login",
                form_data={
                    "username": "admin",
                    "password": get_non_production_superadmin_seed()[1],
                },
            )
        )
    )
    cookies = _extract_cookies_from_response(login_response)
    access_cookie = cookies[ACCESS_TOKEN_COOKIE_KEY]

    db_session = get_db_session()
    try:
        session_id = int(verify_jwt_token(access_cookie)["sid"])
        stored_session = db_session.execute(
            select(UserSession).where(UserSession.id == session_id)
        ).scalar_one()
        stored_session.revoked_at = utc_now()
        db_session.commit()
    finally:
        db_session.close()

    response = asyncio.run(
        main.render_dashboard_view(
            _build_request(
                method="GET",
                path="/dashboard",
                cookies={ACCESS_TOKEN_COOKIE_KEY: access_cookie},
            )
        )
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/"


def test_admin_html_route_denies_org_admin_allows_superadmin_and_rejects_forged_role():
    tenant_admin = _create_org_admin_account()

    org_admin_login = asyncio.run(
        main.submit_login_view(
            _build_request(
                method="POST",
                path="/login",
                form_data={
                    "username": str(tenant_admin["username"]),
                    "password": str(tenant_admin["password"]),
                },
            )
        )
    )
    org_admin_cookies = _extract_cookies_from_response(org_admin_login)
    assert org_admin_login.status_code == 303

    org_admin_response = asyncio.run(
        main.render_admin_view(
            _build_request(
                method="GET",
                path="/admin",
                cookies=org_admin_cookies,
            )
        )
    )
    assert org_admin_response.status_code == 403

    superadmin_login = asyncio.run(
        main.submit_login_view(
            _build_request(
                method="POST",
                path="/login",
                form_data={
                    "username": "admin",
                    "password": get_non_production_superadmin_seed()[1],
                },
            )
        )
    )
    superadmin_cookies = _extract_cookies_from_response(superadmin_login)
    assert superadmin_login.status_code == 303

    superadmin_response = asyncio.run(
        main.render_admin_view(
            _build_request(
                method="GET",
                path="/admin",
                cookies=superadmin_cookies,
            )
        )
    )
    assert superadmin_response.status_code == 200
    assert "PANEL SUPERADMIN" in superadmin_response.body.decode("utf-8")

    access_token = _issue_real_access_cookie(
        username=str(tenant_admin["username"]),
        password=str(tenant_admin["password"]),
    )
    payload = verify_jwt_token(access_token)
    assert payload is not None
    forged_access_token = create_jwt_token(
        {
            "sub": payload["sub"],
            "org_id": payload["org_id"],
            "org_name": payload["org_name"],
            "role": "superadmin",
            "email": payload["email"],
            "sid": payload["sid"],
        },
        expires_in_seconds=3600,
        token_type="access",
    )

    forged_role_response = asyncio.run(
        main.render_admin_view(
            _build_request(
                method="GET",
                path="/admin",
                cookies={ACCESS_TOKEN_COOKIE_KEY: forged_access_token},
            )
        )
    )

    assert forged_role_response.status_code == 403


def test_html_routes_reject_access_tokens_without_session_id():
    token_without_sid = create_jwt_token(
        {
            "sub": "admin",
            "org_id": 1,
            "org_name": "Exportadora Forestal del Chaco S.A.",
            "role": "superadmin",
            "email": "comercial@litoraltrace.com",
        },
        expires_in_seconds=3600,
        token_type="access",
    )

    response = asyncio.run(
        main.render_dashboard_view(
            _build_request(
                method="GET",
                path="/dashboard",
                cookies={ACCESS_TOKEN_COOKIE_KEY: token_without_sid},
            )
        )
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/"


def test_logged_in_html_user_can_reach_vault_and_settings_according_to_rbac():
    login_response = asyncio.run(
        main.submit_login_view(
            _build_request(
                method="POST",
                path="/login",
                form_data={
                    "username": "admin",
                    "password": get_non_production_superadmin_seed()[1],
                },
            )
        )
    )
    cookies = _extract_cookies_from_response(login_response)
    assert login_response.status_code == 303

    vault_response = asyncio.run(
        main.render_vault_view(
            _build_request(
                method="GET",
                path="/vault",
                cookies=cookies,
            )
        )
    )
    settings_response = asyncio.run(
        main.render_settings_view(
            _build_request(
                method="GET",
                path="/settings",
                cookies=cookies,
            )
        )
    )

    assert vault_response.status_code == 200
    assert settings_response.status_code == 200
