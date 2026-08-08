from __future__ import annotations

import asyncio
from datetime import timedelta
from http.cookies import SimpleCookie

import pytest
from fastapi import Response
from sqlalchemy import delete, select
from starlette.requests import Request

import main
from main import logout_submit_view, logout_view
from litoral_trace.api.auth import (
    LoginRequest,
    LogoutRequest,
    RefreshRequest,
    login_b2b,
    logout_b2b_session,
    refresh_b2b_session,
)
from litoral_trace.auth.sessions import (
    ACCESS_TOKEN_COOKIE_KEY,
    REFRESH_TOKEN_COOKIE_KEY,
    hash_refresh_token,
    utc_now,
)
from litoral_trace.auth.tokens import verify_jwt_token
from litoral_trace.db.engine import get_db_session
from litoral_trace.db.models import User, UserSession


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


def _extract_cookies(response: Response) -> dict[str, str]:
    parsed_cookie = SimpleCookie()
    for set_cookie_header in response.headers.getlist("set-cookie"):
        parsed_cookie.load(set_cookie_header)
    return {
        cookie_name: morsel.value
        for cookie_name, morsel in parsed_cookie.items()
    }


def _login():
    response = Response()
    token_response = asyncio.run(
        login_b2b(
            LoginRequest(username="admin", password="admin123"),
            response,
        )
    )
    return token_response, response, _extract_cookies(response)


def _get_all_sessions() -> list[UserSession]:
    db_session = get_db_session()
    try:
        return db_session.execute(
            select(UserSession).order_by(UserSession.id)
        ).scalars().all()
    finally:
        db_session.close()


def _get_session_by_id(session_id: int) -> UserSession:
    db_session = get_db_session()
    try:
        return db_session.execute(
            select(UserSession).where(UserSession.id == session_id)
        ).scalar_one()
    finally:
        db_session.close()


def _build_request(
    *,
    method: str,
    path: str,
    cookies: dict[str, str] | None = None,
) -> Request:
    headers: list[tuple[bytes, bytes]] = []
    if cookies:
        cookie_header = "; ".join(
            f"{cookie_name}={cookie_value}"
            for cookie_name, cookie_value in cookies.items()
        )
        headers.append((b"cookie", cookie_header.encode("utf-8")))

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
    return Request(scope)


def test_login_creates_access_refresh_and_persistent_session():
    token_response, _, cookies = _login()

    refresh_token = cookies.get(REFRESH_TOKEN_COOKIE_KEY)
    assert refresh_token is not None

    payload = verify_jwt_token(token_response.access_token)
    assert payload is not None
    assert payload["org_id"] == 1
    assert int(payload["sid"]) > 0

    sessions = _get_all_sessions()
    assert len(sessions) == 1
    stored_session = sessions[0]
    assert stored_session.organization_id == 1
    assert stored_session.user_id > 0
    assert stored_session.revoked_at is None
    assert stored_session.family_id
    assert stored_session.token_hash != refresh_token
    assert stored_session.token_hash == hash_refresh_token(refresh_token)


def test_refresh_rotates_previous_token_and_keeps_family():
    login_response, _, cookies = _login()
    old_refresh_token = cookies[REFRESH_TOKEN_COOKIE_KEY]
    original_session = _get_all_sessions()[0]

    refresh_response = Response()
    rotated_token_response = asyncio.run(
        refresh_b2b_session(
            refresh_response,
            refresh_token_cookie=old_refresh_token,
        )
    )
    refresh_cookies = _extract_cookies(refresh_response)

    new_refresh_token = refresh_cookies.get(REFRESH_TOKEN_COOKIE_KEY)
    assert new_refresh_token is not None
    assert new_refresh_token != old_refresh_token
    assert rotated_token_response.access_token != login_response.access_token

    sessions = _get_all_sessions()
    assert len(sessions) == 2

    old_session = _get_session_by_id(original_session.id)
    new_session = max(sessions, key=lambda session: session.id)

    assert old_session.revoked_at is not None
    assert old_session.replaced_by_session_id == new_session.id
    assert new_session.revoked_at is None
    assert new_session.family_id == old_session.family_id
    assert new_session.organization_id == 1
    assert new_session.token_hash == hash_refresh_token(new_refresh_token)

    new_payload = verify_jwt_token(rotated_token_response.access_token)
    assert new_payload is not None
    assert new_payload["org_id"] == 1
    assert int(new_payload["sid"]) == new_session.id


def test_refresh_failure_before_commit_does_not_leave_partial_rotation(monkeypatch):
    login_response, _, cookies = _login()
    old_refresh_token = cookies[REFRESH_TOKEN_COOKIE_KEY]
    original_session = _get_all_sessions()[0]

    def _raise_before_commit(*args, **kwargs):
        raise RuntimeError("cookie setup failed")

    monkeypatch.setattr(
        "litoral_trace.api.auth._set_auth_cookies",
        _raise_before_commit,
    )

    with pytest.raises(RuntimeError):
        asyncio.run(
            refresh_b2b_session(
                Response(),
                refresh_token_cookie=old_refresh_token,
            )
        )

    assert verify_jwt_token(login_response.access_token) is not None
    sessions = _get_all_sessions()
    assert len(sessions) == 1
    stored_session = _get_session_by_id(original_session.id)
    assert stored_session.revoked_at is None
    assert stored_session.replaced_by_session_id is None


def test_reuse_detection_revokes_the_entire_family():
    _, _, cookies = _login()
    first_refresh_token = cookies[REFRESH_TOKEN_COOKIE_KEY]

    first_rotation_response = Response()
    asyncio.run(
        refresh_b2b_session(
            first_rotation_response,
            refresh_token_cookie=first_refresh_token,
        )
    )

    with pytest.raises(Exception) as exc_info:
        asyncio.run(
            refresh_b2b_session(
                Response(),
                payload=RefreshRequest(refresh_token=first_refresh_token),
            )
        )

    assert getattr(exc_info.value, "status_code", None) == 401

    sessions = _get_all_sessions()
    assert len(sessions) == 2
    family_ids = {session.family_id for session in sessions}
    assert len(family_ids) == 1
    assert all(session.revoked_at is not None for session in sessions)


def test_expired_refresh_token_is_rejected():
    _, _, cookies = _login()
    refresh_token = cookies[REFRESH_TOKEN_COOKIE_KEY]
    stored_session = _get_all_sessions()[0]

    db_session = get_db_session()
    expiring_session = db_session.execute(
        select(UserSession).where(UserSession.id == stored_session.id)
    ).scalar_one()
    expiring_session.expires_at = utc_now() - timedelta(seconds=1)
    db_session.commit()
    db_session.close()

    with pytest.raises(Exception) as exc_info:
        asyncio.run(
            refresh_b2b_session(
                Response(),
                payload=RefreshRequest(refresh_token=refresh_token),
            )
        )

    assert getattr(exc_info.value, "status_code", None) == 401
    expired_session = _get_session_by_id(stored_session.id)
    assert expired_session.revoked_at is not None


def test_logout_revokes_active_session_and_clears_cookies():
    login_response, _, cookies = _login()
    refresh_token = cookies[REFRESH_TOKEN_COOKIE_KEY]
    session_jwt = cookies[ACCESS_TOKEN_COOKIE_KEY]
    stored_session = _get_all_sessions()[0]

    logout_response = Response()
    logout_payload = asyncio.run(
        logout_b2b_session(
            logout_response,
            payload=LogoutRequest(refresh_token=refresh_token),
            session_jwt=session_jwt,
        )
    )

    assert logout_payload.detail == "Sesion finalizada."
    cleared_cookie_headers = logout_response.headers.getlist("set-cookie")
    assert any(f"{REFRESH_TOKEN_COOKIE_KEY}=" in header for header in cleared_cookie_headers)
    assert any("Max-Age=0" in header for header in cleared_cookie_headers)

    revoked_session = _get_session_by_id(stored_session.id)
    assert revoked_session.revoked_at is not None

    with pytest.raises(Exception) as exc_info:
        asyncio.run(
            refresh_b2b_session(
                Response(),
                payload=RefreshRequest(refresh_token=refresh_token),
            )
        )
    assert getattr(exc_info.value, "status_code", None) == 401

    access_payload = verify_jwt_token(login_response.access_token)
    assert access_payload is not None


def test_web_get_logout_does_not_revoke_session_or_clear_cookies():
    _, _, cookies = _login()
    stored_session = _get_all_sessions()[0]

    response = asyncio.run(
        logout_view(
            _build_request(
                method="GET",
                path="/logout",
                cookies={
                    ACCESS_TOKEN_COOKIE_KEY: cookies[ACCESS_TOKEN_COOKIE_KEY],
                    REFRESH_TOKEN_COOKIE_KEY: cookies[REFRESH_TOKEN_COOKIE_KEY],
                },
            )
        )
    )

    assert response.status_code == 200
    assert "set-cookie" not in response.headers
    untouched_session = _get_session_by_id(stored_session.id)
    assert untouched_session.revoked_at is None


def test_web_post_logout_revokes_session_and_redirects_to_login():
    _, _, cookies = _login()
    stored_session = _get_all_sessions()[0]

    response = asyncio.run(
        logout_submit_view(
            _build_request(
                method="POST",
                path="/logout",
                cookies={
                    ACCESS_TOKEN_COOKIE_KEY: cookies[ACCESS_TOKEN_COOKIE_KEY],
                    REFRESH_TOKEN_COOKIE_KEY: cookies[REFRESH_TOKEN_COOKIE_KEY],
                },
            )
        )
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/"
    cleared_cookie_headers = response.headers.getlist("set-cookie")
    assert any(f"{REFRESH_TOKEN_COOKIE_KEY}=" in header for header in cleared_cookie_headers)
    assert any("Max-Age=0" in header for header in cleared_cookie_headers)

    revoked_session = _get_session_by_id(stored_session.id)
    assert revoked_session.revoked_at is not None


def test_api_logout_does_not_clear_cookies_if_commit_fails(monkeypatch):
    _, _, cookies = _login()
    real_session = get_db_session()

    class FailingCommitSession:
        def __init__(self, inner_session):
            self._inner_session = inner_session

        def __getattr__(self, item):
            return getattr(self._inner_session, item)

        def commit(self):
            raise RuntimeError("commit failed")

    def _fake_get_db_session():
        return FailingCommitSession(real_session)

    monkeypatch.setattr(
        "litoral_trace.api.auth.get_db_session",
        _fake_get_db_session,
    )

    response = Response()
    with pytest.raises(RuntimeError):
        asyncio.run(
            logout_b2b_session(
                response,
                payload=LogoutRequest(refresh_token=cookies[REFRESH_TOKEN_COOKIE_KEY]),
                session_jwt=cookies[ACCESS_TOKEN_COOKIE_KEY],
            )
        )

    assert "set-cookie" not in response.headers
    real_session.close()


def test_invalid_random_refresh_token_is_rejected():
    with pytest.raises(Exception) as exc_info:
        asyncio.run(
            refresh_b2b_session(
                Response(),
                payload=RefreshRequest(refresh_token="invalid-refresh-token"),
            )
        )

    assert getattr(exc_info.value, "status_code", None) == 401
    assert _get_all_sessions() == []


def test_refresh_rejects_organization_mismatch():
    _, _, cookies = _login()
    refresh_token = cookies[REFRESH_TOKEN_COOKIE_KEY]
    stored_session = _get_all_sessions()[0]

    db_session = get_db_session()
    tampered_session = db_session.execute(
        select(UserSession).where(UserSession.id == stored_session.id)
    ).scalar_one()
    tampered_session.organization_id = 999999
    db_session.commit()
    db_session.close()

    with pytest.raises(Exception) as exc_info:
        asyncio.run(
            refresh_b2b_session(
                Response(),
                payload=RefreshRequest(refresh_token=refresh_token),
            )
        )

    assert getattr(exc_info.value, "status_code", None) == 401
    updated_session = _get_session_by_id(stored_session.id)
    assert updated_session.revoked_at is not None
