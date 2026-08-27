from __future__ import annotations

import asyncio
from http.cookies import SimpleCookie
from pathlib import Path

import pytest
from fastapi import Response
from sqlalchemy import delete, select

from litoral_trace.api.auth import (
    LoginRequest,
    LogoutRequest,
    login_b2b,
    logout_b2b_session,
    refresh_b2b_session,
)
from litoral_trace.auth.sessions import (
    ACCESS_TOKEN_COOKIE_KEY,
    REFRESH_TOKEN_COOKIE_KEY,
)
from litoral_trace.db.engine import get_db_session
from litoral_trace.db.init_db import get_non_production_superadmin_seed
from litoral_trace.db.models import User, UserSession


ROOT_DIR = Path(__file__).resolve().parents[1]
COORDINATION_JS = (
    ROOT_DIR
    / "src"
    / "litoral_trace"
    / "static"
    / "src"
    / "js"
    / "session-refresh-coordination.js"
)
SESSIONS_PY = (
    ROOT_DIR
    / "src"
    / "litoral_trace"
    / "auth"
    / "sessions.py"
)


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


def _all_sessions() -> list[UserSession]:
    db_session = get_db_session()
    try:
        return db_session.execute(
            select(UserSession).order_by(UserSession.id)
        ).scalars().all()
    finally:
        db_session.close()


def _login_and_rotate() -> tuple[dict[str, str], str]:
    login_response = Response()
    username, password = get_non_production_superadmin_seed()

    asyncio.run(
        login_b2b(
            LoginRequest(
                username=username,
                password=password,
            ),
            login_response,
        )
    )
    login_cookies = _extract_cookies(login_response)
    parent_refresh_token = login_cookies[REFRESH_TOKEN_COOKIE_KEY]

    refresh_response = Response()
    asyncio.run(
        refresh_b2b_session(
            refresh_response,
            refresh_token_cookie=parent_refresh_token,
        )
    )
    return login_cookies, parent_refresh_token


def test_logout_with_rotated_parent_refresh_revokes_active_successor_family():
    _, parent_refresh_token = _login_and_rotate()

    sessions_after_refresh = _all_sessions()
    assert len(sessions_after_refresh) == 2
    assert sum(
        session.revoked_at is None
        for session in sessions_after_refresh
    ) == 1

    # Models logout carrying the parent refresh cookie after a keepalive refresh
    # committed first. The active descendant must still be revoked.
    logout_response = Response()
    asyncio.run(
        logout_b2b_session(
            logout_response,
            payload=LogoutRequest(
                refresh_token=parent_refresh_token,
            ),
        )
    )

    sessions_after_logout = _all_sessions()
    assert len(sessions_after_logout) == 2
    assert all(
        session.revoked_at is not None
        for session in sessions_after_logout
    )


def test_logout_with_ancestor_access_token_revokes_active_successor_family():
    login_cookies, _ = _login_and_rotate()

    sessions_after_refresh = _all_sessions()
    assert len(sessions_after_refresh) == 2
    assert sum(
        session.revoked_at is None
        for session in sessions_after_refresh
    ) == 1

    # Models a page whose still-cryptographically-valid access JWT references an
    # ancestor while another client already rotated to a descendant.
    logout_response = Response()
    asyncio.run(
        logout_b2b_session(
            logout_response,
            session_jwt=login_cookies[ACCESS_TOKEN_COOKIE_KEY],
        )
    )

    sessions_after_logout = _all_sessions()
    assert len(sessions_after_logout) == 2
    assert all(
        session.revoked_at is not None
        for session in sessions_after_logout
    )


def test_cross_document_marker_has_no_expiring_lease_and_ambiguous_fail_closed():
    source = COORDINATION_JS.read_text(encoding="utf-8")

    assert "COORDINATION_MAX_AGE_SECONDS" not in source
    assert "COORDINATION_WAIT_SECONDS" in source
    assert "Refresh outcome ambiguous" in source
    assert "return ambiguousRefreshResponse();" in source
    assert "response.status >= 500" in source

    mark_function = source.split(
        "function markRefreshInFlight()",
        1,
    )[1].split(
        "function clearRefreshInFlight()",
        1,
    )[0]
    assert "Max-Age=" not in mark_function
    assert "SameSite=Strict" in mark_function

    request_block = source.split(
        "markRefreshInFlight();",
        1,
    )[1]
    assert "finally" not in request_block
    assert "catch (_error)" in request_block


def test_refresh_and_logout_share_family_lock_before_row_lock():
    source = SESSIONS_PY.read_text(encoding="utf-8")

    assert "pg_advisory_xact_lock" in source
    assert "_lookup_session_bootstrap_without_retained_row_lock" in source

    rotation = source.split(
        "def rotate_refresh_session(",
        1,
    )[1].split(
        "\ndef revoke_session(",
        1,
    )[0]
    rotation_family_lock = rotation.index("_acquire_refresh_family_lock(")
    rotation_row_lock = rotation.index("current_session = _get_session_by_id(")
    assert rotation_family_lock < rotation_row_lock
    assert "for_update=True" in rotation[rotation_row_lock:]

    revocation = source.split("def revoke_session(", 1)[1]
    revocation_family_lock = revocation.index("_acquire_refresh_family_lock(")
    revocation_row_lock = revocation.index("session_record = _get_session_by_id(")
    family_revoke = revocation.index("_revoke_family(")

    assert revocation_family_lock < revocation_row_lock < family_revoke
    assert "for_update=True" in revocation[revocation_row_lock:family_revoke]
    assert "family_id=family_id" in revocation[family_revoke:]
    assert "organization_id=family_organization_id" in revocation[family_revoke:]
