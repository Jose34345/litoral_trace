from __future__ import annotations

import asyncio
import os
from contextlib import contextmanager
from datetime import timedelta
from http.cookies import SimpleCookie
from uuid import uuid4

import pytest
from fastapi import Response
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError

from litoral_trace.api.auth import (
    ACCESS_TOKEN_COOKIE_KEY,
    LoginRequest,
    LogoutRequest,
    RefreshRequest,
    login_b2b,
    logout_b2b_session,
    refresh_b2b_session,
)
from litoral_trace.auth.passwords import hash_password
from litoral_trace.auth.sessions import (
    REFRESH_TOKEN_COOKIE_KEY,
    hash_refresh_token,
    utc_now,
)
from litoral_trace.config.settings import normalize_database_url
from litoral_trace.db.engine import reset_engine_state


def _truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}


POSTGRES_RLS_TESTS_ENABLED = _truthy(os.environ.get("ENABLE_POSTGRES_TESTS"))
RUNTIME_TEST_DATABASE_URL = os.environ.get("TEST_POSTGRES_DATABASE_URL")
MIGRATION_TEST_DATABASE_URL = (
    os.environ.get("TEST_POSTGRES_MIGRATION_DATABASE_URL")
    or os.environ.get("MIGRATION_DATABASE_URL")
)

pytestmark = pytest.mark.skipif(
    not (
        POSTGRES_RLS_TESTS_ENABLED
        and RUNTIME_TEST_DATABASE_URL
        and MIGRATION_TEST_DATABASE_URL
    ),
    reason=(
        "PostgreSQL auth RLS tests require ENABLE_POSTGRES_TESTS=1, "
        "TEST_POSTGRES_DATABASE_URL y TEST_POSTGRES_MIGRATION_DATABASE_URL "
        "(o MIGRATION_DATABASE_URL)."
    ),
)


def _runtime_engine():
    return create_engine(
        normalize_database_url(RUNTIME_TEST_DATABASE_URL),
        pool_size=1,
        max_overflow=0,
        pool_pre_ping=True,
    )


def _owner_engine():
    return create_engine(
        normalize_database_url(MIGRATION_TEST_DATABASE_URL),
        pool_pre_ping=True,
    )


def _set_tenant_context(connection, organization_id: int) -> None:
    connection.execute(
        text(
            "SELECT set_config("
            "'app.current_organization_id', "
            ":organization_id, "
            "true"
            ")"
        ),
        {"organization_id": str(organization_id)},
    )


def _extract_cookies(response: Response) -> dict[str, str]:
    parsed_cookie = SimpleCookie()
    for set_cookie_header in response.headers.getlist("set-cookie"):
        parsed_cookie.load(set_cookie_header)
    return {
        cookie_name: morsel.value
        for cookie_name, morsel in parsed_cookie.items()
    }


@contextmanager
def _postgres_runtime_auth_environment():
    original_values = {
        "ENVIRONMENT": os.environ.get("ENVIRONMENT"),
        "DATABASE_URL": os.environ.get("DATABASE_URL"),
        "MIGRATION_DATABASE_URL": os.environ.get("MIGRATION_DATABASE_URL"),
        "TEST_DATABASE_URL": os.environ.get("TEST_DATABASE_URL"),
    }

    os.environ["ENVIRONMENT"] = "development"
    os.environ["DATABASE_URL"] = RUNTIME_TEST_DATABASE_URL or ""
    os.environ["MIGRATION_DATABASE_URL"] = MIGRATION_TEST_DATABASE_URL or ""
    os.environ.pop("TEST_DATABASE_URL", None)
    reset_engine_state()

    try:
        yield
    finally:
        reset_engine_state()
        for variable_name, original_value in original_values.items():
            if original_value is None:
                os.environ.pop(variable_name, None)
            else:
                os.environ[variable_name] = original_value
        reset_engine_state()


@pytest.fixture(scope="module")
def auth_rls_fixture():
    suffix = uuid4().hex[:10]
    runtime_engine = _runtime_engine()
    owner_engine = _owner_engine()

    created_ids: dict[str, int] = {}
    password_a = f"AuthRlsA-{suffix}-secret"
    password_b = f"AuthRlsB-{suffix}-secret"

    with owner_engine.begin() as conn:
        created_ids["org_a_id"] = conn.execute(
            text(
                """
                INSERT INTO organizations (name, slug, tax_id, tier, description, is_active)
                VALUES (:name, :slug, :tax_id, 'pro', 'P1.8C org A', true)
                RETURNING id
                """
            ),
            {
                "name": f"Auth RLS Org A {suffix}",
                "slug": f"auth-rls-org-a-{suffix}",
                "tax_id": f"31-9{suffix[:8]}",
            },
        ).scalar_one()
        created_ids["org_b_id"] = conn.execute(
            text(
                """
                INSERT INTO organizations (name, slug, tax_id, tier, description, is_active)
                VALUES (:name, :slug, :tax_id, 'pro', 'P1.8C org B', true)
                RETURNING id
                """
            ),
            {
                "name": f"Auth RLS Org B {suffix}",
                "slug": f"auth-rls-org-b-{suffix}",
                "tax_id": f"31-8{suffix[:8]}",
            },
        ).scalar_one()

        created_ids["user_a_id"] = conn.execute(
            text(
                """
                INSERT INTO users (
                    organization_id, email, username, password_hash, role, full_name, is_active
                )
                VALUES (
                    :organization_id, :email, :username, :password_hash, 'admin', 'Auth RLS User A', true
                )
                RETURNING id
                """
            ),
            {
                "organization_id": created_ids["org_a_id"],
                "email": f"auth-rls-a-{suffix}@example.com",
                "username": f"auth_rls_a_{suffix}",
                "password_hash": hash_password(password_a),
            },
        ).scalar_one()
        created_ids["user_b_id"] = conn.execute(
            text(
                """
                INSERT INTO users (
                    organization_id, email, username, password_hash, role, full_name, is_active
                )
                VALUES (
                    :organization_id, :email, :username, :password_hash, 'admin', 'Auth RLS User B', true
                )
                RETURNING id
                """
            ),
            {
                "organization_id": created_ids["org_b_id"],
                "email": f"auth-rls-b-{suffix}@example.com",
                "username": f"auth_rls_b_{suffix}",
                "password_hash": hash_password(password_b),
            },
        ).scalar_one()

        created_ids["session_a_id"] = conn.execute(
            text(
                """
                INSERT INTO user_sessions (
                    user_id, organization_id, family_id, token_hash, issued_at, expires_at,
                    created_ip, user_agent
                )
                VALUES (
                    :user_id, :organization_id, :family_id, :token_hash, now(),
                    now() + interval '30 days', '127.0.0.1', 'auth-rls-test-a'
                )
                RETURNING id
                """
            ),
            {
                "user_id": created_ids["user_a_id"],
                "organization_id": created_ids["org_a_id"],
                "family_id": str(uuid4()),
                "token_hash": uuid4().hex + uuid4().hex,
            },
        ).scalar_one()
        created_ids["session_b_id"] = conn.execute(
            text(
                """
                INSERT INTO user_sessions (
                    user_id, organization_id, family_id, token_hash, issued_at, expires_at,
                    created_ip, user_agent
                )
                VALUES (
                    :user_id, :organization_id, :family_id, :token_hash, now(),
                    now() + interval '30 days', '127.0.0.1', 'auth-rls-test-b'
                )
                RETURNING id
                """
            ),
            {
                "user_id": created_ids["user_b_id"],
                "organization_id": created_ids["org_b_id"],
                "family_id": str(uuid4()),
                "token_hash": uuid4().hex + uuid4().hex,
            },
        ).scalar_one()

    yield {
        **created_ids,
        "runtime_engine": runtime_engine,
        "owner_engine": owner_engine,
        "username_a": f"auth_rls_a_{suffix}",
        "username_b": f"auth_rls_b_{suffix}",
        "password_a": password_a,
        "password_b": password_b,
    }

    with owner_engine.begin() as conn:
        conn.execute(
            text(
                """
                DELETE FROM user_sessions
                WHERE organization_id IN (:org_a_id, :org_b_id)
                """
            ),
            {
                "org_a_id": created_ids["org_a_id"],
                "org_b_id": created_ids["org_b_id"],
            },
        )
        conn.execute(
            text(
                """
                DELETE FROM users
                WHERE id IN (:user_a_id, :user_b_id)
                """
            ),
            {
                "user_a_id": created_ids["user_a_id"],
                "user_b_id": created_ids["user_b_id"],
            },
        )
        conn.execute(
            text(
                """
                DELETE FROM organizations
                WHERE id IN (:org_a_id, :org_b_id)
                """
            ),
            {
                "org_a_id": created_ids["org_a_id"],
                "org_b_id": created_ids["org_b_id"],
            },
        )

    runtime_engine.dispose()
    owner_engine.dispose()


def test_users_select_without_context_returns_no_rows(auth_rls_fixture):
    with auth_rls_fixture["runtime_engine"].begin() as conn:
        rows = conn.execute(text("SELECT id, organization_id FROM users")).fetchall()

    assert rows == []


def test_user_sessions_select_without_context_returns_no_rows(auth_rls_fixture):
    with auth_rls_fixture["runtime_engine"].begin() as conn:
        rows = conn.execute(
            text("SELECT id, organization_id FROM user_sessions")
        ).fetchall()

    assert rows == []


@pytest.mark.parametrize(
    ("table_name", "id_column", "expected_org_key"),
    (
        ("users", "organization_id", "org_a_id"),
        ("user_sessions", "organization_id", "org_a_id"),
    ),
)
def test_auth_tables_are_scoped_by_tenant_context(
    auth_rls_fixture,
    table_name,
    id_column,
    expected_org_key,
):
    with auth_rls_fixture["runtime_engine"].begin() as conn:
        _set_tenant_context(conn, auth_rls_fixture["org_a_id"])
        tenant_a_rows = conn.execute(
            text(f"SELECT {id_column} FROM {table_name} ORDER BY id")
        ).fetchall()

    with auth_rls_fixture["runtime_engine"].begin() as conn:
        _set_tenant_context(conn, auth_rls_fixture["org_b_id"])
        tenant_b_rows = conn.execute(
            text(f"SELECT {id_column} FROM {table_name} ORDER BY id")
        ).fetchall()

    assert tenant_a_rows
    assert tenant_b_rows
    assert {row[0] for row in tenant_a_rows} == {auth_rls_fixture[expected_org_key]}
    assert {row[0] for row in tenant_b_rows} == {auth_rls_fixture["org_b_id"]}


@pytest.mark.parametrize(
    ("table_name", "id_value_key"),
    (
        ("users", "user_b_id"),
        ("user_sessions", "session_b_id"),
    ),
)
def test_auth_primary_key_cross_tenant_queries_return_no_rows(
    auth_rls_fixture,
    table_name,
    id_value_key,
):
    with auth_rls_fixture["runtime_engine"].begin() as conn:
        _set_tenant_context(conn, auth_rls_fixture["org_a_id"])
        rows = conn.execute(
            text(f"SELECT id FROM {table_name} WHERE id = :row_id"),
            {"row_id": auth_rls_fixture[id_value_key]},
        ).fetchall()

    assert rows == []


def test_insert_forged_user_tenant_is_rejected_by_postgresql_rls(auth_rls_fixture):
    with pytest.raises(DBAPIError) as exc_info:
        with auth_rls_fixture["runtime_engine"].begin() as conn:
            _set_tenant_context(conn, auth_rls_fixture["org_a_id"])
            conn.execute(
                text(
                    """
                    INSERT INTO users (
                        organization_id, email, username, password_hash, role, full_name, is_active
                    )
                    VALUES (
                        :organization_id, :email, :username, :password_hash, 'admin', 'Forged User', true
                    )
                    """
                ),
                {
                    "organization_id": auth_rls_fixture["org_b_id"],
                    "email": f"forged-user-{uuid4().hex[:8]}@example.com",
                    "username": f"forged_user_{uuid4().hex[:8]}",
                    "password_hash": "x" * 64,
                },
            )

    assert "row-level security" in str(exc_info.value).lower()


def test_update_forged_user_tenant_is_rejected_by_postgresql_rls(auth_rls_fixture):
    with pytest.raises(DBAPIError) as exc_info:
        with auth_rls_fixture["runtime_engine"].begin() as conn:
            _set_tenant_context(conn, auth_rls_fixture["org_a_id"])
            conn.execute(
                text(
                    """
                    UPDATE users
                    SET organization_id = :target_organization_id
                    WHERE id = :user_id
                    """
                ),
                {
                    "target_organization_id": auth_rls_fixture["org_b_id"],
                    "user_id": auth_rls_fixture["user_a_id"],
                },
            )

    assert "row-level security" in str(exc_info.value).lower()


def test_insert_forged_user_session_tenant_is_rejected_by_postgresql_rls(auth_rls_fixture):
    with pytest.raises(DBAPIError) as exc_info:
        with auth_rls_fixture["runtime_engine"].begin() as conn:
            _set_tenant_context(conn, auth_rls_fixture["org_a_id"])
            conn.execute(
                text(
                    """
                    INSERT INTO user_sessions (
                        user_id, organization_id, family_id, token_hash, issued_at, expires_at
                    )
                    VALUES (
                        :user_id, :organization_id, :family_id, :token_hash, now(),
                        now() + interval '30 days'
                    )
                    """
                ),
                {
                    "user_id": auth_rls_fixture["user_b_id"],
                    "organization_id": auth_rls_fixture["org_b_id"],
                    "family_id": str(uuid4()),
                    "token_hash": uuid4().hex + uuid4().hex,
                },
            )

    assert "row-level security" in str(exc_info.value).lower()


def test_update_forged_user_session_tenant_is_rejected_by_postgresql_rls(auth_rls_fixture):
    with pytest.raises(DBAPIError) as exc_info:
        with auth_rls_fixture["runtime_engine"].begin() as conn:
            _set_tenant_context(conn, auth_rls_fixture["org_a_id"])
            conn.execute(
                text(
                    """
                    UPDATE user_sessions
                    SET organization_id = :target_organization_id
                    WHERE id = :session_id
                    """
                ),
                {
                    "target_organization_id": auth_rls_fixture["org_b_id"],
                    "session_id": auth_rls_fixture["session_a_id"],
                },
            )

    assert "row-level security" in str(exc_info.value).lower()


def test_login_succeeds_under_users_rls(auth_rls_fixture):
    with _postgres_runtime_auth_environment():
        response = Response()
        token_response = asyncio.run(
            login_b2b(
                LoginRequest(
                    username=auth_rls_fixture["username_a"],
                    password=auth_rls_fixture["password_a"],
                ),
                response,
            )
        )

    cookies = _extract_cookies(response)
    assert token_response.user_info["organization_id"] == auth_rls_fixture["org_a_id"]
    assert ACCESS_TOKEN_COOKIE_KEY in cookies
    assert REFRESH_TOKEN_COOKIE_KEY in cookies


def test_login_invalid_password_remains_generic_under_users_rls(auth_rls_fixture):
    with _postgres_runtime_auth_environment():
        with pytest.raises(Exception) as exc_info:
            asyncio.run(
                login_b2b(
                    LoginRequest(
                        username=auth_rls_fixture["username_a"],
                        password="wrong-password",
                    ),
                    Response(),
                )
            )

    assert getattr(exc_info.value, "status_code", None) == 401
    assert getattr(exc_info.value, "detail", None) == "Usuario o contrasena incorrectos."


def test_refresh_rotates_session_under_user_sessions_rls(auth_rls_fixture):
    with _postgres_runtime_auth_environment():
        login_response = Response()
        asyncio.run(
            login_b2b(
                LoginRequest(
                    username=auth_rls_fixture["username_a"],
                    password=auth_rls_fixture["password_a"],
                ),
                login_response,
            )
        )
        login_cookies = _extract_cookies(login_response)

        refresh_response = Response()
        asyncio.run(
            refresh_b2b_session(
                refresh_response,
                payload=RefreshRequest(
                    refresh_token=login_cookies[REFRESH_TOKEN_COOKIE_KEY]
                ),
            )
        )
        refresh_cookies = _extract_cookies(refresh_response)

        engine = _runtime_engine()
        try:
            with engine.begin() as conn:
                _set_tenant_context(conn, auth_rls_fixture["org_a_id"])
                rows = conn.execute(
                    text(
                        """
                        SELECT id, organization_id, revoked_at, replaced_by_session_id
                        FROM user_sessions
                        WHERE user_id = :user_id
                        ORDER BY id
                        """
                    ),
                    {"user_id": auth_rls_fixture["user_a_id"]},
                ).mappings().all()
        finally:
            engine.dispose()

    assert refresh_cookies[REFRESH_TOKEN_COOKIE_KEY] != login_cookies[REFRESH_TOKEN_COOKIE_KEY]
    assert len(rows) >= 2
    assert rows[-1]["organization_id"] == auth_rls_fixture["org_a_id"]
    assert rows[-2]["revoked_at"] is not None
    assert rows[-2]["replaced_by_session_id"] == rows[-1]["id"]


def test_refresh_reuse_detection_revokes_family_under_user_sessions_rls(auth_rls_fixture):
    with _postgres_runtime_auth_environment():
        login_response = Response()
        asyncio.run(
            login_b2b(
                LoginRequest(
                    username=auth_rls_fixture["username_b"],
                    password=auth_rls_fixture["password_b"],
                ),
                login_response,
            )
        )
        login_cookies = _extract_cookies(login_response)
        original_refresh_token = login_cookies[REFRESH_TOKEN_COOKIE_KEY]

        first_refresh_response = Response()
        asyncio.run(
            refresh_b2b_session(
                first_refresh_response,
                payload=RefreshRequest(refresh_token=original_refresh_token),
            )
        )

        with pytest.raises(Exception) as exc_info:
            asyncio.run(
                refresh_b2b_session(
                    Response(),
                    payload=RefreshRequest(refresh_token=original_refresh_token),
                )
            )

        engine = _runtime_engine()
        try:
            with engine.begin() as conn:
                _set_tenant_context(conn, auth_rls_fixture["org_b_id"])
                rows = conn.execute(
                    text(
                        """
                        SELECT family_id, revoked_at
                        FROM user_sessions
                        WHERE family_id = (
                            SELECT family_id
                            FROM user_sessions
                            WHERE token_hash = :token_hash
                        )
                        ORDER BY id
                        """
                    ),
                    {"token_hash": hash_refresh_token(original_refresh_token)},
                ).mappings().all()
        finally:
            engine.dispose()

    assert getattr(exc_info.value, "status_code", None) == 401
    assert len({row["family_id"] for row in rows}) == 1
    assert len(rows) >= 2
    assert all(row["revoked_at"] is not None for row in rows)


def test_logout_revokes_session_under_user_sessions_rls(auth_rls_fixture):
    with _postgres_runtime_auth_environment():
        login_response = Response()
        asyncio.run(
            login_b2b(
                LoginRequest(
                    username=auth_rls_fixture["username_a"],
                    password=auth_rls_fixture["password_a"],
                ),
                login_response,
            )
        )
        login_cookies = _extract_cookies(login_response)

        logout_response = Response()
        asyncio.run(
            logout_b2b_session(
                logout_response,
                payload=LogoutRequest(),
                session_jwt=login_cookies[ACCESS_TOKEN_COOKIE_KEY],
            )
        )

        engine = _runtime_engine()
        try:
            with engine.begin() as conn:
                _set_tenant_context(conn, auth_rls_fixture["org_a_id"])
                revoked_at = conn.execute(
                    text(
                        """
                        SELECT revoked_at
                        FROM user_sessions
                        WHERE user_id = :user_id
                        ORDER BY id DESC
                        LIMIT 1
                        """
                    ),
                    {"user_id": auth_rls_fixture["user_a_id"]},
                ).scalar_one()
        finally:
            engine.dispose()

    assert revoked_at is not None
    cleared_cookie_headers = logout_response.headers.getlist("set-cookie")
    assert any("Max-Age=0" in header for header in cleared_cookie_headers)


def test_expired_refresh_still_revokes_under_user_sessions_rls(auth_rls_fixture):
    with _postgres_runtime_auth_environment():
        login_response = Response()
        asyncio.run(
            login_b2b(
                LoginRequest(
                    username=auth_rls_fixture["username_b"],
                    password=auth_rls_fixture["password_b"],
                ),
                login_response,
            )
        )
        login_cookies = _extract_cookies(login_response)

        owner_engine = _owner_engine()
        try:
            with owner_engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        UPDATE user_sessions
                        SET expires_at = :expired_at
                        WHERE token_hash = (
                            SELECT token_hash
                            FROM user_sessions
                            WHERE user_id = :user_id
                            ORDER BY id DESC
                            LIMIT 1
                        )
                        """
                    ),
                    {
                        "expired_at": utc_now() - timedelta(seconds=1),
                        "user_id": auth_rls_fixture["user_b_id"],
                    },
                )
        finally:
            owner_engine.dispose()

        with pytest.raises(Exception) as exc_info:
            asyncio.run(
                refresh_b2b_session(
                    Response(),
                    payload=RefreshRequest(
                        refresh_token=login_cookies[REFRESH_TOKEN_COOKIE_KEY]
                    ),
                )
            )

        runtime_engine = _runtime_engine()
        try:
            with runtime_engine.begin() as conn:
                _set_tenant_context(conn, auth_rls_fixture["org_b_id"])
                revoked_at = conn.execute(
                    text(
                        """
                        SELECT revoked_at
                        FROM user_sessions
                        WHERE user_id = :user_id
                        ORDER BY id DESC
                        LIMIT 1
                        """
                    ),
                    {"user_id": auth_rls_fixture["user_b_id"]},
                ).scalar_one()
        finally:
            runtime_engine.dispose()

    assert getattr(exc_info.value, "status_code", None) == 401
    assert revoked_at is not None


def test_transaction_local_auth_context_does_not_leak(auth_rls_fixture):
    runtime_engine = auth_rls_fixture["runtime_engine"]

    with runtime_engine.begin() as conn:
        _set_tenant_context(conn, auth_rls_fixture["org_a_id"])
        tenant_a_user_ids = conn.execute(
            text("SELECT organization_id FROM users ORDER BY id")
        ).fetchall()

    with runtime_engine.begin() as conn:
        current_setting_value = conn.execute(
            text("SELECT current_setting('app.current_organization_id', true)")
        ).scalar_one()
        no_context_session_ids = conn.execute(
            text("SELECT organization_id FROM user_sessions ORDER BY id")
        ).fetchall()

    with runtime_engine.begin() as conn:
        _set_tenant_context(conn, auth_rls_fixture["org_b_id"])
        tenant_b_session_ids = conn.execute(
            text("SELECT organization_id FROM user_sessions ORDER BY id")
        ).fetchall()

    assert {row[0] for row in tenant_a_user_ids} == {auth_rls_fixture["org_a_id"]}
    assert current_setting_value in (None, "")
    assert no_context_session_ids == []
    assert {row[0] for row in tenant_b_session_ids} == {auth_rls_fixture["org_b_id"]}


def test_bootstrap_functions_are_security_definer_and_not_publicly_executable(
    auth_rls_fixture,
):
    with auth_rls_fixture["owner_engine"].connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT
                    p.proname,
                    pg_get_userbyid(p.proowner) AS owner_name,
                    p.prosecdef,
                    coalesce(array_to_string(p.proconfig, ','), '') AS proconfig,
                    coalesce(p.proacl::text, '') AS acl_text,
                    has_function_privilege(
                        'litoral_trace_app',
                        p.oid,
                        'EXECUTE'
                    ) AS runtime_execute
                FROM pg_proc AS p
                JOIN pg_namespace AS n
                    ON n.oid = p.pronamespace
                WHERE n.nspname = 'public'
                  AND p.proname IN (
                      'bootstrap_auth_user_by_username',
                      'bootstrap_auth_session_by_token_hash'
                  )
                ORDER BY p.proname
                """
            )
        ).mappings().all()

    assert [row["proname"] for row in rows] == [
        "bootstrap_auth_session_by_token_hash",
        "bootstrap_auth_user_by_username",
    ]
    for row in rows:
        assert row["owner_name"] != "litoral_trace_app"
        assert row["prosecdef"] is True
        assert "search_path=public, pg_temp" in row["proconfig"]
        assert "{=X/" not in row["acl_text"]
        assert ",=X/" not in row["acl_text"]
        assert row["runtime_execute"] is True


def test_users_and_user_sessions_have_rls_enabled_and_forced(auth_rls_fixture):
    with auth_rls_fixture["owner_engine"].connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT relname, relrowsecurity, relforcerowsecurity
                FROM pg_class
                WHERE relname IN ('users', 'user_sessions')
                ORDER BY relname
                """
            )
        ).mappings().all()

    assert rows == [
        {
            "relname": "user_sessions",
            "relrowsecurity": True,
            "relforcerowsecurity": True,
        },
        {
            "relname": "users",
            "relrowsecurity": True,
            "relforcerowsecurity": True,
        },
    ]
