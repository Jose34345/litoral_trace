from __future__ import annotations

import asyncio
import os
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from http.cookies import SimpleCookie
from pathlib import Path
from threading import Barrier
from uuid import uuid4

import pytest
from fastapi import Response
from sqlalchemy import create_engine, text

from litoral_trace.api.auth import (
    LoginRequest,
    RefreshRequest,
    login_b2b,
    refresh_b2b_session,
)
from litoral_trace.auth.passwords import (
    hash_password,
)
from litoral_trace.auth.sessions import (
    REFRESH_TOKEN_COOKIE_KEY,
    hash_refresh_token,
)
from litoral_trace.config.settings import (
    normalize_database_url,
)
from litoral_trace.db.engine import (
    reset_engine_state,
)


ROOT_DIR = (
    Path(__file__).resolve().parents[1]
)

INTEGRATION_ENV_PATH = (
    ROOT_DIR / ".env.integration"
)


def _truthy(
    value: str | None,
) -> bool:
    return (
        value or ""
    ).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _read_env_file(
    path: Path,
) -> dict[str, str]:
    values: dict[str, str] = {}

    if not path.exists():
        return values

    for raw_line in path.read_text(
        encoding="utf-8"
    ).splitlines():
        line = raw_line.strip()

        if (
            not line
            or line.startswith("#")
            or "=" not in line
        ):
            continue

        name, value = line.split(
            "=",
            1,
        )

        values[
            name.strip()
        ] = value.strip()

    return values


INTEGRATION_ENV = _read_env_file(
    INTEGRATION_ENV_PATH
)


POSTGRES_TESTS_ENABLED = _truthy(
    INTEGRATION_ENV.get(
        "ENABLE_POSTGRES_TESTS"
    )
)


RUNTIME_DATABASE_URL = (
    INTEGRATION_ENV.get(
        "TEST_POSTGRES_DATABASE_URL"
    )
)


OWNER_DATABASE_URL = (
    INTEGRATION_ENV.get(
        "TEST_POSTGRES_MIGRATION_DATABASE_URL"
    )
    or INTEGRATION_ENV.get(
        "MIGRATION_DATABASE_URL"
    )
)


pytestmark = pytest.mark.skipif(
    not (
        POSTGRES_TESTS_ENABLED
        and RUNTIME_DATABASE_URL
        and OWNER_DATABASE_URL
    ),
    reason=(
        "Refresh concurrency integration test "
        "requires ENABLE_POSTGRES_TESTS=1 plus "
        "runtime and owner PostgreSQL integration URLs."
    ),
)


def _owner_engine():
    return create_engine(
        normalize_database_url(
            OWNER_DATABASE_URL
        ),
        pool_pre_ping=True,
        hide_parameters=True,
    )


def _extract_cookies(
    response: Response,
) -> dict[str, str]:
    parsed_cookie = SimpleCookie()

    for set_cookie_header in (
        response.headers.getlist(
            "set-cookie"
        )
    ):
        parsed_cookie.load(
            set_cookie_header
        )

    return {
        cookie_name: morsel.value
        for (
            cookie_name,
            morsel,
        ) in parsed_cookie.items()
    }


@contextmanager
def _postgres_runtime_auth_environment():
    original_values = {
        "ENVIRONMENT": os.environ.get(
            "ENVIRONMENT"
        ),
        "DATABASE_URL": os.environ.get(
            "DATABASE_URL"
        ),
        "MIGRATION_DATABASE_URL": (
            os.environ.get(
                "MIGRATION_DATABASE_URL"
            )
        ),
        "TEST_DATABASE_URL": (
            os.environ.get(
                "TEST_DATABASE_URL"
            )
        ),
    }

    os.environ[
        "ENVIRONMENT"
    ] = "development"

    os.environ[
        "DATABASE_URL"
    ] = (
        RUNTIME_DATABASE_URL
        or ""
    )

    os.environ[
        "MIGRATION_DATABASE_URL"
    ] = (
        OWNER_DATABASE_URL
        or ""
    )

    os.environ.pop(
        "TEST_DATABASE_URL",
        None,
    )

    reset_engine_state()

    try:
        yield

    finally:
        reset_engine_state()

        for (
            variable_name,
            original_value,
        ) in original_values.items():
            if original_value is None:
                os.environ.pop(
                    variable_name,
                    None,
                )
            else:
                os.environ[
                    variable_name
                ] = original_value

        reset_engine_state()


@pytest.fixture
def refresh_concurrency_fixture():
    suffix = uuid4().hex[:10]

    username = (
        f"refresh_race_{suffix}"
    )

    password = (
        f"RefreshRace-"
        f"{suffix}-Secret!"
    )

    owner_engine = (
        _owner_engine()
    )

    created_ids: dict[
        str,
        int,
    ] = {}

    with owner_engine.begin() as conn:
        created_ids[
            "organization_id"
        ] = conn.execute(
            text(
                """
                INSERT INTO organizations (
                    name,
                    slug,
                    tax_id,
                    tier,
                    description,
                    is_active
                )
                VALUES (
                    :name,
                    :slug,
                    :tax_id,
                    'pro',
                    'Refresh concurrency integration test',
                    true
                )
                RETURNING id
                """
            ),
            {
                "name": (
                    f"Refresh Race Org "
                    f"{suffix}"
                ),
                "slug": (
                    f"refresh-race-"
                    f"{suffix}"
                ),
                "tax_id": (
                    f"39-7"
                    f"{suffix[:8]}"
                ),
            },
        ).scalar_one()

        created_ids[
            "user_id"
        ] = conn.execute(
            text(
                """
                INSERT INTO users (
                    organization_id,
                    email,
                    username,
                    password_hash,
                    role,
                    full_name,
                    is_active
                )
                VALUES (
                    :organization_id,
                    :email,
                    :username,
                    :password_hash,
                    'admin',
                    'Refresh Race User',
                    true
                )
                RETURNING id
                """
            ),
            {
                "organization_id": (
                    created_ids[
                        "organization_id"
                    ]
                ),
                "email": (
                    f"{username}"
                    "@example.com"
                ),
                "username": username,
                "password_hash": (
                    hash_password(
                        password
                    )
                ),
            },
        ).scalar_one()

    try:
        yield {
            **created_ids,
            "username": username,
            "password": password,
            "owner_engine": (
                owner_engine
            ),
        }

    finally:
        with owner_engine.begin() as conn:
            conn.execute(
                text(
                    """
                    DELETE FROM user_sessions
                    WHERE organization_id = :organization_id
                    """
                ),
                {
                    "organization_id": (
                        created_ids[
                            "organization_id"
                        ]
                    )
                },
            )

            conn.execute(
                text(
                    """
                    DELETE FROM users
                    WHERE id = :user_id
                    """
                ),
                {
                    "user_id": (
                        created_ids[
                            "user_id"
                        ]
                    )
                },
            )

            conn.execute(
                text(
                    """
                    DELETE FROM organizations
                    WHERE id = :organization_id
                    """
                ),
                {
                    "organization_id": (
                        created_ids[
                            "organization_id"
                        ]
                    )
                },
            )

        owner_engine.dispose()


def _attempt_refresh(
    refresh_token: str,
    start_barrier: Barrier,
) -> dict[str, object]:
    try:
        # Synchronize only before either request enters
        # the authentication transaction.
        #
        # PostgreSQL itself must provide serialization
        # after this point.
        start_barrier.wait(
            timeout=10
        )

        response = Response()

        token_response = asyncio.run(
            refresh_b2b_session(
                response,
                payload=RefreshRequest(
                    refresh_token=(
                        refresh_token
                    )
                ),
            )
        )

        cookies = _extract_cookies(
            response
        )

        return {
            "outcome": "success",
            "status_code": 200,
            "access_token": (
                token_response
                .access_token
            ),
            "refresh_token": (
                cookies.get(
                    REFRESH_TOKEN_COOKIE_KEY
                )
            ),
        }

    except Exception as exc:
        return {
            "outcome": "error",
            "status_code": getattr(
                exc,
                "status_code",
                None,
            ),
            "detail": getattr(
                exc,
                "detail",
                str(exc),
            ),
            "exception_type": (
                type(exc).__name__
            ),
        }


def test_same_refresh_token_is_serialized_by_postgresql_bootstrap_lock(
    refresh_concurrency_fixture,
):
    """Two concurrent consumers cannot rotate one parent twice.

    PostgreSQL bootstrap_auth_session_by_token_hash() acquires
    SELECT ... FOR UPDATE on the refresh-token session before
    tenant-scoped rotation continues.

    Expected fail-closed behavior:

    1. both requests start concurrently;
    2. one transaction acquires the parent row lock;
    3. that request rotates successfully;
    4. the other transaction waits;
    5. after the first commit, the loser observes revoked_at;
    6. reuse detection returns 401 and revokes the family;
    7. only one child session was ever created.
    """

    fixture = (
        refresh_concurrency_fixture
    )

    with _postgres_runtime_auth_environment():
        login_response = Response()

        asyncio.run(
            login_b2b(
                LoginRequest(
                    username=(
                        fixture[
                            "username"
                        ]
                    ),
                    password=(
                        fixture[
                            "password"
                        ]
                    ),
                ),
                login_response,
            )
        )

        login_cookies = (
            _extract_cookies(
                login_response
            )
        )

        original_refresh_token = (
            login_cookies[
                REFRESH_TOKEN_COOKIE_KEY
            ]
        )

        start_barrier = Barrier(
            2
        )

        with ThreadPoolExecutor(
            max_workers=2
        ) as executor:
            futures = [
                executor.submit(
                    _attempt_refresh,
                    original_refresh_token,
                    start_barrier,
                )
                for _ in range(2)
            ]

            results = [
                future.result(
                    timeout=30
                )
                for future in futures
            ]

    successes = [
        result
        for result in results
        if (
            result["outcome"]
            == "success"
        )
    ]

    failures = [
        result
        for result in results
        if (
            result["outcome"]
            == "error"
        )
    ]

    assert (
        len(successes)
        == 1
    )

    assert (
        len(failures)
        == 1
    )

    assert (
        failures[0][
            "status_code"
        ]
        == 401
    )

    owner_engine = (
        fixture[
            "owner_engine"
        ]
    )

    with owner_engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT
                    id,
                    family_id,
                    token_hash,
                    revoked_at,
                    replaced_by_session_id
                FROM user_sessions
                WHERE
                    organization_id = :organization_id
                    AND user_id = :user_id
                ORDER BY id
                """
            ),
            {
                "organization_id": (
                    fixture[
                        "organization_id"
                    ]
                ),
                "user_id": (
                    fixture[
                        "user_id"
                    ]
                ),
            },
        ).mappings().all()

    # Exactly:
    #
    # parent + one child
    #
    # Never:
    #
    # parent + child A + child B
    assert len(rows) == 2

    original_session = next(
        row
        for row in rows
        if (
            row["token_hash"]
            == hash_refresh_token(
                original_refresh_token
            )
        )
    )

    child_session = next(
        row
        for row in rows
        if (
            row["id"]
            != original_session["id"]
        )
    )

    assert (
        original_session[
            "revoked_at"
        ]
        is not None
    )

    assert (
        original_session[
            "replaced_by_session_id"
        ]
        == child_session["id"]
    )

    assert (
        child_session[
            "family_id"
        ]
        == original_session[
            "family_id"
        ]
    )

    # Existing fail-closed reuse policy revokes the family
    # after the losing concurrent request detects reuse.
    assert (
        child_session[
            "revoked_at"
        ]
        is not None
    )