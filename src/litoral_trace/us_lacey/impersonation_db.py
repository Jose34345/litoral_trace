"""Zero-trust read-only database boundary for U.S. Lacey impersonation."""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import os
from typing import Any, Generator

from fastapi import Cookie, Depends, HTTPException, Request, status
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.engine import Connection

from litoral_trace.services.admin import (
    _map_platform_db_error,
    _require_platform_refresh_token_hash,
)
from litoral_trace.us_lacey.db import get_us_lacey_db_session
from litoral_trace.us_lacey.portal_auth import (
    US_LACEY_SESSION_COOKIE,
    resolve_us_lacey_session,
)


IMPERSONATION_COOKIE = "lt_lacey_impersonation"
IMPERSONATION_ROLE = "litoral_trace_impersonation_reader"
ALLOWED_IMPERSONATION_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})

_engine: Any | None = None


@dataclass(frozen=True)
class ReadOnlyImpersonationContext:
    session_id: int
    admin_user_id: int
    admin_organization_id: int
    target_organization_id: int


def reset_impersonation_engine_state() -> None:
    global _engine
    if _engine is not None:
        _engine.dispose()
    _engine = None


def get_impersonation_database_url() -> str:
    value = str(os.environ.get("US_LACEY_IMPERSONATION_DATABASE_URL", "")).strip()
    if not value:
        raise RuntimeError(
            "US_LACEY_IMPERSONATION_DATABASE_URL is required for read-only impersonation."
        )
    return value


def get_impersonation_engine() -> Any:
    global _engine
    if _engine is None:
        _engine = create_engine(
            get_impersonation_database_url(),
            pool_size=2,
            max_overflow=0,
            pool_recycle=300,
            pool_pre_ping=True,
            echo=False,
        )
    return _engine


def hash_impersonation_token(raw_token: str) -> str:
    normalized = str(raw_token or "").strip()
    if not normalized:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Read-only impersonation session is missing.",
        )
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def resolve_readonly_impersonation_context(
    request: Request,
    us_session: str | None = Cookie(None, alias=US_LACEY_SESSION_COOKIE),
    impersonation_token: str | None = Cookie(None, alias=IMPERSONATION_COOKIE),
) -> ReadOnlyImpersonationContext:
    if request.method.upper() not in ALLOWED_IMPERSONATION_METHODS:
        raise HTTPException(
            status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
            detail="Read-only impersonation accepts GET, HEAD, or OPTIONS only.",
            headers={"Allow": "GET, HEAD, OPTIONS"},
        )

    if not us_session:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="A valid platform session is required.",
        )
    if not impersonation_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Read-only impersonation session is missing.",
        )

    identity = resolve_us_lacey_session(us_session)
    token_hash = hash_impersonation_token(impersonation_token)
    actor_refresh_token_hash = _require_platform_refresh_token_hash(us_session)

    db = get_us_lacey_db_session()
    try:
        row = db.execute(
            text(
                "SELECT * FROM "
                "public.platform_admin_resolve_readonly_impersonation("
                ":actor_refresh_token_hash, :token_hash)"
            ),
            {
                "actor_refresh_token_hash": actor_refresh_token_hash,
                "token_hash": token_hash,
            },
        ).mappings().one()

        return ReadOnlyImpersonationContext(
            session_id=int(row["session_id"]),
            admin_user_id=int(identity.user_id),
            admin_organization_id=int(identity.organization_id),
            target_organization_id=int(row["target_organization_id"]),
        )
    except DBAPIError as exc:
        db.rollback()
        _map_platform_db_error(exc)
        raise
    finally:
        db.close()


def readonly_impersonation_db(
    request: Request,
    context: ReadOnlyImpersonationContext = Depends(
        resolve_readonly_impersonation_context
    ),
) -> Generator[Connection, None, None]:
    if request.method.upper() not in ALLOWED_IMPERSONATION_METHODS:
        raise HTTPException(
            status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
            detail="Read-only impersonation accepts GET, HEAD, or OPTIONS only.",
            headers={"Allow": "GET, HEAD, OPTIONS"},
        )

    engine = get_impersonation_engine()

    with engine.connect() as connection:
        transaction = connection.begin()
        try:
            connection.exec_driver_sql("SET TRANSACTION READ ONLY")
            connection.exec_driver_sql(
                f"SET LOCAL ROLE {IMPERSONATION_ROLE}"
            )
            connection.execute(
                text(
                    "SELECT set_config("
                    "'app.current_organization_id', :org_id, true)"
                ),
                {"org_id": str(context.target_organization_id)},
            )

            effective_role = connection.execute(
                text("SELECT current_user")
            ).scalar_one()
            if effective_role != IMPERSONATION_ROLE:
                raise RuntimeError(
                    "Read-only impersonation database role was not activated."
                )

            yield connection
        finally:
            transaction.rollback()
