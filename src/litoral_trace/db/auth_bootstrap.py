"""Bootstrap helpers for auth flows that precede tenant context."""
from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from litoral_trace.db.models import User, UserSession


BOOTSTRAP_USER_BY_USERNAME_FUNCTION = "public.bootstrap_auth_user_by_username"
BOOTSTRAP_SESSION_BY_TOKEN_HASH_FUNCTION = (
    "public.bootstrap_auth_session_by_token_hash"
)


@dataclass(frozen=True)
class LoginBootstrapUser:
    id: int
    organization_id: int
    password_hash: str
    is_active: bool


@dataclass(frozen=True)
class SessionBootstrapLookup:
    id: int
    user_id: int
    organization_id: int


def _supports_postgresql_bootstrap_functions(db_session: Session) -> bool:
    bind = db_session.get_bind()
    return bind is not None and bind.dialect.name == "postgresql"


def lookup_login_bootstrap_user(
    db_session: Session,
    *,
    username: str,
) -> LoginBootstrapUser | None:
    normalized_username = username.strip()
    if not normalized_username:
        return None

    if _supports_postgresql_bootstrap_functions(db_session):
        row = db_session.execute(
            text(
                "SELECT id, organization_id, password_hash, is_active "
                f"FROM {BOOTSTRAP_USER_BY_USERNAME_FUNCTION}(:username)"
            ),
            {"username": normalized_username},
        ).mappings().one_or_none()

        if row is None:
            return None

        return LoginBootstrapUser(
            id=int(row["id"]),
            organization_id=int(row["organization_id"]),
            password_hash=str(row["password_hash"]),
            is_active=bool(row["is_active"]),
        )

    user = db_session.execute(
        select(User).where(User.username == normalized_username)
    ).scalar_one_or_none()
    if user is None:
        return None

    return LoginBootstrapUser(
        id=user.id,
        organization_id=user.organization_id,
        password_hash=user.password_hash,
        is_active=user.is_active,
    )


def lookup_session_bootstrap_by_token_hash(
    db_session: Session,
    *,
    token_hash: str,
) -> SessionBootstrapLookup | None:
    if _supports_postgresql_bootstrap_functions(db_session):
        row = db_session.execute(
            text(
                "SELECT id, user_id, organization_id "
                f"FROM {BOOTSTRAP_SESSION_BY_TOKEN_HASH_FUNCTION}(:token_hash)"
            ),
            {"token_hash": token_hash},
        ).mappings().one_or_none()

        if row is None:
            return None

        return SessionBootstrapLookup(
            id=int(row["id"]),
            user_id=int(row["user_id"]),
            organization_id=int(row["organization_id"]),
        )

    session_record = db_session.execute(
        select(UserSession).where(UserSession.token_hash == token_hash)
    ).scalar_one_or_none()
    if session_record is None:
        return None

    return SessionBootstrapLookup(
        id=session_record.id,
        user_id=session_record.user_id,
        organization_id=session_record.organization_id,
    )
