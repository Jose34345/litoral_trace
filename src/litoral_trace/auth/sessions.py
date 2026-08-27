"""Servicios de sesiones persistentes y refresh tokens."""
from __future__ import annotations

import hashlib
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from sqlalchemy import Select, select
from sqlalchemy.orm import Session

from litoral_trace.config import get_settings
from litoral_trace.db.auth_bootstrap import lookup_session_bootstrap_by_token_hash
from litoral_trace.db.models import Organization, User, UserSession
from litoral_trace.db.tenant import set_tenant_db_context

ACCESS_TOKEN_COOKIE_KEY = "session_jwt"
REFRESH_TOKEN_COOKIE_KEY = "refresh_token"


class SessionSecurityError(RuntimeError):
    """Error de autenticacion seguro para refresh/logout."""

    def __init__(
        self,
        message: str,
        *,
        code: str = "session_security_error",
        organization_id: int | None = None,
        user_id: int | None = None,
        session_id: int | None = None,
        family_id: str | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.organization_id = organization_id
        self.user_id = user_id
        self.session_id = session_id
        self.family_id = family_id


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def ensure_utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def build_refresh_token_expiration(*, now: datetime | None = None) -> datetime:
    issued_at = now or utc_now()
    return issued_at + timedelta(days=get_settings().jwt.refresh_token_expire_days)


def generate_refresh_token() -> str:
    return secrets.token_urlsafe(48)


def hash_refresh_token(refresh_token: str) -> str:
    normalized_refresh_token = refresh_token.strip()
    if not normalized_refresh_token:
        raise SessionSecurityError("Refresh token invalido o expirado.")

    return hashlib.sha256(
        normalized_refresh_token.encode("utf-8")
    ).hexdigest()


def sanitize_ip_address(ip_address: str | None) -> str | None:
    if not ip_address:
        return None
    normalized_ip = ip_address.strip()
    return normalized_ip[:45] or None


def sanitize_user_agent(user_agent: str | None) -> str | None:
    if not user_agent:
        return None
    normalized_user_agent = user_agent.strip()
    return normalized_user_agent[:512] or None


def _supports_row_locking(db_session: Session) -> bool:
    bind = db_session.get_bind()
    return bind is not None and bind.dialect.name == "postgresql"


def _build_session_lookup_statement(token_hash: str) -> Select[tuple[UserSession]]:
    return select(UserSession).where(UserSession.token_hash == token_hash)


def _get_session_by_token_hash(
    db_session: Session,
    *,
    token_hash: str,
    for_update: bool,
) -> UserSession | None:
    statement = _build_session_lookup_statement(token_hash)
    if for_update and _supports_row_locking(db_session):
        statement = statement.with_for_update()
    return db_session.execute(statement).scalar_one_or_none()


def _get_session_by_id(
    db_session: Session,
    *,
    session_id: int,
    for_update: bool,
) -> UserSession | None:
    statement = select(UserSession).where(UserSession.id == session_id)
    if for_update and _supports_row_locking(db_session):
        statement = statement.with_for_update()
    return db_session.execute(statement).scalar_one_or_none()


def _revoke_family(
    db_session: Session,
    *,
    family_id: str,
    organization_id: int,
    revoked_at: datetime,
) -> None:
    family_sessions = db_session.execute(
        select(UserSession).where(
            UserSession.family_id == family_id,
            UserSession.organization_id == organization_id,
            UserSession.revoked_at.is_(None),
        )
    ).scalars().all()

    for family_session in family_sessions:
        family_session.revoked_at = revoked_at


def _assert_user_and_organization_are_active(
    *,
    user: User | None,
    organization: Organization | None,
    current_session: UserSession,
    db_session: Session,
    now: datetime,
) -> tuple[User, Organization]:
    if user is None or organization is None:
        _revoke_family(
            db_session,
            family_id=current_session.family_id,
            organization_id=current_session.organization_id,
            revoked_at=now,
        )
        raise SessionSecurityError("Refresh token invalido o expirado.")

    if user.organization_id != current_session.organization_id:
        _revoke_family(
            db_session,
            family_id=current_session.family_id,
            organization_id=current_session.organization_id,
            revoked_at=now,
        )
        raise SessionSecurityError("Refresh token invalido o expirado.")

    if not user.is_active or not organization.is_active:
        _revoke_family(
            db_session,
            family_id=current_session.family_id,
            organization_id=current_session.organization_id,
            revoked_at=now,
        )
        raise SessionSecurityError("Refresh token invalido o expirado.")

    return user, organization


@dataclass(frozen=True)
class IssuedSession:
    session: UserSession
    refresh_token: str


def create_user_session(
    db_session: Session,
    *,
    user: User,
    organization: Organization,
    family_id: str | None = None,
    created_ip: str | None = None,
    user_agent: str | None = None,
    now: datetime | None = None,
) -> IssuedSession:
    issued_at = now or utc_now()
    refresh_token = generate_refresh_token()
    session_record = UserSession(
        user_id=user.id,
        organization_id=organization.id,
        family_id=family_id or str(uuid4()),
        token_hash=hash_refresh_token(refresh_token),
        issued_at=issued_at,
        expires_at=build_refresh_token_expiration(now=issued_at),
        created_ip=sanitize_ip_address(created_ip),
        user_agent=sanitize_user_agent(user_agent),
    )
    db_session.add(session_record)
    db_session.flush()

    return IssuedSession(
        session=session_record,
        refresh_token=refresh_token,
    )


@dataclass(frozen=True)
class RotatedSession:
    previous_session: UserSession
    new_session: UserSession
    refresh_token: str
    user: User
    organization: Organization


def rotate_refresh_session(
    db_session: Session,
    *,
    refresh_token: str,
    created_ip: str | None = None,
    user_agent: str | None = None,
    now: datetime | None = None,
) -> RotatedSession:
    rotation_time = ensure_utc_datetime(now or utc_now())
    session_lookup = lookup_session_bootstrap_by_token_hash(
        db_session,
        token_hash=hash_refresh_token(refresh_token),
    )

    if session_lookup is None:
        raise SessionSecurityError("Refresh token invalido o expirado.")

    set_tenant_db_context(db_session, session_lookup.organization_id)
    # The bootstrap SECURITY DEFINER function already serializes PostgreSQL
    # consumers with FOR UPDATE before tenant context exists. Lock the same
    # parent again under the tenant-scoped ORM path as defense in depth so the
    # reuse/rotation invariant remains explicit even if bootstrap internals
    # change later.
    current_session = _get_session_by_id(
        db_session,
        session_id=session_lookup.id,
        for_update=True,
    )
    if current_session is None:
        raise SessionSecurityError("Refresh token invalido o expirado.")

    if current_session.revoked_at is not None:
        _revoke_family(
            db_session,
            family_id=current_session.family_id,
            organization_id=current_session.organization_id,
            revoked_at=rotation_time,
        )
        raise SessionSecurityError(
            "Refresh token invalido o expirado.",
            code="refresh_reuse",
            organization_id=current_session.organization_id,
            user_id=current_session.user_id,
            session_id=current_session.id,
            family_id=current_session.family_id,
        )

    if ensure_utc_datetime(current_session.expires_at) <= rotation_time:
        current_session.revoked_at = rotation_time
        raise SessionSecurityError(
            "Refresh token invalido o expirado.",
            code="refresh_expired",
            organization_id=current_session.organization_id,
            user_id=current_session.user_id,
            session_id=current_session.id,
            family_id=current_session.family_id,
        )

    user = db_session.get(User, current_session.user_id)
    organization = db_session.get(Organization, current_session.organization_id)
    user, organization = _assert_user_and_organization_are_active(
        user=user,
        organization=organization,
        current_session=current_session,
        db_session=db_session,
        now=rotation_time,
    )

    issued_session = create_user_session(
        db_session,
        user=user,
        organization=organization,
        family_id=current_session.family_id,
        created_ip=created_ip,
        user_agent=user_agent,
        now=rotation_time,
    )

    current_session.revoked_at = rotation_time
    current_session.replaced_by_session_id = issued_session.session.id
    user.last_login_at = rotation_time
    db_session.flush()

    return RotatedSession(
        previous_session=current_session,
        new_session=issued_session.session,
        refresh_token=issued_session.refresh_token,
        user=user,
        organization=organization,
    )


def revoke_session(
    db_session: Session,
    *,
    refresh_token: str | None = None,
    session_id: int | None = None,
    now: datetime | None = None,
) -> UserSession | None:
    """Revoca una sesión y toda su familia de rotación.

    Una familia representa un único linaje de login/refresh. Revocarla completa
    garantiza que un logout explícito prevalezca incluso si una renovación
    keepalive concurrente ya creó un sucesor antes de que el logout obtenga el
    lock de la sesión padre.
    """

    revoked_at = ensure_utc_datetime(now or utc_now())
    session_record: UserSession | None = None

    if refresh_token:
        session_lookup = lookup_session_bootstrap_by_token_hash(
            db_session,
            token_hash=hash_refresh_token(refresh_token),
        )
        if session_lookup is not None:
            set_tenant_db_context(db_session, session_lookup.organization_id)
            # The bootstrap lookup already takes FOR UPDATE on PostgreSQL. Lock
            # again through the tenant-scoped path for the same explicit
            # serialization invariant used by refresh rotation.
            session_record = _get_session_by_id(
                db_session,
                session_id=session_lookup.id,
                for_update=True,
            )
    elif session_id is not None:
        session_record = _get_session_by_id(
            db_session,
            session_id=session_id,
            for_update=True,
        )

    if session_record is None:
        return None

    _revoke_family(
        db_session,
        family_id=session_record.family_id,
        organization_id=session_record.organization_id,
        revoked_at=revoked_at,
    )
    db_session.flush()

    return session_record
