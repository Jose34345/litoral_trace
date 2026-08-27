"""Servicios de sesiones persistentes y refresh tokens."""
from __future__ import annotations

import hashlib
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from sqlalchemy import Select, select, text
from sqlalchemy.engine import Connection, Engine
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


def _engine_from_session(db_session: Session) -> Engine | None:
    bind = db_session.get_bind()
    if isinstance(bind, Engine):
        return bind
    if isinstance(bind, Connection):
        return bind.engine
    return None


def _lookup_session_bootstrap_without_retained_row_lock(
    db_session: Session,
    *,
    token_hash: str,
):
    """Resolve pre-tenant session identity without retaining its bootstrap row lock.

    The legacy SECURITY DEFINER bootstrap function intentionally uses
    ``FOR UPDATE``. For family-level serialization we must not carry that row
    lock into the transaction that will acquire the family advisory lock,
    otherwise logout on an ancestor and refresh on a descendant can deadlock by
    taking row/family locks in opposite orders.

    PostgreSQL therefore performs the bootstrap lookup in a short independent
    transaction. Closing that transaction releases its bootstrap row lock before
    the caller establishes tenant context and acquires the shared family lock.
    SQLite/non-locking test paths keep the original in-session lookup.
    """

    if not _supports_row_locking(db_session):
        return lookup_session_bootstrap_by_token_hash(
            db_session,
            token_hash=token_hash,
        )

    engine = _engine_from_session(db_session)
    if engine is None:
        raise SessionSecurityError("Refresh token invalido o expirado.")

    with Session(bind=engine) as bootstrap_session:
        session_lookup = lookup_session_bootstrap_by_token_hash(
            bootstrap_session,
            token_hash=token_hash,
        )
        # The lookup is read-only. Rollback is deliberate: it releases the
        # SECURITY DEFINER FOR UPDATE lock without persisting any state.
        bootstrap_session.rollback()
        return session_lookup


def _family_advisory_lock_key(*, organization_id: int, family_id: str) -> int:
    material = f"litoral-trace:user-session-family:{organization_id}:{family_id}"
    digest = hashlib.sha256(material.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], byteorder="big", signed=True)


def _acquire_refresh_family_lock(
    db_session: Session,
    *,
    organization_id: int,
    family_id: str,
) -> None:
    """Serialize every refresh/logout mutation for one session family.

    PostgreSQL transaction advisory locks are independent of which generation
    (ancestor/descendant) the caller currently holds. This gives rotation and
    logout one stable mutex for the entire login lineage and avoids the distinct
    row-lock race identified during pre-pilot review.
    """

    normalized_family_id = str(family_id).strip()
    if organization_id <= 0 or not normalized_family_id:
        raise SessionSecurityError("Refresh token invalido o expirado.")

    if not _supports_row_locking(db_session):
        return

    db_session.execute(
        text("SELECT pg_advisory_xact_lock(:family_lock_key)"),
        {
            "family_lock_key": _family_advisory_lock_key(
                organization_id=organization_id,
                family_id=normalized_family_id,
            )
        },
    )


def _get_session_family_reference(
    db_session: Session,
    *,
    session_id: int,
) -> tuple[str, int] | None:
    row = db_session.execute(
        select(
            UserSession.family_id,
            UserSession.organization_id,
        ).where(UserSession.id == session_id)
    ).one_or_none()
    if row is None:
        return None

    family_id = str(row.family_id).strip()
    organization_id = int(row.organization_id)
    if not family_id or organization_id <= 0:
        return None
    return family_id, organization_id


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
    token_hash = hash_refresh_token(refresh_token)
    session_lookup = _lookup_session_bootstrap_without_retained_row_lock(
        db_session,
        token_hash=token_hash,
    )

    if session_lookup is None:
        raise SessionSecurityError("Refresh token invalido o expirado.")

    set_tenant_db_context(db_session, session_lookup.organization_id)
    family_reference = _get_session_family_reference(
        db_session,
        session_id=session_lookup.id,
    )
    if family_reference is None:
        raise SessionSecurityError("Refresh token invalido o expirado.")

    family_id, family_organization_id = family_reference
    if family_organization_id != session_lookup.organization_id:
        raise SessionSecurityError("Refresh token invalido o expirado.")

    # Family lock must precede every tenant-scoped row lock. Logout can target an
    # ancestor while refresh targets a descendant, so a generation-specific row
    # lock is not a sufficient mutex for the login lineage.
    _acquire_refresh_family_lock(
        db_session,
        organization_id=family_organization_id,
        family_id=family_id,
    )
    current_session = _get_session_by_id(
        db_session,
        session_id=session_lookup.id,
        for_update=True,
    )
    if current_session is None:
        raise SessionSecurityError("Refresh token invalido o expirado.")
    if (
        current_session.organization_id != family_organization_id
        or current_session.family_id != family_id
        or current_session.token_hash != token_hash
    ):
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

    Rotación y logout comparten el mismo advisory lock de familia antes de tomar
    locks de filas. Por eso un logout dirigido a cualquier ancestro puede esperar
    una rotación concurrente y luego revocar también su sucesor, o bien ganar
    primero e impedir que la rotación cree un sucesor válido.
    """

    revoked_at = ensure_utc_datetime(now or utc_now())
    target_session_id: int | None = None
    expected_organization_id: int | None = None

    if refresh_token:
        token_hash = hash_refresh_token(refresh_token)
        session_lookup = _lookup_session_bootstrap_without_retained_row_lock(
            db_session,
            token_hash=token_hash,
        )
        if session_lookup is None:
            return None
        set_tenant_db_context(db_session, session_lookup.organization_id)
        target_session_id = session_lookup.id
        expected_organization_id = session_lookup.organization_id
    elif session_id is not None:
        target_session_id = session_id
    else:
        return None

    family_reference = _get_session_family_reference(
        db_session,
        session_id=target_session_id,
    )
    if family_reference is None:
        return None

    family_id, family_organization_id = family_reference
    if (
        expected_organization_id is not None
        and family_organization_id != expected_organization_id
    ):
        raise SessionSecurityError("Refresh token invalido o expirado.")

    _acquire_refresh_family_lock(
        db_session,
        organization_id=family_organization_id,
        family_id=family_id,
    )
    session_record = _get_session_by_id(
        db_session,
        session_id=target_session_id,
        for_update=True,
    )
    if session_record is None:
        return None
    if (
        session_record.organization_id != family_organization_id
        or session_record.family_id != family_id
    ):
        raise SessionSecurityError("Refresh token invalido o expirado.")

    _revoke_family(
        db_session,
        family_id=family_id,
        organization_id=family_organization_id,
        revoked_at=revoked_at,
    )
    db_session.flush()

    return session_record
