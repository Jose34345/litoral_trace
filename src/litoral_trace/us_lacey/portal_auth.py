"""Isolated browser authentication for the U.S. Lacey portal.

The portal intentionally does not issue or verify the generic Litoral Trace JWT.
It uses a high-entropy opaque cookie whose SHA-256 digest is persisted in the
existing tenant-scoped ``user_sessions`` table inside the U.S.-only database.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import os
import secrets
from uuid import uuid4

from sqlalchemy import select

from litoral_trace.auth.passwords import verify_password
from litoral_trace.db.auth_bootstrap import (
    lookup_login_bootstrap_user,
    lookup_session_bootstrap_by_token_hash,
)
from litoral_trace.db.models import (
    Organization,
    User,
    UserSession,
    UsLaceyOrganizationProfile,
)
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.us_lacey.db import get_us_lacey_db_session


US_LACEY_SESSION_COOKIE = "us_lacey_session"


class UsLaceyPortalAuthError(RuntimeError):
    """Sanitized browser-auth failure with a stable application code."""

    def __init__(self, message: str, *, code: str = "auth_error") -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class UsLaceyPortalIdentity:
    user_id: int
    organization_id: int
    email: str
    full_name: str
    legal_name: str
    business_type: str
    account_status: str


@dataclass(frozen=True)
class UsLaceyPortalLoginResult:
    session_token: str
    expires_at: datetime
    identity: UsLaceyPortalIdentity


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _session_token_hash(token: str) -> str:
    normalized = str(token or "").strip()
    if not normalized:
        raise UsLaceyPortalAuthError(
            "Your session is invalid or expired.", code="session_invalid"
        )
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def get_us_lacey_session_ttl_hours() -> int:
    raw = str(os.environ.get("US_LACEY_SESSION_TTL_HOURS", "12")).strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise UsLaceyPortalAuthError(
            "U.S. portal session configuration is invalid.", code="configuration"
        ) from exc
    if value < 1 or value > 720:
        raise UsLaceyPortalAuthError(
            "U.S. portal session configuration is invalid.", code="configuration"
        )
    return value


def _build_identity(
    *,
    user: User,
    profile: UsLaceyOrganizationProfile,
) -> UsLaceyPortalIdentity:
    return UsLaceyPortalIdentity(
        user_id=user.id,
        organization_id=user.organization_id,
        email=user.email,
        full_name=(user.full_name or user.email).strip(),
        legal_name=profile.legal_name,
        business_type=profile.business_type,
        account_status=profile.account_status,
    )


def login_us_lacey_user(
    *,
    email: str,
    password: str,
    client_ip: str | None = None,
    user_agent: str | None = None,
    now: datetime | None = None,
) -> UsLaceyPortalLoginResult:
    """Authenticate a verified U.S. account and create one opaque web session."""
    normalized_email = str(email or "").strip().lower()
    if not normalized_email or not password:
        raise UsLaceyPortalAuthError(
            "Email or password is incorrect.", code="invalid_credentials"
        )

    issued_at = _ensure_utc(now or _utc_now())
    raw_token = secrets.token_urlsafe(48)
    token_hash = _session_token_hash(raw_token)
    expires_at = issued_at + timedelta(hours=get_us_lacey_session_ttl_hours())

    session = get_us_lacey_db_session()
    try:
        bootstrap = lookup_login_bootstrap_user(session, username=normalized_email)
        if bootstrap is None or not bootstrap.is_active:
            raise UsLaceyPortalAuthError(
                "Email or password is incorrect.", code="invalid_credentials"
            )
        if not verify_password(password, bootstrap.password_hash):
            raise UsLaceyPortalAuthError(
                "Email or password is incorrect.", code="invalid_credentials"
            )

        set_tenant_db_context(session, bootstrap.organization_id)
        user = session.execute(
            select(User).where(
                User.id == bootstrap.id,
                User.organization_id == bootstrap.organization_id,
            )
        ).scalar_one_or_none()
        organization = session.execute(
            select(Organization).where(Organization.id == bootstrap.organization_id)
        ).scalar_one_or_none()
        profile = session.execute(
            select(UsLaceyOrganizationProfile).where(
                UsLaceyOrganizationProfile.organization_id == bootstrap.organization_id
            )
        ).scalar_one_or_none()

        if user is None or organization is None or profile is None:
            raise UsLaceyPortalAuthError(
                "Email or password is incorrect.", code="invalid_credentials"
            )
        if not user.is_active or not organization.is_active:
            raise UsLaceyPortalAuthError(
                "This account is not available.", code="account_disabled"
            )
        if profile.account_status == "PENDING_EMAIL":
            raise UsLaceyPortalAuthError(
                "Verify your email before signing in.", code="email_unverified"
            )
        if profile.account_status == "SUSPENDED":
            raise UsLaceyPortalAuthError(
                "This account is suspended. Contact support.", code="account_suspended"
            )
        if profile.account_status not in {"PAYMENT_PENDING", "PILOT", "ACTIVE"}:
            raise UsLaceyPortalAuthError(
                "This account cannot sign in yet.", code="account_unavailable"
            )

        session_record = UserSession(
            user_id=user.id,
            organization_id=organization.id,
            family_id=str(uuid4()),
            token_hash=token_hash,
            issued_at=issued_at,
            expires_at=expires_at,
            created_ip=(str(client_ip or "").strip()[:45] or None),
            user_agent=(str(user_agent or "").strip()[:512] or None),
        )
        user.last_login_at = issued_at
        session.add(session_record)
        session.commit()
        return UsLaceyPortalLoginResult(
            session_token=raw_token,
            expires_at=expires_at,
            identity=_build_identity(user=user, profile=profile),
        )
    except UsLaceyPortalAuthError:
        session.rollback()
        raise
    except Exception as exc:
        session.rollback()
        raise UsLaceyPortalAuthError(
            "Unable to sign in right now.", code="auth_unavailable"
        ) from exc
    finally:
        session.close()


def resolve_us_lacey_session(
    token: str,
    *,
    now: datetime | None = None,
) -> UsLaceyPortalIdentity:
    """Resolve an opaque browser session without trusting client tenant input."""
    token_hash = _session_token_hash(token)
    current_time = _ensure_utc(now or _utc_now())
    session = get_us_lacey_db_session()
    try:
        bootstrap = lookup_session_bootstrap_by_token_hash(
            session, token_hash=token_hash
        )
        if bootstrap is None:
            raise UsLaceyPortalAuthError(
                "Your session is invalid or expired.", code="session_invalid"
            )

        set_tenant_db_context(session, bootstrap.organization_id)
        session_record = session.execute(
            select(UserSession).where(
                UserSession.id == bootstrap.id,
                UserSession.user_id == bootstrap.user_id,
                UserSession.organization_id == bootstrap.organization_id,
            )
        ).scalar_one_or_none()
        if (
            session_record is None
            or session_record.revoked_at is not None
            or _ensure_utc(session_record.expires_at) <= current_time
        ):
            raise UsLaceyPortalAuthError(
                "Your session is invalid or expired.", code="session_invalid"
            )

        user = session.execute(
            select(User).where(
                User.id == bootstrap.user_id,
                User.organization_id == bootstrap.organization_id,
            )
        ).scalar_one_or_none()
        organization = session.execute(
            select(Organization).where(Organization.id == bootstrap.organization_id)
        ).scalar_one_or_none()
        profile = session.execute(
            select(UsLaceyOrganizationProfile).where(
                UsLaceyOrganizationProfile.organization_id == bootstrap.organization_id
            )
        ).scalar_one_or_none()

        if user is None or organization is None or profile is None:
            raise UsLaceyPortalAuthError(
                "Your session is invalid or expired.", code="session_invalid"
            )
        if not user.is_active or not organization.is_active:
            raise UsLaceyPortalAuthError(
                "This account is not available.", code="account_disabled"
            )
        if profile.account_status == "SUSPENDED":
            raise UsLaceyPortalAuthError(
                "This account is suspended. Contact support.", code="account_suspended"
            )
        return _build_identity(user=user, profile=profile)
    except UsLaceyPortalAuthError:
        session.rollback()
        raise
    except Exception as exc:
        session.rollback()
        raise UsLaceyPortalAuthError(
            "Unable to validate your session.", code="auth_unavailable"
        ) from exc
    finally:
        session.close()


def logout_us_lacey_user(token: str, *, now: datetime | None = None) -> None:
    """Revoke the exact browser session. Logout remains idempotent."""
    try:
        token_hash = _session_token_hash(token)
    except UsLaceyPortalAuthError:
        return

    session = get_us_lacey_db_session()
    try:
        bootstrap = lookup_session_bootstrap_by_token_hash(
            session, token_hash=token_hash
        )
        if bootstrap is None:
            session.rollback()
            return
        set_tenant_db_context(session, bootstrap.organization_id)
        session_record = session.execute(
            select(UserSession).where(
                UserSession.id == bootstrap.id,
                UserSession.organization_id == bootstrap.organization_id,
            )
        ).scalar_one_or_none()
        if session_record is not None and session_record.revoked_at is None:
            session_record.revoked_at = _ensure_utc(now or _utc_now())
            session.commit()
        else:
            session.rollback()
    except Exception:
        session.rollback()
    finally:
        session.close()
