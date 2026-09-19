"""Isolated browser authentication for the U.S. Lacey portal.

The portal intentionally does not issue or verify the generic Litoral Trace JWT.
It uses a high-entropy opaque cookie whose SHA-256 digest is persisted in the
U.S.-only database. All pre-tenant auth operations cross FORCE RLS only through
narrow SECURITY DEFINER functions owned by the dedicated platform role.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import os
import secrets
from uuid import uuid4

from sqlalchemy import text

from litoral_trace.auth.passwords import verify_password
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


def _identity_from_row(row) -> UsLaceyPortalIdentity:
    return UsLaceyPortalIdentity(
        user_id=int(row["user_id"]),
        organization_id=int(row["organization_id"]),
        email=str(row["email"]),
        full_name=str(row["full_name"]),
        legal_name=str(row["legal_name"]),
        business_type=str(row["business_type"]),
        account_status=str(row["account_status"]),
    )


def _lookup_session_row(session, token_hash: str):
    return session.execute(
        text(
            "SELECT * FROM public.us_lacey_portal_session_lookup(:token_hash)"
        ),
        {"token_hash": token_hash},
    ).mappings().one_or_none()


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
        login_row = session.execute(
            text(
                "SELECT * FROM public.us_lacey_portal_login_lookup(:email)"
            ),
            {"email": normalized_email},
        ).mappings().one_or_none()
        if login_row is None:
            raise UsLaceyPortalAuthError(
                "Email or password is incorrect.", code="invalid_credentials"
            )
        if not verify_password(password, str(login_row["password_hash"])):
            raise UsLaceyPortalAuthError(
                "Email or password is incorrect.", code="invalid_credentials"
            )
        if not bool(login_row["user_is_active"]) or not bool(
            login_row["organization_is_active"]
        ):
            raise UsLaceyPortalAuthError(
                "This account is not available.", code="account_disabled"
            )

        account_status = str(login_row["account_status"])
        if account_status == "PENDING_EMAIL":
            raise UsLaceyPortalAuthError(
                "Verify your email before signing in.", code="email_unverified"
            )
        if account_status == "SUSPENDED":
            raise UsLaceyPortalAuthError(
                "This account is suspended. Contact support.", code="account_suspended"
            )
        if account_status not in {"PAYMENT_PENDING", "PILOT", "ACTIVE"}:
            raise UsLaceyPortalAuthError(
                "This account cannot sign in yet.", code="account_unavailable"
            )

        session.execute(
            text(
                """
                SELECT * FROM public.us_lacey_portal_create_session(
                    :user_id,
                    :organization_id,
                    :token_hash,
                    :family_id,
                    :expires_at,
                    :client_ip,
                    :user_agent
                )
                """
            ),
            {
                "user_id": int(login_row["user_id"]),
                "organization_id": int(login_row["organization_id"]),
                "token_hash": token_hash,
                "family_id": str(uuid4()),
                "expires_at": expires_at,
                "client_ip": str(client_ip or "").strip()[:45] or None,
                "user_agent": str(user_agent or "").strip()[:512] or None,
            },
        ).mappings().one()

        identity_row = _lookup_session_row(session, token_hash)
        if identity_row is None:
            raise UsLaceyPortalAuthError(
                "Unable to establish your session.", code="auth_unavailable"
            )

        session.commit()
        return UsLaceyPortalLoginResult(
            session_token=raw_token,
            expires_at=expires_at,
            identity=_identity_from_row(identity_row),
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
        row = _lookup_session_row(session, token_hash)
        if row is None:
            raise UsLaceyPortalAuthError(
                "Your session is invalid or expired.", code="session_invalid"
            )
        expires_at = _ensure_utc(row["expires_at"])
        if expires_at <= current_time:
            raise UsLaceyPortalAuthError(
                "Your session is invalid or expired.", code="session_invalid"
            )
        return _identity_from_row(row)
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
    del now  # PostgreSQL supplies the authoritative revocation timestamp.
    try:
        token_hash = _session_token_hash(token)
    except UsLaceyPortalAuthError:
        return

    session = get_us_lacey_db_session()
    try:
        session.execute(
            text(
                "SELECT public.us_lacey_portal_revoke_session(:token_hash)"
            ),
            {"token_hash": token_hash},
        )
        session.commit()
    except Exception:
        session.rollback()
    finally:
        session.close()
