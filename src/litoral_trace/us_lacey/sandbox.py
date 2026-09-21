"""Ephemeral zero-touch sandbox primitives for the U.S. Lacey product.

Sandbox visitors are real tenant-scoped principals. The browser receives only a
high-entropy opaque session token; organization identity is always resolved
server-side through the existing hardened portal session lookup.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import secrets
from uuid import uuid4

from sqlalchemy import select, text

from litoral_trace.auth.passwords import hash_password
from litoral_trace.db.models import UsLaceyOperation, UsLaceySubscription
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.us_lacey.db import get_us_lacey_db_session


SANDBOX_TTL_HOURS = 4
SANDBOX_MAX_OPERATIONS = 1
SANDBOX_MAX_DOCUMENTS_PER_OPERATION = 3


class UsLaceySandboxError(RuntimeError):
    """Safe sandbox error suitable for a browser response."""

    def __init__(self, message: str, *, code: str = "sandbox_error") -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True, slots=True)
class UsLaceySandboxSession:
    organization_id: int
    user_id: int
    session_id: int
    session_token: str
    expires_at: datetime


@dataclass(frozen=True, slots=True)
class UsLaceySandboxPolicy:
    organization_id: int
    is_sandbox: bool
    expires_at: datetime | None
    max_operations: int = SANDBOX_MAX_OPERATIONS
    max_documents_per_operation: int = SANDBOX_MAX_DOCUMENTS_PER_OPERATION


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _token_hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def provision_us_lacey_sandbox(
    *,
    client_ip: str | None = None,
    user_agent: str | None = None,
    learning_opt_in: bool = False,
) -> UsLaceySandboxSession:
    """Atomically create an ephemeral tenant, principal and opaque web session."""

    raw_token = secrets.token_urlsafe(48)
    token_hash = _token_hash(raw_token)
    family_id = str(uuid4())

    # The password is intentionally unknowable and never returned. Sandbox auth
    # is session-only; it cannot later be recovered through the normal login form.
    password_hash = hash_password(secrets.token_urlsafe(32))

    session = get_us_lacey_db_session()
    try:
        row = session.execute(
            text(
                """
                SELECT *
                FROM public.us_lacey_sandbox_provision(
                    :token_hash,
                    :family_id,
                    :client_ip,
                    :user_agent,
                    :password_hash
                )
                """
            ),
            {
                "token_hash": token_hash,
                "family_id": family_id,
                "client_ip": str(client_ip or "").strip()[:45] or None,
                "user_agent": str(user_agent or "").strip()[:512] or None,
                "password_hash": password_hash,
            },
        ).mappings().one()

        session.execute(
            text(
                """
                SELECT public.us_lacey_sandbox_set_learning_consent(
                    :token_hash,
                    :organization_id,
                    :learning_opt_in
                )
                """
            ),
            {
                "token_hash": token_hash,
                "organization_id": int(row["organization_id"]),
                "learning_opt_in": bool(learning_opt_in),
            },
        )

        session.commit()
        return UsLaceySandboxSession(
            organization_id=int(row["organization_id"]),
            user_id=int(row["user_id"]),
            session_id=int(row["session_id"]),
            session_token=raw_token,
            expires_at=_utc(row["expires_at"]),
        )
    except Exception as exc:
        session.rollback()
        if getattr(exc, "sqlstate", None) == "P4290":
            raise UsLaceySandboxError(
                "Too many sandbox sessions were started from this network. Try again later.",
                code="rate_limited",
            ) from exc
        raise UsLaceySandboxError(
            "Unable to start a sandbox workspace right now.",
            code="provision_failed",
        ) from exc
    finally:
        session.close()


def get_us_lacey_sandbox_policy(
    *,
    organization_id: int,
) -> UsLaceySandboxPolicy:
    """Read sandbox metadata through the tenant-scoped runtime connection."""

    org_id = int(organization_id)
    if org_id <= 0:
        raise UsLaceySandboxError("Sandbox organization is invalid.")

    session = get_us_lacey_db_session()
    try:
        set_tenant_db_context(session, org_id)
        subscription = session.scalar(
            select(UsLaceySubscription).where(
                UsLaceySubscription.organization_id == org_id
            )
        )
        if subscription is None:
            raise UsLaceySandboxError("Workspace is unavailable.")
        is_sandbox = str(subscription.plan_code) == "SANDBOX"
        expires_at = (
            _utc(subscription.renews_at)
            if is_sandbox and subscription.renews_at is not None
            else None
        )
        return UsLaceySandboxPolicy(
            organization_id=org_id,
            is_sandbox=is_sandbox,
            expires_at=expires_at,
        )
    except UsLaceySandboxError:
        raise
    except Exception as exc:
        raise UsLaceySandboxError(
            "Unable to verify sandbox limits right now."
        ) from exc
    finally:
        session.close()


def enforce_sandbox_document_capacity(
    *,
    organization_id: int,
    operation_id: int,
    incoming_document_count: int,
    now: datetime | None = None,
) -> None:
    """Fail before Vault writes when an ephemeral tenant exceeds its document cap."""

    incoming = int(incoming_document_count)
    if incoming <= 0:
        raise UsLaceySandboxError("Document count must be positive.")

    org_id = int(organization_id)
    op_id = int(operation_id)
    current_time = _utc(now or datetime.now(timezone.utc))

    session = get_us_lacey_db_session()
    try:
        set_tenant_db_context(session, org_id)
        subscription = session.scalar(
            select(UsLaceySubscription).where(
                UsLaceySubscription.organization_id == org_id
            )
        )
        if subscription is None:
            raise UsLaceySandboxError("Workspace is unavailable.")
        if str(subscription.plan_code) != "SANDBOX":
            return

        expires_at = (
            _utc(subscription.renews_at)
            if subscription.renews_at is not None
            else None
        )
        if expires_at is None or expires_at <= current_time:
            raise UsLaceySandboxError(
                "This sandbox has expired. Start a new sandbox to continue."
            )

        operation = session.scalar(
            select(UsLaceyOperation).where(
                UsLaceyOperation.organization_id == org_id,
                UsLaceyOperation.id == op_id,
            )
        )
        if operation is None:
            raise UsLaceySandboxError("Operation not found.")

        projected = int(operation.document_count) + incoming
        if projected > SANDBOX_MAX_DOCUMENTS_PER_OPERATION:
            raise UsLaceySandboxError(
                "Sandbox limit reached: up to 3 documents are allowed per operation."
            )
    except UsLaceySandboxError:
        raise
    except Exception as exc:
        raise UsLaceySandboxError(
            "Unable to verify sandbox document limits right now."
        ) from exc
    finally:
        session.close()
