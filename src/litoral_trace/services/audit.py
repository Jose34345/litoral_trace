"""Centralized audit trail helpers for tenant-aware enterprise events."""
from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
import re
from typing import Any
from uuid import uuid4

from fastapi import Request
from sqlalchemy.orm import Session

from litoral_trace.auth.sessions import sanitize_ip_address, sanitize_user_agent
from litoral_trace.db.engine import get_db_session
from litoral_trace.db.models import AuditLog
from litoral_trace.db.tenant import set_tenant_db_context


class AuditOutcome(StrEnum):
    SUCCESS = "success"
    FAILURE = "failure"
    DENIED = "denied"


class AuditAction(StrEnum):
    AUTH_LOGIN_SUCCESS = "auth.login.success"
    AUTH_LOGIN_FAILURE = "auth.login.failure"
    AUTH_REFRESH_SUCCESS = "auth.refresh.success"
    AUTH_REFRESH_REUSE = "auth.refresh.reuse"
    AUTH_LOGOUT = "auth.logout"
    LOTE_CREATE = "lote.create"
    LOTE_UPDATE = "lote.update"
    LOTE_DELETE = "lote.delete"
    LOTE_BATCH_UPLOAD = "lote.batch_upload"
    LOTE_BATCH_EVIDENCE_LINK = "lote.batch_evidence.link"
    LOTE_BATCH_EVIDENCE_UNLINK = "lote.batch_evidence.unlink"
    SATELLITE_NDVI_RUN = "satellite.ndvi.run"
    SATELLITE_JOB_SUBMIT = "satellite.job.submit"
    SATELLITE_JOB_SUCCEEDED = "satellite.job.succeeded"
    SATELLITE_JOB_FAILED = "satellite.job.failed"
    VAULT_UPLOAD = "vault.upload"
    VAULT_DOWNLOAD = "vault.download"
    VAULT_DELETE = "vault.delete"
    VAULT_INTEGRITY_FAILURE = "vault.integrity_failure"
    SETTINGS_INVITE_DEMO = "settings.invite_demo_user"
    PLATFORM_ORGANIZATION_CREATE = "platform.organization.create"
    PLATFORM_ORGANIZATION_STATUS_CHANGE = "platform.organization.status_change"
    PLATFORM_ORGANIZATION_ADMIN_CREATE = "platform.organization_admin.create"
    PLATFORM_LICENSE_CREATE = "platform.license.create"
    PLATFORM_LICENSE_UPDATE = "platform.license.update"


SENSITIVE_METADATA_KEYS = frozenset(
    {
        "password",
        "password_hash",
        "token",
        "access_token",
        "refresh_token",
        "token_hash",
        "refresh_token_hash",
        "lease_token",
        "authorization",
        "cookie",
        "set-cookie",
        "api_key",
        "apikey",
        "secret",
        "database_url",
        "migration_database_url",
        "worker_database_url",
        "private_key",
        "private_key_id",
        "service_account",
        "service_account_json",
        "credentials",
        "google_application_credentials",
        "polygon_wkt",
        "polygon_wkt_snapshot",
        "idempotency_key",
        "jwt",
        "object_key",
        "storage_key",
        "storage_bucket",
        "bucket_name",
        "access_key_id",
        "secret_access_key",
        "session_token",
        "presigned_url",
    }
)


_MAX_AUDIT_DETAIL_LENGTH = 2048
_SENSITIVE_DETAIL_PATTERNS = (
    re.compile(
        r"postgres(?:ql)?(?:\+[a-z0-9_]+)?://[^\s]+",
        re.IGNORECASE,
    ),
    re.compile(r"Bearer\s+[A-Za-z0-9._~+\-/=]+", re.IGNORECASE),
    re.compile(
        (
            r"\b(?:access_token|refresh_token|lease_token|authorization|"
            r"password|api_key|client_secret|private_key|credentials|"
            r"secret_access_key|session_token|object_key|presigned_url)"
            r"\s*[:=]\s*(?:\"[^\"]*\"|'[^']*'|[^\s,;]+)"
        ),
        re.IGNORECASE,
    ),
    re.compile(
        r"-----BEGIN [^-]*PRIVATE KEY-----.*?-----END [^-]*PRIVATE KEY-----",
        re.IGNORECASE | re.DOTALL,
    ),
)
_TRACEBACK_PATTERN = re.compile(
    r"Traceback\s*\(most recent call last\):",
    re.IGNORECASE,
)
_EXCEPTION_REPR_PATTERN = re.compile(
    r"\b[A-Za-z_][A-Za-z0-9_.]*(?:Error|Exception)\s*\(",
)


@dataclass(frozen=True)
class AuditActor:
    organization_id: int
    user_id: int | None
    username: str | None
    role: str | None


@dataclass(frozen=True)
class AuditRequestContext:
    request_id: str
    ip_address: str | None
    user_agent: str | None


def build_audit_actor(
    *,
    organization_id: int,
    user_id: int | None,
    username: str | None,
    role: str | None,
) -> AuditActor:
    return AuditActor(
        organization_id=int(organization_id),
        user_id=user_id,
        username=(username or "").strip() or None,
        role=(role or "").strip() or None,
    )


def build_audit_actor_from_user(user: Any) -> AuditActor:
    return build_audit_actor(
        organization_id=int(user.organization_id),
        user_id=getattr(user, "user_id", None) or getattr(user, "id", None),
        username=getattr(user, "username", None),
        role=getattr(user, "role", None),
    )


def build_request_audit_context(
    request: Request | None,
) -> AuditRequestContext:
    request_id = None
    ip_address = None
    user_agent = None

    if request is not None:
        request_id = (request.headers.get("x-request-id") or "").strip() or None
        if request.client is not None:
            ip_address = sanitize_ip_address(request.client.host)
        user_agent = sanitize_user_agent(request.headers.get("user-agent"))

    return AuditRequestContext(
        request_id=request_id or uuid4().hex,
        ip_address=ip_address,
        user_agent=user_agent,
    )


def _sanitize_json_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        return value[:2048]
    if isinstance(value, dict):
        sanitized: dict[str, Any] = {}
        for raw_key, raw_value in value.items():
            normalized_key = str(raw_key).strip()
            if normalized_key.lower() in SENSITIVE_METADATA_KEYS:
                continue

            sanitized_value = _sanitize_json_value(raw_value)
            if sanitized_value not in (None, {}, []):
                sanitized[normalized_key] = sanitized_value
        return sanitized
    if isinstance(value, (list, tuple, set)):
        return [
            item
            for item in (_sanitize_json_value(item) for item in value)
            if item not in (None, {}, [])
        ]
    return str(value)[:2048]


def sanitize_audit_metadata(metadata: dict[str, Any] | None) -> dict[str, Any] | None:
    if metadata is None:
        return None

    sanitized = _sanitize_json_value(metadata)
    if not isinstance(sanitized, dict):
        return None

    return sanitized or None


def sanitize_audit_detail(detail: str | None) -> str | None:
    """Bound audit detail and remove common secret-bearing representations."""

    normalized_detail = str(detail or "").strip()
    if not normalized_detail:
        return None

    if (
        _TRACEBACK_PATTERN.search(normalized_detail)
        or _EXCEPTION_REPR_PATTERN.search(normalized_detail)
    ):
        return "[REDACTED_EXCEPTION_DETAIL]"

    for pattern in _SENSITIVE_DETAIL_PATTERNS:
        normalized_detail = pattern.sub("[REDACTED]", normalized_detail)

    return normalized_detail.strip()[:_MAX_AUDIT_DETAIL_LENGTH] or None


def record_audit_event(
    db_session: Session,
    *,
    actor: AuditActor,
    action: AuditAction | str,
    entity_type: str,
    outcome: AuditOutcome | str,
    target_organization_id: int | None = None,
    entity_id: int | None = None,
    request_context: AuditRequestContext | None = None,
    metadata: dict[str, Any] | None = None,
    before_data: dict[str, Any] | None = None,
    after_data: dict[str, Any] | None = None,
    detail: str | None = None,
) -> AuditLog:
    try:
        resolved_action = AuditAction(str(action))
    except ValueError as exc:
        raise ValueError(f"Unsupported audit action: {action}") from exc

    try:
        resolved_outcome = AuditOutcome(str(outcome))
    except ValueError as exc:
        raise ValueError(f"Unsupported audit outcome: {outcome}") from exc

    normalized_entity_type = entity_type.strip()
    if not normalized_entity_type:
        raise ValueError("entity_type is required for audit events.")

    effective_organization_id = target_organization_id or actor.organization_id
    set_tenant_db_context(db_session, effective_organization_id)

    request_payload = request_context or AuditRequestContext(
        request_id=uuid4().hex,
        ip_address=None,
        user_agent=None,
    )

    audit_metadata = sanitize_audit_metadata(metadata) or {}
    if actor.organization_id != effective_organization_id:
        audit_metadata["actor_organization_id"] = actor.organization_id

    envelope = {
        "outcome": resolved_outcome.value,
        "request_id": request_payload.request_id,
        "actor_role": actor.role,
        "user_agent": request_payload.user_agent,
        "metadata": audit_metadata or None,
    }

    if after_data is not None:
        envelope["state_after"] = sanitize_audit_metadata(after_data)

    audit_log = AuditLog(
        organization_id=effective_organization_id,
        user_id=actor.user_id,
        username=actor.username,
        action=resolved_action.value,
        entity_type=normalized_entity_type,
        entity_id=entity_id,
        before_data=sanitize_audit_metadata(before_data),
        after_data=envelope,
        detail=sanitize_audit_detail(detail),
        ip_address=request_payload.ip_address,
    )
    db_session.add(audit_log)
    db_session.flush()
    return audit_log


def record_audit_event_now(
    *,
    actor: AuditActor,
    action: AuditAction | str,
    entity_type: str,
    outcome: AuditOutcome | str,
    target_organization_id: int | None = None,
    entity_id: int | None = None,
    request_context: AuditRequestContext | None = None,
    metadata: dict[str, Any] | None = None,
    before_data: dict[str, Any] | None = None,
    after_data: dict[str, Any] | None = None,
    detail: str | None = None,
    best_effort: bool = False,
) -> bool:
    db_session = get_db_session()
    if db_session is None:
        if best_effort:
            return False
        raise RuntimeError("Servicio de base de datos no disponible.")

    try:
        record_audit_event(
            db_session,
            actor=actor,
            action=action,
            entity_type=entity_type,
            outcome=outcome,
            target_organization_id=target_organization_id,
            entity_id=entity_id,
            request_context=request_context,
            metadata=metadata,
            before_data=before_data,
            after_data=after_data,
            detail=detail,
        )
        db_session.commit()
        return True
    except Exception:
        db_session.rollback()
        if best_effort:
            return False
        raise
    finally:
        db_session.close()
