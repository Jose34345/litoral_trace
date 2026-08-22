"""Centralized application RBAC for Litoral Trace."""
from __future__ import annotations

from collections.abc import Callable
from enum import StrEnum
from typing import Any

from fastapi import Depends, HTTPException, status


class Permission(StrEnum):
    LOTE_READ = "lote:read"
    LOTE_CREATE = "lote:create"
    LOTE_UPDATE = "lote:update"
    LOTE_DELETE = "lote:delete"
    TRACEABILITY_OPERATE = "traceability:operate"
    TRACEABILITY_DISPATCH = "traceability:dispatch"
    TRACEABILITY_EVIDENCE = "traceability:evidence"
    INTEGRATION_READ = "integration:read"
    INTEGRATION_MANAGE = "integration:manage"
    SATELLITE_RUN = "satellite:run"
    VAULT_READ = "vault:read"
    VAULT_UPLOAD = "vault:upload"
    VAULT_DELETE = "vault:delete"
    LICENSE_READ = "license:read"
    SETTINGS_WRITE = "settings:write"
    PLATFORM_ADMIN = "platform:admin"


ALL_PERMISSIONS = frozenset(Permission)

ROLE_PERMISSIONS: dict[str, frozenset[Permission]] = {
    "superadmin": ALL_PERMISSIONS,
    "admin": frozenset(
        {
            Permission.LOTE_READ,
            Permission.LOTE_CREATE,
            Permission.LOTE_UPDATE,
            Permission.LOTE_DELETE,
            Permission.TRACEABILITY_OPERATE,
            Permission.TRACEABILITY_DISPATCH,
            Permission.TRACEABILITY_EVIDENCE,
            Permission.INTEGRATION_READ,
            Permission.INTEGRATION_MANAGE,
            Permission.SATELLITE_RUN,
            Permission.VAULT_READ,
            Permission.VAULT_UPLOAD,
            Permission.VAULT_DELETE,
            Permission.LICENSE_READ,
            Permission.SETTINGS_WRITE,
        }
    ),
    "manager": frozenset(
        {
            Permission.LOTE_READ,
            Permission.LOTE_CREATE,
            Permission.LOTE_UPDATE,
            Permission.LOTE_DELETE,
            Permission.TRACEABILITY_OPERATE,
            Permission.TRACEABILITY_DISPATCH,
            Permission.TRACEABILITY_EVIDENCE,
            Permission.INTEGRATION_READ,
            Permission.INTEGRATION_MANAGE,
            Permission.SATELLITE_RUN,
            Permission.VAULT_READ,
            Permission.VAULT_UPLOAD,
            Permission.LICENSE_READ,
        }
    ),
    "auditor": frozenset(
        {
            Permission.LOTE_READ,
            Permission.INTEGRATION_READ,
            Permission.VAULT_READ,
            Permission.LICENSE_READ,
        }
    ),
    "cliente": frozenset(
        {
            Permission.LOTE_READ,
            Permission.VAULT_READ,
            Permission.LICENSE_READ,
        }
    ),
}


def _normalize_role(role: str | None) -> str:
    return (role or "").strip().lower()


def _coerce_permission(permission: Permission | str) -> Permission | None:
    if isinstance(permission, Permission):
        return permission

    normalized_permission = permission.strip()
    if not normalized_permission:
        return None

    try:
        return Permission(normalized_permission)
    except ValueError:
        return None


def permissions_for_role(role: str | None) -> frozenset[Permission]:
    """Return the permission set for a role, failing closed for unknown roles."""
    return ROLE_PERMISSIONS.get(_normalize_role(role), frozenset())


def _extract_role(subject: Any) -> str | None:
    if isinstance(subject, str) or subject is None:
        return subject
    return getattr(subject, "role", None)


def has_permission(
    subject: Any,
    permission: Permission | str,
) -> bool:
    """Return whether the subject's role grants the requested permission."""
    resolved_permission = _coerce_permission(permission)
    if resolved_permission is None:
        return False

    return resolved_permission in permissions_for_role(_extract_role(subject))


def ensure_permission(
    subject: Any,
    permission: Permission | str,
) -> None:
    """Raise HTTP 403 when an authenticated subject lacks a capability."""
    resolved_permission = _coerce_permission(permission)
    if resolved_permission is None or not has_permission(subject, resolved_permission):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=(
                "El usuario autenticado no posee la capacidad requerida "
                f"('{permission}') para esta operacion."
            ),
        )


def require_permission(
    permission: Permission,
) -> Callable[..., Any]:
    """FastAPI dependency factory for a single required capability."""
    from litoral_trace.api.auth import get_current_tenant_user

    def dependency(
        user=Depends(get_current_tenant_user),
    ):
        ensure_permission(user, permission)
        return user

    return dependency


def require_any_permission(
    *permissions: Permission,
) -> Callable[..., Any]:
    """FastAPI dependency factory for any-of capability checks."""
    from litoral_trace.api.auth import get_current_tenant_user

    def dependency(
        user=Depends(get_current_tenant_user),
    ):
        if not any(has_permission(user, permission) for permission in permissions):
            rendered_permissions = ", ".join(permission.value for permission in permissions)
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=(
                    "El usuario autenticado no posee ninguna de las capacidades "
                    f"requeridas ({rendered_permissions})."
                ),
            )
        return user

    return dependency
