"""Control de acceso basado en roles (RBAC) para Litoral Trace."""
from __future__ import annotations

from typing import Any, Callable
from functools import wraps

from fastapi import HTTPException, status


ROLE_LEVELS: dict[str, int] = {
    "superadmin": 125,
    "admin": 100,
    "manager": 75,
    "auditor": 50,
    "cliente": 25,
    "guest": 0,
}


def get_role_level(role: str | None) -> int:
    """Devuelve el nivel jerárquico de un rol."""
    if not role:
        return 0

    return ROLE_LEVELS.get(role.strip().lower(), 0)


def has_permission(
    user_role: str | None,
    required_role: str,
) -> bool:
    """Comprueba si un rol tiene permisos suficientes."""
    return get_role_level(user_role) >= get_role_level(required_role)


def require_role(
    user_role: str | None,
    required_role: str,
) -> None:
    """
    Valida un rol para uso directo desde endpoints FastAPI.

    Lanza HTTP 403 si el usuario no posee privilegios suficientes.
    """
    if not has_permission(user_role, required_role):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=(
                f"El rol '{user_role or 'guest'}' no tiene permisos "
                f"para esta operación. "
                f"Se requiere como mínimo '{required_role}'."
            ),
        )


def require_any_role(
    user_role: str | None,
    allowed_roles: set[str],
) -> None:
    """
    Valida que el usuario pertenezca a alguno de los roles permitidos.
    """
    normalized_role = (user_role or "guest").strip().lower()
    normalized_allowed = {
        role.strip().lower()
        for role in allowed_roles
    }

    if normalized_role not in normalized_allowed:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="El usuario no posee permisos para esta operación.",
        )
