"""Utilidades de aislamiento multi-tenant.

El contexto de organización pertenece a la capa de autenticación/autorización
(FastAPI + JWT) y debe proporcionarse explícitamente a estas funciones.

Este módulo no depende de Streamlit ni de ninguna capa de presentación.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import Select

from litoral_trace.db.models import (
    ApiKey,
    AuditLog,
    License,
    Lote,
    User,
)


TenantModel = type[Lote | User | AuditLog | ApiKey | License]
TenantEntity = Lote | User | AuditLog | ApiKey | License


def get_current_organization_id() -> int | None:
    """Retorna el contexto de organización local, si existe.

    El contexto de tenant no se obtiene desde Streamlit.
    En la arquitectura FastAPI actual, el organization_id debe provenir
    explícitamente del usuario autenticado/JWT.

    Se conserva esta función por compatibilidad con código existente.
    """
    return None


def require_tenant_context() -> int:
    """Exige un contexto de organización explícito.

    En la arquitectura actual el organization_id debe ser proporcionado
    por la capa de autenticación/autorización.
    """
    organization_id = get_current_organization_id()

    if organization_id is None:
        raise ValueError(
            "Error de Seguridad Multi-Tenant: "
            "No existe contexto de organización activo."
        )

    return organization_id


def apply_tenant_filter(
    query: Select[Any],
    model: TenantModel,
    organization_id: int | None = None,
) -> Select[Any]:
    """Aplica aislamiento multi-tenant a una consulta SQL.

    Args:
        query: Consulta SELECT base.
        model: Modelo que contiene organization_id.
        organization_id: ID de organización autorizado.

    Returns:
        Consulta con filtro organization_id.

    Raises:
        ValueError: Si no se proporciona un organization_id válido.
    """
    if organization_id is None:
        organization_id = require_tenant_context()

    return query.where(model.organization_id == organization_id)


def verify_tenant_access(
    entity: TenantEntity,
    organization_id: int | None = None,
) -> bool:
    """Verifica que una entidad pertenezca al tenant autorizado.

    Args:
        entity: Entidad que se desea verificar.
        organization_id: ID de organización autorizado.

    Returns:
        True si la entidad pertenece al tenant indicado.
        False en cualquier otro caso.
    """
    if entity is None or organization_id is None:
        return False

    return getattr(entity, "organization_id", None) == organization_id