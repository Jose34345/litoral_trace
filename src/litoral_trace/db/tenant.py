"""Aislamiento multi-tenant y middleware de contexto de organización."""
from __future__ import annotations
from typing import Any
import streamlit as st
from sqlalchemy import Select
from sqlalchemy.orm import Session
from litoral_trace.db.models import Organization, User, Lote, AuditLog, ApiKey, License

def get_current_organization_id() -> int | None:
    """Obtiene el ID de organización del usuario autenticado en la sesión actual."""
    try:
        return st.session_state.get("organization_id")
    except Exception:
        return None

def require_tenant_context() -> int:
    """Garantiza la existencia de un contexto de organización activo.
    
    Raises:
        ValueError: Si la sesión no contiene un organization_id válido.
    """
    org_id = get_current_organization_id()
    if org_id is None:
        raise ValueError("Error de Seguridad Multi-Tenant: No existe contexto de organización activo en la sesión.")
    return org_id

def apply_tenant_filter(
    query: Select[Any],
    model: type[Lote | User | AuditLog | ApiKey | License],
    organization_id: int | None = None,
) -> Select[Any]:
    """Aplica de forma estricta la cláusula WHERE organization_id = :org_id a la consulta SQL.
    
    Args:
        query: Consulta SELECT base.
        model: Clase del modelo filtrable.
        organization_id: ID explícito o lectura de la sesión si es None.
    
    Returns:
        Consulta con el aislamiento multi-tenant garantizado.
    """
    if organization_id is None:
        organization_id = require_tenant_context()
    return query.where(model.organization_id == organization_id)

def verify_tenant_access(
    entity: Lote | User | AuditLog | ApiKey | License,
    organization_id: int | None = None,
) -> bool:
    """Verifica si una entidad específica pertenece a la organización autorizada."""
    if organization_id is None:
        organization_id = get_current_organization_id()
    if organization_id is None or entity is None:
        return False
    return getattr(entity, "organization_id", None) == organization_id
