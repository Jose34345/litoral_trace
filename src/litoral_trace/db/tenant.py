"""Utilidades de aislamiento multi-tenant.

El contexto de organización pertenece a la capa de autenticación/autorización
(FastAPI + JWT) y debe proporcionarse explícitamente a estas funciones.

Este módulo no depende de Streamlit ni de ninguna capa de presentación.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import Select, text
from sqlalchemy.orm import Session

from litoral_trace.db.engine import get_db_session
from litoral_trace.db.models import (
    ApiKey,
    AuditLog,
    BatchImport,
    ExternalEntity,
    ExternalEntityVersion,
    ExternalReference,
    IntegrationConnection,
    IntegrationDocument,
    IntegrationEvent,
    IntegrationSyncRun,
    License,
    Lote,
    SatelliteJob,
    SatelliteNdviObservation,
    Shipment,
    TraceabilityBatch,
    TraceabilityEvent,
    User,
    UsLaceyOperation,
    UsLaceyOperationDocument,
    UsLaceyOperationField,
    UsLaceyOrganizationProfile,
)


TENANT_CONTEXT_GUC = "app.current_organization_id"

TenantModel = type[
    Lote
    | User
    | AuditLog
    | ApiKey
    | License
    | SatelliteJob
    | SatelliteNdviObservation
    | BatchImport
    | TraceabilityBatch
    | TraceabilityEvent
    | Shipment
    | IntegrationConnection
    | IntegrationSyncRun
    | ExternalEntity
    | ExternalEntityVersion
    | ExternalReference
    | IntegrationDocument
    | IntegrationEvent
    | UsLaceyOrganizationProfile
    | UsLaceyOperation
    | UsLaceyOperationDocument
    | UsLaceyOperationField
]
TenantEntity = (
    Lote
    | User
    | AuditLog
    | ApiKey
    | License
    | SatelliteJob
    | SatelliteNdviObservation
    | BatchImport
    | TraceabilityBatch
    | TraceabilityEvent
    | Shipment
    | IntegrationConnection
    | IntegrationSyncRun
    | ExternalEntity
    | ExternalEntityVersion
    | ExternalReference
    | IntegrationDocument
    | IntegrationEvent
    | UsLaceyOrganizationProfile
    | UsLaceyOperation
    | UsLaceyOperationDocument
    | UsLaceyOperationField
)


def _normalize_organization_id(organization_id: int | str) -> int:
    try:
        normalized_organization_id = int(organization_id)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "organization_id debe ser un entero valido."
        ) from exc

    if normalized_organization_id <= 0:
        raise ValueError(
            "organization_id debe ser mayor que cero."
        )

    return normalized_organization_id


def _supports_transaction_local_db_context(session: Session) -> bool:
    bind = session.get_bind()
    return bind is not None and bind.dialect.name == "postgresql"


def set_tenant_db_context(
    session: Session,
    organization_id: int | str,
) -> int:
    """Instala el tenant context a nivel transaccional para PostgreSQL.

    El contexto se limita a la transaccion actual mediante `set_config(..., true)`.
    No realiza commit y en motores no PostgreSQL se comporta como no-op luego
    de validar organization_id.
    """
    normalized_organization_id = _normalize_organization_id(organization_id)

    if not _supports_transaction_local_db_context(session):
        return normalized_organization_id

    session.execute(
        text(
            "SELECT set_config("
            f"'{TENANT_CONTEXT_GUC}', "
            ":organization_id, "
            "true"
            ")"
        ),
        {"organization_id": str(normalized_organization_id)},
    )
    return normalized_organization_id


def get_tenant_scoped_db_session(
    organization_id: int | str,
) -> Session | None:
    """Obtiene una sesion y aplica tenant context transaccional si corresponde."""
    session = get_db_session()
    if session is None:
        return None

    set_tenant_db_context(session, organization_id)
    return session


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
    """Aplica aislamiento multi-tenant a una consulta SQL."""
    if organization_id is None:
        organization_id = require_tenant_context()

    return query.where(model.organization_id == organization_id)


def verify_tenant_access(
    entity: TenantEntity,
    organization_id: int | None = None,
) -> bool:
    """Verifica que una entidad pertenezca al tenant autorizado."""
    if entity is None or organization_id is None:
        return False

    return getattr(entity, "organization_id", None) == organization_id
