"""Router REST de Configuracion Tenant, Licencias y Onboarding de Clientes."""
from __future__ import annotations

from dataclasses import asdict

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from litoral_trace.api.auth import UserTenantContext
from litoral_trace.auth.rbac import Permission, require_permission
from litoral_trace.config import get_settings
from litoral_trace.services.audit import (
    AuditAction,
    AuditOutcome,
    build_audit_actor_from_user,
    build_request_audit_context,
    record_audit_event_now,
)
from litoral_trace.services.licenses import (
    generar_invitacion_demo_prospecto,
    obtener_cuota_tenant,
)

router = APIRouter(
    prefix="/api/v1/settings",
    tags=["Configuracion & Licencias B2B"],
)


class InviteDemoUserRequest(BaseModel):
    cuit_empresa: str = Field(
        ...,
        json_schema_extra={"example": "30-71234567-8"},
    )
    nombre_contacto: str = Field(
        ...,
        json_schema_extra={"example": "Mario Dario Benitez"},
    )
    email_contacto: str = Field(
        ...,
        json_schema_extra={"example": "mario.benitez@despachantes.com"},
    )
    especie_principal: str = Field(
        default="Madera Aserrada (Pino)",
        json_schema_extra={"example": "Madera Aserrada (Pino)"},
    )


@router.get("/license", tags=["Configuracion & Licencias B2B"])
async def consultar_licencia_tenant(
    user: UserTenantContext = Depends(require_permission(Permission.LICENSE_READ)),
) -> JSONResponse:
    """Consulta el estado de la licencia, consumo mensual y limites de la organizacion."""
    status_obj = obtener_cuota_tenant(
        user.organization_id,
        organization_name=user.organization_name,
    )
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content=asdict(status_obj),
    )


@router.post("/invite_demo_user", tags=["Configuracion & Licencias B2B"])
async def generar_invitacion_demo_endpoint(
    payload: InviteDemoUserRequest,
    request: Request = None,
    user: UserTenantContext = Depends(require_permission(Permission.SETTINGS_WRITE)),
) -> JSONResponse:
    """Genera credenciales de acceso demo comercial para un prospecto."""
    if get_settings().is_production:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Not found.",
        )

    demo_credentials = generar_invitacion_demo_prospecto(
        cuit_empresa=payload.cuit_empresa,
        nombre_contacto=payload.nombre_contacto,
        email_contacto=payload.email_contacto,
        especie_principal=payload.especie_principal,
    )
    record_audit_event_now(
        actor=build_audit_actor_from_user(user),
        action=AuditAction.SETTINGS_INVITE_DEMO,
        entity_type="demo_invitation",
        entity_id=None,
        outcome=AuditOutcome.SUCCESS,
        request_context=build_request_audit_context(request),
        metadata={
            "cuit_empresa": payload.cuit_empresa,
            "especie_principal": payload.especie_principal,
        },
        best_effort=True,
    )
    return JSONResponse(
        status_code=status.HTTP_201_CREATED,
        content={
            "status": "success",
            "message": "Credenciales de demostracion comercial generadas exitosamente.",
            "demo_account": demo_credentials,
        },
    )
