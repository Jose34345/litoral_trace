"""Router REST de Configuracion Tenant, Licencias y Onboarding de Clientes."""
from __future__ import annotations

from dataclasses import asdict

from fastapi import APIRouter, Depends, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from litoral_trace.api.auth import UserTenantContext, get_current_tenant_user
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
    user: UserTenantContext = Depends(get_current_tenant_user),
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
    user: UserTenantContext = Depends(get_current_tenant_user),
) -> JSONResponse:
    """Genera credenciales de acceso demo comercial para un prospecto."""
    demo_credentials = generar_invitacion_demo_prospecto(
        cuit_empresa=payload.cuit_empresa,
        nombre_contacto=payload.nombre_contacto,
        email_contacto=payload.email_contacto,
        especie_principal=payload.especie_principal,
    )
    return JSONResponse(
        status_code=status.HTTP_201_CREATED,
        content={
            "status": "success",
            "message": "Credenciales de demostracion comercial generadas exitosamente.",
            "demo_account": demo_credentials,
        },
    )
