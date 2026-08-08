"""Router REST de Configuración Tenant, Licencias y Onboarding de Clientes."""
from __future__ import annotations
from dataclasses import asdict
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from litoral_trace.api.auth import get_current_tenant_user, UserTenantContext
from litoral_trace.services.licenses import obtener_cuota_tenant, generar_invitacion_demo_prospecto

router = APIRouter(prefix="/api/v1/settings", tags=["Configuración & Licencias B2B"])

class InviteDemoUserRequest(BaseModel):
    cuit_empresa: str = Field(..., example="30-71234567-8")
    nombre_contacto: str = Field(..., example="Mario Darío Benítez")
    email_contacto: str = Field(..., example="mario.benitez@despachantes.com")
    especie_principal: str = Field(default="Madera Aserrada (Pino)", example="Madera Aserrada (Pino)")

@router.get("/license", tags=["Configuración & Licencias B2B"])
async def consultar_licencia_tenant(
    user: UserTenantContext = Depends(get_current_tenant_user)
) -> JSONResponse:
    """Consulta el estado de la licencia, consumo de cuota mensual y límites de la organización."""
    status_obj = obtener_cuota_tenant(user.organization_id)
    return JSONResponse(status_code=status.HTTP_200_OK, content=asdict(status_obj))

@router.post("/invite_demo_user", tags=["Configuración & Licencias B2B"])
async def generar_invitacion_demo_endpoint(
    payload: InviteDemoUserRequest,
    user: UserTenantContext = Depends(get_current_tenant_user)
) -> JSONResponse:
    """Genera credenciales de acceso demo comercial para un prospecto en Resistencia o Corrientes."""
    demo_credentials = generar_invitacion_demo_prospecto(
        cuit_empresa=payload.cuit_empresa,
        nombre_contacto=payload.nombre_contacto,
        email_contacto=payload.email_contacto,
        especie_principal=payload.especie_principal
    )
    return JSONResponse(
        status_code=status.HTTP_201_CREATED,
        content={
            "status": "success",
            "message": "Credenciales de demostración comercial generadas exitosamente.",
            "demo_account": demo_credentials
        }
    )
