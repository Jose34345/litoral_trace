"""Router REST SuperAdmin para gestion B2B de clientes, empresas y credenciales."""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from litoral_trace.api.auth import UserTenantContext, get_current_tenant_user
from litoral_trace.services.admin import (
    alternar_estado_empresa,
    crear_nueva_empresa_cliente,
    listar_empresas_superadmin,
)

router = APIRouter(prefix="/api/v1/admin", tags=["SuperAdmin B2B"])


class CrearEmpresaClienteRequest(BaseModel):
    name: str = Field(
        ...,
        json_schema_extra={"example": "Aserradero Don Juan S.A."},
    )
    tax_id: str = Field(
        ...,
        json_schema_extra={"example": "30-71234567-8"},
    )
    admin_email: str = Field(
        ...,
        json_schema_extra={"example": "juan@donjuan.com"},
    )
    admin_username: str = Field(
        ...,
        json_schema_extra={"example": "donjuan_admin"},
    )
    admin_password: str = Field(
        ...,
        json_schema_extra={"example": "ClaveSegura2026!"},
    )
    tier: str = Field(
        default="pro",
        json_schema_extra={"example": "pro"},
    )
    monthly_lote_limit: int = Field(default=50, ge=1)
    monthly_ton_limit: float = Field(default=3000.0, ge=1.0)


def require_superadmin_role(
    user: UserTenantContext = Depends(get_current_tenant_user),
) -> UserTenantContext:
    """Verifica que el usuario autenticado posea rol de SuperAdmin."""
    if user.role.lower() != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Acceso Denegado: Esta funcion esta reservada exclusivamente para el SuperAdmin.",
        )
    return user


@router.get("/organizations", tags=["SuperAdmin B2B"])
async def listar_organizaciones_endpoint(
    admin: UserTenantContext = Depends(require_superadmin_role),
) -> JSONResponse:
    """Lista todas las empresas clientes registradas en la plataforma."""
    empresas = listar_empresas_superadmin()
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={"total": len(empresas), "organizations": empresas},
    )


@router.post("/organizations", tags=["SuperAdmin B2B"])
async def crear_organizacion_endpoint(
    payload: CrearEmpresaClienteRequest,
    admin: UserTenantContext = Depends(require_superadmin_role),
) -> JSONResponse:
    """Crea una nueva empresa cliente, su usuario principal y emite credenciales B2B."""
    res = crear_nueva_empresa_cliente(
        name=payload.name,
        tax_id=payload.tax_id,
        admin_email=payload.admin_email,
        admin_username=payload.admin_username,
        admin_password=payload.admin_password,
        tier=payload.tier,
        monthly_lote_limit=payload.monthly_lote_limit,
        monthly_ton_limit=payload.monthly_ton_limit,
    )
    return JSONResponse(status_code=status.HTTP_201_CREATED, content=res)


@router.post("/organizations/{org_id}/toggle_status", tags=["SuperAdmin B2B"])
async def toggle_organizacion_status_endpoint(
    org_id: int,
    admin: UserTenantContext = Depends(require_superadmin_role),
) -> JSONResponse:
    """Activa o suspende el acceso de una empresa cliente."""
    ok = alternar_estado_empresa(org_id)
    if not ok:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Empresa con ID {org_id} no encontrada.",
        )
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={"status": "updated", "organization_id": org_id},
    )
