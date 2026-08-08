"""Router REST SuperAdmin para gestion B2B de clientes, empresas y credenciales."""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from litoral_trace.api.auth import UserTenantContext
from litoral_trace.auth.rbac import Permission, ensure_permission, require_permission
from litoral_trace.services.audit import (
    AuditAction,
    AuditOutcome,
    build_audit_actor_from_user,
    build_request_audit_context,
    record_audit_event_now,
)
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
    user: UserTenantContext = Depends(require_permission(Permission.PLATFORM_ADMIN)),
) -> UserTenantContext:
    """Compatibilidad: delega la autorizacion de plataforma al RBAC central."""
    ensure_permission(user, Permission.PLATFORM_ADMIN)
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
    request: Request = None,
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
    record_audit_event_now(
        actor=build_audit_actor_from_user(admin),
        action=AuditAction.PLATFORM_ORGANIZATION_CREATE,
        entity_type="organization",
        entity_id=int(res["organization_id"]),
        outcome=AuditOutcome.SUCCESS,
        request_context=build_request_audit_context(request),
        metadata={
            "target_organization_id": int(res["organization_id"]),
            "organization_name": payload.name,
            "tax_id": payload.tax_id,
            "tier": payload.tier,
        },
        best_effort=True,
    )
    return JSONResponse(status_code=status.HTTP_201_CREATED, content=res)


@router.post("/organizations/{org_id}/toggle_status", tags=["SuperAdmin B2B"])
async def toggle_organizacion_status_endpoint(
    org_id: int,
    request: Request = None,
    admin: UserTenantContext = Depends(require_superadmin_role),
) -> JSONResponse:
    """Activa o suspende el acceso de una empresa cliente."""
    ok = alternar_estado_empresa(org_id)
    if not ok:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Empresa con ID {org_id} no encontrada.",
        )
    record_audit_event_now(
        actor=build_audit_actor_from_user(admin),
        action=AuditAction.PLATFORM_ORGANIZATION_STATUS_CHANGE,
        entity_type="organization",
        entity_id=org_id,
        outcome=AuditOutcome.SUCCESS,
        request_context=build_request_audit_context(request),
        metadata={"target_organization_id": org_id},
        best_effort=True,
    )
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={"status": "updated", "organization_id": org_id},
    )
