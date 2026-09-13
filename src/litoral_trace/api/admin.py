"""Persistent platform-admin API for tenant organizations and licenses."""
from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Cookie, Depends, Form, HTTPException, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field, ValidationError

from litoral_trace.api.auth import UserTenantContext
from litoral_trace.auth.rbac import Permission, ensure_permission, require_permission
from litoral_trace.auth.sessions import REFRESH_TOKEN_COOKIE_KEY
from litoral_trace.services.audit import build_request_audit_context
from litoral_trace.services.admin import (
    alternar_estado_empresa,
    crear_nueva_empresa_cliente,
    listar_empresas_superadmin,
    upsert_license_superadmin,
)
from litoral_trace.services.us_lacey_admin import (
    list_us_lacey_accounts_superadmin, list_platform_users_superadmin,
    list_failed_jobs_superadmin, reset_pilot_account_superadmin,
    revoke_user_sessions_superadmin, set_us_lacey_account_status_superadmin,
    set_us_lacey_operation_limit_superadmin,
)
from litoral_trace.web.templates import templates

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
    max_batch_rows: int = Field(default=500, ge=1)
    valid_until: datetime | None = None


class UpsertOrganizationLicenseRequest(BaseModel):
    plan_type: str = Field(
        default="pro",
        json_schema_extra={"example": "enterprise"},
    )
    max_lotes: int = Field(default=100, ge=1)
    max_volume_tons: float = Field(default=10000.0, ge=1.0)
    max_batch_rows: int = Field(default=500, ge=1)
    valid_until: datetime | None = None
    is_active: bool = True


class UsLaceyStatusRequest(BaseModel):
    account_status: str


class UsLaceyLimitRequest(BaseModel):
    monthly_operation_limit: int = Field(..., ge=1, le=100000)


async def _coerce_create_payload(
    payload: CrearEmpresaClienteRequest | None,
    request: Request | None,
) -> CrearEmpresaClienteRequest:
    if payload is not None:
        return payload
    if request is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="No se recibieron datos para crear la organizacion.",
        )

    content_type = (request.headers.get("content-type") or "").lower()
    if "application/json" in content_type:
        raw_payload = await request.json()
    else:
        raw_payload = dict(await request.form())

    try:
        return CrearEmpresaClienteRequest.model_validate(raw_payload)
    except ValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=jsonable_encoder(exc.errors()),
        ) from exc


def require_superadmin_role(
    user: UserTenantContext = Depends(require_permission(Permission.PLATFORM_ADMIN)),
) -> UserTenantContext:
    """Compatibilidad: delega la autorizacion de plataforma al RBAC central."""
    ensure_permission(user, Permission.PLATFORM_ADMIN)
    return user


@router.get("/organizations", tags=["SuperAdmin B2B"])
async def listar_organizaciones_endpoint(
    refresh_token_cookie: str | None = Cookie(None, alias=REFRESH_TOKEN_COOKIE_KEY),
    admin: UserTenantContext = Depends(require_superadmin_role),
) -> JSONResponse:
    """Lista todas las empresas clientes registradas en la plataforma."""
    empresas = listar_empresas_superadmin(refresh_token=refresh_token_cookie)
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={"total": len(empresas), "organizations": empresas},
    )


@router.get("/us-lacey/accounts", tags=["SuperAdmin B2B", "U.S. Lacey"])
async def listar_cuentas_us_lacey_endpoint(
    refresh_token_cookie: str | None = Cookie(None, alias=REFRESH_TOKEN_COOKIE_KEY),
    admin: UserTenantContext = Depends(require_superadmin_role),
) -> JSONResponse:
    """Return the read-only owner overview for U.S. Lacey customer accounts."""
    accounts = list_us_lacey_accounts_superadmin(refresh_token=refresh_token_cookie)
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content=jsonable_encoder({"total": len(accounts), "accounts": accounts}),
    )


@router.get(
    "/us-lacey/accounts/fragment",
    response_class=HTMLResponse,
    tags=["SuperAdmin B2B", "U.S. Lacey"],
)
async def render_cuentas_us_lacey_fragment_endpoint(
    request: Request,
    refresh_token_cookie: str | None = Cookie(None, alias=REFRESH_TOKEN_COOKIE_KEY),
    admin: UserTenantContext = Depends(require_superadmin_role),
) -> HTMLResponse:
    """Render the read-only U.S. account console inside the existing admin page."""
    accounts = list_us_lacey_accounts_superadmin(refresh_token=refresh_token_cookie)
    users = list_platform_users_superadmin(refresh_token=refresh_token_cookie)
    failed_jobs = list_failed_jobs_superadmin(refresh_token=refresh_token_cookie)
    active_count = sum(1 for account in accounts if account.get("account_status") == "ACTIVE")
    pilot_count = sum(1 for account in accounts if account.get("account_status") == "PILOT")
    pending_count = sum(
        1
        for account in accounts
        if account.get("account_status") in {"PENDING_EMAIL", "PAYMENT_PENDING"}
    )
    failed_job_count = sum(int(account.get("failed_jobs") or 0) for account in accounts)
    content = templates.get_template("admin_us_lacey_accounts.html").render(
        request=request,
        accounts=accounts,
        account_count=len(accounts),
        active_count=active_count,
        pilot_count=pilot_count,
        pending_count=pending_count,
        failed_job_count=failed_job_count,
        users=users,
        failed_jobs=failed_jobs,
    )
    response = HTMLResponse(content=content, status_code=status.HTTP_200_OK)
    response.headers["Cache-Control"] = "no-store, max-age=0"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    return response


@router.get("/users", tags=["SuperAdmin B2B"])
async def listar_usuarios_platform_endpoint(refresh_token_cookie: str | None = Cookie(None, alias=REFRESH_TOKEN_COOKIE_KEY), admin: UserTenantContext = Depends(require_superadmin_role)) -> JSONResponse:
    return JSONResponse(content=jsonable_encoder({"users": list_platform_users_superadmin(refresh_token=refresh_token_cookie)}))


@router.get("/us-lacey/failed-jobs", tags=["SuperAdmin B2B", "U.S. Lacey"])
async def listar_fallas_us_lacey_endpoint(refresh_token_cookie: str | None = Cookie(None, alias=REFRESH_TOKEN_COOKIE_KEY), admin: UserTenantContext = Depends(require_superadmin_role)) -> JSONResponse:
    return JSONResponse(content=jsonable_encoder({"jobs": list_failed_jobs_superadmin(refresh_token=refresh_token_cookie)}))


@router.post("/us-lacey/accounts/{organization_id}/status", tags=["SuperAdmin B2B", "U.S. Lacey"])
async def cambiar_estado_us_lacey_endpoint(organization_id: int, request: Request, account_status: str = Form(...), refresh_token_cookie: str | None = Cookie(None, alias=REFRESH_TOKEN_COOKIE_KEY), admin: UserTenantContext = Depends(require_superadmin_role)) -> JSONResponse:
    return JSONResponse(content=set_us_lacey_account_status_superadmin(refresh_token=refresh_token_cookie, organization_id=organization_id, account_status=account_status))


@router.post("/us-lacey/accounts/{organization_id}/operation-limit", tags=["SuperAdmin B2B", "U.S. Lacey"])
async def cambiar_limite_us_lacey_endpoint(organization_id: int, request: Request, monthly_operation_limit: int = Form(...), refresh_token_cookie: str | None = Cookie(None, alias=REFRESH_TOKEN_COOKIE_KEY), admin: UserTenantContext = Depends(require_superadmin_role)) -> JSONResponse:
    return JSONResponse(content=set_us_lacey_operation_limit_superadmin(refresh_token=refresh_token_cookie, organization_id=organization_id, monthly_operation_limit=monthly_operation_limit))


@router.post("/users/{user_id}/revoke-sessions", tags=["SuperAdmin B2B"])
async def revocar_sesiones_endpoint(user_id: int, request: Request, refresh_token_cookie: str | None = Cookie(None, alias=REFRESH_TOKEN_COOKIE_KEY), admin: UserTenantContext = Depends(require_superadmin_role)) -> JSONResponse:
    return JSONResponse(content=revoke_user_sessions_superadmin(refresh_token=refresh_token_cookie, user_id=user_id))


@router.post("/us-lacey/accounts/{organization_id}/reset-pilot", tags=["SuperAdmin B2B", "U.S. Lacey"])
async def resetear_pilot_endpoint(organization_id: int, request: Request, refresh_token_cookie: str | None = Cookie(None, alias=REFRESH_TOKEN_COOKIE_KEY), admin: UserTenantContext = Depends(require_superadmin_role)) -> JSONResponse:
    return JSONResponse(content=reset_pilot_account_superadmin(refresh_token=refresh_token_cookie, organization_id=organization_id))


@router.post("/organizations", tags=["SuperAdmin B2B"])
async def crear_organizacion_endpoint(
    payload: CrearEmpresaClienteRequest | None = None,
    request: Request = None,
    refresh_token_cookie: str | None = Cookie(None, alias=REFRESH_TOKEN_COOKIE_KEY),
    admin: UserTenantContext = Depends(require_superadmin_role),
) -> JSONResponse:
    """Create a persisted tenant organization, license, and initial admin."""
    resolved_payload = await _coerce_create_payload(payload, request)
    res = crear_nueva_empresa_cliente(
        refresh_token=refresh_token_cookie,
        name=resolved_payload.name,
        tax_id=resolved_payload.tax_id,
        admin_email=resolved_payload.admin_email,
        admin_username=resolved_payload.admin_username,
        admin_password=resolved_payload.admin_password,
        tier=resolved_payload.tier,
        monthly_lote_limit=resolved_payload.monthly_lote_limit,
        monthly_ton_limit=resolved_payload.monthly_ton_limit,
        max_batch_rows=resolved_payload.max_batch_rows,
        valid_until=resolved_payload.valid_until,
        audit_request_context=build_request_audit_context(request),
    )
    return JSONResponse(status_code=status.HTTP_201_CREATED, content=res)


@router.post("/organizations/{org_id}/toggle_status", tags=["SuperAdmin B2B"])
async def toggle_organizacion_status_endpoint(
    org_id: int,
    request: Request = None,
    refresh_token_cookie: str | None = Cookie(None, alias=REFRESH_TOKEN_COOKIE_KEY),
    admin: UserTenantContext = Depends(require_superadmin_role),
) -> JSONResponse:
    """Activa o suspende el acceso de una empresa cliente."""
    status_result = alternar_estado_empresa(
        refresh_token=refresh_token_cookie,
        org_id=org_id,
        audit_request_context=build_request_audit_context(request),
    )
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={
            "status": "updated",
            "organization_id": int(status_result["organization_id"]),
            "is_active": bool(status_result["is_active"]),
            "revoked_session_count": int(status_result["revoked_session_count"]),
        },
    )


@router.put("/organizations/{org_id}/license", tags=["SuperAdmin B2B"])
async def upsert_organizacion_license_endpoint(
    org_id: int,
    payload: UpsertOrganizationLicenseRequest,
    request: Request = None,
    refresh_token_cookie: str | None = Cookie(None, alias=REFRESH_TOKEN_COOKIE_KEY),
    admin: UserTenantContext = Depends(require_superadmin_role),
) -> JSONResponse:
    """Create or update the persisted license metadata for a tenant."""
    result = upsert_license_superadmin(
        refresh_token=refresh_token_cookie,
        organization_id=org_id,
        plan_type=payload.plan_type,
        max_lotes=payload.max_lotes,
        max_volume_tons=payload.max_volume_tons,
        max_batch_rows=payload.max_batch_rows,
        valid_until=payload.valid_until,
        is_active=payload.is_active,
        audit_request_context=build_request_audit_context(request),
    )
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content=result,
    )
