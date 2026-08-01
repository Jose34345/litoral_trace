"""Servicio SuperAdmin para gestión de empresas clientes (Tenants), licencias y credenciales."""
from __future__ import annotations
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any

@dataclass
class OrganizationClientDetail:
    id: int
    name: str
    slug: str
    tax_id: str
    tier: str
    is_active: bool
    admin_email: str
    admin_username: str
    monthly_lote_limit: int
    monthly_ton_limit: float
    created_at: str

# Almacenamiento demo de empresas clientes creadas por SuperAdmin
EMPRESAS_REGISTRADAS_DB: list[OrganizationClientDetail] = [
    OrganizationClientDetail(
        id=1,
        name="Exportadora Forestal del Chaco S.A.",
        slug="exp-chaco",
        tax_id="30-55555555-9",
        tier="enterprise",
        is_active=True,
        admin_email="comercial@litoraltrace.com",
        admin_username="admin",
        monthly_lote_limit=100,
        monthly_ton_limit=10000.0,
        created_at="2026-01-01 10:00"
    ),
    OrganizationClientDetail(
        id=2,
        name="Aserradero Don Juan S.A.",
        slug="aserradero-don-juan",
        tax_id="30-71234567-8",
        tier="pro",
        is_active=True,
        admin_email="juan@donjuan.com",
        admin_username="donjuan_admin",
        monthly_lote_limit=50,
        monthly_ton_limit=3000.0,
        created_at="2026-07-20 14:30"
    )
]

def listar_empresas_superadmin() -> list[dict[str, Any]]:
    """Lista todas las empresas clientes registradas por el SuperAdmin."""
    return [asdict(emp) for emp in EMPRESAS_REGISTRADAS_DB]

def crear_nueva_empresa_cliente(
    name: str,
    tax_id: str,
    admin_email: str,
    admin_username: str,
    admin_password: str,
    tier: str = "pro",
    monthly_lote_limit: int = 50,
    monthly_ton_limit: float = 3000.0
) -> dict[str, Any]:
    """Crea una nueva organización cliente B2B, su usuario administrador y su licencia de pago."""
    next_id = len(EMPRESAS_REGISTRADAS_DB) + 1
    slug = name.lower().replace(" ", "-").replace(".", "").replace(",", "")[:50]
    created_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")

    nueva_empresa = OrganizationClientDetail(
        id=next_id,
        name=name.strip(),
        slug=slug,
        tax_id=tax_id.strip(),
        tier=tier.strip(),
        is_active=True,
        admin_email=admin_email.strip(),
        admin_username=admin_username.strip(),
        monthly_lote_limit=monthly_lote_limit,
        monthly_ton_limit=monthly_ton_limit,
        created_at=created_at
    )
    
    EMPRESAS_REGISTRADAS_DB.append(nueva_empresa)

    # Generar Tarjeta de Ficha de Fichado / Bienvenida Comercial
    welcome_brief = {
        "status": "success",
        "organization_id": next_id,
        "organization_name": name.strip(),
        "tax_id": tax_id.strip(),
        "plan_tier": tier.upper(),
        "access_credentials": {
            "login_url": "https://litoraltrace.com",
            "username": admin_username.strip(),
            "password": admin_password.strip(),
            "email": admin_email.strip()
        },
        "limits": {
            "lotes_mensuales": monthly_lote_limit,
            "toneladas_mensuales": monthly_ton_limit
        },
        "whatsapp_share_text": (
            f"🌲 *LITORAL TRACE B2B — Credenciales de Acceso*\n\n"
            f"Estimado/a cliente de *{name.strip()}*,\n"
            f"Su cuenta corporativa ha sido activada exitosamente.\n\n"
            f"🌐 *Enlace de Ingreso*: https://litoraltrace.com\n"
            f"👤 *Usuario*: {admin_username.strip()}\n"
            f"🔑 *Contraseña*: {admin_password.strip()}\n"
            f"📋 *Plan*: {tier.upper()} ({monthly_lote_limit} lotes/mes)\n\n"
            f"Ante cualquier duda, estamos a su disposición."
        )
    }

    return welcome_brief

def alternar_estado_empresa(org_id: int) -> bool:
    """Activa o suspende el acceso de una empresa cliente."""
    for emp in EMPRESAS_REGISTRADAS_DB:
        if emp.id == org_id:
            # Alternar booleano creando una nueva instancia inmune a dataclass frozen
            idx = EMPRESAS_REGISTRADAS_DB.index(emp)
            EMPRESAS_REGISTRADAS_DB[idx] = OrganizationClientDetail(
                id=emp.id,
                name=emp.name,
                slug=emp.slug,
                tax_id=emp.tax_id,
                tier=emp.tier,
                is_active=not emp.is_active,
                admin_email=emp.admin_email,
                admin_username=emp.admin_username,
                monthly_lote_limit=emp.monthly_lote_limit,
                monthly_ton_limit=emp.monthly_ton_limit,
                created_at=emp.created_at
            )
            return True
    return False
