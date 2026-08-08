"""Servicio de Gestión de Licencias, Cuotas Mensuales y Onboarding SaaS."""
from __future__ import annotations
from dataclasses import dataclass, asdict
from datetime import datetime, timezone

@dataclass
class LicenseQuotaStatus:
    organization_id: int
    organization_name: str
    plan_name: str               # Demo Pro, Enterprise B2B, Pyme Forestal
    monthly_lote_limit: int
    monthly_lotes_used: int
    monthly_ton_limit: float
    monthly_tons_used: float
    active_users_count: int
    valid_until: str
    quota_available: bool

def obtener_cuota_tenant(organization_id: int) -> LicenseQuotaStatus:
    """Obtiene el estado actual de la licencia y cuotas de uso de la organización."""
    # Datos de demostración de licencias B2B
    return LicenseQuotaStatus(
        organization_id=organization_id,
        organization_name="Exportadora Forestal del Chaco S.A.",
        plan_name="Enterprise B2B (Especies Forestales EUDR)",
        monthly_lote_limit=100,
        monthly_lotes_used=12,
        monthly_ton_limit=10000.0,
        monthly_tons_used=1450.0,
        active_users_count=4,
        valid_until="2027-12-31",
        quota_available=True
    )

def generar_invitacion_demo_prospecto(
    cuit_empresa: str,
    nombre_contacto: str,
    email_contacto: str,
    especie_principal: str = "Madera Aserrada (Pino)"
) -> dict[str, str]:
    """Genera credenciales temporales e invitación para demostración comercial en vivo."""
    username_demo = f"demo_{email_contacto.split('@')[0].lower()}"
    password_demo = f"DemoChaco{datetime.now().year}!"
    
    return {
        "cuit_empresa": cuit_empresa,
        "nombre_contacto": nombre_contacto,
        "email_contacto": email_contacto,
        "especie_principal": especie_principal,
        "username_demo": username_demo,
        "password_demo": password_demo,
        "login_url": "https://litoraltrace.com/login",
        "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    }
