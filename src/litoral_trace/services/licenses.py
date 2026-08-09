"""Tenant license services backed by persistent database state."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from fastapi import HTTPException, status
from sqlalchemy import func, select

from litoral_trace.config import get_settings
from litoral_trace.db.engine import get_db_session
from litoral_trace.db.models import License, Lote, Organization, User
from litoral_trace.db.tenant import get_tenant_scoped_db_session


@dataclass
class LicenseQuotaStatus:
    organization_id: int
    organization_name: str
    plan_name: str
    monthly_lote_limit: int
    monthly_lotes_used: int
    monthly_ton_limit: float
    monthly_tons_used: float
    active_users_count: int
    valid_until: str | None
    quota_available: bool


def _serialize_valid_until(valid_until: datetime | None) -> str | None:
    if valid_until is None:
        return None
    if valid_until.tzinfo is None:
        valid_until = valid_until.replace(tzinfo=timezone.utc)
    return valid_until.astimezone(timezone.utc).date().isoformat()


def obtener_cuota_tenant(
    organization_id: int,
    organization_name: str | None = None,
) -> LicenseQuotaStatus:
    """Return the persisted license state and current tenant usage metrics."""
    db_session = get_tenant_scoped_db_session(organization_id)
    if db_session is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Servicio de base de datos no disponible.",
        )

    try:
        organization = db_session.get(Organization, organization_id)
        if organization is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="La organizacion solicitada no existe.",
            )

        license_record = db_session.execute(
            select(License).where(License.organization_id == organization_id)
        ).scalar_one_or_none()
        if license_record is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="La organizacion no posee una licencia configurada.",
            )

        monthly_lotes_used = db_session.execute(
            select(func.count()).select_from(Lote).where(
                Lote.organization_id == organization_id
            )
        ).scalar_one()
        monthly_tons_used = db_session.execute(
            select(func.coalesce(func.sum(Lote.volumen_exportar_ton), 0.0)).where(
                Lote.organization_id == organization_id
            )
        ).scalar_one()
        active_users_count = db_session.execute(
            select(func.count()).select_from(User).where(
                User.organization_id == organization_id,
                User.is_active.is_(True),
            )
        ).scalar_one()

        quota_available = (
            organization.is_active
            and license_record.is_active
            and (
                license_record.valid_until is None
                or license_record.valid_until.astimezone(timezone.utc)
                > datetime.now(timezone.utc)
            )
            and monthly_lotes_used < license_record.max_lotes
            and monthly_tons_used <= license_record.max_volume_tons
        )

        return LicenseQuotaStatus(
            organization_id=organization_id,
            organization_name=organization_name or organization.name,
            plan_name=license_record.plan_type,
            monthly_lote_limit=license_record.max_lotes,
            monthly_lotes_used=int(monthly_lotes_used),
            monthly_ton_limit=float(license_record.max_volume_tons),
            monthly_tons_used=float(monthly_tons_used or 0.0),
            active_users_count=int(active_users_count),
            valid_until=_serialize_valid_until(license_record.valid_until),
            quota_available=quota_available,
        )
    finally:
        db_session.close()


def generar_invitacion_demo_prospecto(
    cuit_empresa: str,
    nombre_contacto: str,
    email_contacto: str,
    especie_principal: str = "Madera Aserrada (Pino)",
) -> dict[str, str]:
    """Legacy demo-only commercial invitation helper kept outside auth flow."""
    if get_settings().is_production:
        raise RuntimeError(
            "La generacion de credenciales demo no esta disponible en produccion."
        )

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
        "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"),
    }
