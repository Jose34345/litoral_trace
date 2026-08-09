"""Persistent platform control-plane services for organizations and licenses."""
from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from fastapi import HTTPException, status
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.orm import Session

from litoral_trace.auth.passwords import hash_password
from litoral_trace.auth.sessions import (
    build_refresh_token_expiration,
    hash_refresh_token,
    utc_now,
)
from litoral_trace.db.auth_bootstrap import lookup_session_bootstrap_by_token_hash
from litoral_trace.db.engine import get_db_session
from litoral_trace.db.models import License, Organization, User, UserSession
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.services.audit import (
    AuditAction,
    AuditOutcome,
    AuditRequestContext,
    build_audit_actor,
    record_audit_event,
)


DEFAULT_LICENSE_BATCH_LIMIT = 500


def _supports_platform_control_plane_functions(db_session: Session) -> bool:
    bind = db_session.get_bind()
    return bind is not None and bind.dialect.name == "postgresql"


def _normalize_non_empty(value: str, *, field_name: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"El campo '{field_name}' es obligatorio.",
        )
    return normalized


def _normalize_optional_text(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


def _build_slug(name: str) -> str:
    base_slug = re.sub(r"[^a-z0-9]+", "-", name.strip().lower()).strip("-")
    if not base_slug:
        base_slug = "tenant"
    return f"{base_slug[:80]}-{uuid4().hex[:8]}"


def _require_platform_refresh_token_hash(refresh_token: str | None) -> str:
    normalized = (refresh_token or "").strip()
    if not normalized:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Se requiere la sesion persistente de plataforma.",
        )
    return hash_refresh_token(normalized)


def _map_platform_db_error(exc: DBAPIError) -> None:
    original_error = getattr(exc, "orig", None)
    sqlstate = getattr(original_error, "pgcode", None) or getattr(
        original_error, "sqlstate", None
    )

    if sqlstate == "28000":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Sesion de plataforma invalida o expirada.",
        ) from exc
    if sqlstate == "42501":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="La cuenta autenticada no posee permisos de plataforma.",
        ) from exc
    if sqlstate == "P0002":
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="La organizacion solicitada no existe.",
        ) from exc
    if sqlstate in {"22023", "23502"}:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Los datos enviados para la operacion de plataforma son invalidos.",
        ) from exc
    if sqlstate == "23505":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Ya existe una organizacion o usuario con esos identificadores.",
        ) from exc

    raise exc


def _assert_platform_actor(
    db_session: Session,
    *,
    refresh_token: str | None,
) -> User:
    token_hash = _require_platform_refresh_token_hash(refresh_token)
    session_lookup = lookup_session_bootstrap_by_token_hash(
        db_session,
        token_hash=token_hash,
    )
    if session_lookup is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Sesion de plataforma invalida o expirada.",
        )

    set_tenant_db_context(db_session, session_lookup.organization_id)
    platform_session = db_session.get(UserSession, session_lookup.id)
    if platform_session is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Sesion de plataforma invalida o expirada.",
        )

    now = utc_now()
    if (
        platform_session.revoked_at is not None
        or platform_session.expires_at.astimezone(timezone.utc) <= now
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Sesion de plataforma invalida o expirada.",
        )

    user = db_session.get(User, platform_session.user_id)
    organization = db_session.get(Organization, platform_session.organization_id)
    if (
        user is None
        or organization is None
        or not user.is_active
        or not organization.is_active
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Sesion de plataforma invalida o expirada.",
        )

    if user.role != "superadmin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="La cuenta autenticada no posee permisos de plataforma.",
        )

    return user


def _serialize_organization_row(
    organization: Organization,
    *,
    admin_user: User | None,
    license_record: License | None,
) -> dict[str, Any]:
    return {
        "id": organization.id,
        "name": organization.name,
        "slug": organization.slug,
        "tax_id": organization.tax_id,
        "tier": organization.tier,
        "is_active": organization.is_active,
        "admin_user_id": getattr(admin_user, "id", None),
        "admin_email": getattr(admin_user, "email", None),
        "admin_username": getattr(admin_user, "username", None),
        "license_id": getattr(license_record, "id", None),
        "license_plan_type": getattr(license_record, "plan_type", None),
        "license_max_lotes": getattr(license_record, "max_lotes", None),
        "license_max_volume_tons": getattr(license_record, "max_volume_tons", None),
        "license_max_batch_rows": getattr(license_record, "max_batch_rows", None),
        "license_valid_until": (
            license_record.valid_until.isoformat()
            if getattr(license_record, "valid_until", None) is not None
            else None
        ),
        "license_is_active": getattr(license_record, "is_active", None),
        "created_at": organization.created_at.isoformat(),
        "updated_at": organization.updated_at.isoformat(),
    }


def _list_organizations_direct(db_session: Session) -> list[dict[str, Any]]:
    organizations = db_session.execute(
        select(Organization).order_by(Organization.id)
    ).scalars().all()

    admin_users = db_session.execute(
        select(User).order_by(User.organization_id, User.id)
    ).scalars().all()
    licenses = db_session.execute(
        select(License).order_by(License.organization_id, License.id)
    ).scalars().all()

    admin_by_organization: dict[int, User] = {}
    for user in admin_users:
        current = admin_by_organization.get(user.organization_id)
        if current is None:
            admin_by_organization[user.organization_id] = user
            continue
        if current.role != "admin" and user.role == "admin":
            admin_by_organization[user.organization_id] = user

    license_by_organization: dict[int, License] = {
        license_record.organization_id: license_record
        for license_record in licenses
    }

    return [
        _serialize_organization_row(
            organization,
            admin_user=admin_by_organization.get(organization.id),
            license_record=license_by_organization.get(organization.id),
        )
        for organization in organizations
    ]


def _record_platform_audit_event_direct(
    db_session: Session,
    *,
    actor_user: User,
    action: AuditAction,
    entity_type: str,
    entity_id: int | None,
    target_organization_id: int,
    metadata: dict[str, Any] | None,
    request_context: AuditRequestContext | None,
) -> None:
    record_audit_event(
        db_session,
        actor=build_audit_actor(
            organization_id=actor_user.organization_id,
            user_id=actor_user.id,
            username=actor_user.username,
            role=actor_user.role,
        ),
        action=action,
        entity_type=entity_type,
        entity_id=entity_id,
        outcome=AuditOutcome.SUCCESS,
        target_organization_id=target_organization_id,
        request_context=request_context,
        metadata=metadata,
    )


def listar_empresas_superadmin(
    *,
    refresh_token: str | None,
) -> list[dict[str, Any]]:
    db_session = get_db_session()
    if db_session is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Servicio de base de datos no disponible.",
        )

    try:
        if _supports_platform_control_plane_functions(db_session):
            token_hash = _require_platform_refresh_token_hash(refresh_token)
            rows = db_session.execute(
                text(
                    """
                    SELECT *
                    FROM public.platform_list_organizations(
                        :actor_refresh_token_hash
                    )
                    ORDER BY id
                    """
                ),
                {"actor_refresh_token_hash": token_hash},
            ).mappings().all()
            return [dict(row) for row in rows]

        _assert_platform_actor(db_session, refresh_token=refresh_token)
        return _list_organizations_direct(db_session)
    except DBAPIError as exc:
        _map_platform_db_error(exc)
        raise
    finally:
        db_session.close()


def _create_organization_direct(
    db_session: Session,
    *,
    name: str,
    tax_id: str,
    admin_email: str,
    admin_username: str,
    admin_password_hash: str,
    tier: str,
    monthly_lote_limit: int,
    monthly_ton_limit: float,
    max_batch_rows: int,
    valid_until: datetime | None,
    organization_description: str | None,
) -> dict[str, Any]:
    organization = Organization(
        name=name,
        slug=_build_slug(name),
        tax_id=tax_id,
        tier=tier,
        description=organization_description,
        is_active=True,
    )
    db_session.add(organization)
    db_session.flush()

    admin_user = User(
        organization_id=organization.id,
        email=admin_email,
        username=admin_username,
        password_hash=admin_password_hash,
        role="admin",
        full_name=None,
        is_active=True,
    )
    db_session.add(admin_user)
    db_session.flush()

    license_record = License(
        organization_id=organization.id,
        plan_type=tier,
        max_lotes=monthly_lote_limit,
        max_volume_tons=monthly_ton_limit,
        max_batch_rows=max_batch_rows,
        valid_until=valid_until,
        is_active=True,
    )
    db_session.add(license_record)
    db_session.flush()

    return {
        "status": "success",
        "organization_id": organization.id,
        "organization_name": organization.name,
        "slug": organization.slug,
        "tax_id": organization.tax_id,
        "plan_tier": license_record.plan_type,
        "admin_user_id": admin_user.id,
        "admin_username": admin_user.username,
        "admin_email": admin_user.email,
        "license_id": license_record.id,
        "limits": {
            "lotes_mensuales": license_record.max_lotes,
            "toneladas_mensuales": license_record.max_volume_tons,
            "max_batch_rows": license_record.max_batch_rows,
        },
    }


def crear_nueva_empresa_cliente(
    *,
    refresh_token: str | None,
    name: str,
    tax_id: str,
    admin_email: str,
    admin_username: str,
    admin_password: str,
    tier: str = "pro",
    monthly_lote_limit: int = 50,
    monthly_ton_limit: float = 3000.0,
    max_batch_rows: int = DEFAULT_LICENSE_BATCH_LIMIT,
    valid_until: datetime | None = None,
    organization_description: str | None = None,
    audit_request_context: AuditRequestContext | None = None,
) -> dict[str, Any]:
    normalized_name = _normalize_non_empty(name, field_name="name")
    normalized_tax_id = _normalize_non_empty(tax_id, field_name="tax_id")
    normalized_admin_email = _normalize_non_empty(
        admin_email,
        field_name="admin_email",
    )
    normalized_admin_username = _normalize_non_empty(
        admin_username,
        field_name="admin_username",
    )
    normalized_tier = _normalize_non_empty(tier, field_name="tier").lower()
    normalized_description = _normalize_optional_text(organization_description)

    if monthly_lote_limit <= 0 or monthly_ton_limit <= 0 or max_batch_rows <= 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Los limites de licencia deben ser mayores que cero.",
        )

    password_hash = hash_password(admin_password)
    db_session = get_db_session()
    if db_session is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Servicio de base de datos no disponible.",
        )

    try:
        if _supports_platform_control_plane_functions(db_session):
            token_hash = _require_platform_refresh_token_hash(refresh_token)
            created = db_session.execute(
                text(
                    """
                    SELECT *
                    FROM public.platform_create_organization(
                        :actor_refresh_token_hash,
                        :organization_name,
                        :organization_slug,
                        :tax_id,
                        :tier,
                        :description,
                        :admin_email,
                        :admin_username,
                        :admin_password_hash,
                        :admin_full_name,
                        :license_plan_type,
                        :license_max_lotes,
                        :license_max_volume_tons,
                        :license_max_batch_rows,
                        :license_valid_until,
                        :license_is_active
                    )
                    """
                ),
                {
                    "actor_refresh_token_hash": token_hash,
                    "organization_name": normalized_name,
                    "organization_slug": _build_slug(normalized_name),
                    "tax_id": normalized_tax_id,
                    "tier": normalized_tier,
                    "description": normalized_description,
                    "admin_email": normalized_admin_email,
                    "admin_username": normalized_admin_username,
                    "admin_password_hash": password_hash,
                    "admin_full_name": None,
                    "license_plan_type": normalized_tier,
                    "license_max_lotes": monthly_lote_limit,
                    "license_max_volume_tons": monthly_ton_limit,
                    "license_max_batch_rows": max_batch_rows,
                    "license_valid_until": valid_until,
                    "license_is_active": True,
                },
            ).mappings().one()
            db_session.commit()
            return {
                "status": "success",
                "organization_id": int(created["organization_id"]),
                "organization_name": str(created["organization_name"]),
                "slug": str(created["organization_slug"]),
                "tax_id": normalized_tax_id,
                "plan_tier": str(created["license_plan_type"]),
                "admin_user_id": int(created["admin_user_id"]),
                "admin_username": str(created["admin_username"]),
                "admin_email": str(created["admin_email"]),
                "license_id": int(created["license_id"]),
                "limits": {
                    "lotes_mensuales": int(created["license_max_lotes"]),
                    "toneladas_mensuales": float(created["license_max_volume_tons"]),
                    "max_batch_rows": int(created["license_max_batch_rows"]),
                },
            }

        actor_user = _assert_platform_actor(db_session, refresh_token=refresh_token)
        result = _create_organization_direct(
            db_session,
            name=normalized_name,
            tax_id=normalized_tax_id,
            admin_email=normalized_admin_email,
            admin_username=normalized_admin_username,
            admin_password_hash=password_hash,
            tier=normalized_tier,
            monthly_lote_limit=monthly_lote_limit,
            monthly_ton_limit=monthly_ton_limit,
            max_batch_rows=max_batch_rows,
            valid_until=valid_until,
            organization_description=normalized_description,
        )
        _record_platform_audit_event_direct(
            db_session,
            actor_user=actor_user,
            action=AuditAction.PLATFORM_ORGANIZATION_CREATE,
            entity_type="organization",
            entity_id=int(result["organization_id"]),
            target_organization_id=int(result["organization_id"]),
            metadata={
                "target_organization_id": int(result["organization_id"]),
                "organization_name": normalized_name,
                "tax_id": normalized_tax_id,
                "tier": normalized_tier,
            },
            request_context=audit_request_context,
        )
        _record_platform_audit_event_direct(
            db_session,
            actor_user=actor_user,
            action=AuditAction.PLATFORM_ORGANIZATION_ADMIN_CREATE,
            entity_type="user",
            entity_id=int(result["admin_user_id"]),
            target_organization_id=int(result["organization_id"]),
            metadata={
                "target_organization_id": int(result["organization_id"]),
                "admin_username": normalized_admin_username,
            },
            request_context=audit_request_context,
        )
        _record_platform_audit_event_direct(
            db_session,
            actor_user=actor_user,
            action=AuditAction.PLATFORM_LICENSE_CREATE,
            entity_type="license",
            entity_id=int(result["license_id"]),
            target_organization_id=int(result["organization_id"]),
            metadata={
                "target_organization_id": int(result["organization_id"]),
                "plan_type": normalized_tier,
            },
            request_context=audit_request_context,
        )
        db_session.commit()
        return result
    except IntegrityError as exc:
        db_session.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Ya existe una organizacion o usuario con esos identificadores.",
        ) from exc
    except DBAPIError as exc:
        db_session.rollback()
        _map_platform_db_error(exc)
        raise
    except Exception:
        db_session.rollback()
        raise
    finally:
        db_session.close()


def _toggle_organization_status_direct(
    db_session: Session,
    *,
    organization_id: int,
) -> dict[str, Any]:
    organization = db_session.get(Organization, organization_id)
    if organization is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="La organizacion solicitada no existe.",
        )

    organization.is_active = not organization.is_active
    revoked_session_count = 0
    if not organization.is_active:
        revoked_session_count = db_session.execute(
            text(
                """
                UPDATE user_sessions
                SET revoked_at = :revoked_at
                WHERE organization_id = :organization_id
                  AND revoked_at IS NULL
                """
            ),
            {
                "revoked_at": utc_now(),
                "organization_id": organization_id,
            },
        ).rowcount or 0
    db_session.flush()
    return {
        "organization_id": organization.id,
        "is_active": organization.is_active,
        "revoked_session_count": revoked_session_count,
    }


def alternar_estado_empresa(
    *,
    refresh_token: str | None,
    org_id: int,
    audit_request_context: AuditRequestContext | None = None,
) -> dict[str, Any]:
    db_session = get_db_session()
    if db_session is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Servicio de base de datos no disponible.",
        )

    try:
        if _supports_platform_control_plane_functions(db_session):
            token_hash = _require_platform_refresh_token_hash(refresh_token)
            result = db_session.execute(
                text(
                    """
                    SELECT *
                    FROM public.platform_toggle_organization_status(
                        :actor_refresh_token_hash,
                        :organization_id
                    )
                    """
                ),
                {
                    "actor_refresh_token_hash": token_hash,
                    "organization_id": org_id,
                },
            ).mappings().one()
            db_session.commit()
            return dict(result)

        actor_user = _assert_platform_actor(db_session, refresh_token=refresh_token)
        result = _toggle_organization_status_direct(
            db_session,
            organization_id=org_id,
        )
        _record_platform_audit_event_direct(
            db_session,
            actor_user=actor_user,
            action=AuditAction.PLATFORM_ORGANIZATION_STATUS_CHANGE,
            entity_type="organization",
            entity_id=int(result["organization_id"]),
            target_organization_id=int(result["organization_id"]),
            metadata={
                "target_organization_id": int(result["organization_id"]),
                "is_active": bool(result["is_active"]),
                "revoked_session_count": int(result["revoked_session_count"]),
            },
            request_context=audit_request_context,
        )
        db_session.commit()
        return result
    except DBAPIError as exc:
        db_session.rollback()
        _map_platform_db_error(exc)
        raise
    except Exception:
        db_session.rollback()
        raise
    finally:
        db_session.close()


def _upsert_license_direct(
    db_session: Session,
    *,
    organization_id: int,
    plan_type: str,
    max_lotes: int,
    max_volume_tons: float,
    max_batch_rows: int,
    valid_until: datetime | None,
    is_active: bool,
) -> tuple[dict[str, Any], AuditAction]:
    organization = db_session.get(Organization, organization_id)
    if organization is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="La organizacion solicitada no existe.",
        )

    license_record = db_session.execute(
        select(License).where(License.organization_id == organization_id)
    ).scalar_one_or_none()
    license_created = license_record is None
    if license_created:
        license_record = License(
            organization_id=organization_id,
            plan_type=plan_type,
            max_lotes=max_lotes,
            max_volume_tons=max_volume_tons,
            max_batch_rows=max_batch_rows,
            valid_until=valid_until,
            is_active=is_active,
        )
        db_session.add(license_record)
    else:
        license_record.plan_type = plan_type
        license_record.max_lotes = max_lotes
        license_record.max_volume_tons = max_volume_tons
        license_record.max_batch_rows = max_batch_rows
        license_record.valid_until = valid_until
        license_record.is_active = is_active

    db_session.flush()
    return (
        {
            "license_id": license_record.id,
            "organization_id": organization_id,
            "plan_type": license_record.plan_type,
            "max_lotes": license_record.max_lotes,
            "max_volume_tons": license_record.max_volume_tons,
            "max_batch_rows": license_record.max_batch_rows,
            "valid_until": (
                license_record.valid_until.isoformat()
                if license_record.valid_until is not None
                else None
            ),
            "is_active": license_record.is_active,
        },
        (
            AuditAction.PLATFORM_LICENSE_CREATE
            if license_created
            else AuditAction.PLATFORM_LICENSE_UPDATE
        ),
    )


def upsert_license_superadmin(
    *,
    refresh_token: str | None,
    organization_id: int,
    plan_type: str,
    max_lotes: int,
    max_volume_tons: float,
    max_batch_rows: int,
    valid_until: datetime | None = None,
    is_active: bool = True,
    audit_request_context: AuditRequestContext | None = None,
) -> dict[str, Any]:
    normalized_plan_type = _normalize_non_empty(plan_type, field_name="plan_type").lower()
    if max_lotes <= 0 or max_volume_tons <= 0 or max_batch_rows <= 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Los limites de licencia deben ser mayores que cero.",
        )

    db_session = get_db_session()
    if db_session is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Servicio de base de datos no disponible.",
        )

    try:
        if _supports_platform_control_plane_functions(db_session):
            token_hash = _require_platform_refresh_token_hash(refresh_token)
            result = db_session.execute(
                text(
                    """
                    SELECT *
                    FROM public.platform_upsert_license(
                        :actor_refresh_token_hash,
                        :organization_id,
                        :plan_type,
                        :max_lotes,
                        :max_volume_tons,
                        :max_batch_rows,
                        :valid_until,
                        :is_active
                    )
                    """
                ),
                {
                    "actor_refresh_token_hash": token_hash,
                    "organization_id": organization_id,
                    "plan_type": normalized_plan_type,
                    "max_lotes": max_lotes,
                    "max_volume_tons": max_volume_tons,
                    "max_batch_rows": max_batch_rows,
                    "valid_until": valid_until,
                    "is_active": is_active,
                },
            ).mappings().one()
            db_session.commit()
            return dict(result)

        actor_user = _assert_platform_actor(db_session, refresh_token=refresh_token)
        result, audit_action = _upsert_license_direct(
            db_session,
            organization_id=organization_id,
            plan_type=normalized_plan_type,
            max_lotes=max_lotes,
            max_volume_tons=max_volume_tons,
            max_batch_rows=max_batch_rows,
            valid_until=valid_until,
            is_active=is_active,
        )
        _record_platform_audit_event_direct(
            db_session,
            actor_user=actor_user,
            action=audit_action,
            entity_type="license",
            entity_id=int(result["license_id"]),
            target_organization_id=int(result["organization_id"]),
            metadata={
                "target_organization_id": int(result["organization_id"]),
                "plan_type": normalized_plan_type,
            },
            request_context=audit_request_context,
        )
        db_session.commit()
        return result
    except IntegrityError as exc:
        db_session.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="No fue posible persistir la licencia para esa organizacion.",
        ) from exc
    except DBAPIError as exc:
        db_session.rollback()
        _map_platform_db_error(exc)
        raise
    except Exception:
        db_session.rollback()
        raise
    finally:
        db_session.close()
