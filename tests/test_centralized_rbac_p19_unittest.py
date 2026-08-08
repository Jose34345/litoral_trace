from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest
from fastapi import HTTPException, Response
from sqlalchemy import select

from litoral_trace.api.admin import (
    listar_organizaciones_endpoint,
    require_superadmin_role,
)
from litoral_trace.api.auth import (
    LoginRequest,
    get_current_tenant_user,
    login_b2b,
)
from litoral_trace.api.lotes import (
    LoteCreateRequest,
    LoteUpdateRequest,
    crear_lote,
    listar_lotes_tenant,
)
from litoral_trace.api.satellite import (
    SatelliteQueryByLoteRequest,
    consultar_ndvi_satelital_lote_endpoint,
)
from litoral_trace.api.settings import (
    InviteDemoUserRequest,
    consultar_licencia_tenant,
    generar_invitacion_demo_endpoint,
)
from litoral_trace.api.vault import consultar_documentos_boveda
from litoral_trace.auth.passwords import hash_password
from litoral_trace.auth.rbac import (
    Permission,
    has_permission,
    permissions_for_role,
    require_permission,
)
from litoral_trace.auth.tokens import create_jwt_token
from litoral_trace.db.engine import get_db_session
from litoral_trace.db.init_db import get_non_production_superadmin_seed
from litoral_trace.db.models import Lote, Organization, User


def _create_tenant_account(
    *,
    role: str,
    with_lote: bool = True,
) -> dict[str, int | str]:
    suffix = uuid4().hex[:10]
    password = f"Rbac-{suffix}-Password!"
    db_session = get_db_session()

    try:
        organization = Organization(
            name=f"RBAC Tenant {suffix}",
            slug=f"rbac-tenant-{suffix}",
            tax_id=f"30-{suffix[:8]}",
            tier="pro",
            is_active=True,
        )
        db_session.add(organization)
        db_session.commit()
        db_session.refresh(organization)

        user = User(
            organization_id=organization.id,
            email=f"{suffix}@example.com",
            username=f"rbac_user_{suffix}",
            password_hash=hash_password(password),
            role=role,
            full_name="RBAC Test User",
            is_active=True,
        )
        db_session.add(user)
        db_session.commit()
        db_session.refresh(user)

        lote_id: int | None = None
        if with_lote:
            lote = Lote(
                organization_id=organization.id,
                identificador=f"RBAC-LOTE-{suffix}",
                productor_id=f"20-{suffix[:8]}",
                producto_forestal="Madera Aserrada (Pino)",
                hectareas=33.0,
                latitud=-27.31,
                longitud=-58.71,
                polygon_wkt=(
                    "POLYGON(("
                    "-58.72 -27.32, -58.70 -27.32, "
                    "-58.70 -27.30, -58.72 -27.30, "
                    "-58.72 -27.32"
                    "))"
                ),
                estatus="Pendiente",
                volumen_ingresado_ton=50.0,
                volumen_exportar_ton=15.0,
            )
            db_session.add(lote)
            db_session.commit()
            db_session.refresh(lote)
            lote_id = lote.id

        return {
            "organization_id": organization.id,
            "organization_name": organization.name,
            "organization_slug": organization.slug,
            "username": user.username,
            "password": password,
            "role": user.role,
            "email": user.email,
            "lote_id": lote_id,
        }
    finally:
        db_session.close()


def _login_access_token(*, username: str, password: str) -> str:
    token_response = asyncio.run(
        login_b2b(
            LoginRequest(username=username, password=password),
            Response(),
        )
    )
    return token_response.access_token


def _authenticated_context_from_token(token: str):
    return get_current_tenant_user(authorization=f"Bearer {token}")


def _authenticated_context(
    *,
    username: str,
    password: str,
):
    return _authenticated_context_from_token(
        _login_access_token(username=username, password=password)
    )


def _run_guard(permission: Permission, user):
    return require_permission(permission)(user=user)


def test_permission_matrix_superadmin_admin_and_unknown_role():
    assert permissions_for_role("superadmin") == frozenset(Permission)
    assert Permission.PLATFORM_ADMIN in permissions_for_role("superadmin")

    admin_permissions = permissions_for_role("admin")
    assert Permission.LOTE_CREATE in admin_permissions
    assert Permission.SETTINGS_WRITE in admin_permissions
    assert Permission.PLATFORM_ADMIN not in admin_permissions

    assert permissions_for_role("unknown-role") == frozenset()
    assert has_permission("unknown-role", Permission.LOTE_READ) is False


def test_manager_and_auditor_permissions_follow_current_semantics():
    manager_permissions = permissions_for_role("manager")
    auditor_permissions = permissions_for_role("auditor")

    assert Permission.LOTE_DELETE in manager_permissions
    assert Permission.SATELLITE_RUN in manager_permissions
    assert Permission.SETTINGS_WRITE not in manager_permissions

    assert Permission.LOTE_READ in auditor_permissions
    assert Permission.VAULT_READ in auditor_permissions
    assert Permission.LOTE_CREATE not in auditor_permissions
    assert Permission.SATELLITE_RUN not in auditor_permissions


def test_cliente_can_read_license_and_vault_but_cannot_write_lotes_or_settings():
    account = _create_tenant_account(role="cliente")
    user = _authenticated_context(
        username=str(account["username"]),
        password=str(account["password"]),
    )

    license_response = asyncio.run(consultar_licencia_tenant(user=user))
    assert license_response.status_code == 200

    vault_response = asyncio.run(
        consultar_documentos_boveda(
            q=None,
            type=None,
            user=user,
        )
    )
    assert vault_response.status_code == 200

    with pytest.raises(HTTPException) as create_exc:
        _run_guard(Permission.LOTE_CREATE, user)
    assert create_exc.value.status_code == 403

    with pytest.raises(HTTPException) as update_exc:
        _run_guard(Permission.LOTE_UPDATE, user)
    assert update_exc.value.status_code == 403

    with pytest.raises(HTTPException) as delete_exc:
        _run_guard(Permission.LOTE_DELETE, user)
    assert delete_exc.value.status_code == 403

    with pytest.raises(HTTPException) as settings_exc:
        _run_guard(Permission.SETTINGS_WRITE, user)
    assert settings_exc.value.status_code == 403


def test_admin_can_write_settings_but_not_platform_admin():
    account = _create_tenant_account(role="admin")
    user = _authenticated_context(
        username=str(account["username"]),
        password=str(account["password"]),
    )

    _run_guard(Permission.SETTINGS_WRITE, user)
    invite_response = asyncio.run(
        generar_invitacion_demo_endpoint(
            InviteDemoUserRequest(
                cuit_empresa="30-71234567-8",
                nombre_contacto="Mario Dario Benitez",
                email_contacto="mario.benitez@example.com",
                especie_principal="Madera Aserrada (Pino)",
            ),
            user=user,
        )
    )
    assert invite_response.status_code == 201

    with pytest.raises(HTTPException) as platform_exc:
        require_superadmin_role(user=user)
    assert platform_exc.value.status_code == 403


def test_manager_can_write_lotes_but_cannot_write_settings():
    account = _create_tenant_account(role="manager")
    user = _authenticated_context(
        username=str(account["username"]),
        password=str(account["password"]),
    )

    _run_guard(Permission.LOTE_CREATE, user)
    create_response = asyncio.run(
        crear_lote(
            payload=LoteCreateRequest(
                identificador=f"MANAGER-{uuid4().hex[:6]}",
                productor_id="20-44444444-4",
                producto_forestal="Madera Aserrada (Pino)",
                hectareas=12.0,
                latitud=-27.41,
                longitud=-58.81,
                volumen_ingresado_ton=10.0,
                volumen_exportar_ton=3.0,
            ),
            user=user,
        )
    )
    assert create_response.organization_id == user.organization_id

    with pytest.raises(HTTPException) as settings_exc:
        _run_guard(Permission.SETTINGS_WRITE, user)
    assert settings_exc.value.status_code == 403


def test_auditor_can_read_lotes_but_cannot_write():
    account = _create_tenant_account(role="auditor")
    user = _authenticated_context(
        username=str(account["username"]),
        password=str(account["password"]),
    )

    _run_guard(Permission.LOTE_READ, user)
    list_response = asyncio.run(listar_lotes_tenant(user=user))
    assert list_response.status_code == 200

    with pytest.raises(HTTPException) as create_exc:
        _run_guard(Permission.LOTE_CREATE, user)
    assert create_exc.value.status_code == 403


def test_unknown_role_fails_closed_on_authenticated_endpoint():
    account = _create_tenant_account(role="mystery-role")
    user = _authenticated_context(
        username=str(account["username"]),
        password=str(account["password"]),
    )

    with pytest.raises(HTTPException) as license_exc:
        _run_guard(Permission.LICENSE_READ, user)
    assert license_exc.value.status_code == 403


def test_satellite_requires_capability_before_external_call(monkeypatch):
    account = _create_tenant_account(role="cliente")
    user = _authenticated_context(
        username=str(account["username"]),
        password=str(account["password"]),
    )
    external_call_triggered = False

    def _unexpected_gee_call(*args, **kwargs):
        nonlocal external_call_triggered
        external_call_triggered = True
        raise AssertionError("GEE no debe ejecutarse sin satellite:run")

    monkeypatch.setattr(
        "litoral_trace.api.satellite.consultar_serie_temporal_ndvi_gee",
        _unexpected_gee_call,
    )

    with pytest.raises(HTTPException) as satellite_exc:
        _run_guard(Permission.SATELLITE_RUN, user)
    assert satellite_exc.value.status_code == 403
    assert external_call_triggered is False

    manager_account = _create_tenant_account(role="manager")
    manager_user = _authenticated_context(
        username=str(manager_account["username"]),
        password=str(manager_account["password"]),
    )
    _run_guard(Permission.SATELLITE_RUN, manager_user)


def test_superadmin_has_platform_admin_endpoint_access():
    superadmin = _authenticated_context(
        username="admin",
        password=get_non_production_superadmin_seed()[1],
    )
    platform_user = require_superadmin_role(user=superadmin)
    response = asyncio.run(
        listar_organizaciones_endpoint(admin=platform_user)
    )

    assert response.status_code == 200


def test_forged_signed_role_claim_does_not_grant_extra_permissions():
    account = _create_tenant_account(role="cliente")
    forged_token = create_jwt_token(
        {
            "sub": str(account["username"]),
            "org_id": int(account["organization_id"]),
            "org_name": str(account["organization_name"]),
            "role": "admin",
            "email": str(account["email"]),
        },
        expires_in_seconds=3600,
    )

    context = _authenticated_context_from_token(forged_token)
    assert context.role == "cliente"

    with pytest.raises(HTTPException) as create_exc:
        _run_guard(Permission.LOTE_CREATE, context)
    assert create_exc.value.status_code == 403


def test_tampered_jwt_is_rejected_with_401():
    token = _login_access_token(
        username="admin",
        password=get_non_production_superadmin_seed()[1],
    )
    tampered_last_char = "a" if token[-1] != "a" else "b"
    tampered_token = token[:-1] + tampered_last_char

    with pytest.raises(HTTPException) as auth_exc:
        _authenticated_context_from_token(tampered_token)
    assert auth_exc.value.status_code == 401


def test_role_downgrade_in_db_revokes_old_admin_capabilities():
    account = _create_tenant_account(role="admin")
    token = _login_access_token(
        username=str(account["username"]),
        password=str(account["password"]),
    )

    db_session = get_db_session()
    try:
        user = db_session.execute(
            select(User).where(User.username == account["username"])
        ).scalar_one()
        user.role = "cliente"
        db_session.commit()
    finally:
        db_session.close()

    downgraded_context = _authenticated_context_from_token(token)
    assert downgraded_context.role == "cliente"

    with pytest.raises(HTTPException) as downgrade_exc:
        _run_guard(Permission.LOTE_CREATE, downgraded_context)
    assert downgrade_exc.value.status_code == 403


def test_satellite_endpoint_still_runs_for_manager():
    account = _create_tenant_account(role="manager")
    user = _authenticated_context(
        username=str(account["username"]),
        password=str(account["password"]),
    )

    _run_guard(Permission.SATELLITE_RUN, user)
    response = asyncio.run(
        consultar_ndvi_satelital_lote_endpoint(
            SatelliteQueryByLoteRequest(lote_id=int(account["lote_id"])),
            user=user,
        )
    )

    assert response.status_code == 200
