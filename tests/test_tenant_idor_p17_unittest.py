from __future__ import annotations

import asyncio
import json
from http.cookies import SimpleCookie
from uuid import uuid4

import pytest
from fastapi import HTTPException, Response
from sqlalchemy import select

from litoral_trace.api.admin import require_superadmin_role
from litoral_trace.api.auth import (
    LoginRequest,
    RefreshRequest,
    get_current_tenant_user,
    login_b2b,
    refresh_b2b_session,
)
from litoral_trace.api.lotes import (
    LoteCreateRequest,
    LoteUpdateRequest,
    actualizar_lote,
    crear_lote,
    eliminar_lote,
    listar_lotes_tenant,
    obtener_lote,
)
from litoral_trace.api.satellite import (
    SatelliteQueryByLoteRequest,
    consultar_ndvi_satelital_lote_endpoint,
)
from litoral_trace.api.settings import consultar_licencia_tenant
from litoral_trace.api.vault import descargar_documento_boveda
from litoral_trace.auth.passwords import hash_password
from litoral_trace.db.engine import get_db_session
from litoral_trace.db.init_db import get_non_production_superadmin_seed
from litoral_trace.db.models import Lote, Organization, User


def _extract_cookies(response: Response) -> dict[str, str]:
    parsed_cookie = SimpleCookie()
    for set_cookie_header in response.headers.getlist("set-cookie"):
        parsed_cookie.load(set_cookie_header)
    return {
        cookie_name: morsel.value
        for cookie_name, morsel in parsed_cookie.items()
    }


@pytest.fixture(scope="module")
def tenant_fixture():
    suffix = uuid4().hex[:8]
    organization_slug = f"tenant-b-{suffix}"
    username = f"tenant_b_admin_{suffix}"
    email = f"{username}@example.com"
    password = "TenantBPassword2026!"

    db_session = get_db_session()
    organization_b = Organization(
        name=f"Tenant B {suffix}",
        slug=organization_slug,
        tax_id=f"30-77{suffix[:6]}",
        tier="pro",
        is_active=True,
    )
    db_session.add(organization_b)
    db_session.commit()
    db_session.refresh(organization_b)

    user_b = User(
        organization_id=organization_b.id,
        email=email,
        username=username,
        password_hash=hash_password(password),
        role="admin",
        full_name="Tenant B Admin",
        is_active=True,
    )
    db_session.add(user_b)

    lote_b = Lote(
        organization_id=organization_b.id,
        identificador=f"LOTE-B-{suffix}",
        productor_id=f"20-{suffix[:8]}",
        producto_forestal="Madera Aserrada (Pino)",
        hectareas=55.0,
        latitud=-27.3,
        longitud=-58.7,
        polygon_wkt="POLYGON((-58.71 -27.31, -58.69 -27.31, -58.69 -27.29, -58.71 -27.29, -58.71 -27.31))",
        estatus="Pendiente",
        volumen_ingresado_ton=80.0,
        volumen_exportar_ton=20.0,
    )
    db_session.add(lote_b)
    db_session.commit()
    db_session.refresh(user_b)
    db_session.refresh(lote_b)
    organization_b_id = organization_b.id
    organization_b_name = organization_b.name
    lote_b_id = lote_b.id
    lote_b_identificador = lote_b.identificador
    db_session.close()

    token_a = asyncio.run(
        login_b2b(
            LoginRequest(
                username="admin",
                password=get_non_production_superadmin_seed()[1],
            ),
            Response(),
        )
    )
    context_a = get_current_tenant_user(
        authorization=f"Bearer {token_a.access_token}"
    )

    token_b = asyncio.run(
        login_b2b(
            LoginRequest(username=username, password=password),
            Response(),
        )
    )
    context_b = get_current_tenant_user(
        authorization=f"Bearer {token_b.access_token}"
    )

    return {
        "organization_b_id": organization_b_id,
        "organization_b_name": organization_b_name,
        "username_b": username,
        "password_b": password,
        "lote_b_id": lote_b_id,
        "lote_b_identificador": lote_b_identificador,
        "context_a": context_a,
        "context_b": context_b,
        "token_a": token_a,
        "token_b": token_b,
    }


def test_listado_tenant_a_y_b_aislado(tenant_fixture):
    response_a = asyncio.run(
        listar_lotes_tenant(user=tenant_fixture["context_a"])
    )
    body_a = json.loads(response_a.body.decode("utf-8"))
    lote_ids_a = {lote["id"] for lote in body_a["lotes"]}
    assert tenant_fixture["lote_b_id"] not in lote_ids_a

    response_b = asyncio.run(
        listar_lotes_tenant(user=tenant_fixture["context_b"])
    )
    body_b = json.loads(response_b.body.decode("utf-8"))
    lote_ids_b = {lote["id"] for lote in body_b["lotes"]}
    assert tenant_fixture["lote_b_id"] in lote_ids_b
    assert 101 not in lote_ids_b
    assert 102 not in lote_ids_b


def test_get_cross_tenant_lote_devuelve_404(tenant_fixture):
    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(
            obtener_lote(
                lote_id=101,
                user=tenant_fixture["context_b"],
            )
        )

    assert exc_info.value.status_code == 404


def test_update_cross_tenant_lote_es_rechazado_y_no_modifica(tenant_fixture):
    payload = LoteUpdateRequest(
        identificador="MALICIOUS-UPDATE",
        producto_forestal="Carbón Vegetal",
    )

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(
            actualizar_lote(
                lote_id=101,
                payload=payload,
                user=tenant_fixture["context_b"],
            )
        )

    assert exc_info.value.status_code == 404

    db_session = get_db_session()
    try:
        lote = db_session.execute(
            select(Lote).where(Lote.id == 101)
        ).scalar_one()
        assert lote.identificador == "RODAL-NORTE-01"
        assert lote.producto_forestal == "Madera Aserrada (Pino)"
    finally:
        db_session.close()


def test_delete_cross_tenant_lote_es_rechazado_y_permanece(tenant_fixture):
    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(
            eliminar_lote(
                lote_id=101,
                user=tenant_fixture["context_b"],
            )
        )

    assert exc_info.value.status_code == 404

    db_session = get_db_session()
    try:
        lote = db_session.execute(
            select(Lote).where(Lote.id == 101)
        ).scalar_one_or_none()
        assert lote is not None
    finally:
        db_session.close()


def test_create_with_forged_organization_id_uses_authenticated_tenant(tenant_fixture):
    forged_payload = LoteCreateRequest.model_validate(
        {
            "organization_id": tenant_fixture["organization_b_id"],
            "identificador": f"FORGED-{uuid4().hex[:6]}",
            "productor_id": "20-55555555-5",
            "producto_forestal": "Madera Aserrada (Pino)",
            "hectareas": 10.0,
            "latitud": -27.4,
            "longitud": -58.8,
            "volumen_ingresado_ton": 15.0,
            "volumen_exportar_ton": 5.0,
        }
    )

    created = asyncio.run(
        crear_lote(
            payload=forged_payload,
            user=tenant_fixture["context_a"],
        )
    )

    assert created.organization_id == tenant_fixture["context_a"].organization_id
    assert created.organization_id != tenant_fixture["organization_b_id"]

    db_session = get_db_session()
    try:
        forged_lote = db_session.execute(
            select(Lote).where(Lote.id == created.id)
        ).scalar_one()
        assert forged_lote.organization_id == tenant_fixture["context_a"].organization_id
    finally:
        db_session.close()


def test_satellite_cross_tenant_rejected_before_external_call(monkeypatch, tenant_fixture):
    external_call_triggered = False

    def _unexpected_gee_call(*args, **kwargs):
        nonlocal external_call_triggered
        external_call_triggered = True
        raise AssertionError("GEE no debe ejecutarse para acceso cross-tenant")

    monkeypatch.setattr(
        "litoral_trace.api.satellite.consultar_serie_temporal_ndvi_gee",
        _unexpected_gee_call,
    )

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(
            consultar_ndvi_satelital_lote_endpoint(
                SatelliteQueryByLoteRequest(lote_id=101),
                user=tenant_fixture["context_b"],
            )
        )

    assert exc_info.value.status_code == 404
    assert external_call_triggered is False


def test_vault_download_cross_tenant_rejected(tenant_fixture):
    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(
            descargar_documento_boveda(
                doc_id="DOC-DDS-2026-001",
                user=tenant_fixture["context_b"],
            )
        )

    assert exc_info.value.status_code == 404


def test_license_endpoint_uses_authenticated_tenant_metadata(tenant_fixture):
    response = asyncio.run(
        consultar_licencia_tenant(user=tenant_fixture["context_b"])
    )
    body = json.loads(response.body.decode("utf-8"))

    assert body["organization_id"] == tenant_fixture["organization_b_id"]
    assert body["organization_name"] == tenant_fixture["organization_b_name"]


def test_refresh_preserves_session_tenant_context(tenant_fixture):
    login_response = Response()
    token_response = asyncio.run(
        login_b2b(
            LoginRequest(
                username=tenant_fixture["username_b"],
                password=tenant_fixture["password_b"],
            ),
            login_response,
        )
    )
    cookies = _extract_cookies(login_response)

    refreshed_response = asyncio.run(
        refresh_b2b_session(
            Response(),
            refresh_token_cookie=cookies["refresh_token"],
        )
    )

    assert token_response.user_info["organization_id"] == tenant_fixture["organization_b_id"]
    assert refreshed_response.user_info["organization_id"] == tenant_fixture["organization_b_id"]


def test_org_admin_is_not_platform_superadmin(tenant_fixture):
    with pytest.raises(HTTPException) as exc_info:
        require_superadmin_role(
            user=tenant_fixture["context_b"],
        )

    assert exc_info.value.status_code == 403
    assert tenant_fixture["context_a"].is_platform_superadmin is True
