from __future__ import annotations

import asyncio
import io
import json
from http.cookies import SimpleCookie
from uuid import uuid4

import pandas as pd
import pytest
from fastapi import HTTPException, Response, status
from sqlalchemy import delete, select
from starlette.datastructures import UploadFile
from starlette.requests import Request

from litoral_trace.api.admin import (
    CrearEmpresaClienteRequest,
    crear_organizacion_endpoint,
    toggle_organizacion_status_endpoint,
    UpsertOrganizationLicenseRequest,
    upsert_organizacion_license_endpoint,
)
from litoral_trace.api.auth import (
    LoginRequest,
    LogoutRequest,
    RefreshRequest,
    get_current_tenant_user,
    login_b2b,
    logout_b2b_session,
    refresh_b2b_session,
)
from litoral_trace.api.lotes import (
    LoteCreateRequest,
    LoteUpdateRequest,
    actualizar_lote,
    crear_lote,
    eliminar_lote,
    procesar_batch_excel_endpoint,
)
from litoral_trace.api.satellite import (
    SatelliteQueryByLoteRequest,
    consultar_ndvi_satelital_lote_endpoint,
)
from litoral_trace.api.settings import (
    InviteDemoUserRequest,
    generar_invitacion_demo_endpoint,
)
from litoral_trace.api.vault import descargar_documento_boveda
from litoral_trace.auth.sessions import (
    ACCESS_TOKEN_COOKIE_KEY,
    REFRESH_TOKEN_COOKIE_KEY,
)
from litoral_trace.auth.tokens import verify_jwt_token
from litoral_trace.db.engine import get_db_session
from litoral_trace.db.init_db import get_non_production_superadmin_seed
from litoral_trace.db.models import AuditLog, Lote, Organization, User, UserSession
from litoral_trace.services.audit import (
    AuditAction,
    AuditOutcome,
    SENSITIVE_METADATA_KEYS,
    build_audit_actor,
    record_audit_event,
    sanitize_audit_metadata,
)


@pytest.fixture(autouse=True)
def cleanup_audit_state():
    db_session = get_db_session()
    db_session.execute(delete(AuditLog))
    db_session.execute(delete(UserSession))
    created_test_orgs = db_session.execute(
        select(Organization).where(Organization.name.like("P110 Demo Org %"))
    ).scalars().all()
    for organization in created_test_orgs:
        db_session.delete(organization)
    admin_user = db_session.execute(
        select(User).where(User.username == "admin")
    ).scalar_one()
    admin_user.last_login_at = None
    db_session.commit()
    db_session.close()

    yield

    db_session = get_db_session()
    db_session.execute(delete(AuditLog))
    db_session.execute(delete(UserSession))
    created_test_orgs = db_session.execute(
        select(Organization).where(Organization.name.like("P110 Demo Org %"))
    ).scalars().all()
    for organization in created_test_orgs:
        db_session.delete(organization)
    admin_user = db_session.execute(
        select(User).where(User.username == "admin")
    ).scalar_one()
    admin_user.last_login_at = None
    db_session.commit()
    db_session.close()


def _extract_cookies(response: Response) -> dict[str, str]:
    parsed_cookie = SimpleCookie()
    for set_cookie_header in response.headers.getlist("set-cookie"):
        parsed_cookie.load(set_cookie_header)
    return {
        cookie_name: morsel.value
        for cookie_name, morsel in parsed_cookie.items()
    }


def _build_request(
    *,
    method: str,
    path: str,
    cookies: dict[str, str] | None = None,
    request_id: str | None = None,
    user_agent: str = "pytest-audit/1.0",
) -> Request:
    headers: list[tuple[bytes, bytes]] = [
        (b"user-agent", user_agent.encode("utf-8")),
    ]
    if request_id:
        headers.append((b"x-request-id", request_id.encode("utf-8")))
    if cookies:
        cookie_header = "; ".join(
            f"{cookie_name}={cookie_value}"
            for cookie_name, cookie_value in cookies.items()
        )
        headers.append((b"cookie", cookie_header.encode("utf-8")))

    scope = {
        "type": "http",
        "http_version": "1.1",
        "method": method,
        "scheme": "http",
        "path": path,
        "raw_path": path.encode("utf-8"),
        "query_string": b"",
        "headers": headers,
        "client": ("203.0.113.10", 50123),
        "server": ("testserver", 80),
        "root_path": "",
    }
    return Request(scope)


def _login(
    *,
    username: str = "admin",
    password: str | None = None,
    request_id: str | None = None,
) -> tuple[object, Response, dict[str, str]]:
    if password is None:
        password = get_non_production_superadmin_seed()[1]
    response = Response()
    token_response = asyncio.run(
        login_b2b(
            LoginRequest(username=username, password=password),
            response,
            _build_request(
                method="POST",
                path="/api/v1/auth/login",
                request_id=request_id,
            ),
        )
    )
    return token_response, response, _extract_cookies(response)


def _authenticated_context(
    *,
    username: str = "admin",
    password: str | None = None,
):
    if password is None:
        password = get_non_production_superadmin_seed()[1]
    token_response, _, _ = _login(username=username, password=password)
    return get_current_tenant_user(
        authorization=f"Bearer {token_response.access_token}"
    )


def _audit_events(action: str | None = None) -> list[AuditLog]:
    db_session = get_db_session()
    try:
        query = select(AuditLog).order_by(AuditLog.id)
        if action is not None:
            query = query.where(AuditLog.action == action)
        return db_session.execute(query).scalars().all()
    finally:
        db_session.close()


def _latest_audit(action: str) -> AuditLog:
    events = _audit_events(action)
    assert events, f"No audit events found for action {action!r}"
    return events[-1]


def _serialized_audit_event(event: AuditLog) -> str:
    return json.dumps(
        {
            "before_data": event.before_data,
            "after_data": event.after_data,
            "detail": event.detail,
            "username": event.username,
        },
        sort_keys=True,
        default=str,
    ).lower()


def _collect_keys(value) -> set[str]:
    if isinstance(value, dict):
        keys = {str(key).lower() for key in value}
        for nested_value in value.values():
            keys.update(_collect_keys(nested_value))
        return keys
    if isinstance(value, list):
        keys: set[str] = set()
        for item in value:
            keys.update(_collect_keys(item))
        return keys
    return set()


def _assert_no_sensitive_fields(
    event: AuditLog,
    *,
    forbidden_values: list[str] | None = None,
) -> None:
    serialized = _serialized_audit_event(event)
    event_keys = _collect_keys(event.before_data) | _collect_keys(event.after_data)
    assert event_keys.isdisjoint(SENSITIVE_METADATA_KEYS)

    for forbidden_value in forbidden_values or []:
        assert forbidden_value.lower() not in serialized


def test_metadata_sanitizer_blocks_sensitive_keys_case_insensitively():
    sanitized = sanitize_audit_metadata(
        {
            "Password": "should-drop",
            "normal_key": "keep-me",
            "nested": {
                "REFRESH_TOKEN": "should-drop-too",
                "safe": "value",
            },
            "items": [
                {"Authorization": "drop"},
                {"safe_item": "keep"},
            ],
            "COOKIE": "drop-cookie",
        }
    )

    assert sanitized == {
        "normal_key": "keep-me",
        "nested": {"safe": "value"},
        "items": [{"safe_item": "keep"}],
    }


def test_unknown_audit_action_is_rejected():
    db_session = get_db_session()
    try:
        with pytest.raises(ValueError, match="Unsupported audit action"):
            record_audit_event(
                db_session,
                actor=build_audit_actor(
                    organization_id=1,
                    user_id=1,
                    username="admin",
                    role="superadmin",
                ),
                action="custom.invalid.action",
                entity_type="test_entity",
                outcome=AuditOutcome.SUCCESS,
            )
    finally:
        db_session.rollback()
        db_session.close()


def test_login_success_and_failure_are_audited_without_passwords_or_tokens():
    request_id = f"login-success-{uuid4().hex[:8]}"
    token_response, _, _ = _login(request_id=request_id)
    login_event = _latest_audit(AuditAction.AUTH_LOGIN_SUCCESS.value)

    assert login_event.organization_id == 1
    assert login_event.user_id is not None
    assert login_event.username == "admin"
    assert login_event.after_data["outcome"] == AuditOutcome.SUCCESS.value
    assert login_event.after_data["request_id"] == request_id
    assert login_event.after_data["user_agent"] == "pytest-audit/1.0"
    assert login_event.after_data["metadata"]["session_family_id"]
    assert int(verify_jwt_token(token_response.access_token)["sid"]) == login_event.entity_id
    _assert_no_sensitive_fields(
        login_event,
        forbidden_values=[
            token_response.access_token,
            get_non_production_superadmin_seed()[1],
        ],
    )

    bad_password = "WrongPassword-P110!"
    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(
            login_b2b(
                LoginRequest(username="admin", password=bad_password),
                Response(),
                _build_request(
                    method="POST",
                    path="/api/v1/auth/login",
                    request_id=f"login-failure-{uuid4().hex[:8]}",
                ),
            )
        )

    assert exc_info.value.status_code == status.HTTP_401_UNAUTHORIZED
    failure_event = _latest_audit(AuditAction.AUTH_LOGIN_FAILURE.value)
    assert failure_event.after_data["outcome"] == AuditOutcome.FAILURE.value
    assert failure_event.after_data["metadata"] == {
        "reason": "credential_validation_failed"
    }
    _assert_no_sensitive_fields(
        failure_event,
        forbidden_values=[bad_password],
    )


def test_refresh_success_reuse_and_logout_are_audited():
    _, _, cookies = _login(request_id=f"refresh-base-{uuid4().hex[:8]}")
    original_refresh_token = cookies[REFRESH_TOKEN_COOKIE_KEY]
    session_jwt = cookies[ACCESS_TOKEN_COOKIE_KEY]

    refresh_response = Response()
    refreshed = asyncio.run(
        refresh_b2b_session(
            refresh_response,
            payload=RefreshRequest(refresh_token=original_refresh_token),
            request=_build_request(
                method="POST",
                path="/api/v1/auth/refresh",
                request_id="refresh-success-request",
            ),
        )
    )

    refresh_event = _latest_audit(AuditAction.AUTH_REFRESH_SUCCESS.value)
    assert refresh_event.after_data["outcome"] == AuditOutcome.SUCCESS.value
    assert refresh_event.after_data["request_id"] == "refresh-success-request"
    assert refresh_event.after_data["metadata"]["previous_session_id"] is not None
    assert refresh_event.after_data["metadata"]["session_family_id"]
    _assert_no_sensitive_fields(
        refresh_event,
        forbidden_values=[
            original_refresh_token,
            refreshed.access_token,
        ],
    )

    with pytest.raises(HTTPException) as reuse_exc:
        asyncio.run(
            refresh_b2b_session(
                Response(),
                payload=RefreshRequest(refresh_token=original_refresh_token),
                request=_build_request(
                    method="POST",
                    path="/api/v1/auth/refresh",
                    request_id="refresh-reuse-request",
                ),
            )
        )

    assert reuse_exc.value.status_code == status.HTTP_401_UNAUTHORIZED
    reuse_event = _latest_audit(AuditAction.AUTH_REFRESH_REUSE.value)
    assert reuse_event.after_data["outcome"] == AuditOutcome.FAILURE.value
    assert reuse_event.after_data["request_id"] == "refresh-reuse-request"
    assert reuse_event.after_data["metadata"]["session_family_id"]
    _assert_no_sensitive_fields(
        reuse_event,
        forbidden_values=[original_refresh_token],
    )

    logout_response = Response()
    logout_payload = asyncio.run(
        logout_b2b_session(
            logout_response,
            payload=LogoutRequest(),
            request=_build_request(
                method="POST",
                path="/api/v1/auth/logout",
                cookies={
                    ACCESS_TOKEN_COOKIE_KEY: session_jwt,
                    REFRESH_TOKEN_COOKIE_KEY: original_refresh_token,
                },
                request_id="logout-request",
            ),
            session_jwt=session_jwt,
        )
    )

    assert logout_payload.detail == "Sesion finalizada."
    logout_event = _latest_audit(AuditAction.AUTH_LOGOUT.value)
    assert logout_event.after_data["outcome"] == AuditOutcome.SUCCESS.value
    assert logout_event.after_data["request_id"] == "logout-request"
    assert logout_event.after_data["metadata"] == {"logout_via": "access_token"}
    _assert_no_sensitive_fields(
        logout_event,
        forbidden_values=[session_jwt, original_refresh_token],
    )


def test_lote_create_update_delete_generate_audit_events_with_before_after_state():
    user = _authenticated_context()
    lote_identifier = f"P110-LOTE-{uuid4().hex[:6]}"

    created = asyncio.run(
        crear_lote(
            payload=LoteCreateRequest(
                identificador=lote_identifier,
                productor_id="20-11111111-1",
                producto_forestal="Madera Aserrada (Pino)",
                hectareas=14.0,
                latitud=-27.4,
                longitud=-58.8,
                volumen_ingresado_ton=11.0,
                volumen_exportar_ton=4.0,
            ),
            request=_build_request(
                method="POST",
                path="/api/v1/lotes",
                request_id="lote-create-request",
            ),
            user=user,
        )
    )

    create_event = _latest_audit(AuditAction.LOTE_CREATE.value)
    assert create_event.entity_id == created.id
    assert create_event.before_data is None
    assert create_event.after_data["state_after"]["identificador"] == lote_identifier
    assert create_event.after_data["metadata"] == {"identificador": lote_identifier}

    updated = asyncio.run(
        actualizar_lote(
            lote_id=created.id,
            payload=LoteUpdateRequest(
                estatus="Aprobado",
                volumen_exportar_ton=7.5,
            ),
            request=_build_request(
                method="PUT",
                path=f"/api/v1/lotes/{created.id}",
                request_id="lote-update-request",
            ),
            user=user,
        )
    )

    update_event = _latest_audit(AuditAction.LOTE_UPDATE.value)
    assert update_event.entity_id == created.id
    assert update_event.before_data["estatus"] == "Pendiente"
    assert update_event.after_data["state_after"]["estatus"] == "Aprobado"
    assert update_event.after_data["state_after"]["volumen_exportar_ton"] == 7.5
    assert updated.estatus == "Aprobado"

    asyncio.run(
        eliminar_lote(
            lote_id=created.id,
            request=_build_request(
                method="DELETE",
                path=f"/api/v1/lotes/{created.id}",
                request_id="lote-delete-request",
            ),
            user=user,
        )
    )

    delete_event = _latest_audit(AuditAction.LOTE_DELETE.value)
    assert delete_event.entity_id == created.id
    assert delete_event.before_data["identificador"] == lote_identifier
    assert delete_event.after_data["metadata"] == {"identificador": lote_identifier}

    db_session = get_db_session()
    try:
        assert db_session.get(Lote, created.id) is None
    finally:
        db_session.close()


def test_lote_commit_failure_rolls_back_business_change_and_audit_event(monkeypatch):
    user = _authenticated_context()
    real_session = get_db_session()
    lote_identifier = f"P110-ROLLBACK-{uuid4().hex[:6]}"

    class FailingCommitSession:
        def __init__(self, inner_session):
            self._inner_session = inner_session

        def __getattr__(self, item):
            return getattr(self._inner_session, item)

        def commit(self):
            raise RuntimeError("forced commit failure")

    monkeypatch.setattr(
        "litoral_trace.api.lotes.get_tenant_scoped_db_session",
        lambda organization_id: FailingCommitSession(real_session),
    )

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(
            crear_lote(
                payload=LoteCreateRequest(
                    identificador=lote_identifier,
                    productor_id="20-22222222-2",
                    producto_forestal="Madera Aserrada (Pino)",
                    hectareas=9.0,
                    latitud=-27.5,
                    longitud=-58.9,
                    volumen_ingresado_ton=8.0,
                    volumen_exportar_ton=2.0,
                ),
                request=_build_request(
                    method="POST",
                    path="/api/v1/lotes",
                    request_id="lote-rollback-request",
                ),
                user=user,
            )
        )

    assert exc_info.value.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR

    verification_session = get_db_session()
    try:
        lote = verification_session.execute(
            select(Lote).where(Lote.identificador == lote_identifier)
        ).scalar_one_or_none()
        assert lote is None
        assert _audit_events(AuditAction.LOTE_CREATE.value) == []
    finally:
        verification_session.close()
        real_session.close()


def test_batch_upload_and_vault_download_emit_audit_events(monkeypatch):
    user = _authenticated_context()

    monkeypatch.setattr(
        "litoral_trace.api.lotes.pd.read_excel",
        lambda *_args, **_kwargs: pd.DataFrame([{"identificador": "A"}, {"identificador": "B"}]),
    )
    monkeypatch.setattr(
        "litoral_trace.api.lotes.procesar_lote_masivo",
        lambda df: (df.to_dict(orient="records"), b"zip-bytes"),
    )

    upload = UploadFile(filename="audit_batch.xlsx", file=io.BytesIO(b"excel-content"))
    batch_response = asyncio.run(
        procesar_batch_excel_endpoint(
            file=upload,
            request=_build_request(
                method="POST",
                path="/api/v1/batch/upload",
                request_id="batch-upload-request",
            ),
            user=user,
        )
    )

    assert batch_response.headers["content-disposition"].startswith("attachment;")
    batch_event = _latest_audit(AuditAction.LOTE_BATCH_UPLOAD.value)
    assert batch_event.after_data["outcome"] == AuditOutcome.SUCCESS.value
    assert batch_event.after_data["metadata"] == {
        "filename": "audit_batch.xlsx",
        "row_count": 2,
    }

    download_response = asyncio.run(
        descargar_documento_boveda(
            doc_id="DOC-DDS-2026-001",
            request=_build_request(
                method="GET",
                path="/api/v1/vault/download/DOC-DDS-2026-001",
                request_id="vault-download-request",
            ),
            user=user,
        )
    )

    assert download_response.headers["content-disposition"].endswith(".json")
    vault_event = _latest_audit(AuditAction.VAULT_DOWNLOAD.value)
    assert vault_event.after_data["outcome"] == AuditOutcome.SUCCESS.value
    assert vault_event.after_data["metadata"]["document_id"] == "DOC-DDS-2026-001"
    assert vault_event.after_data["metadata"]["doc_type"] == "DDS_JSON_TRACES"


def test_settings_satellite_and_platform_admin_emit_audit_events(monkeypatch):
    user = _authenticated_context()
    _, _, cookies = _login()

    settings_response = asyncio.run(
        generar_invitacion_demo_endpoint(
            InviteDemoUserRequest(
                cuit_empresa="30-71234567-8",
                nombre_contacto="Prospecto Demo",
                email_contacto="prospecto@example.com",
                especie_principal="Madera Aserrada (Pino)",
            ),
            request=_build_request(
                method="POST",
                path="/api/v1/settings/invite_demo_user",
                request_id="settings-invite-request",
            ),
            user=user,
        )
    )

    assert settings_response.status_code == status.HTTP_201_CREATED
    settings_event = _latest_audit(AuditAction.SETTINGS_INVITE_DEMO.value)
    assert settings_event.after_data["metadata"] == {
        "cuit_empresa": "30-71234567-8",
        "especie_principal": "Madera Aserrada (Pino)",
    }
    _assert_no_sensitive_fields(
        settings_event,
        forbidden_values=["prospecto@example.com"],
    )

    monkeypatch.setattr(
        "litoral_trace.api.satellite.get_cached_satellite_data",
        lambda _cache_key: (None, 0),
    )
    monkeypatch.setattr(
        "litoral_trace.api.satellite.set_cached_satellite_data",
        lambda *_args, **_kwargs: (True, 0),
    )
    monkeypatch.setattr(
        "litoral_trace.api.satellite.consultar_serie_temporal_ndvi_gee",
        lambda **_kwargs: {
            "status": "success",
            "gee_connected": False,
            "gee_initialization_ms": 0,
            "gee_query_ms": 0,
            "observations": [
                {
                    "observation_date": "2026-08-01",
                    "ndvi_mean": 0.61,
                    "scene_cloud_percentage": 4.0,
                    "valid_pixel_percentage": 97.0,
                    "satellite": "Sentinel-2_TestMock",
                    "collection": "COPERNICUS/S2_SR_HARMONIZED",
                    "processing_date": "2026-08-08T00:00:00+00:00",
                }
            ],
        },
    )

    satellite_response = asyncio.run(
        consultar_ndvi_satelital_lote_endpoint(
            SatelliteQueryByLoteRequest(
                lote_id=101,
                start_date="2026-07-01",
                end_date="2026-08-01",
            ),
            request=_build_request(
                method="POST",
                path="/api/v1/satellite/ndvi",
                request_id="satellite-success-request",
            ),
            user=user,
        )
    )

    assert satellite_response.status_code == status.HTTP_200_OK
    satellite_event = _latest_audit(AuditAction.SATELLITE_NDVI_RUN.value)
    assert satellite_event.after_data["outcome"] == AuditOutcome.SUCCESS.value
    assert satellite_event.after_data["metadata"]["total_observations"] == 1
    assert satellite_event.after_data["metadata"]["start_date"] == "2026-07-01"

    suffix = uuid4().hex[:8]
    create_name = f"P110 Demo Org {suffix}"
    create_username = f"demo_admin_p110_{suffix}"
    create_email = f"new-admin-{suffix}@example.com"
    create_response = asyncio.run(
        crear_organizacion_endpoint(
            CrearEmpresaClienteRequest(
                name=create_name,
                tax_id=f"30-99{suffix}",
                admin_email=create_email,
                admin_username=create_username,
                admin_password="DemoPassword-P110!",
                tier="enterprise",
                monthly_lote_limit=120,
                monthly_ton_limit=8500.0,
            ),
            request=_build_request(
                method="POST",
                path="/api/v1/admin/organizations",
                request_id="platform-create-request",
            ),
            refresh_token_cookie=cookies[REFRESH_TOKEN_COOKIE_KEY],
            admin=user,
        )
    )
    create_body = json.loads(create_response.body.decode("utf-8"))

    create_event = _latest_audit(AuditAction.PLATFORM_ORGANIZATION_CREATE.value)
    assert create_event.after_data["outcome"] == AuditOutcome.SUCCESS.value
    assert create_event.after_data["metadata"]["target_organization_id"] == int(
        create_body["organization_id"]
    )
    assert create_event.after_data["metadata"]["organization_name"] == create_name
    assert create_event.organization_id == int(create_body["organization_id"])
    _assert_no_sensitive_fields(
        create_event,
        forbidden_values=["DemoPassword-P110!", create_email],
    )

    create_admin_event = _latest_audit(
        AuditAction.PLATFORM_ORGANIZATION_ADMIN_CREATE.value
    )
    assert create_admin_event.after_data["metadata"]["admin_username"] == create_username

    create_license_event = _latest_audit(AuditAction.PLATFORM_LICENSE_CREATE.value)
    assert create_license_event.after_data["metadata"]["plan_type"] == "enterprise"

    license_response = asyncio.run(
        upsert_organizacion_license_endpoint(
            org_id=int(create_body["organization_id"]),
            payload=UpsertOrganizationLicenseRequest(
                plan_type="custom",
                max_lotes=250,
                max_volume_tons=15000.0,
                max_batch_rows=750,
                is_active=True,
            ),
            request=_build_request(
                method="PUT",
                path=f"/api/v1/admin/organizations/{create_body['organization_id']}/license",
                request_id="platform-license-request",
            ),
            refresh_token_cookie=cookies[REFRESH_TOKEN_COOKIE_KEY],
            admin=user,
        )
    )

    assert license_response.status_code == status.HTTP_200_OK
    license_event = _latest_audit(AuditAction.PLATFORM_LICENSE_UPDATE.value)
    assert license_event.after_data["outcome"] == AuditOutcome.SUCCESS.value
    assert license_event.after_data["metadata"]["plan_type"] == "custom"

    toggle_response = asyncio.run(
        toggle_organizacion_status_endpoint(
            org_id=int(create_body["organization_id"]),
            request=_build_request(
                method="POST",
                path=f"/api/v1/admin/organizations/{create_body['organization_id']}/toggle_status",
                request_id="platform-toggle-request",
            ),
            refresh_token_cookie=cookies[REFRESH_TOKEN_COOKIE_KEY],
            admin=user,
        )
    )

    assert toggle_response.status_code == status.HTTP_200_OK
    toggle_event = _latest_audit(
        AuditAction.PLATFORM_ORGANIZATION_STATUS_CHANGE.value
    )
    assert toggle_event.after_data["outcome"] == AuditOutcome.SUCCESS.value
    assert toggle_event.organization_id == int(create_body["organization_id"])
    assert toggle_event.after_data["metadata"]["target_organization_id"] == int(
        create_body["organization_id"]
    )
    assert toggle_event.after_data["metadata"]["is_active"] is False


def test_satellite_unexpected_failure_is_audited_without_error_details(monkeypatch):
    user = _authenticated_context()

    monkeypatch.setattr(
        "litoral_trace.api.satellite.get_cached_satellite_data",
        lambda _cache_key: (None, 0),
    )
    monkeypatch.setattr(
        "litoral_trace.api.satellite.consultar_serie_temporal_ndvi_gee",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("gee secret exploded")),
    )

    with pytest.raises(RuntimeError):
        asyncio.run(
            consultar_ndvi_satelital_lote_endpoint(
                SatelliteQueryByLoteRequest(lote_id=101),
                request=_build_request(
                    method="POST",
                    path="/api/v1/satellite/ndvi",
                    request_id="satellite-failure-request",
                ),
                user=user,
            )
        )

    failure_event = _latest_audit(AuditAction.SATELLITE_NDVI_RUN.value)
    assert failure_event.after_data["outcome"] == AuditOutcome.FAILURE.value
    assert failure_event.after_data["metadata"]["status_code"] == 500
    assert "gee secret exploded" not in _serialized_audit_event(failure_event)
