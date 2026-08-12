from __future__ import annotations

import asyncio
import json
from http.cookies import SimpleCookie
from uuid import uuid4

import pytest
from fastapi import Response, status
from pydantic import ValidationError
from sqlalchemy import delete, select
from starlette.requests import Request

from main import app
from litoral_trace.api.auth import (
    LoginRequest,
    get_current_tenant_user,
    login_b2b,
)
from litoral_trace.api.satellite import (
    SatelliteJobSubmitRequest,
    SatelliteQueryByLoteRequest,
    consultar_ndvi_satelital_lote_endpoint,
    submit_satellite_job_endpoint,
)
from litoral_trace.auth.passwords import hash_password
from litoral_trace.db.engine import get_db_session
from litoral_trace.db.models import AuditLog, License, Lote, Organization, SatelliteJob, User
from litoral_trace.services.audit import AuditAction


@pytest.fixture(autouse=True)
def cleanup_p22e2_state():
    db_session = get_db_session()
    try:
        created_orgs = db_session.execute(
            select(Organization).where(
                Organization.slug.like("p22e2-%")
            )
        ).scalars().all()
        db_session.execute(delete(AuditLog))
        db_session.execute(delete(SatelliteJob))
        for organization in created_orgs:
            db_session.delete(organization)
        db_session.commit()
    finally:
        db_session.close()

    yield

    db_session = get_db_session()
    try:
        created_orgs = db_session.execute(
            select(Organization).where(
                Organization.slug.like("p22e2-%")
            )
        ).scalars().all()
        db_session.execute(delete(AuditLog))
        db_session.execute(delete(SatelliteJob))
        for organization in created_orgs:
            db_session.delete(organization)
        db_session.commit()
    finally:
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
    request_id: str | None = None,
    user_agent: str = "pytest-p22e2/1.0",
) -> Request:
    headers: list[tuple[bytes, bytes]] = [
        (b"user-agent", user_agent.encode("utf-8")),
    ]
    if request_id:
        headers.append((b"x-request-id", request_id.encode("utf-8")))

    scope = {
        "type": "http",
        "http_version": "1.1",
        "method": method,
        "scheme": "http",
        "path": path,
        "raw_path": path.encode("utf-8"),
        "query_string": b"",
        "headers": headers,
        "client": ("203.0.113.20", 50124),
        "server": ("testserver", 80),
        "root_path": "",
    }
    return Request(scope)


def _create_tenant_account(*, role: str) -> dict[str, int | str]:
    suffix = uuid4().hex[:10]
    password = f"P22E2-{suffix}-Password!"
    db_session = get_db_session()

    try:
        organization = Organization(
            name=f"P22E2 Org {suffix}",
            slug=f"p22e2-{suffix}",
            tax_id=f"31-{suffix[:8]}",
            tier="pro",
            is_active=True,
        )
        db_session.add(organization)
        db_session.flush()

        license_record = License(
            organization_id=organization.id,
            plan_type="pro",
            max_lotes=100,
            max_volume_tons=5000.0,
            max_batch_rows=500,
            is_active=True,
        )
        db_session.add(license_record)

        user = User(
            organization_id=organization.id,
            email=f"{suffix}@example.com",
            username=f"p22e2_user_{suffix}",
            password_hash=hash_password(password),
            role=role,
            full_name="P22E2 Test User",
            is_active=True,
        )
        db_session.add(user)
        db_session.flush()

        lote = Lote(
            organization_id=organization.id,
            identificador=f"P22E2-LOTE-{suffix}",
            productor_id=f"21-{suffix[:8]}",
            producto_forestal="Madera Aserrada (Pino)",
            hectareas=42.0,
            latitud=-27.45,
            longitud=-58.90,
            polygon_wkt=(
                "POLYGON(("
                "-58.91 -27.46, -58.89 -27.46, "
                "-58.89 -27.44, -58.91 -27.44, "
                "-58.91 -27.46"
                "))"
            ),
            estatus="Pendiente",
            volumen_ingresado_ton=20.0,
            volumen_exportar_ton=5.0,
        )
        db_session.add(lote)
        db_session.commit()

        return {
            "organization_id": organization.id,
            "organization_name": organization.name,
            "username": user.username,
            "password": password,
            "role": user.role,
            "email": user.email,
            "lote_id": lote.id,
        }
    finally:
        db_session.close()


def _authenticated_context(*, username: str, password: str):
    response = Response()
    token_response = asyncio.run(
        login_b2b(
            LoginRequest(username=username, password=password),
            response,
        )
    )
    _extract_cookies(response)
    return get_current_tenant_user(
        authorization=f"Bearer {token_response.access_token}"
    )


def _latest_submit_audit() -> AuditLog:
    db_session = get_db_session()
    try:
        event = db_session.execute(
            select(AuditLog)
            .where(AuditLog.action == AuditAction.SATELLITE_JOB_SUBMIT.value)
            .order_by(AuditLog.id.desc())
        ).scalar_one()
        return event
    finally:
        db_session.close()


def test_submit_request_validation_rejects_invalid_payloads():
    with pytest.raises(ValidationError):
        SatelliteJobSubmitRequest(
            lote_id=0,
            start_date="2026-07-01",
            end_date="2026-08-01",
        )

    with pytest.raises(ValidationError):
        SatelliteJobSubmitRequest(
            lote_id=101,
            start_date="2026-08-02",
            end_date="2026-08-01",
        )

    with pytest.raises(ValidationError):
        SatelliteJobSubmitRequest(
            lote_id=101,
            start_date="2026-07-01",
            end_date="2026-08-01",
            max_cloud_pct=101.0,
        )

    with pytest.raises(ValidationError):
        SatelliteJobSubmitRequest(
            lote_id=101,
            start_date="2026-07-01",
            end_date="2026-08-01",
            idempotency_key="   ",
        )

    with pytest.raises(ValidationError):
        SatelliteJobSubmitRequest.model_validate(
            {
                "organization_id": 999,
                "lote_id": 101,
                "start_date": "2026-07-01",
                "end_date": "2026-08-01",
            }
        )

    with pytest.raises(ValidationError):
        SatelliteJobSubmitRequest.model_validate(
            {
                "status": "queued",
                "lease_token": "secret",
                "lote_id": 101,
                "start_date": "2026-07-01",
                "end_date": "2026-08-01",
            }
        )


def test_submit_openapi_contract_exposes_one_public_route_and_minimal_request_schema():
    openapi_schema = app.openapi()
    satellite_paths = openapi_schema["paths"]

    assert "/api/v1/satellite/jobs" in satellite_paths
    assert "/api/v1/satellite/ndvi" in satellite_paths
    assert list(path for path in satellite_paths if path == "/api/v1/satellite/jobs") == [
        "/api/v1/satellite/jobs"
    ]

    submit_operation = satellite_paths["/api/v1/satellite/jobs"]["post"]
    request_schema = openapi_schema["components"]["schemas"][
        "SatelliteJobSubmitRequest"
    ]

    assert submit_operation["requestBody"]["required"] is True
    assert submit_operation["requestBody"]["content"]["application/json"][
        "schema"
    ] == {"$ref": "#/components/schemas/SatelliteJobSubmitRequest"}

    assert set(request_schema["properties"]) == {
        "lote_id",
        "start_date",
        "end_date",
        "max_cloud_pct",
        "idempotency_key",
    }
    assert request_schema["required"] == ["lote_id", "start_date", "end_date"]
    assert request_schema["additionalProperties"] is False
    assert request_schema["properties"]["lote_id"]["type"] == "integer"
    assert request_schema["properties"]["lote_id"]["exclusiveMinimum"] == 0.0
    assert request_schema["properties"]["start_date"]["format"] == "date"
    assert request_schema["properties"]["end_date"]["format"] == "date"
    assert request_schema["properties"]["max_cloud_pct"]["type"] == "number"
    assert request_schema["properties"]["max_cloud_pct"]["minimum"] == 0.0
    assert request_schema["properties"]["max_cloud_pct"]["maximum"] == 100.0
    assert request_schema["properties"]["max_cloud_pct"]["default"] == 20.0
    assert request_schema["properties"]["idempotency_key"]["anyOf"][0][
        "maxLength"
    ] == 255

    serialized_request_schema = json.dumps(
        request_schema,
        sort_keys=True,
    ).lower()
    for forbidden_field in (
        "organization_id",
        "status",
        "job_type",
        "attempt_count",
        "max_attempts",
        "next_attempt_at",
        "polygon_wkt_snapshot",
        "geometry_hash",
        "algorithm_version",
        "locked_by",
        "lease_token",
        "heartbeat_at",
        "error_message",
    ):
        assert forbidden_field not in serialized_request_schema


def test_submit_openapi_contract_documents_success_and_conflict_responses():
    openapi_schema = app.openapi()
    submit_operation = openapi_schema["paths"]["/api/v1/satellite/jobs"]["post"]
    responses = submit_operation["responses"]

    assert set(responses) >= {"200", "202", "401", "403", "404", "409", "422"}
    assert responses["200"]["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/SatelliteJobSubmitResponse"
    }
    assert responses["202"]["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/SatelliteJobSubmitResponse"
    }
    assert "Location" in responses["200"]["headers"]
    assert "Location" in responses["202"]["headers"]
    assert "idempotent replay" in responses["200"]["description"].lower()
    assert "accepted" in responses["202"]["description"].lower()
    assert "lote no encontrado" in responses["404"]["description"].lower()
    assert "idempotency key" in responses["409"]["description"].lower()

    response_schema = openapi_schema["components"]["schemas"][
        "SatelliteJobSubmitResponse"
    ]
    assert set(response_schema["properties"]) == {
        "job_id",
        "job_type",
        "status",
        "created_at",
        "next_attempt_at",
    }
    serialized_response_schema = json.dumps(
        response_schema,
        sort_keys=True,
    ).lower()
    for forbidden_field in (
        "organization_id",
        "lote_id",
        "idempotency_key",
        "geometry_hash",
        "algorithm_version",
        "polygon_wkt_snapshot",
        "attempt_count",
        "max_attempts",
        "lease_token",
        "locked_by",
        "heartbeat_at",
        "error_message",
    ):
        assert forbidden_field not in serialized_response_schema


def test_submit_endpoint_creates_job_with_minimal_response_and_server_derived_fields():
    account = _create_tenant_account(role="manager")
    user = _authenticated_context(
        username=str(account["username"]),
        password=str(account["password"]),
    )

    response = asyncio.run(
        submit_satellite_job_endpoint(
            SatelliteJobSubmitRequest(
                lote_id=int(account["lote_id"]),
                start_date="2026-07-01",
                end_date="2026-08-01",
                max_cloud_pct=12.5,
                idempotency_key=f"submit-{uuid4().hex}",
            ),
            request=_build_request(
                method="POST",
                path="/api/v1/satellite/jobs",
                request_id="p22e2-submit-create",
            ),
            user=user,
        )
    )
    body = json.loads(response.body.decode("utf-8"))

    assert response.status_code == status.HTTP_202_ACCEPTED
    assert response.headers["Location"] == f"/api/v1/satellite/jobs/{body['job_id']}"
    assert set(body) == {
        "job_id",
        "job_type",
        "status",
        "created_at",
        "next_attempt_at",
    }
    assert body["job_type"] == "ndvi_timeseries"
    assert body["status"] == "queued"
    assert "organization_id" not in body
    assert "lote_id" not in body
    assert "idempotency_key" not in body
    assert "lease_token" not in body
    assert "locked_by" not in body
    assert "heartbeat_at" not in body
    assert "polygon_wkt_snapshot" not in body
    assert "geometry_hash" not in body
    assert "algorithm_version" not in body
    assert "attempt_count" not in body
    assert "error_message" not in body

    db_session = get_db_session()
    try:
        job = db_session.execute(
            select(SatelliteJob).where(SatelliteJob.id == body["job_id"])
        ).scalar_one()
        assert job.organization_id == int(account["organization_id"])
        assert job.lote_id == int(account["lote_id"])
        assert job.job_type == "ndvi_timeseries"
        assert job.status == "queued"
        assert job.attempt_count == 0
        assert job.max_attempts == 3
        assert job.geometry_hash
        assert job.algorithm_version
        assert job.polygon_wkt_snapshot
        assert job.next_attempt_at is not None
    finally:
        db_session.close()


def test_submit_endpoint_replays_same_key_same_payload_with_200_same_job():
    account = _create_tenant_account(role="manager")
    user = _authenticated_context(
        username=str(account["username"]),
        password=str(account["password"]),
    )
    idempotency_key = f"submit-replay-{uuid4().hex}"

    first_response = asyncio.run(
        submit_satellite_job_endpoint(
            SatelliteJobSubmitRequest(
                lote_id=int(account["lote_id"]),
                start_date="2026-07-01",
                end_date="2026-08-01",
                idempotency_key=idempotency_key,
            ),
            user=user,
        )
    )
    second_response = asyncio.run(
        submit_satellite_job_endpoint(
            SatelliteJobSubmitRequest(
                lote_id=int(account["lote_id"]),
                start_date="2026-07-01",
                end_date="2026-08-01",
                idempotency_key=idempotency_key,
            ),
            user=user,
        )
    )

    first_body = json.loads(first_response.body.decode("utf-8"))
    second_body = json.loads(second_response.body.decode("utf-8"))

    assert first_response.status_code == status.HTTP_202_ACCEPTED
    assert second_response.status_code == status.HTTP_200_OK
    assert first_body["job_id"] == second_body["job_id"]
    assert second_response.headers["Location"] == (
        f"/api/v1/satellite/jobs/{first_body['job_id']}"
    )


def test_submit_endpoint_returns_409_for_same_key_different_payload():
    account = _create_tenant_account(role="manager")
    user = _authenticated_context(
        username=str(account["username"]),
        password=str(account["password"]),
    )
    idempotency_key = f"submit-conflict-{uuid4().hex}"

    first_response = asyncio.run(
        submit_satellite_job_endpoint(
            SatelliteJobSubmitRequest(
                lote_id=int(account["lote_id"]),
                start_date="2026-07-01",
                end_date="2026-08-01",
                idempotency_key=idempotency_key,
            ),
            user=user,
        )
    )
    assert first_response.status_code == status.HTTP_202_ACCEPTED

    with pytest.raises(Exception) as exc_info:
        asyncio.run(
            submit_satellite_job_endpoint(
                SatelliteJobSubmitRequest(
                    lote_id=int(account["lote_id"]),
                    start_date="2026-07-02",
                    end_date="2026-08-01",
                    idempotency_key=idempotency_key,
                ),
                user=user,
            )
        )

    http_exc = exc_info.value
    assert getattr(http_exc, "status_code", None) == 409
    assert (
        getattr(http_exc, "detail", "")
        == "El idempotency_key ya fue utilizado para un payload satelital diferente."
    )
    serialized_detail = json.dumps({"detail": http_exc.detail}).lower()
    assert idempotency_key.lower() not in serialized_detail
    assert "geometry_hash" not in serialized_detail
    assert "algorithm_version" not in serialized_detail
    assert "polygon" not in serialized_detail
    assert "constraint" not in serialized_detail


def test_submit_endpoint_without_key_creates_distinct_jobs():
    account = _create_tenant_account(role="manager")
    user = _authenticated_context(
        username=str(account["username"]),
        password=str(account["password"]),
    )

    first_response = asyncio.run(
        submit_satellite_job_endpoint(
            SatelliteJobSubmitRequest(
                lote_id=int(account["lote_id"]),
                start_date="2026-07-01",
                end_date="2026-08-01",
            ),
            user=user,
        )
    )
    second_response = asyncio.run(
        submit_satellite_job_endpoint(
            SatelliteJobSubmitRequest(
                lote_id=int(account["lote_id"]),
                start_date="2026-07-01",
                end_date="2026-08-01",
            ),
            user=user,
        )
    )

    first_body = json.loads(first_response.body.decode("utf-8"))
    second_body = json.loads(second_response.body.decode("utf-8"))

    assert first_response.status_code == status.HTTP_202_ACCEPTED
    assert second_response.status_code == status.HTTP_202_ACCEPTED
    assert first_body["job_id"] != second_body["job_id"]


def test_submit_endpoint_returns_404_for_cross_tenant_and_invisible_lote():
    account_a = _create_tenant_account(role="manager")
    account_b = _create_tenant_account(role="manager")
    user_b = _authenticated_context(
        username=str(account_b["username"]),
        password=str(account_b["password"]),
    )

    with pytest.raises(Exception) as cross_exc:
        asyncio.run(
            submit_satellite_job_endpoint(
                SatelliteJobSubmitRequest(
                    lote_id=int(account_a["lote_id"]),
                    start_date="2026-07-01",
                    end_date="2026-08-01",
                ),
                user=user_b,
            )
        )
    assert getattr(cross_exc.value, "status_code", None) == 404
    assert getattr(cross_exc.value, "detail", None) == "Lote no encontrado."

    with pytest.raises(Exception) as missing_exc:
        asyncio.run(
            submit_satellite_job_endpoint(
                SatelliteJobSubmitRequest(
                    lote_id=999999,
                    start_date="2026-07-01",
                    end_date="2026-08-01",
                ),
                user=user_b,
            )
        )
    assert getattr(missing_exc.value, "status_code", None) == 404
    assert getattr(missing_exc.value, "detail", None) == "Lote no encontrado."


def test_submit_endpoint_returns_safe_500_when_persistence_or_audit_fails(monkeypatch):
    account = _create_tenant_account(role="manager")
    user = _authenticated_context(
        username=str(account["username"]),
        password=str(account["password"]),
    )
    idempotency_key = f"submit-500-{uuid4().hex}"

    def _raise_audit_failure(*_args, **_kwargs):
        raise RuntimeError("duplicate key leak should stay internal")

    monkeypatch.setattr(
        "litoral_trace.api.satellite.record_audit_event",
        _raise_audit_failure,
    )

    with pytest.raises(Exception) as exc_info:
        asyncio.run(
            submit_satellite_job_endpoint(
                SatelliteJobSubmitRequest(
                    lote_id=int(account["lote_id"]),
                    start_date="2026-07-01",
                    end_date="2026-08-01",
                    idempotency_key=idempotency_key,
                ),
                request=_build_request(
                    method="POST",
                    path="/api/v1/satellite/jobs",
                    request_id="p22e2-submit-500",
                ),
                user=user,
            )
        )

    http_exc = exc_info.value
    assert getattr(http_exc, "status_code", None) == 500
    assert (
        getattr(http_exc, "detail", None)
        == "No fue posible registrar el satellite job."
    )
    serialized_detail = json.dumps({"detail": http_exc.detail}).lower()
    assert idempotency_key.lower() not in serialized_detail
    assert "duplicate key" not in serialized_detail
    assert "traceback" not in serialized_detail
    assert "geometry_hash" not in serialized_detail
    assert "lease_token" not in serialized_detail


def test_submit_endpoint_audit_metadata_contains_no_sensitive_values():
    account = _create_tenant_account(role="manager")
    user = _authenticated_context(
        username=str(account["username"]),
        password=str(account["password"]),
    )
    idempotency_key = f"submit-audit-{uuid4().hex}"

    response = asyncio.run(
        submit_satellite_job_endpoint(
            SatelliteJobSubmitRequest(
                lote_id=int(account["lote_id"]),
                start_date="2026-07-01",
                end_date="2026-08-01",
                max_cloud_pct=33.0,
                idempotency_key=idempotency_key,
            ),
            request=_build_request(
                method="POST",
                path="/api/v1/satellite/jobs",
                request_id="p22e2-submit-audit",
            ),
            user=user,
        )
    )
    assert response.status_code == status.HTTP_202_ACCEPTED

    event = _latest_submit_audit()
    serialized_event = json.dumps(
        {
            "before_data": event.before_data,
            "after_data": event.after_data,
            "detail": event.detail,
        },
        sort_keys=True,
        default=str,
    ).lower()

    assert event.after_data["metadata"]["created"] is True
    assert event.after_data["metadata"]["replayed"] is False
    assert event.after_data["metadata"]["lote_id"] == int(account["lote_id"])
    assert event.after_data["metadata"]["job_type"] == "ndvi_timeseries"
    assert event.after_data["metadata"]["start_date"] == "2026-07-01"
    assert event.after_data["metadata"]["end_date"] == "2026-08-01"
    assert event.after_data["metadata"]["max_cloud_pct"] == 33.0
    assert "idempotency_key" not in serialized_event
    assert idempotency_key.lower() not in serialized_event
    assert "polygon_wkt_snapshot" not in serialized_event
    assert "geometry_hash" not in serialized_event
    assert "algorithm_version" not in serialized_event
    assert "lease_token" not in serialized_event
    assert "locked_by" not in serialized_event
    assert "heartbeat_at" not in serialized_event


def test_legacy_sync_satellite_endpoint_remains_unchanged_for_manager(monkeypatch):
    account = _create_tenant_account(role="manager")
    user = _authenticated_context(
        username=str(account["username"]),
        password=str(account["password"]),
    )

    monkeypatch.setattr(
        "litoral_trace.api.satellite.get_cached_satellite_data",
        lambda _cache_key: (None, 0),
    )
    monkeypatch.setattr(
        "litoral_trace.api.satellite.set_cached_satellite_data",
        lambda *_args, **_kwargs: ({}, 0),
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

    response = asyncio.run(
        consultar_ndvi_satelital_lote_endpoint(
            SatelliteQueryByLoteRequest(lote_id=int(account["lote_id"])),
            user=user,
        )
    )
    body = json.loads(response.body.decode("utf-8"))

    assert response.status_code == status.HTTP_200_OK
    assert body["lote_id"] == int(account["lote_id"])
    assert body["organization_id"] == int(account["organization_id"])
    assert body["status"] == "success"
