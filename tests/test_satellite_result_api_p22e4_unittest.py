from __future__ import annotations

import asyncio
import json
from datetime import date, datetime, timezone
from uuid import uuid4

import pytest
from fastapi import HTTPException, Response, status
from sqlalchemy import delete, select

from main import app
from litoral_trace.api.auth import LoginRequest, get_current_tenant_user, login_b2b
from litoral_trace.api.satellite import (
    SatelliteJobResultResponse,
    get_satellite_job_result_endpoint,
)
from litoral_trace.auth.passwords import hash_password
from litoral_trace.auth.rbac import Permission, ensure_permission
from litoral_trace.db.engine import get_db_session
from litoral_trace.db.models import (
    AuditLog,
    License,
    Lote,
    Organization,
    SatelliteJob,
    SatelliteJobResult,
    User,
)
from litoral_trace.services.satellite_job_results import (
    NDVI_TIMESERIES_RESULT_SCHEMA_VERSION,
    compute_satellite_job_result_payload_sha256,
)


RESULT_FIELDS = {
    "schema_version",
    "job_id",
    "lote_id",
    "geometry_hash",
    "algorithm_version",
    "total_observations",
    "observations",
    "created_at",
}
FORBIDDEN_FIELDS = {
    "organization_id",
    "payload_sha256",
    "idempotency_key",
    "polygon_wkt_snapshot",
    "locked_by",
    "locked_at",
    "heartbeat_at",
    "lease_token",
    "error_message",
}


@pytest.fixture(autouse=True)
def cleanup_e4_state():
    session = get_db_session()
    try:
        organizations = session.execute(
            select(Organization).where(Organization.slug.like("p22e4-%"))
        ).scalars().all()
        organization_ids = [organization.id for organization in organizations]
        if organization_ids:
            session.execute(
                delete(SatelliteJobResult).where(
                    SatelliteJobResult.organization_id.in_(organization_ids)
                )
            )
            session.execute(
                delete(AuditLog).where(AuditLog.organization_id.in_(organization_ids))
            )
            session.execute(
                delete(SatelliteJob).where(
                    SatelliteJob.organization_id.in_(organization_ids)
                )
            )
            for organization in organizations:
                session.delete(organization)
            session.commit()
    finally:
        session.close()

    yield

    session = get_db_session()
    try:
        organizations = session.execute(
            select(Organization).where(Organization.slug.like("p22e4-%"))
        ).scalars().all()
        organization_ids = [organization.id for organization in organizations]
        if organization_ids:
            session.execute(
                delete(SatelliteJobResult).where(
                    SatelliteJobResult.organization_id.in_(organization_ids)
                )
            )
            session.execute(
                delete(AuditLog).where(AuditLog.organization_id.in_(organization_ids))
            )
            session.execute(
                delete(SatelliteJob).where(
                    SatelliteJob.organization_id.in_(organization_ids)
                )
            )
            for organization in organizations:
                session.delete(organization)
            session.commit()
    finally:
        session.close()


def _create_account(*, role: str) -> dict[str, int | str]:
    suffix = uuid4().hex[:10]
    password = f"P22E4-{suffix}-Password!"
    session = get_db_session()
    try:
        organization = Organization(
            name=f"P22E4 Org {suffix}",
            slug=f"p22e4-{suffix}",
            tax_id=f"84-{suffix[:8]}",
            tier="pro",
            is_active=True,
        )
        session.add(organization)
        session.flush()
        session.add(
            License(
                organization_id=organization.id,
                plan_type="pro",
                max_lotes=100,
                max_volume_tons=5000.0,
                max_batch_rows=500,
                is_active=True,
            )
        )
        user = User(
            organization_id=organization.id,
            email=f"{suffix}@example.com",
            username=f"p22e4_user_{suffix}",
            password_hash=hash_password(password),
            role=role,
            full_name="P22E4 Test User",
            is_active=True,
        )
        lote = Lote(
            organization_id=organization.id,
            identificador=f"P22E4-LOTE-{suffix}",
            productor_id=f"46-{suffix[:8]}",
            producto_forestal="Madera Aserrada (Pino)",
            hectareas=25.0,
            latitud=-27.45,
            longitud=-58.90,
            polygon_wkt=(
                "POLYGON((-58.91 -27.46, -58.89 -27.46, "
                "-58.89 -27.44, -58.91 -27.44, -58.91 -27.46))"
            ),
            estatus="Pendiente",
            volumen_ingresado_ton=20.0,
            volumen_exportar_ton=5.0,
        )
        session.add_all([user, lote])
        session.commit()
        session.refresh(lote)
        return {
            "organization_id": int(organization.id),
            "username": user.username,
            "password": password,
            "lote_id": int(lote.id),
        }
    finally:
        session.close()


def _authenticate(account):
    token = asyncio.run(
        login_b2b(
            LoginRequest(
                username=str(account["username"]),
                password=str(account["password"]),
            ),
            Response(),
        )
    )
    return get_current_tenant_user(authorization=f"Bearer {token.access_token}")


def _result_payload(*, job_id: int, lote_id: int) -> dict[str, object]:
    return {
        "schema_version": NDVI_TIMESERIES_RESULT_SCHEMA_VERSION,
        "job_id": job_id,
        "lote_id": lote_id,
        "geometry_hash": "e" * 64,
        "algorithm_version": "p22e4-test",
        "total_observations": 1,
        "observations": [
            {
                "observation_date": "2026-08-01",
                "ndvi_mean": 0.61,
                "ndvi_min": 0.55,
                "ndvi_max": 0.68,
                "ndvi_std": 0.03,
                "scene_cloud_percentage": 5.0,
                "aoi_cloud_percentage": 1.0,
                "valid_pixel_count": 10,
                "valid_pixel_percentage": 98.0,
                "satellite": "Sentinel-2",
                "collection": "COPERNICUS/S2_SR_HARMONIZED",
                "processing_date": "2026-08-01T12:00:00+00:00",
            }
        ],
    }


def _create_job(account, *, job_status: str, error_code: str | None = None,
                error_message: str | None = None, with_result: bool = False,
                malformed_result: bool = False) -> int:
    session = get_db_session()
    try:
        now = datetime.now(timezone.utc)
        job = SatelliteJob(
            organization_id=int(account["organization_id"]),
            lote_id=int(account["lote_id"]),
            job_type="ndvi_timeseries",
            status=job_status,
            attempt_count=0 if job_status == "queued" else 1,
            max_attempts=3,
            next_attempt_at=now,
            started_at=None if job_status == "queued" else now,
            finished_at=now if job_status in {"succeeded", "failed"} else None,
            error_code=error_code,
            error_message=error_message,
            idempotency_key=f"p22e4-{uuid4().hex}",
            request_start_date=date(2026, 7, 1),
            request_end_date=date(2026, 8, 1),
            max_cloud_pct=20.0,
            geometry_hash="e" * 64,
            algorithm_version="p22e4-test",
            polygon_wkt_snapshot=(
                "POLYGON((-58.91 -27.46, -58.89 -27.46, "
                "-58.89 -27.44, -58.91 -27.44, -58.91 -27.46))"
            ),
        )
        session.add(job)
        session.flush()
        if with_result:
            payload = _result_payload(job_id=int(job.id), lote_id=int(job.lote_id))
            if malformed_result:
                payload["unexpected_internal_field"] = "must fail closed"
            session.add(
                SatelliteJobResult(
                    satellite_job_id=int(job.id),
                    organization_id=int(account["organization_id"]),
                    lote_id=int(account["lote_id"]),
                    result_schema_version=NDVI_TIMESERIES_RESULT_SCHEMA_VERSION,
                    geometry_hash="e" * 64,
                    algorithm_version="p22e4-test",
                    result_payload=payload,
                    payload_sha256=compute_satellite_job_result_payload_sha256(payload),
                )
            )
        session.commit()
        return int(job.id)
    finally:
        session.close()


def _invoke(job_id: int, user):
    try:
        response = asyncio.run(
            get_satellite_job_result_endpoint(job_id=job_id, user=user)
        )
        if isinstance(response, SatelliteJobResultResponse):
            return 200, response.model_dump(mode="json")
        return response.status_code, json.loads(response.body.decode())
    except HTTPException as exc:
        return exc.status_code, {"detail": exc.detail}


def test_result_openapi_contract_is_explicit_and_safe():
    schema = app.openapi()
    operation = schema["paths"]["/api/v1/satellite/jobs/{job_id}/result"]["get"]
    responses = operation["responses"]
    result_schema = schema["components"]["schemas"]["SatelliteJobResultResponse"]

    assert set(responses) >= {"200", "401", "403", "404", "409", "422", "500"}
    assert "202" not in responses
    assert responses["200"]["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/SatelliteJobResultResponse"
    }
    assert set(result_schema["properties"]) == RESULT_FIELDS
    serialized = json.dumps(result_schema, sort_keys=True).lower()
    for field_name in FORBIDDEN_FIELDS:
        assert field_name not in serialized


def test_result_auth_and_rbac_follow_satellite_run_permission():
    with pytest.raises(HTTPException) as auth_exc:
        get_current_tenant_user(
            authorization=None,
            bearer_token=None,
            session_jwt=None,
        )
    assert auth_exc.value.status_code == status.HTTP_401_UNAUTHORIZED

    cliente = _authenticate(_create_account(role="cliente"))
    with pytest.raises(HTTPException) as permission_exc:
        ensure_permission(cliente, Permission.SATELLITE_RUN)
    assert permission_exc.value.status_code == status.HTTP_403_FORBIDDEN

    manager = _authenticate(_create_account(role="manager"))
    ensure_permission(manager, Permission.SATELLITE_RUN)


def test_result_not_found_and_cross_tenant_are_indistinguishable():
    account_a = _create_account(role="manager")
    account_b = _create_account(role="manager")
    job_id = _create_job(account_a, job_status="queued")
    user_b = _authenticate(account_b)

    assert _invoke(job_id, user_b) == _invoke(999999999, user_b) == (
        404,
        {"detail": "Satellite job no encontrado."},
    )


@pytest.mark.parametrize("job_status", ["queued", "running"])
def test_result_pending_states_return_409(job_status: str):
    account = _create_account(role="manager")
    job_id = _create_job(account, job_status=job_status)

    status_code, body = _invoke(job_id, _authenticate(account))

    assert status_code == status.HTTP_409_CONFLICT
    assert set(body) == {"job_id", "status", "detail", "next_attempt_at"}
    assert body["job_id"] == job_id
    assert body["status"] == job_status


def test_failed_result_returns_only_safe_error_code():
    account = _create_account(role="manager")
    raw_error = "postgresql://user:password@private-host/internal traceback"
    job_id = _create_job(
        account,
        job_status="failed",
        error_code="invalid_job_payload",
        error_message=raw_error,
    )

    status_code, body = _invoke(job_id, _authenticate(account))

    assert status_code == status.HTTP_409_CONFLICT
    assert body == {
        "job_id": job_id,
        "status": "failed",
        "error_code": "invalid_job_payload",
        "detail": "El satellite job finalizo sin un resultado disponible.",
    }
    assert raw_error not in json.dumps(body)


def test_succeeded_result_returns_validated_immutable_payload():
    account = _create_account(role="manager")
    job_id = _create_job(account, job_status="succeeded", with_result=True)

    status_code, body = _invoke(job_id, _authenticate(account))

    assert status_code == status.HTTP_200_OK
    assert set(body) == RESULT_FIELDS
    assert body["job_id"] == job_id
    assert body["lote_id"] == int(account["lote_id"])
    assert body["schema_version"] == NDVI_TIMESERIES_RESULT_SCHEMA_VERSION
    assert body["total_observations"] == len(body["observations"]) == 1
    assert body["observations"][0]["ndvi_mean"] == 0.61
    for field_name in FORBIDDEN_FIELDS:
        assert field_name not in body


@pytest.mark.parametrize("malformed_result", [False, True])
def test_succeeded_without_valid_result_fails_closed(malformed_result: bool):
    account = _create_account(role="manager")
    job_id = _create_job(
        account,
        job_status="succeeded",
        with_result=malformed_result,
        malformed_result=malformed_result,
    )

    status_code, body = _invoke(job_id, _authenticate(account))

    assert status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
    assert body == {
        "detail": "El resultado persistido del satellite job no esta disponible."
    }
    assert "unexpected_internal_field" not in json.dumps(body)


def test_result_read_creates_no_audit_event_and_existing_routes_remain_registered():
    account = _create_account(role="manager")
    job_id = _create_job(account, job_status="succeeded", with_result=True)
    user = _authenticate(account)
    session = get_db_session()
    try:
        before = session.execute(
            select(AuditLog).where(
                AuditLog.organization_id == int(account["organization_id"])
            )
        ).scalars().all()
    finally:
        session.close()

    status_code, _ = _invoke(job_id, user)

    session = get_db_session()
    try:
        after = session.execute(
            select(AuditLog).where(
                AuditLog.organization_id == int(account["organization_id"])
            )
        ).scalars().all()
    finally:
        session.close()

    paths = app.openapi()["paths"]
    assert status_code == 200
    assert len(after) == len(before)
    assert "post" in paths["/api/v1/satellite/jobs"]
    assert "get" in paths["/api/v1/satellite/jobs/{job_id}"]
    assert "get" in paths["/api/v1/satellite/jobs/{job_id}/result"]
    assert "post" in paths["/api/v1/satellite/ndvi"]
