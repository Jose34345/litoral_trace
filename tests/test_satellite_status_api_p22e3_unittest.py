from __future__ import annotations

import asyncio
import json
from datetime import date, datetime, timedelta, timezone
from uuid import uuid4

import pytest
from fastapi import HTTPException, Response, status
from sqlalchemy import delete, select

from main import app
from litoral_trace.api.auth import LoginRequest, get_current_tenant_user, login_b2b
from litoral_trace.api.satellite import (
    SatelliteJobStatusResponse,
    SatelliteJobSubmitRequest,
    SatelliteQueryByLoteRequest,
    consultar_ndvi_satelital_lote_endpoint,
    get_satellite_job_status_endpoint,
    submit_satellite_job_endpoint,
)
from litoral_trace.auth.passwords import hash_password
from litoral_trace.auth.rbac import Permission, ensure_permission
from litoral_trace.db.engine import get_db_session
from litoral_trace.db.models import AuditLog, License, Lote, Organization, SatelliteJob, User


PUBLIC_STATUS_FIELDS = {
    "job_id",
    "lote_id",
    "job_type",
    "status",
    "attempt_count",
    "max_attempts",
    "created_at",
    "updated_at",
    "started_at",
    "finished_at",
    "next_attempt_at",
    "error_code",
}

FORBIDDEN_PUBLIC_FIELDS = {
    "organization_id",
    "idempotency_key",
    "request_start_date",
    "request_end_date",
    "max_cloud_pct",
    "polygon_wkt_snapshot",
    "geometry_hash",
    "algorithm_version",
    "locked_by",
    "locked_at",
    "heartbeat_at",
    "lease_token",
    "error_message",
}


@pytest.fixture(autouse=True)
def cleanup_p22e3_state():
    db_session = get_db_session()
    try:
        organizations = db_session.execute(
            select(Organization).where(Organization.slug.like("p22e3-%"))
        ).scalars().all()
        db_session.execute(delete(AuditLog))
        db_session.execute(delete(SatelliteJob))
        for organization in organizations:
            db_session.delete(organization)
        db_session.commit()
    finally:
        db_session.close()

    yield

    db_session = get_db_session()
    try:
        organizations = db_session.execute(
            select(Organization).where(Organization.slug.like("p22e3-%"))
        ).scalars().all()
        db_session.execute(delete(AuditLog))
        db_session.execute(delete(SatelliteJob))
        for organization in organizations:
            db_session.delete(organization)
        db_session.commit()
    finally:
        db_session.close()


def _create_tenant_account(*, role: str) -> dict[str, int | str]:
    suffix = uuid4().hex[:10]
    password = f"P22E3-{suffix}-Password!"
    db_session = get_db_session()
    try:
        organization = Organization(
            name=f"P22E3 Org {suffix}",
            slug=f"p22e3-{suffix}",
            tax_id=f"32-{suffix[:8]}",
            tier="pro",
            is_active=True,
        )
        db_session.add(organization)
        db_session.flush()
        db_session.add(
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
            username=f"p22e3_user_{suffix}",
            password_hash=hash_password(password),
            role=role,
            full_name="P22E3 Test User",
            is_active=True,
        )
        lote = Lote(
            organization_id=organization.id,
            identificador=f"P22E3-LOTE-{suffix}",
            productor_id=f"22-{suffix[:8]}",
            producto_forestal="Madera Aserrada (Pino)",
            hectareas=42.0,
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
        db_session.add_all([user, lote])
        db_session.commit()
        db_session.refresh(user)
        db_session.refresh(lote)
        return {
            "organization_id": organization.id,
            "username": user.username,
            "password": password,
            "lote_id": lote.id,
        }
    finally:
        db_session.close()


def _authenticated_context(account: dict[str, int | str]):
    token_response = asyncio.run(
        login_b2b(
            LoginRequest(
                username=str(account["username"]),
                password=str(account["password"]),
            ),
            Response(),
        )
    )
    return get_current_tenant_user(
        authorization=f"Bearer {token_response.access_token}"
    ), token_response.access_token


def _create_job(
    account: dict[str, int | str],
    *,
    job_status: str = "queued",
    attempt_count: int = 0,
    error_code: str | None = None,
    error_message: str | None = "internal GEE exception must never be public",
    started_at: datetime | None = None,
    finished_at: datetime | None = None,
    next_attempt_at: datetime | None = None,
) -> int:
    now = datetime.now(timezone.utc)
    db_session = get_db_session()
    try:
        job = SatelliteJob(
            organization_id=int(account["organization_id"]),
            lote_id=int(account["lote_id"]),
            job_type="ndvi_timeseries",
            status=job_status,
            attempt_count=attempt_count,
            max_attempts=3,
            next_attempt_at=next_attempt_at or now,
            started_at=started_at,
            finished_at=finished_at,
            error_code=error_code,
            error_message=error_message,
            idempotency_key=f"p22e3-{uuid4().hex}",
            request_start_date=date(2026, 7, 1),
            request_end_date=date(2026, 8, 1),
            max_cloud_pct=20.0,
            geometry_hash="a" * 64,
            algorithm_version="p22e3-test",
            polygon_wkt_snapshot="POLYGON((-58.91 -27.46, -58.89 -27.46, -58.89 -27.44, -58.91 -27.46))",
        )
        db_session.add(job)
        db_session.commit()
        db_session.refresh(job)
        return int(job.id)
    finally:
        db_session.close()


def _parse_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def test_status_openapi_contract_is_single_route_with_only_safe_response_fields():
    openapi_schema = app.openapi()
    operation = openapi_schema["paths"]["/api/v1/satellite/jobs/{job_id}"]["get"]
    response_schema = openapi_schema["components"]["schemas"][
        "SatelliteJobStatusResponse"
    ]

    assert set(operation["responses"]) >= {"200", "401", "403", "404", "422"}
    assert operation["responses"]["200"]["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/SatelliteJobStatusResponse"
    }
    parameter = next(
        item for item in operation["parameters"] if item["name"] == "job_id"
    )
    assert parameter["required"] is True
    assert parameter["schema"]["type"] == "integer"
    assert parameter["schema"]["exclusiveMinimum"] == 0
    assert set(response_schema["properties"]) == PUBLIC_STATUS_FIELDS
    serialized_schema = json.dumps(response_schema, sort_keys=True).lower()
    for forbidden_field in FORBIDDEN_PUBLIC_FIELDS:
        assert forbidden_field not in serialized_schema


def test_status_requires_auth_and_documents_positive_path_id():
    with pytest.raises(HTTPException) as exc_info:
        get_current_tenant_user(
            authorization=None,
            bearer_token=None,
            session_jwt=None,
        )
    assert exc_info.value.status_code == status.HTTP_401_UNAUTHORIZED

    openapi_schema = app.openapi()
    parameter = next(
        item
        for item in openapi_schema["paths"]["/api/v1/satellite/jobs/{job_id}"]["get"][
            "parameters"
        ]
        if item["name"] == "job_id"
    )
    assert parameter["schema"]["type"] == "integer"
    assert parameter["schema"]["exclusiveMinimum"] == 0


def test_status_requires_satellite_run_capability():
    account = _create_tenant_account(role="cliente")
    user, _ = _authenticated_context(account)

    with pytest.raises(HTTPException) as exc_info:
        ensure_permission(user, Permission.SATELLITE_RUN)
    assert exc_info.value.status_code == status.HTTP_403_FORBIDDEN

    manager_account = _create_tenant_account(role="manager")
    manager_user, _ = _authenticated_context(manager_account)
    ensure_permission(manager_user, Permission.SATELLITE_RUN)


@pytest.mark.parametrize(
    ("job_status", "attempt_count", "error_code", "expected_started", "expected_finished"),
    [
        ("queued", 0, None, False, False),
        ("running", 1, None, True, False),
        ("succeeded", 1, None, True, True),
        ("failed", 3, "gee_timeout", True, True),
    ],
)
def test_status_returns_safe_lifecycle_view_for_each_persisted_status(
    job_status: str,
    attempt_count: int,
    error_code: str | None,
    expected_started: bool,
    expected_finished: bool,
):
    account = _create_tenant_account(role="manager")
    user, _ = _authenticated_context(account)
    now = datetime.now(timezone.utc)
    job_id = _create_job(
        account,
        job_status=job_status,
        attempt_count=attempt_count,
        error_code=error_code,
        started_at=now if expected_started else None,
        finished_at=now if expected_finished else None,
    )

    response = asyncio.run(
        get_satellite_job_status_endpoint(job_id=job_id, user=user)
    )
    body = response.model_dump(mode="json")

    assert set(body) == PUBLIC_STATUS_FIELDS
    assert body["job_id"] == job_id
    assert body["lote_id"] == int(account["lote_id"])
    assert body["job_type"] == "ndvi_timeseries"
    assert body["status"] == job_status
    assert body["attempt_count"] == attempt_count
    assert body["max_attempts"] == 3
    assert (body["started_at"] is not None) is expected_started
    assert (body["finished_at"] is not None) is expected_finished
    assert body["error_code"] == error_code
    for field_name in ("created_at", "updated_at", "next_attempt_at"):
        assert _parse_datetime(body[field_name]).tzinfo is not None
    for forbidden_field in FORBIDDEN_PUBLIC_FIELDS:
        assert forbidden_field not in body


def test_status_represents_retry_as_queued_with_attempts_and_future_schedule():
    account = _create_tenant_account(role="manager")
    user, _ = _authenticated_context(account)
    historical_start = datetime.now(timezone.utc) - timedelta(minutes=5)
    future_retry = datetime.now(timezone.utc) + timedelta(minutes=10)
    job_id = _create_job(
        account,
        job_status="queued",
        attempt_count=1,
        started_at=historical_start,
        next_attempt_at=future_retry,
    )

    response = asyncio.run(
        get_satellite_job_status_endpoint(job_id=job_id, user=user)
    )
    body = response.model_dump(mode="json")

    assert body["status"] == "queued"
    assert body["attempt_count"] == 1
    assert body["started_at"] is not None
    assert body["finished_at"] is None
    assert _parse_datetime(body["next_attempt_at"]) > datetime.now(timezone.utc)
    assert body["error_code"] is None


def test_status_does_not_leak_nonfailed_error_code_or_internal_error_message():
    account = _create_tenant_account(role="manager")
    user, _ = _authenticated_context(account)
    job_id = _create_job(
        account,
        job_status="running",
        attempt_count=1,
        error_code="stale_internal_error",
        started_at=datetime.now(timezone.utc),
    )

    response = asyncio.run(
        get_satellite_job_status_endpoint(job_id=job_id, user=user)
    )
    body = response.model_dump(mode="json")

    assert body["error_code"] is None
    assert "error_message" not in body
    assert "internal GEE exception" not in json.dumps(body)


def test_status_cross_tenant_and_nonexistent_jobs_are_indistinguishable():
    account_a = _create_tenant_account(role="manager")
    account_b = _create_tenant_account(role="manager")
    user_b, _ = _authenticated_context(account_b)
    job_a_id = _create_job(account_a)

    with pytest.raises(HTTPException) as cross_tenant_exc:
        asyncio.run(get_satellite_job_status_endpoint(job_id=job_a_id, user=user_b))
    with pytest.raises(HTTPException) as missing_exc:
        asyncio.run(get_satellite_job_status_endpoint(job_id=999999, user=user_b))

    assert cross_tenant_exc.value.status_code == status.HTTP_404_NOT_FOUND
    assert missing_exc.value.status_code == status.HTTP_404_NOT_FOUND
    assert cross_tenant_exc.value.detail == missing_exc.value.detail == (
        "Satellite job no encontrado."
    )


def test_status_uses_authenticated_tenant_not_forged_query_context():
    account_a = _create_tenant_account(role="manager")
    account_b = _create_tenant_account(role="manager")
    user_a, _ = _authenticated_context(account_a)
    own_job_id = _create_job(account_a)
    foreign_job_id = _create_job(account_b)

    own_response = asyncio.run(
        get_satellite_job_status_endpoint(job_id=own_job_id, user=user_a)
    )
    with pytest.raises(HTTPException) as foreign_exc:
        asyncio.run(
            get_satellite_job_status_endpoint(job_id=foreign_job_id, user=user_a)
        )

    assert own_response.job_id == own_job_id
    assert foreign_exc.value.status_code == status.HTTP_404_NOT_FOUND


def test_status_read_creates_no_audit_event():
    account = _create_tenant_account(role="manager")
    user, _ = _authenticated_context(account)
    job_id = _create_job(account)
    db_session = get_db_session()
    try:
        before_count = len(db_session.execute(select(AuditLog)).scalars().all())
    finally:
        db_session.close()

    response = asyncio.run(
        get_satellite_job_status_endpoint(job_id=job_id, user=user)
    )

    db_session = get_db_session()
    try:
        after_count = len(db_session.execute(select(AuditLog)).scalars().all())
    finally:
        db_session.close()

    assert response.job_id == job_id
    assert after_count == before_count


def test_existing_submit_and_legacy_ndvi_endpoints_remain_available(monkeypatch):
    account = _create_tenant_account(role="manager")
    user, _ = _authenticated_context(account)

    submit_response = asyncio.run(
        submit_satellite_job_endpoint(
            SatelliteJobSubmitRequest(
                lote_id=int(account["lote_id"]),
                start_date="2026-07-01",
                end_date="2026-08-01",
            ),
            user=user,
        )
    )
    assert submit_response.status_code == status.HTTP_202_ACCEPTED

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
            "observations": [],
        },
    )
    legacy_response = asyncio.run(
        consultar_ndvi_satelital_lote_endpoint(
            SatelliteQueryByLoteRequest(lote_id=int(account["lote_id"])),
            user=user,
        )
    )
    assert legacy_response.status_code == status.HTTP_200_OK


def test_status_endpoint_returns_typed_public_response_directly():
    account = _create_tenant_account(role="manager")
    user, _ = _authenticated_context(account)
    job_id = _create_job(account)

    response = asyncio.run(
        get_satellite_job_status_endpoint(job_id=job_id, user=user)
    )

    assert isinstance(response, SatelliteJobStatusResponse)
    assert set(response.model_dump(mode="json")) == PUBLIC_STATUS_FIELDS
