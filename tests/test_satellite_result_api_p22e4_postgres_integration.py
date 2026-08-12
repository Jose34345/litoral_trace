from __future__ import annotations

import asyncio
import json
import os
from contextlib import contextmanager
from datetime import date, datetime, timezone
from uuid import uuid4

import pytest
from fastapi import Response, status
from sqlalchemy import create_engine, text

from main import app
from litoral_trace.api.auth import LoginRequest, login_b2b
from litoral_trace.auth.passwords import hash_password
from litoral_trace.config.settings import normalize_database_url
from litoral_trace.db.engine import reset_engine_state
from litoral_trace.services.satellite_job_results import (
    NDVI_TIMESERIES_RESULT_SCHEMA_VERSION,
    compute_satellite_job_result_payload_sha256,
)


def _truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}


ENABLED = _truthy(os.environ.get("ENABLE_POSTGRES_TESTS"))
RUNTIME_URL = os.environ.get("TEST_POSTGRES_DATABASE_URL")
OWNER_URL = os.environ.get("TEST_POSTGRES_MIGRATION_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not (ENABLED and RUNTIME_URL and OWNER_URL),
    reason="P2.2E4 requires isolated PostgreSQL runtime and owner URLs.",
)


def _owner_engine():
    return create_engine(
        normalize_database_url(OWNER_URL),
        pool_pre_ping=True,
        hide_parameters=True,
    )


@contextmanager
def _runtime_environment():
    keys = ("ENVIRONMENT", "DATABASE_URL", "MIGRATION_DATABASE_URL", "TEST_DATABASE_URL")
    original = {key: os.environ.get(key) for key in keys}
    os.environ["ENVIRONMENT"] = "development"
    os.environ["DATABASE_URL"] = RUNTIME_URL or ""
    os.environ["MIGRATION_DATABASE_URL"] = (
        "postgresql://blocked_migration_guard:blocked_guard@127.0.0.1:1/"
        "blocked_migration_guard"
    )
    os.environ.pop("TEST_DATABASE_URL", None)
    reset_engine_state()
    try:
        yield
    finally:
        reset_engine_state()
        for key, value in original.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        reset_engine_state()


@pytest.fixture
def runtime_environment():
    with _runtime_environment():
        yield


@pytest.fixture(scope="module")
def owner_engine():
    engine = _owner_engine()
    try:
        yield engine
    finally:
        engine.dispose()


def _create_account(owner_engine, *, label: str, role: str) -> dict[str, object]:
    suffix = uuid4().hex[:10]
    username = f"p22e4_{label.lower()}_{suffix}"
    password = f"P22E4-{label}-{suffix}-Password!"
    with owner_engine.begin() as conn:
        organization_id = conn.execute(text("""
            INSERT INTO organizations (name, slug, tax_id, tier, is_active)
            VALUES (:name, :slug, :tax_id, 'pro', true) RETURNING id
        """), {
            "name": f"P22E4 {label} {suffix}",
            "slug": f"p22e4-{label.lower()}-{suffix}",
            "tax_id": f"85-{suffix[:8]}",
        }).scalar_one()
        license_id = conn.execute(text("""
            INSERT INTO licenses (organization_id, plan_type, max_lotes, max_volume_tons, max_batch_rows, is_active)
            VALUES (:organization_id, 'pro', 100, 5000.0, 500, true) RETURNING id
        """), {"organization_id": organization_id}).scalar_one()
        user_id = conn.execute(text("""
            INSERT INTO users (organization_id, email, username, password_hash, role, full_name, is_active)
            VALUES (:organization_id, :email, :username, :password_hash, :role, :full_name, true)
            RETURNING id
        """), {
            "organization_id": organization_id,
            "email": f"{username}@example.com",
            "username": username,
            "password_hash": hash_password(password),
            "role": role,
            "full_name": f"P22E4 {label} User",
        }).scalar_one()
        lote_id = conn.execute(text("""
            INSERT INTO lotes (
                organization_id, identificador, productor_id, producto_forestal,
                hectareas, latitud, longitud, polygon_wkt, estatus,
                volumen_ingresado_ton, volumen_exportar_ton
            ) VALUES (
                :organization_id, :identificador, :productor_id,
                'Madera Aserrada (Pino)', 20.0, -27.45, -58.90,
                :polygon, 'Pendiente', 20.0, 5.0
            ) RETURNING id
        """), {
            "organization_id": organization_id,
            "identificador": f"P22E4-{label}-{suffix}",
            "productor_id": f"47-{suffix[:8]}",
            "polygon": "POLYGON((-58.91 -27.46, -58.89 -27.46, -58.89 -27.44, -58.91 -27.44, -58.91 -27.46))",
        }).scalar_one()
    return {
        "organization_id": int(organization_id), "license_id": int(license_id),
        "user_id": int(user_id), "lote_id": int(lote_id),
        "username": username, "password": password,
    }


@contextmanager
def _fixture(owner_engine):
    accounts = [
        _create_account(owner_engine, label="A", role="manager"),
        _create_account(owner_engine, label="B", role="manager"),
        _create_account(owner_engine, label="C", role="cliente"),
    ]
    fixture = {
        "a": accounts[0], "b": accounts[1], "c": accounts[2],
        "organization_ids": [account["organization_id"] for account in accounts],
        "license_ids": [account["license_id"] for account in accounts],
        "user_ids": [account["user_id"] for account in accounts],
        "lote_ids": [account["lote_id"] for account in accounts],
        "job_ids": [],
    }
    try:
        yield fixture
    finally:
        with owner_engine.begin() as conn:
            if fixture["job_ids"]:
                conn.execute(text("DELETE FROM satellite_job_results WHERE satellite_job_id = ANY(:ids)"), {"ids": fixture["job_ids"]})
                conn.execute(text("DELETE FROM satellite_ndvi_observations WHERE satellite_job_id = ANY(:ids)"), {"ids": fixture["job_ids"]})
            conn.execute(text("DELETE FROM audit_logs WHERE organization_id = ANY(:ids)"), {"ids": fixture["organization_ids"]})
            conn.execute(text("DELETE FROM user_sessions WHERE user_id = ANY(:ids)"), {"ids": fixture["user_ids"]})
            if fixture["job_ids"]:
                conn.execute(text("DELETE FROM satellite_jobs WHERE id = ANY(:ids)"), {"ids": fixture["job_ids"]})
            conn.execute(text("DELETE FROM lotes WHERE id = ANY(:ids)"), {"ids": fixture["lote_ids"]})
            conn.execute(text("DELETE FROM users WHERE id = ANY(:ids)"), {"ids": fixture["user_ids"]})
            conn.execute(text("DELETE FROM licenses WHERE id = ANY(:ids)"), {"ids": fixture["license_ids"]})
            conn.execute(text("DELETE FROM organizations WHERE id = ANY(:ids)"), {"ids": fixture["organization_ids"]})


def _payload(job_id: int, lote_id: int) -> dict[str, object]:
    return {
        "schema_version": NDVI_TIMESERIES_RESULT_SCHEMA_VERSION,
        "job_id": job_id,
        "lote_id": lote_id,
        "geometry_hash": "f" * 64,
        "algorithm_version": "p22e4-postgres-test",
        "total_observations": 1,
        "observations": [{
            "observation_date": "2026-08-01", "ndvi_mean": 0.62,
            "ndvi_min": 0.55, "ndvi_max": 0.69, "ndvi_std": 0.03,
            "scene_cloud_percentage": 5.0, "aoi_cloud_percentage": 1.0,
            "valid_pixel_count": 10, "valid_pixel_percentage": 98.0,
            "satellite": "Sentinel-2", "collection": "COPERNICUS/S2_SR_HARMONIZED",
            "processing_date": "2026-08-01T12:00:00+00:00",
        }],
    }


def _insert_job(owner_engine, fixture, account, *, job_status: str,
                error_code: str | None = None, error_message: str | None = None,
                with_result: bool = False) -> int:
    now = datetime.now(timezone.utc)
    with owner_engine.begin() as conn:
        job_id = int(conn.execute(text("""
            INSERT INTO satellite_jobs (
                organization_id, lote_id, job_type, status, attempt_count, max_attempts,
                next_attempt_at, started_at, finished_at, error_code, error_message,
                idempotency_key, request_start_date, request_end_date, max_cloud_pct,
                geometry_hash, algorithm_version, polygon_wkt_snapshot
            ) VALUES (
                :organization_id, :lote_id, 'ndvi_timeseries', :status, :attempt_count, 3,
                :next_attempt_at, :started_at, :finished_at, :error_code, :error_message,
                :idempotency_key, :start_date, :end_date, 20.0, :geometry_hash,
                'p22e4-postgres-test', :polygon
            ) RETURNING id
        """), {
            "organization_id": account["organization_id"], "lote_id": account["lote_id"],
            "status": job_status, "attempt_count": 0 if job_status == "queued" else 1,
            "next_attempt_at": now, "started_at": None if job_status == "queued" else now,
            "finished_at": now if job_status in {"succeeded", "failed"} else None,
            "error_code": error_code, "error_message": error_message,
            "idempotency_key": f"p22e4-{uuid4().hex}",
            "start_date": date(2026, 7, 1), "end_date": date(2026, 8, 1),
            "geometry_hash": "f" * 64,
            "polygon": "POLYGON((-58.91 -27.46, -58.89 -27.46, -58.89 -27.44, -58.91 -27.44, -58.91 -27.46))",
        }).scalar_one())
        if with_result:
            payload = _payload(job_id, int(account["lote_id"]))
            conn.execute(text("""
                INSERT INTO satellite_job_results (
                    satellite_job_id, organization_id, lote_id, result_schema_version,
                    geometry_hash, algorithm_version, result_payload, payload_sha256
                ) VALUES (
                    :job_id, :organization_id, :lote_id, :schema_version,
                    :geometry_hash, :algorithm_version, CAST(:payload AS jsonb), :sha256
                )
            """), {
                "job_id": job_id, "organization_id": account["organization_id"],
                "lote_id": account["lote_id"], "schema_version": NDVI_TIMESERIES_RESULT_SCHEMA_VERSION,
                "geometry_hash": "f" * 64, "algorithm_version": "p22e4-postgres-test",
                "payload": json.dumps(payload),
                "sha256": compute_satellite_job_result_payload_sha256(payload),
            })
    fixture["job_ids"].append(job_id)
    return job_id


def _authenticate(account) -> str:
    response = asyncio.run(login_b2b(LoginRequest(
        username=str(account["username"]), password=str(account["password"])
    ), Response()))
    return response.access_token


async def _asgi_get(path: str, token: str | None) -> dict[str, object]:
    headers = [(b"host", b"testserver")]
    if token:
        headers.append((b"authorization", f"Bearer {token}".encode()))
    scope = {
        "type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1",
        "method": "GET", "scheme": "http", "path": path, "raw_path": path.encode(),
        "query_string": b"", "headers": headers, "client": ("203.0.113.44", 50124),
        "server": ("testserver", 80), "root_path": "",
    }
    messages = []
    received = False
    async def receive():
        nonlocal received
        if received:
            return {"type": "http.disconnect"}
        received = True
        return {"type": "http.request", "body": b"", "more_body": False}
    async def send(message):
        messages.append(message)
    await app(scope, receive, send)
    start = next(message for message in messages if message["type"] == "http.response.start")
    body = b"".join(message.get("body", b"") for message in messages if message["type"] == "http.response.body")
    return {"status_code": int(start["status"]), "body": json.loads(body.decode())}


def _get(job_id: int, token: str | None):
    return asyncio.run(_asgi_get(f"/api/v1/satellite/jobs/{job_id}/result", token))


def _snapshot(owner_engine, job_id: int):
    with owner_engine.connect() as conn:
        job = dict(conn.execute(text("""
            SELECT status, attempt_count, next_attempt_at, started_at, finished_at,
                   locked_by, locked_at, heartbeat_at, lease_token, error_code,
                   error_message, updated_at FROM satellite_jobs WHERE id = :id
        """), {"id": job_id}).mappings().one())
        result = dict(conn.execute(text("""
            SELECT result_payload, payload_sha256, created_at
            FROM satellite_job_results WHERE satellite_job_id = :id
        """), {"id": job_id}).mappings().one())
    return {"job": job, "result": result}


def _audit_count(owner_engine, organization_id: int, job_id: int) -> int:
    with owner_engine.connect() as conn:
        return int(conn.execute(text("""
            SELECT COUNT(*) FROM audit_logs
            WHERE organization_id = :organization_id
              AND entity_type = 'satellite_job' AND entity_id = :job_id
        """), {"organization_id": organization_id, "job_id": job_id}).scalar_one())


def test_postgres_succeeded_result_is_read_through_runtime_rls(owner_engine, runtime_environment):
    with _fixture(owner_engine) as fixture:
        job_id = _insert_job(owner_engine, fixture, fixture["a"], job_status="succeeded", with_result=True)
        result = _get(job_id, _authenticate(fixture["a"]))
    assert result["status_code"] == status.HTTP_200_OK
    assert result["body"]["job_id"] == job_id
    assert result["body"]["total_observations"] == 1
    assert result["body"]["observations"][0]["ndvi_mean"] == 0.62
    for forbidden in ("organization_id", "payload_sha256", "lease_token", "error_message"):
        assert forbidden not in result["body"]


def test_postgres_result_is_tenant_invisible_and_auth_rbac_protected(owner_engine, runtime_environment):
    with _fixture(owner_engine) as fixture:
        job_id = _insert_job(owner_engine, fixture, fixture["a"], job_status="succeeded", with_result=True)
        tenant_b = _get(job_id, _authenticate(fixture["b"]))
        nonexistent = _get(999999999, _authenticate(fixture["b"]))
        unauthenticated = _get(job_id, None)
        denied = _get(job_id, _authenticate(fixture["c"]))
    assert tenant_b == nonexistent == {"status_code": 404, "body": {"detail": "Satellite job no encontrado."}}
    assert unauthenticated["status_code"] == 401
    assert denied["status_code"] == 403


def test_postgres_result_state_semantics_are_sanitized(owner_engine, runtime_environment):
    with _fixture(owner_engine) as fixture:
        token = _authenticate(fixture["a"])
        queued_id = _insert_job(owner_engine, fixture, fixture["a"], job_status="queued")
        running_id = _insert_job(owner_engine, fixture, fixture["a"], job_status="running")
        failed_id = _insert_job(owner_engine, fixture, fixture["a"], job_status="failed", error_code="invalid_job_payload", error_message="private DSN and traceback")
        missing_id = _insert_job(owner_engine, fixture, fixture["a"], job_status="succeeded")
        queued, running = _get(queued_id, token), _get(running_id, token)
        failed, missing = _get(failed_id, token), _get(missing_id, token)
    assert queued["status_code"] == running["status_code"] == 409
    assert queued["body"]["status"] == "queued"
    assert running["body"]["status"] == "running"
    assert failed == {"status_code": 409, "body": {
        "job_id": failed_id, "status": "failed", "error_code": "invalid_job_payload",
        "detail": "El satellite job finalizo sin un resultado disponible.",
    }}
    assert "private DSN" not in json.dumps(failed)
    assert missing == {"status_code": 500, "body": {
        "detail": "El resultado persistido del satellite job no esta disponible."
    }}


def test_postgres_result_polling_is_read_only_and_has_no_audit_noise(owner_engine, runtime_environment):
    with _fixture(owner_engine) as fixture:
        account = fixture["a"]
        job_id = _insert_job(owner_engine, fixture, account, job_status="succeeded", with_result=True)
        token = _authenticate(account)
        before = _snapshot(owner_engine, job_id)
        audits_before = _audit_count(owner_engine, int(account["organization_id"]), job_id)
        responses = [_get(job_id, token) for _ in range(3)]
        after = _snapshot(owner_engine, job_id)
        audits_after = _audit_count(owner_engine, int(account["organization_id"]), job_id)
    assert [response["status_code"] for response in responses] == [200, 200, 200]
    assert after == before
    assert audits_after == audits_before
