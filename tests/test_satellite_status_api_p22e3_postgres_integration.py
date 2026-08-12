from __future__ import annotations

import asyncio
import json
import os
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from uuid import uuid4

import pytest
from fastapi import Response, status
from sqlalchemy import create_engine, text

from main import app
from litoral_trace.api.auth import LoginRequest, login_b2b
from litoral_trace.auth.passwords import hash_password
from litoral_trace.config.settings import normalize_database_url
from litoral_trace.db.engine import reset_engine_state


def _truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}


POSTGRES_RLS_TESTS_ENABLED = _truthy(os.environ.get("ENABLE_POSTGRES_TESTS"))
RUNTIME_TEST_DATABASE_URL = os.environ.get("TEST_POSTGRES_DATABASE_URL")
MIGRATION_TEST_DATABASE_URL = os.environ.get("TEST_POSTGRES_MIGRATION_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not (
        POSTGRES_RLS_TESTS_ENABLED
        and RUNTIME_TEST_DATABASE_URL
        and MIGRATION_TEST_DATABASE_URL
    ),
    reason="PostgreSQL P2.2E-3 tests require isolated test runtime and owner URLs.",
)

PUBLIC_STATUS_FIELDS = {
    "job_id", "lote_id", "job_type", "status", "attempt_count", "max_attempts",
    "created_at", "updated_at", "started_at", "finished_at", "next_attempt_at",
    "error_code",
}
FORBIDDEN_PUBLIC_FIELDS = {
    "organization_id", "idempotency_key", "request_start_date", "request_end_date",
    "max_cloud_pct", "polygon_wkt_snapshot", "geometry_hash", "algorithm_version",
    "locked_by", "locked_at", "heartbeat_at", "lease_token", "error_message",
}


def _owner_engine():
    return create_engine(
        normalize_database_url(MIGRATION_TEST_DATABASE_URL),
        pool_pre_ping=True,
        hide_parameters=True,
    )


@contextmanager
def _runtime_environment():
    original = {
        key: os.environ.get(key)
        for key in ("ENVIRONMENT", "DATABASE_URL", "MIGRATION_DATABASE_URL", "TEST_DATABASE_URL")
    }
    os.environ["ENVIRONMENT"] = "development"
    os.environ["DATABASE_URL"] = RUNTIME_TEST_DATABASE_URL or ""
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
def runtime_service_environment():
    with _runtime_environment():
        yield


@pytest.fixture(scope="module")
def owner_engine():
    engine = _owner_engine()
    try:
        yield engine
    finally:
        engine.dispose()


def _create_account(owner_engine, *, role: str, label: str) -> dict[str, int | str]:
    suffix = uuid4().hex[:10]
    password = f"P22E3-{label}-{suffix}-Password!"
    username = f"p22e3_{label.lower()}_{suffix}"
    with owner_engine.begin() as conn:
        organization_id = conn.execute(text("""
            INSERT INTO organizations (name, slug, tax_id, tier, is_active)
            VALUES (:name, :slug, :tax_id, 'pro', true) RETURNING id
        """), {
            "name": f"P22E3 {label} {suffix}",
            "slug": f"p22e3-{label.lower()}-{suffix}",
            "tax_id": f"83-{suffix[:8]}",
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
            "full_name": f"P22E3 {label} User",
        }).scalar_one()
        lote_id = conn.execute(text("""
            INSERT INTO lotes (
                organization_id, identificador, productor_id, producto_forestal,
                hectareas, latitud, longitud, polygon_wkt, estatus,
                volumen_ingresado_ton, volumen_exportar_ton
            ) VALUES (
                :organization_id, :identificador, :productor_id,
                'Madera Aserrada (Pino)', 25.0, -27.45, -58.90,
                :polygon_wkt, 'Pendiente', 20.0, 5.0
            ) RETURNING id
        """), {
            "organization_id": organization_id,
            "identificador": f"P22E3-{label}-LOTE-{suffix}",
            "productor_id": f"45-{suffix[:8]}",
            "polygon_wkt": "POLYGON((-58.91 -27.46, -58.89 -27.46, -58.89 -27.44, -58.91 -27.44, -58.91 -27.46))",
        }).scalar_one()
    return {
        "organization_id": int(organization_id), "license_id": int(license_id),
        "user_id": int(user_id), "username": username, "password": password,
        "lote_id": int(lote_id),
    }


@contextmanager
def _fixture(owner_engine):
    accounts = [
        _create_account(owner_engine, role="manager", label="A"),
        _create_account(owner_engine, role="manager", label="B"),
        _create_account(owner_engine, role="cliente", label="C"),
    ]
    fixture = {
        "account_a": accounts[0], "account_b": accounts[1], "account_c": accounts[2],
        "organization_ids": [int(account["organization_id"]) for account in accounts],
        "license_ids": [int(account["license_id"]) for account in accounts],
        "user_ids": [int(account["user_id"]) for account in accounts],
        "lote_ids": [int(account["lote_id"]) for account in accounts],
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


def _insert_job(owner_engine, fixture, *, account, job_status="queued", attempt_count=0,
                started_at=None, finished_at=None, next_attempt_at=None,
                error_code=None, error_message=None) -> int:
    with owner_engine.begin() as conn:
        job_id = conn.execute(text("""
            INSERT INTO satellite_jobs (
                organization_id, lote_id, job_type, status, attempt_count, max_attempts,
                next_attempt_at, started_at, finished_at, error_code, error_message,
                idempotency_key, request_start_date, request_end_date, max_cloud_pct,
                geometry_hash, algorithm_version, polygon_wkt_snapshot
            ) VALUES (
                :organization_id, :lote_id, 'ndvi_timeseries', :status, :attempt_count, 3,
                :next_attempt_at, :started_at, :finished_at, :error_code, :error_message,
                :idempotency_key, :start_date, :end_date, 20.0, :geometry_hash,
                'p22e3-postgres-test', :polygon
            ) RETURNING id
        """), {
            "organization_id": int(account["organization_id"]), "lote_id": int(account["lote_id"]),
            "status": job_status, "attempt_count": attempt_count,
            "next_attempt_at": next_attempt_at or datetime.now(timezone.utc),
            "started_at": started_at, "finished_at": finished_at,
            "error_code": error_code, "error_message": error_message,
            "idempotency_key": f"p22e3-{uuid4().hex}", "start_date": date(2026, 7, 1),
            "end_date": date(2026, 8, 1), "geometry_hash": "e" * 64,
            "polygon": "POLYGON((-58.91 -27.46, -58.89 -27.46, -58.89 -27.44, -58.91 -27.44, -58.91 -27.46))",
        }).scalar_one()
    fixture["job_ids"].append(int(job_id))
    return int(job_id)


def _authenticate(account) -> str:
    response = asyncio.run(login_b2b(LoginRequest(
        username=str(account["username"]), password=str(account["password"])
    ), Response()))
    return response.access_token


async def _asgi_get(path: str, *, token: str | None = None) -> dict[str, object]:
    headers = [(b"host", b"testserver")]
    if token is not None:
        headers.append((b"authorization", f"Bearer {token}".encode("utf-8")))
    scope = {
        "type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1",
        "method": "GET", "scheme": "http", "path": path, "raw_path": path.encode(),
        "query_string": b"", "headers": headers, "client": ("203.0.113.43", 50124),
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


def _get_status(job_id: int, token: str | None) -> dict[str, object]:
    return asyncio.run(_asgi_get(f"/api/v1/satellite/jobs/{job_id}", token=token))


def _snapshot(owner_engine, job_id: int) -> dict[str, object]:
    with owner_engine.connect() as conn:
        row = conn.execute(text("""
            SELECT status, attempt_count, max_attempts, next_attempt_at, started_at,
                   finished_at, locked_by, locked_at, heartbeat_at, lease_token,
                   error_code, error_message, updated_at
            FROM satellite_jobs WHERE id = :job_id
        """), {"job_id": job_id}).mappings().one()
    return dict(row)


def _job_audit_count(owner_engine, organization_id: int, job_id: int) -> int:
    with owner_engine.connect() as conn:
        return int(conn.execute(text("""
            SELECT COUNT(*) FROM audit_logs
            WHERE organization_id = :organization_id
              AND entity_type = 'satellite_job' AND entity_id = :job_id
        """), {"organization_id": organization_id, "job_id": job_id}).scalar_one())


def _assert_public(body: dict[str, object], job_id: int) -> None:
    assert set(body) == PUBLIC_STATUS_FIELDS
    assert body["job_id"] == job_id
    for field_name in FORBIDDEN_PUBLIC_FIELDS:
        assert field_name not in body


def test_queued_status_is_public_safe_and_tenant_scoped(owner_engine, runtime_service_environment):
    with _fixture(owner_engine) as fixture:
        account = fixture["account_a"]
        job_id = _insert_job(owner_engine, fixture, account=account)
        result = _get_status(job_id, _authenticate(account))
    assert result["status_code"] == status.HTTP_200_OK
    body = result["body"]
    _assert_public(body, job_id)
    assert body["lote_id"] == account["lote_id"]
    assert body["job_type"] == "ndvi_timeseries"
    assert body["status"] == "queued"
    assert body["attempt_count"] == 0
    assert body["started_at"] is None
    assert body["finished_at"] is None
    assert body["error_code"] is None


def test_cross_tenant_and_nonexistent_statuses_are_indistinguishable(owner_engine, runtime_service_environment):
    with _fixture(owner_engine) as fixture:
        job_id = _insert_job(owner_engine, fixture, account=fixture["account_a"])
        token_b = _authenticate(fixture["account_b"])
        cross_tenant = _get_status(job_id, token_b)
        nonexistent = _get_status(999999999, token_b)
    assert cross_tenant == nonexistent == {
        "status_code": status.HTTP_404_NOT_FOUND,
        "body": {"detail": "Satellite job no encontrado."},
    }


def test_running_retry_succeeded_and_failed_statuses_are_safe(owner_engine, runtime_service_environment):
    with _fixture(owner_engine) as fixture:
        account = fixture["account_a"]
        token = _authenticate(account)
        now = datetime.now(timezone.utc)
        running_id = _insert_job(owner_engine, fixture, account=account, job_status="running", attempt_count=1, started_at=now)
        retry_id = _insert_job(owner_engine, fixture, account=account, attempt_count=1, started_at=now - timedelta(minutes=1), next_attempt_at=now + timedelta(minutes=5))
        succeeded_id = _insert_job(owner_engine, fixture, account=account, job_status="succeeded", attempt_count=1, started_at=now - timedelta(minutes=1), finished_at=now)
        failed_id = _insert_job(owner_engine, fixture, account=account, job_status="failed", attempt_count=1, started_at=now - timedelta(minutes=1), finished_at=now, error_code="invalid_job_payload", error_message="raw worker error must remain private")
        running, retry = _get_status(running_id, token), _get_status(retry_id, token)
        succeeded, failed = _get_status(succeeded_id, token), _get_status(failed_id, token)
    assert running["status_code"] == status.HTTP_200_OK
    assert running["body"]["status"] == "running"
    assert running["body"]["attempt_count"] == 1
    assert running["body"]["started_at"] is not None
    assert running["body"]["finished_at"] is None
    assert running["body"]["error_code"] is None
    assert retry["body"]["status"] == "queued"
    assert retry["body"]["attempt_count"] >= 1
    assert retry["body"]["finished_at"] is None
    assert datetime.fromisoformat(retry["body"]["next_attempt_at"]) > now
    assert retry["body"]["error_code"] is None
    assert succeeded["body"]["status"] == "succeeded"
    assert succeeded["body"]["started_at"] is not None
    assert succeeded["body"]["finished_at"] is not None
    assert succeeded["body"]["error_code"] is None
    assert failed["body"]["status"] == "failed"
    assert failed["body"]["finished_at"] is not None
    assert failed["body"]["error_code"] == "invalid_job_payload"
    assert "error_message" not in failed["body"]
    assert "raw worker error" not in json.dumps(failed["body"])
    for result, job_id in ((running, running_id), (retry, retry_id), (succeeded, succeeded_id), (failed, failed_id)):
        _assert_public(result["body"], job_id)


def test_polling_is_read_only_and_writes_no_audit_event(owner_engine, runtime_service_environment):
    with _fixture(owner_engine) as fixture:
        account = fixture["account_a"]
        job_id = _insert_job(owner_engine, fixture, account=account)
        token = _authenticate(account)
        before = _snapshot(owner_engine, job_id)
        audits_before = _job_audit_count(owner_engine, int(account["organization_id"]), job_id)
        results = [_get_status(job_id, token) for _ in range(3)]
        after = _snapshot(owner_engine, job_id)
        audits_after = _job_audit_count(owner_engine, int(account["organization_id"]), job_id)
    assert [item["status_code"] for item in results] == [200, 200, 200]
    assert after == before
    assert after["updated_at"] == before["updated_at"]
    assert audits_after == audits_before


def test_http_path_validation_auth_and_rbac_write_no_audit(owner_engine, runtime_service_environment):
    with _fixture(owner_engine) as fixture:
        account = fixture["account_a"]
        job_id = _insert_job(owner_engine, fixture, account=account)
        manager_token = _authenticate(account)
        cliente_token = _authenticate(fixture["account_c"])
        before = _job_audit_count(owner_engine, int(account["organization_id"]), job_id)
        missing_auth = _get_status(job_id, None)
        denied = _get_status(job_id, cliente_token)
        zero = asyncio.run(_asgi_get("/api/v1/satellite/jobs/0", token=manager_token))
        negative = asyncio.run(_asgi_get("/api/v1/satellite/jobs/-1", token=manager_token))
        after = _job_audit_count(owner_engine, int(account["organization_id"]), job_id)
    assert missing_auth["status_code"] == status.HTTP_401_UNAUTHORIZED
    assert denied["status_code"] == status.HTTP_403_FORBIDDEN
    assert zero["status_code"] == status.HTTP_422_UNPROCESSABLE_ENTITY
    assert negative["status_code"] == status.HTTP_422_UNPROCESSABLE_ENTITY
    assert after == before
