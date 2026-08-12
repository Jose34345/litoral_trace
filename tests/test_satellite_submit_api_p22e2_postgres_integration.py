from __future__ import annotations

import asyncio
import json
import os
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import date
from http.cookies import SimpleCookie
from threading import Barrier
from uuid import uuid4

import pytest
from fastapi import HTTPException, Response, status
from sqlalchemy import create_engine, text

from litoral_trace.api.auth import LoginRequest, get_current_tenant_user, login_b2b
from litoral_trace.api.satellite import (
    SatelliteJobSubmitRequest,
    SatelliteQueryByLoteRequest,
    consultar_ndvi_satelital_lote_endpoint,
    submit_satellite_job_endpoint,
)
from litoral_trace.auth.passwords import hash_password
from litoral_trace.config.settings import normalize_database_url
from litoral_trace.db.engine import reset_engine_state
from litoral_trace.services import satellite_jobs as satellite_jobs_module
from litoral_trace.services.audit import AuditAction, AuditOutcome


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
    reason=(
        "PostgreSQL P2.2E-2 tests require ENABLE_POSTGRES_TESTS=1 plus isolated "
        "TEST_POSTGRES_DATABASE_URL and TEST_POSTGRES_MIGRATION_DATABASE_URL."
    ),
)


def _owner_engine():
    return create_engine(
        normalize_database_url(MIGRATION_TEST_DATABASE_URL),
        pool_pre_ping=True,
        hide_parameters=True,
    )


@contextmanager
def _postgres_runtime_service_environment():
    original_values = {
        "ENVIRONMENT": os.environ.get("ENVIRONMENT"),
        "DATABASE_URL": os.environ.get("DATABASE_URL"),
        "MIGRATION_DATABASE_URL": os.environ.get("MIGRATION_DATABASE_URL"),
        "TEST_DATABASE_URL": os.environ.get("TEST_DATABASE_URL"),
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
        for variable_name, original_value in original_values.items():
            if original_value is None:
                os.environ.pop(variable_name, None)
            else:
                os.environ[variable_name] = original_value
        reset_engine_state()


@pytest.fixture
def runtime_service_environment():
    with _postgres_runtime_service_environment():
        yield


@pytest.fixture(scope="module")
def owner_engine():
    engine = _owner_engine()
    try:
        yield engine
    finally:
        engine.dispose()


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
    request_id: str,
    user_agent: str = "pytest-p22e2-postgres/1.0",
):
    from starlette.requests import Request

    scope = {
        "type": "http",
        "http_version": "1.1",
        "method": method,
        "scheme": "http",
        "path": path,
        "raw_path": path.encode("utf-8"),
        "query_string": b"",
        "headers": [
            (b"user-agent", user_agent.encode("utf-8")),
            (b"x-request-id", request_id.encode("utf-8")),
        ],
        "client": ("203.0.113.40", 50124),
        "server": ("testserver", 80),
        "root_path": "",
    }
    return Request(scope)


def _create_account_entities(owner_engine, *, prefix: str) -> dict[str, int | str]:
    suffix = uuid4().hex[:10]
    password = f"{prefix}-{suffix}-Password!"

    with owner_engine.begin() as conn:
        organization_id = conn.execute(
            text(
                """
                INSERT INTO organizations (name, slug, tax_id, tier, is_active)
                VALUES (:name, :slug, :tax_id, 'pro', true)
                RETURNING id
                """
            ),
            {
                "name": f"{prefix} Org {suffix}",
                "slug": f"{prefix.lower()}-{suffix}",
                "tax_id": f"82-{suffix[:8]}",
            },
        ).scalar_one()
        license_id = conn.execute(
            text(
                """
                INSERT INTO licenses (
                    organization_id, plan_type, max_lotes, max_volume_tons,
                    max_batch_rows, is_active
                )
                VALUES (:organization_id, 'pro', 100, 5000.0, 500, true)
                RETURNING id
                """
            ),
            {"organization_id": organization_id},
        ).scalar_one()
        user_id = conn.execute(
            text(
                """
                INSERT INTO users (
                    organization_id, email, username, password_hash, role,
                    full_name, is_active
                )
                VALUES (
                    :organization_id, :email, :username, :password_hash,
                    'manager', :full_name, true
                )
                RETURNING id
                """
            ),
            {
                "organization_id": organization_id,
                "email": f"{prefix.lower()}-{suffix}@example.com",
                "username": f"{prefix.lower()}_user_{suffix}",
                "password_hash": hash_password(password),
                "full_name": f"{prefix} User {suffix}",
            },
        ).scalar_one()
        lote_id = conn.execute(
            text(
                """
                INSERT INTO lotes (
                    organization_id, identificador, productor_id, producto_forestal,
                    hectareas, latitud, longitud, polygon_wkt, estatus,
                    volumen_ingresado_ton, volumen_exportar_ton
                )
                VALUES (
                    :organization_id, :identificador, :productor_id,
                    'Madera Aserrada (Pino)', 25.0, -27.45, -58.90,
                    :polygon_wkt, 'Pendiente', 20.0, 5.0
                )
                RETURNING id
                """
            ),
            {
                "organization_id": organization_id,
                "identificador": f"{prefix}-LOTE-{suffix}",
                "productor_id": f"44-{suffix[:8]}",
                "polygon_wkt": (
                    "POLYGON(("
                    "-58.91 -27.46, -58.89 -27.46, "
                    "-58.89 -27.44, -58.91 -27.44, "
                    "-58.91 -27.46"
                    "))"
                ),
            },
        ).scalar_one()

    return {
        "organization_id": int(organization_id),
        "license_id": int(license_id),
        "user_id": int(user_id),
        "username": f"{prefix.lower()}_user_{suffix}",
        "password": password,
        "lote_id": int(lote_id),
    }


@contextmanager
def _submit_fixture(owner_engine):
    account_a = _create_account_entities(owner_engine, prefix="P22E2A")
    account_b = _create_account_entities(owner_engine, prefix="P22E2B")
    org_ids = [int(account_a["organization_id"]), int(account_b["organization_id"])]
    user_ids = [int(account_a["user_id"]), int(account_b["user_id"])]
    lote_ids = [int(account_a["lote_id"]), int(account_b["lote_id"])]
    license_ids = [int(account_a["license_id"]), int(account_b["license_id"])]

    try:
        yield {
            "account_a": account_a,
            "account_b": account_b,
        }
    finally:
        with owner_engine.begin() as conn:
            conn.execute(
                text(
                    "DELETE FROM satellite_job_results "
                    "WHERE organization_id = ANY(:organization_ids)"
                ),
                {"organization_ids": org_ids},
            )
            conn.execute(
                text(
                    "DELETE FROM satellite_ndvi_observations "
                    "WHERE organization_id = ANY(:organization_ids)"
                ),
                {"organization_ids": org_ids},
            )
            conn.execute(
                text(
                    "DELETE FROM audit_logs "
                    "WHERE organization_id = ANY(:organization_ids)"
                ),
                {"organization_ids": org_ids},
            )
            conn.execute(
                text(
                    "DELETE FROM user_sessions "
                    "WHERE organization_id = ANY(:organization_ids)"
                ),
                {"organization_ids": org_ids},
            )
            conn.execute(
                text(
                    "DELETE FROM satellite_jobs "
                    "WHERE organization_id = ANY(:organization_ids)"
                ),
                {"organization_ids": org_ids},
            )
            conn.execute(
                text("DELETE FROM lotes WHERE id = ANY(:lote_ids)"),
                {"lote_ids": lote_ids},
            )
            conn.execute(
                text("DELETE FROM users WHERE id = ANY(:user_ids)"),
                {"user_ids": user_ids},
            )
            conn.execute(
                text("DELETE FROM licenses WHERE id = ANY(:license_ids)"),
                {"license_ids": license_ids},
            )
            conn.execute(
                text("DELETE FROM organizations WHERE id = ANY(:organization_ids)"),
                {"organization_ids": org_ids},
            )


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


def _invoke_submit(
    *,
    payload: SatelliteJobSubmitRequest,
    user,
    request_id: str,
):
    try:
        response = asyncio.run(
            submit_satellite_job_endpoint(
                payload,
                request=_build_request(
                    method="POST",
                    path="/api/v1/satellite/jobs",
                    request_id=request_id,
                ),
                user=user,
            )
        )
        return {
            "status_code": response.status_code,
            "body": json.loads(response.body.decode("utf-8")),
            "location": response.headers.get("Location"),
        }
    except HTTPException as exc:
        return {
            "status_code": exc.status_code,
            "detail": exc.detail,
        }


def _invoke_legacy_ndvi(
    *,
    lote_id: int,
    user,
    request_id: str,
):
    try:
        response = asyncio.run(
            consultar_ndvi_satelital_lote_endpoint(
                SatelliteQueryByLoteRequest(lote_id=lote_id),
                request=_build_request(
                    method="POST",
                    path="/api/v1/satellite/ndvi",
                    request_id=request_id,
                ),
                user=user,
            )
        )
        return {
            "status_code": response.status_code,
            "body": json.loads(response.body.decode("utf-8")),
        }
    except HTTPException as exc:
        return {
            "status_code": exc.status_code,
            "detail": exc.detail,
        }


def _count_jobs(owner_engine, *, organization_id: int, idempotency_key: str | None = None) -> int:
    query = (
        "SELECT COUNT(*) FROM satellite_jobs WHERE organization_id = :organization_id"
    )
    params: dict[str, object] = {"organization_id": organization_id}
    if idempotency_key is None:
        query += " AND idempotency_key IS NULL"
    else:
        query += " AND idempotency_key = :idempotency_key"
        params["idempotency_key"] = idempotency_key

    with owner_engine.connect() as conn:
        return int(conn.execute(text(query), params).scalar_one())


def _fetch_job_row(owner_engine, *, job_id: int):
    with owner_engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT id, organization_id, lote_id, job_type, status, attempt_count,
                       max_attempts, next_attempt_at, request_start_date,
                       request_end_date, max_cloud_pct, geometry_hash,
                       algorithm_version, polygon_wkt_snapshot, idempotency_key
                FROM satellite_jobs
                WHERE id = :job_id
                """
            ),
            {"job_id": job_id},
        ).mappings().one()
    return dict(row)


def _latest_submit_audit_for_request(owner_engine, *, request_id: str):
    with owner_engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT id, organization_id, entity_id, action, detail, after_data
                FROM audit_logs
                WHERE action = :action
                  AND after_data->>'request_id' = :request_id
                ORDER BY id DESC
                LIMIT 1
                """
            ),
            {
                "action": AuditAction.SATELLITE_JOB_SUBMIT.value,
                "request_id": request_id,
            },
        ).mappings().one_or_none()
    return dict(row) if row is not None else None


def _count_submit_audits_for_request(
    owner_engine,
    *,
    request_id: str,
    outcome: str | None = None,
) -> int:
    query = (
        "SELECT COUNT(*) FROM audit_logs "
        "WHERE action = :action "
        "AND after_data->>'request_id' = :request_id"
    )
    params: dict[str, object] = {
        "action": AuditAction.SATELLITE_JOB_SUBMIT.value,
        "request_id": request_id,
    }
    if outcome is not None:
        query += " AND after_data->>'outcome' = :outcome"
        params["outcome"] = outcome

    with owner_engine.connect() as conn:
        return int(conn.execute(text(query), params).scalar_one())


def _serialize_audit_row(row: dict[str, object] | None) -> str:
    return json.dumps(row or {}, sort_keys=True, default=str).lower()


@contextmanager
def _barrier_on_build_record(monkeypatch, *, parties: int):
    original = satellite_jobs_module._build_satellite_job_record
    barrier = Barrier(parties)

    def _wrapped(*args, **kwargs):
        barrier.wait(timeout=10)
        return original(*args, **kwargs)

    monkeypatch.setattr(
        satellite_jobs_module,
        "_build_satellite_job_record",
        _wrapped,
    )
    try:
        yield
    finally:
        monkeypatch.setattr(
            satellite_jobs_module,
            "_build_satellite_job_record",
            original,
        )


def _run_concurrent_submit(calls: list[dict[str, object]]):
    start_barrier = Barrier(len(calls))

    def _runner(call: dict[str, object]):
        start_barrier.wait(timeout=10)
        return _invoke_submit(
            payload=call["payload"],
            user=call["user"],
            request_id=call["request_id"],
        )

    with ThreadPoolExecutor(max_workers=len(calls)) as executor:
        return list(executor.map(_runner, calls))


def test_submit_endpoint_creates_real_postgres_job_with_minimal_response(
    owner_engine,
    runtime_service_environment,
):
    with _submit_fixture(owner_engine) as fixture:
        account = fixture["account_a"]
        user = _authenticated_context(
            username=str(account["username"]),
            password=str(account["password"]),
        )

        result = _invoke_submit(
            payload=SatelliteJobSubmitRequest(
                lote_id=int(account["lote_id"]),
                start_date="2026-07-01",
                end_date="2026-08-01",
                max_cloud_pct=12.5,
                idempotency_key=f"p22e2-create-{uuid4().hex}",
            ),
            user=user,
            request_id="p22e2-postgres-create",
        )

        assert result["status_code"] == status.HTTP_202_ACCEPTED
        assert set(result["body"]) == {
            "job_id",
            "job_type",
            "status",
            "created_at",
            "next_attempt_at",
        }
        assert result["location"] == (
            f"/api/v1/satellite/jobs/{result['body']['job_id']}"
        )

        response_blob = json.dumps(result["body"], sort_keys=True).lower()
        for forbidden_key in (
            "organization_id",
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
            assert forbidden_key not in response_blob

        job_row = _fetch_job_row(owner_engine, job_id=result["body"]["job_id"])
        assert job_row["organization_id"] == int(account["organization_id"])
        assert job_row["job_type"] == "ndvi_timeseries"
        assert job_row["status"] == "queued"
        assert job_row["attempt_count"] == 0
        assert job_row["max_attempts"] == 3
        assert job_row["request_start_date"] == date(2026, 7, 1)
        assert job_row["request_end_date"] == date(2026, 8, 1)
        assert float(job_row["max_cloud_pct"]) == 12.5
        assert job_row["geometry_hash"]
        assert job_row["algorithm_version"]
        assert job_row["polygon_wkt_snapshot"]
        assert job_row["next_attempt_at"] is not None


def test_submit_endpoint_replays_same_key_same_payload_in_postgres(
    owner_engine,
    runtime_service_environment,
):
    with _submit_fixture(owner_engine) as fixture:
        account = fixture["account_a"]
        user = _authenticated_context(
            username=str(account["username"]),
            password=str(account["password"]),
        )
        idempotency_key = f"p22e2-replay-{uuid4().hex}"

        first_result = _invoke_submit(
            payload=SatelliteJobSubmitRequest(
                lote_id=int(account["lote_id"]),
                start_date="2026-07-01",
                end_date="2026-08-01",
                idempotency_key=idempotency_key,
            ),
            user=user,
            request_id="p22e2-postgres-replay-create",
        )
        second_result = _invoke_submit(
            payload=SatelliteJobSubmitRequest(
                lote_id=int(account["lote_id"]),
                start_date="2026-07-01",
                end_date="2026-08-01",
                idempotency_key=idempotency_key,
            ),
            user=user,
            request_id="p22e2-postgres-replay-reuse",
        )

        assert first_result["status_code"] == status.HTTP_202_ACCEPTED
        assert second_result["status_code"] == status.HTTP_200_OK
        assert first_result["body"]["job_id"] == second_result["body"]["job_id"]
        assert first_result["location"] == second_result["location"]
        assert _count_jobs(
            owner_engine,
            organization_id=int(account["organization_id"]),
            idempotency_key=idempotency_key,
        ) == 1


def test_submit_endpoint_rejects_same_key_different_client_payload_in_postgres(
    owner_engine,
    runtime_service_environment,
):
    with _submit_fixture(owner_engine) as fixture:
        account = fixture["account_a"]
        user = _authenticated_context(
            username=str(account["username"]),
            password=str(account["password"]),
        )
        idempotency_key = f"p22e2-conflict-{uuid4().hex}"

        created = _invoke_submit(
            payload=SatelliteJobSubmitRequest(
                lote_id=int(account["lote_id"]),
                start_date="2026-07-01",
                end_date="2026-08-01",
                idempotency_key=idempotency_key,
            ),
            user=user,
            request_id="p22e2-postgres-conflict-create",
        )
        conflict = _invoke_submit(
            payload=SatelliteJobSubmitRequest(
                lote_id=int(account["lote_id"]),
                start_date="2026-07-02",
                end_date="2026-08-01",
                idempotency_key=idempotency_key,
            ),
            user=user,
            request_id="p22e2-postgres-conflict-drift",
        )

        assert created["status_code"] == status.HTTP_202_ACCEPTED
        assert conflict["status_code"] == status.HTTP_409_CONFLICT
        assert _count_jobs(
            owner_engine,
            organization_id=int(account["organization_id"]),
            idempotency_key=idempotency_key,
        ) == 1
        original_row = _fetch_job_row(owner_engine, job_id=created["body"]["job_id"])
        assert original_row["request_start_date"] == date(2026, 7, 1)


def test_submit_endpoint_rejects_same_key_when_server_derived_geometry_changes(
    owner_engine,
    runtime_service_environment,
):
    with _submit_fixture(owner_engine) as fixture:
        account = fixture["account_a"]
        user = _authenticated_context(
            username=str(account["username"]),
            password=str(account["password"]),
        )
        idempotency_key = f"p22e2-geometry-{uuid4().hex}"

        created = _invoke_submit(
            payload=SatelliteJobSubmitRequest(
                lote_id=int(account["lote_id"]),
                start_date="2026-07-01",
                end_date="2026-08-01",
                idempotency_key=idempotency_key,
            ),
            user=user,
            request_id="p22e2-postgres-geometry-create",
        )
        assert created["status_code"] == status.HTTP_202_ACCEPTED

        with owner_engine.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE lotes
                    SET polygon_wkt = :polygon_wkt
                    WHERE id = :lote_id
                    """
                ),
                {
                    "lote_id": int(account["lote_id"]),
                    "polygon_wkt": (
                        "POLYGON(("
                        "-58.95 -27.50, -58.85 -27.50, "
                        "-58.85 -27.40, -58.95 -27.40, "
                        "-58.95 -27.50"
                        "))"
                    ),
                },
            )

        conflicted = _invoke_submit(
            payload=SatelliteJobSubmitRequest(
                lote_id=int(account["lote_id"]),
                start_date="2026-07-01",
                end_date="2026-08-01",
                idempotency_key=idempotency_key,
            ),
            user=user,
            request_id="p22e2-postgres-geometry-conflict",
        )

        assert conflicted["status_code"] == status.HTTP_409_CONFLICT
        assert _count_jobs(
            owner_engine,
            organization_id=int(account["organization_id"]),
            idempotency_key=idempotency_key,
        ) == 1


def test_submit_endpoint_allows_same_idempotency_key_across_tenants(
    owner_engine,
    runtime_service_environment,
):
    with _submit_fixture(owner_engine) as fixture:
        account_a = fixture["account_a"]
        account_b = fixture["account_b"]
        user_a = _authenticated_context(
            username=str(account_a["username"]),
            password=str(account_a["password"]),
        )
        user_b = _authenticated_context(
            username=str(account_b["username"]),
            password=str(account_b["password"]),
        )
        idempotency_key = f"p22e2-tenant-local-{uuid4().hex}"

        result_a = _invoke_submit(
            payload=SatelliteJobSubmitRequest(
                lote_id=int(account_a["lote_id"]),
                start_date="2026-07-01",
                end_date="2026-08-01",
                idempotency_key=idempotency_key,
            ),
            user=user_a,
            request_id="p22e2-postgres-tenant-a",
        )
        result_b = _invoke_submit(
            payload=SatelliteJobSubmitRequest(
                lote_id=int(account_b["lote_id"]),
                start_date="2026-07-01",
                end_date="2026-08-01",
                idempotency_key=idempotency_key,
            ),
            user=user_b,
            request_id="p22e2-postgres-tenant-b",
        )

        assert result_a["status_code"] == status.HTTP_202_ACCEPTED
        assert result_b["status_code"] == status.HTTP_202_ACCEPTED
        assert result_a["body"]["job_id"] != result_b["body"]["job_id"]
        assert _count_jobs(
            owner_engine,
            organization_id=int(account_a["organization_id"]),
            idempotency_key=idempotency_key,
        ) == 1
        assert _count_jobs(
            owner_engine,
            organization_id=int(account_b["organization_id"]),
            idempotency_key=idempotency_key,
        ) == 1


def test_submit_endpoint_concurrent_same_key_same_payload_replays_cleanly(
    owner_engine,
    runtime_service_environment,
    monkeypatch,
):
    with _submit_fixture(owner_engine) as fixture:
        account = fixture["account_a"]
        user = _authenticated_context(
            username=str(account["username"]),
            password=str(account["password"]),
        )
        idempotency_key = f"p22e2-race-replay-{uuid4().hex}"

        with _barrier_on_build_record(monkeypatch, parties=2):
            results = _run_concurrent_submit(
                [
                    {
                        "payload": SatelliteJobSubmitRequest(
                            lote_id=int(account["lote_id"]),
                            start_date="2026-07-01",
                            end_date="2026-08-01",
                            idempotency_key=idempotency_key,
                        ),
                        "user": user,
                        "request_id": "p22e2-race-replay-a",
                    },
                    {
                        "payload": SatelliteJobSubmitRequest(
                            lote_id=int(account["lote_id"]),
                            start_date="2026-07-01",
                            end_date="2026-08-01",
                            idempotency_key=idempotency_key,
                        ),
                        "user": user,
                        "request_id": "p22e2-race-replay-b",
                    },
                ]
            )

        assert {result["status_code"] for result in results} == {
            status.HTTP_200_OK,
            status.HTTP_202_ACCEPTED,
        }
        job_ids = {result["body"]["job_id"] for result in results}
        locations = {result["location"] for result in results}
        assert len(job_ids) == 1
        assert locations == {f"/api/v1/satellite/jobs/{next(iter(job_ids))}"}
        assert _count_jobs(
            owner_engine,
            organization_id=int(account["organization_id"]),
            idempotency_key=idempotency_key,
        ) == 1


def test_submit_endpoint_concurrent_same_key_different_payload_returns_conflict(
    owner_engine,
    runtime_service_environment,
    monkeypatch,
):
    with _submit_fixture(owner_engine) as fixture:
        account = fixture["account_a"]
        user = _authenticated_context(
            username=str(account["username"]),
            password=str(account["password"]),
        )
        idempotency_key = f"p22e2-race-conflict-{uuid4().hex}"
        calls = [
            {
                "payload": SatelliteJobSubmitRequest(
                    lote_id=int(account["lote_id"]),
                    start_date="2026-07-01",
                    end_date="2026-08-01",
                    idempotency_key=idempotency_key,
                ),
                "user": user,
                "request_id": "p22e2-race-conflict-a",
                "expected_start_date": date(2026, 7, 1),
            },
            {
                "payload": SatelliteJobSubmitRequest(
                    lote_id=int(account["lote_id"]),
                    start_date="2026-07-05",
                    end_date="2026-08-01",
                    idempotency_key=idempotency_key,
                ),
                "user": user,
                "request_id": "p22e2-race-conflict-b",
                "expected_start_date": date(2026, 7, 5),
            },
        ]

        with _barrier_on_build_record(monkeypatch, parties=2):
            results = _run_concurrent_submit(calls)

        assert {result["status_code"] for result in results} == {
            status.HTTP_202_ACCEPTED,
            status.HTTP_409_CONFLICT,
        }

        winning_index = next(
            index
            for index, result in enumerate(results)
            if result["status_code"] == status.HTTP_202_ACCEPTED
        )
        winning_job_id = results[winning_index]["body"]["job_id"]
        persisted_row = _fetch_job_row(owner_engine, job_id=winning_job_id)
        assert persisted_row["request_start_date"] == calls[winning_index]["expected_start_date"]
        assert _count_jobs(
            owner_engine,
            organization_id=int(account["organization_id"]),
            idempotency_key=idempotency_key,
        ) == 1


def test_submit_endpoint_concurrent_without_key_creates_two_jobs(
    owner_engine,
    runtime_service_environment,
    monkeypatch,
):
    with _submit_fixture(owner_engine) as fixture:
        account = fixture["account_a"]
        user = _authenticated_context(
            username=str(account["username"]),
            password=str(account["password"]),
        )

        with _barrier_on_build_record(monkeypatch, parties=2):
            results = _run_concurrent_submit(
                [
                    {
                        "payload": SatelliteJobSubmitRequest(
                            lote_id=int(account["lote_id"]),
                            start_date="2026-07-01",
                            end_date="2026-08-01",
                        ),
                        "user": user,
                        "request_id": "p22e2-race-no-key-a",
                    },
                    {
                        "payload": SatelliteJobSubmitRequest(
                            lote_id=int(account["lote_id"]),
                            start_date="2026-07-01",
                            end_date="2026-08-01",
                        ),
                        "user": user,
                        "request_id": "p22e2-race-no-key-b",
                    },
                ]
            )

        assert [result["status_code"] for result in results] == [
            status.HTTP_202_ACCEPTED,
            status.HTTP_202_ACCEPTED,
        ]
        assert results[0]["body"]["job_id"] != results[1]["body"]["job_id"]
        assert _count_jobs(
            owner_engine,
            organization_id=int(account["organization_id"]),
            idempotency_key=None,
        ) == 2


def test_submit_endpoint_hides_cross_tenant_and_missing_lote_under_postgres(
    owner_engine,
    runtime_service_environment,
):
    with _submit_fixture(owner_engine) as fixture:
        account_a = fixture["account_a"]
        account_b = fixture["account_b"]
        attacker = _authenticated_context(
            username=str(account_b["username"]),
            password=str(account_b["password"]),
        )

        cross_tenant = _invoke_submit(
            payload=SatelliteJobSubmitRequest(
                lote_id=int(account_a["lote_id"]),
                start_date="2026-07-01",
                end_date="2026-08-01",
            ),
            user=attacker,
            request_id="p22e2-cross-tenant",
        )
        missing = _invoke_submit(
            payload=SatelliteJobSubmitRequest(
                lote_id=999999999,
                start_date="2026-07-01",
                end_date="2026-08-01",
            ),
            user=attacker,
            request_id="p22e2-missing-lote",
        )

        assert cross_tenant == {
            "status_code": status.HTTP_404_NOT_FOUND,
            "detail": "Lote no encontrado.",
        }
        assert missing == {
            "status_code": status.HTTP_404_NOT_FOUND,
            "detail": "Lote no encontrado.",
        }
        assert _count_jobs(
            owner_engine,
            organization_id=int(account_b["organization_id"]),
            idempotency_key=None,
        ) == 0


def test_submit_create_and_success_audit_commit_atomically(
    owner_engine,
    runtime_service_environment,
    monkeypatch,
):
    with _submit_fixture(owner_engine) as fixture:
        account = fixture["account_a"]
        user = _authenticated_context(
            username=str(account["username"]),
            password=str(account["password"]),
        )
        idempotency_key = f"p22e2-audit-atomic-{uuid4().hex}"
        request_id = "p22e2-audit-atomic-create"

        def _raise_audit_failure(*args, **kwargs):
            raise RuntimeError("forced success audit failure")

        monkeypatch.setattr(
            "litoral_trace.api.satellite.record_audit_event",
            _raise_audit_failure,
        )

        failed = _invoke_submit(
            payload=SatelliteJobSubmitRequest(
                lote_id=int(account["lote_id"]),
                start_date="2026-07-01",
                end_date="2026-08-01",
                idempotency_key=idempotency_key,
            ),
            user=user,
            request_id=request_id,
        )

        assert failed == {
            "status_code": status.HTTP_500_INTERNAL_SERVER_ERROR,
            "detail": "No fue posible registrar el satellite job.",
        }
        assert _count_jobs(
            owner_engine,
            organization_id=int(account["organization_id"]),
            idempotency_key=idempotency_key,
        ) == 0
        assert _count_submit_audits_for_request(
            owner_engine,
            request_id=request_id,
            outcome=AuditOutcome.SUCCESS.value,
        ) == 0


def test_submit_replay_audit_failure_does_not_delete_preexisting_job(
    owner_engine,
    runtime_service_environment,
    monkeypatch,
):
    with _submit_fixture(owner_engine) as fixture:
        account = fixture["account_a"]
        user = _authenticated_context(
            username=str(account["username"]),
            password=str(account["password"]),
        )
        idempotency_key = f"p22e2-audit-replay-{uuid4().hex}"

        created = _invoke_submit(
            payload=SatelliteJobSubmitRequest(
                lote_id=int(account["lote_id"]),
                start_date="2026-07-01",
                end_date="2026-08-01",
                idempotency_key=idempotency_key,
            ),
            user=user,
            request_id="p22e2-audit-replay-create",
        )
        assert created["status_code"] == status.HTTP_202_ACCEPTED
        original_job_id = created["body"]["job_id"]

        def _raise_audit_failure(*args, **kwargs):
            raise RuntimeError("forced replay audit failure")

        monkeypatch.setattr(
            "litoral_trace.api.satellite.record_audit_event",
            _raise_audit_failure,
        )

        replay = _invoke_submit(
            payload=SatelliteJobSubmitRequest(
                lote_id=int(account["lote_id"]),
                start_date="2026-07-01",
                end_date="2026-08-01",
                idempotency_key=idempotency_key,
            ),
            user=user,
            request_id="p22e2-audit-replay-failure",
        )

        assert replay == {
            "status_code": status.HTTP_500_INTERNAL_SERVER_ERROR,
            "detail": "No fue posible registrar el satellite job.",
        }
        assert _count_jobs(
            owner_engine,
            organization_id=int(account["organization_id"]),
            idempotency_key=idempotency_key,
        ) == 1
        persisted_row = _fetch_job_row(owner_engine, job_id=original_job_id)
        assert persisted_row["id"] == original_job_id
        assert _count_submit_audits_for_request(
            owner_engine,
            request_id="p22e2-audit-replay-failure",
            outcome=AuditOutcome.SUCCESS.value,
        ) == 0


def test_submit_replay_success_audit_metadata_is_safe_and_replayed(
    owner_engine,
    runtime_service_environment,
):
    with _submit_fixture(owner_engine) as fixture:
        account = fixture["account_a"]
        user = _authenticated_context(
            username=str(account["username"]),
            password=str(account["password"]),
        )
        idempotency_key = f"p22e2-audit-success-{uuid4().hex}"

        created = _invoke_submit(
            payload=SatelliteJobSubmitRequest(
                lote_id=int(account["lote_id"]),
                start_date="2026-07-01",
                end_date="2026-08-01",
                max_cloud_pct=22.0,
                idempotency_key=idempotency_key,
            ),
            user=user,
            request_id="p22e2-audit-success-create",
        )
        replay = _invoke_submit(
            payload=SatelliteJobSubmitRequest(
                lote_id=int(account["lote_id"]),
                start_date="2026-07-01",
                end_date="2026-08-01",
                max_cloud_pct=22.0,
                idempotency_key=idempotency_key,
            ),
            user=user,
            request_id="p22e2-audit-success-replay",
        )

        assert created["status_code"] == status.HTTP_202_ACCEPTED
        assert replay["status_code"] == status.HTTP_200_OK

        audit_row = _latest_submit_audit_for_request(
            owner_engine,
            request_id="p22e2-audit-success-replay",
        )
        assert audit_row is not None
        assert audit_row["entity_id"] == replay["body"]["job_id"]
        assert audit_row["after_data"]["metadata"] == {
            "created": False,
            "replayed": True,
            "job_type": "ndvi_timeseries",
            "lote_id": int(account["lote_id"]),
            "start_date": "2026-07-01",
            "end_date": "2026-08-01",
            "max_cloud_pct": 22.0,
        }

        serialized = _serialize_audit_row(audit_row)
        for forbidden_value in (
            idempotency_key.lower(),
            "polygon_wkt_snapshot",
            "geometry_hash",
            "algorithm_version",
            "lease_token",
            "locked_by",
            "heartbeat_at",
            "credential",
        ):
            assert forbidden_value not in serialized


def test_submit_conflict_audit_is_sanitized(
    owner_engine,
    runtime_service_environment,
):
    with _submit_fixture(owner_engine) as fixture:
        account = fixture["account_a"]
        user = _authenticated_context(
            username=str(account["username"]),
            password=str(account["password"]),
        )
        idempotency_key = f"p22e2-audit-conflict-{uuid4().hex}"

        created = _invoke_submit(
            payload=SatelliteJobSubmitRequest(
                lote_id=int(account["lote_id"]),
                start_date="2026-07-01",
                end_date="2026-08-01",
                idempotency_key=idempotency_key,
            ),
            user=user,
            request_id="p22e2-audit-conflict-create",
        )
        conflicted = _invoke_submit(
            payload=SatelliteJobSubmitRequest(
                lote_id=int(account["lote_id"]),
                start_date="2026-07-05",
                end_date="2026-08-01",
                idempotency_key=idempotency_key,
            ),
            user=user,
            request_id="p22e2-audit-conflict-reuse",
        )

        assert created["status_code"] == status.HTTP_202_ACCEPTED
        assert conflicted["status_code"] == status.HTTP_409_CONFLICT

        audit_row = _latest_submit_audit_for_request(
            owner_engine,
            request_id="p22e2-audit-conflict-reuse",
        )
        assert audit_row is not None
        assert audit_row["after_data"]["outcome"] == AuditOutcome.FAILURE.value
        assert audit_row["detail"] == "Conflicto de idempotencia satelital."

        serialized = _serialize_audit_row(audit_row)
        for forbidden_value in (
            idempotency_key.lower(),
            "polygon_wkt_snapshot",
            "geometry_hash",
            "algorithm_version",
            "lease_token",
            "locked_by",
            "heartbeat_at",
            "duplicate key",
            "constraint",
        ):
            assert forbidden_value not in serialized


def test_unrelated_integrity_error_fails_closed_in_submit_endpoint(
    owner_engine,
    runtime_service_environment,
    monkeypatch,
):
    with _submit_fixture(owner_engine) as fixture:
        account = fixture["account_a"]
        user = _authenticated_context(
            username=str(account["username"]),
            password=str(account["password"]),
        )
        idempotency_key = f"p22e2-integrity-{uuid4().hex}"
        original = satellite_jobs_module._build_satellite_job_record

        def _build_invalid_record(*args, **kwargs):
            job = original(*args, **kwargs)
            job.status = "invalid-status"
            return job

        monkeypatch.setattr(
            satellite_jobs_module,
            "_build_satellite_job_record",
            _build_invalid_record,
        )

        failed = _invoke_submit(
            payload=SatelliteJobSubmitRequest(
                lote_id=int(account["lote_id"]),
                start_date="2026-07-01",
                end_date="2026-08-01",
                idempotency_key=idempotency_key,
            ),
            user=user,
            request_id="p22e2-unrelated-integrity",
        )

        assert failed == {
            "status_code": status.HTTP_500_INTERNAL_SERVER_ERROR,
            "detail": "No fue posible registrar el satellite job.",
        }
        assert _count_jobs(
            owner_engine,
            organization_id=int(account["organization_id"]),
            idempotency_key=idempotency_key,
        ) == 0


def test_legacy_sync_ndvi_endpoint_remains_unchanged_under_postgres(
    owner_engine,
    runtime_service_environment,
    monkeypatch,
):
    with _submit_fixture(owner_engine) as fixture:
        account = fixture["account_a"]
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

        result = _invoke_legacy_ndvi(
            lote_id=int(account["lote_id"]),
            user=user,
            request_id="p22e2-legacy-sync",
        )

        assert result["status_code"] == status.HTTP_200_OK
        assert result["body"]["lote_id"] == int(account["lote_id"])
        assert result["body"]["organization_id"] == int(account["organization_id"])
        assert result["body"]["status"] == "success"
