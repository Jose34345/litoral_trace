from __future__ import annotations

import asyncio
import json
import os
import secrets
from contextlib import contextmanager
from datetime import date, datetime, timezone
from uuid import uuid4

import pytest
from fastapi import Response
from psycopg import ClientCursor, sql
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import sessionmaker

from main import app
from litoral_trace.api.auth import LoginRequest, login_b2b
from litoral_trace.auth.passwords import hash_password
from litoral_trace.config.settings import normalize_database_url
from litoral_trace.db.engine import reset_engine_state
from litoral_trace.services.satellite_job_results import (
    NDVI_TIMESERIES_RESULT_SCHEMA_VERSION,
    compute_satellite_job_result_payload_sha256,
)
from litoral_trace.services.satellite_ndvi_processing import (
    NdviObservationRecord,
    NormalizedNdviExecutionResult,
)
from litoral_trace.workers.satellite_worker import (
    RetryDisposition,
    SatelliteWorker,
    SatelliteWorkerExecutionError,
    WorkerRunStatus,
)


def _truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}


ENABLED = _truthy(os.environ.get("ENABLE_POSTGRES_TESTS"))
RUNTIME_URL = os.environ.get("TEST_POSTGRES_DATABASE_URL")
OWNER_URL = os.environ.get("TEST_POSTGRES_MIGRATION_DATABASE_URL")
WORKER_CAPABILITY_ROLE = "litoral_trace_worker_executor"

pytestmark = pytest.mark.skipif(
    not (ENABLED and RUNTIME_URL and OWNER_URL),
    reason="P2.2E6 requires isolated PostgreSQL runtime and owner URLs.",
)


def _engine(url: str, *, pool_size: int = 2):
    return create_engine(
        normalize_database_url(url),
        pool_size=pool_size,
        max_overflow=0,
        pool_pre_ping=True,
        hide_parameters=True,
    )


def _run_owner_cursor_statement(connection, statement, params=()) -> None:
    driver_connection = connection.connection.driver_connection
    with ClientCursor(driver_connection) as cursor:
        cursor.execute(statement, params)


@contextmanager
def _ephemeral_worker_login(owner_engine):
    role_name = f"litoral_trace_worker_e6_{uuid4().hex[:16]}"
    password = secrets.token_urlsafe(24)
    base_url = make_url(normalize_database_url(OWNER_URL))
    worker_url = base_url.set(username=role_name, password=password)
    worker_engine = None
    membership_granted = False

    try:
        with owner_engine.connect() as conn:
            transaction = conn.begin()
            try:
                _run_owner_cursor_statement(
                    conn,
                    sql.SQL(
                        """
                        CREATE ROLE {} LOGIN INHERIT NOSUPERUSER NOCREATEDB
                        NOCREATEROLE NOBYPASSRLS PASSWORD %s
                        """
                    ).format(sql.Identifier(role_name)),
                    (password,),
                )
                _run_owner_cursor_statement(
                    conn,
                    sql.SQL("GRANT {} TO {}").format(
                        sql.Identifier(WORKER_CAPABILITY_ROLE),
                        sql.Identifier(role_name),
                    ),
                )
                membership_granted = True
                transaction.commit()
            except Exception:
                transaction.rollback()
                raise

        worker_engine = _engine(
            worker_url.render_as_string(hide_password=False),
            pool_size=3,
        )
        yield worker_engine
    finally:
        if worker_engine is not None:
            worker_engine.dispose()
        with owner_engine.connect() as conn:
            transaction = conn.begin()
            try:
                if membership_granted:
                    _run_owner_cursor_statement(
                        conn,
                        sql.SQL("REVOKE {} FROM {}").format(
                            sql.Identifier(WORKER_CAPABILITY_ROLE),
                            sql.Identifier(role_name),
                        ),
                    )
                _run_owner_cursor_statement(
                    conn,
                    sql.SQL("DROP ROLE IF EXISTS {}").format(
                        sql.Identifier(role_name)
                    ),
                )
                transaction.commit()
            except Exception:
                transaction.rollback()
                raise


@contextmanager
def _runtime_environment():
    keys = (
        "ENVIRONMENT",
        "DATABASE_URL",
        "MIGRATION_DATABASE_URL",
        "TEST_DATABASE_URL",
    )
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


def _create_account(owner_engine, *, label: str) -> dict[str, object]:
    suffix = uuid4().hex[:10]
    username = f"p22e6_{label.lower()}_{suffix}"
    password = f"P22E6-{label}-{suffix}-Password!"
    with owner_engine.begin() as conn:
        organization_id = int(
            conn.execute(
                text(
                    """
                    INSERT INTO organizations (name, slug, tax_id, tier, is_active)
                    VALUES (:name, :slug, :tax_id, 'pro', true) RETURNING id
                    """
                ),
                {
                    "name": f"P22E6 {label} {suffix}",
                    "slug": f"p22e6-{label.lower()}-{suffix}",
                    "tax_id": f"89-{suffix[:8]}",
                },
            ).scalar_one()
        )
        license_id = int(
            conn.execute(
                text(
                    """
                    INSERT INTO licenses (
                        organization_id, plan_type, max_lotes, max_volume_tons,
                        max_batch_rows, is_active
                    ) VALUES (:organization_id, 'pro', 100, 5000.0, 500, true)
                    RETURNING id
                    """
                ),
                {"organization_id": organization_id},
            ).scalar_one()
        )
        user_id = int(
            conn.execute(
                text(
                    """
                    INSERT INTO users (
                        organization_id, email, username, password_hash, role,
                        full_name, is_active
                    ) VALUES (
                        :organization_id, :email, :username, :password_hash,
                        'manager', :full_name, true
                    ) RETURNING id
                    """
                ),
                {
                    "organization_id": organization_id,
                    "email": f"{username}@example.com",
                    "username": username,
                    "password_hash": hash_password(password),
                    "full_name": f"P22E6 {label} User",
                },
            ).scalar_one()
        )
        lote_id = int(
            conn.execute(
                text(
                    """
                    INSERT INTO lotes (
                        organization_id, identificador, productor_id,
                        producto_forestal, hectareas, latitud, longitud,
                        polygon_wkt, estatus, volumen_ingresado_ton,
                        volumen_exportar_ton
                    ) VALUES (
                        :organization_id, :identificador, :productor_id,
                        'Madera Aserrada (Pino)', 20.0, -27.45, -58.90,
                        :polygon, 'Pendiente', 20.0, 5.0
                    ) RETURNING id
                    """
                ),
                {
                    "organization_id": organization_id,
                    "identificador": f"P22E6-{label}-{suffix}",
                    "productor_id": f"51-{suffix[:8]}",
                    "polygon": (
                        "POLYGON((-58.91 -27.46, -58.89 -27.46, "
                        "-58.89 -27.44, -58.91 -27.44, -58.91 -27.46))"
                    ),
                },
            ).scalar_one()
        )
    return {
        "organization_id": organization_id,
        "license_id": license_id,
        "user_id": user_id,
        "lote_id": lote_id,
        "username": username,
        "password": password,
    }


@contextmanager
def _fixture():
    owner_engine = _engine(OWNER_URL)
    runtime_engine = _engine(RUNTIME_URL, pool_size=5)
    accounts = [
        _create_account(owner_engine, label="A"),
        _create_account(owner_engine, label="B"),
    ]
    fixture = {
        "owner_engine": owner_engine,
        "runtime_engine": runtime_engine,
        "a": accounts[0],
        "b": accounts[1],
        "organization_ids": [item["organization_id"] for item in accounts],
        "license_ids": [item["license_id"] for item in accounts],
        "user_ids": [item["user_id"] for item in accounts],
        "lote_ids": [item["lote_id"] for item in accounts],
        "job_ids": [],
    }
    try:
        with _runtime_environment():
            yield fixture
    finally:
        reset_engine_state()
        with owner_engine.begin() as conn:
            if fixture["job_ids"]:
                conn.execute(
                    text(
                        "DELETE FROM satellite_job_results "
                        "WHERE satellite_job_id = ANY(:ids)"
                    ),
                    {"ids": fixture["job_ids"]},
                )
                conn.execute(
                    text(
                        "DELETE FROM satellite_ndvi_observations "
                        "WHERE satellite_job_id = ANY(:ids)"
                    ),
                    {"ids": fixture["job_ids"]},
                )
            conn.execute(
                text("DELETE FROM audit_logs WHERE organization_id = ANY(:ids)"),
                {"ids": fixture["organization_ids"]},
            )
            conn.execute(
                text("DELETE FROM user_sessions WHERE user_id = ANY(:ids)"),
                {"ids": fixture["user_ids"]},
            )
            if fixture["job_ids"]:
                conn.execute(
                    text("DELETE FROM satellite_jobs WHERE id = ANY(:ids)"),
                    {"ids": fixture["job_ids"]},
                )
            conn.execute(
                text("DELETE FROM lotes WHERE id = ANY(:ids)"),
                {"ids": fixture["lote_ids"]},
            )
            conn.execute(
                text("DELETE FROM users WHERE id = ANY(:ids)"),
                {"ids": fixture["user_ids"]},
            )
            conn.execute(
                text("DELETE FROM licenses WHERE id = ANY(:ids)"),
                {"ids": fixture["license_ids"]},
            )
            conn.execute(
                text("DELETE FROM organizations WHERE id = ANY(:ids)"),
                {"ids": fixture["organization_ids"]},
            )
        runtime_engine.dispose()
        owner_engine.dispose()


def _authenticate(account) -> str:
    response = asyncio.run(
        login_b2b(
            LoginRequest(
                username=str(account["username"]),
                password=str(account["password"]),
            ),
            Response(),
        )
    )
    return response.access_token


async def _asgi_request(
    method: str,
    path: str,
    *,
    token: str,
    payload: dict[str, object] | None = None,
) -> dict[str, object]:
    body = json.dumps(payload).encode("utf-8") if payload is not None else b""
    headers = [
        (b"host", b"testserver"),
        (b"authorization", f"Bearer {token}".encode("utf-8")),
    ]
    if body:
        headers.extend(
            [
                (b"content-type", b"application/json"),
                (b"content-length", str(len(body)).encode("ascii")),
            ]
        )
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": method,
        "scheme": "http",
        "path": path,
        "raw_path": path.encode("utf-8"),
        "query_string": b"",
        "headers": headers,
        "client": ("203.0.113.52", 50124),
        "server": ("testserver", 80),
        "root_path": "",
    }
    messages: list[dict[str, object]] = []
    received = False

    async def receive():
        nonlocal received
        if received:
            return {"type": "http.disconnect"}
        received = True
        return {"type": "http.request", "body": body, "more_body": False}

    async def send(message):
        messages.append(message)

    await app(scope, receive, send)
    start = next(
        item for item in messages if item["type"] == "http.response.start"
    )
    response_body = b"".join(
        item.get("body", b"")
        for item in messages
        if item["type"] == "http.response.body"
    )
    return {
        "status_code": int(start["status"]),
        "body": json.loads(response_body.decode("utf-8")),
        "location": dict(start["headers"]).get(b"location", b"").decode("utf-8"),
    }


def _submit(token: str, payload: dict[str, object]) -> dict[str, object]:
    return asyncio.run(
        _asgi_request(
            "POST",
            "/api/v1/satellite/jobs",
            token=token,
            payload=payload,
        )
    )


def _get(token: str, path: str) -> dict[str, object]:
    return asyncio.run(_asgi_request("GET", path, token=token))


def _submit_payload(account, *, key: str, start_date: str = "2026-07-01"):
    return {
        "lote_id": int(account["lote_id"]),
        "start_date": start_date,
        "end_date": "2026-08-01",
        "max_cloud_pct": 20.0,
        "idempotency_key": key,
    }


def _session_factory(engine):
    return sessionmaker(
        bind=engine,
        autoflush=False,
        autocommit=False,
    )


def _worker(
    fixture,
    worker_engine,
    *,
    worker_id: str,
    adapter,
) -> SatelliteWorker:
    return SatelliteWorker(
        worker_id=worker_id,
        heartbeat_seconds=60,
        stale_recovery_interval_seconds=None,
        claim_session_factory=_session_factory(worker_engine),
        tenant_session_factory=_session_factory(fixture["runtime_engine"]),
        gee_ndvi_adapter=adapter,
        retry_base_seconds=30,
        retry_max_seconds=900,
    )


def _normalized_result(request, *, values: tuple[float, float]):
    observations = []
    for offset, (day, value) in enumerate(zip((1, 2), values, strict=True)):
        observations.append(
            NdviObservationRecord(
                observation_date=date(2026, 8, day),
                ndvi_mean=value,
                ndvi_min=value - 0.05,
                ndvi_max=value + 0.05,
                ndvi_std=0.03,
                scene_cloud_percentage=5.0 + offset,
                aoi_cloud_percentage=1.0 + offset,
                valid_pixel_count=100 + offset,
                valid_pixel_percentage=98.0 - offset,
                satellite="Sentinel-2",
                collection="COPERNICUS/S2_SR_HARMONIZED",
                geometry_hash=request.geometry_hash,
                algorithm_version=request.algorithm_version,
                processing_date=datetime.now(timezone.utc),
            )
        )
    return NormalizedNdviExecutionResult(
        geometry_hash=request.geometry_hash,
        algorithm_version=request.algorithm_version,
        observations=tuple(observations),
    )


class _SuccessAdapter:
    def __init__(self, *, values, on_execute=None):
        self.values = values
        self.on_execute = on_execute
        self.calls = 0

    def execute(self, request):
        self.calls += 1
        if self.on_execute is not None:
            self.on_execute()
        return _normalized_result(request, values=self.values)


class _FailureAdapter:
    def __init__(self, *, retryable: bool):
        self.retryable = retryable

    def execute(self, _request):
        raise SatelliteWorkerExecutionError(
            (
                "gee_temporary_service_failure"
                if self.retryable
                else "invalid_job_payload"
            ),
            "private provider response and credential details",
            retry_disposition=(
                RetryDisposition.RETRYABLE
                if self.retryable
                else RetryDisposition.NON_RETRYABLE
            ),
        )


def _job_snapshot(owner_engine, job_id: int) -> dict[str, object]:
    with owner_engine.connect() as conn:
        job = dict(
            conn.execute(
                text(
                    """
                    SELECT id, organization_id, lote_id, status, attempt_count,
                           max_attempts, next_attempt_at, started_at, finished_at,
                           locked_by, locked_at, heartbeat_at, lease_token,
                           error_code, error_message, geometry_hash,
                           algorithm_version, updated_at
                    FROM satellite_jobs WHERE id = :job_id
                    """
                ),
                {"job_id": job_id},
            ).mappings().one()
        )
        results = [
            dict(row)
            for row in conn.execute(
                text(
                    """
                    SELECT satellite_job_id, organization_id, lote_id,
                           result_schema_version, geometry_hash,
                           algorithm_version, result_payload, payload_sha256,
                           created_at
                    FROM satellite_job_results WHERE satellite_job_id = :job_id
                    """
                ),
                {"job_id": job_id},
            ).mappings().all()
        ]
        observations = [
            dict(row)
            for row in conn.execute(
                text(
                    """
                    SELECT id, satellite_job_id, observation_date, ndvi_mean
                    FROM satellite_ndvi_observations
                    WHERE satellite_job_id = :job_id ORDER BY observation_date
                    """
                ),
                {"job_id": job_id},
            ).mappings().all()
        ]
    return {"job": job, "results": results, "observations": observations}


def _job_audit_count(owner_engine, account, job_id: int) -> int:
    with owner_engine.connect() as conn:
        return int(
            conn.execute(
                text(
                    """
                    SELECT count(*) FROM audit_logs
                    WHERE organization_id = :organization_id
                      AND user_id = :user_id
                      AND entity_type = 'satellite_job'
                      AND entity_id = :job_id
                    """
                ),
                {
                    "organization_id": account["organization_id"],
                    "user_id": account["user_id"],
                    "job_id": job_id,
                },
            ).scalar_one()
        )


def _assert_public_status(body: dict[str, object], expected: str) -> None:
    assert body["status"] == expected
    serialized = json.dumps(body, sort_keys=True).lower()
    for field in (
        "organization_id", "idempotency_key", "polygon_wkt_snapshot",
        "geometry_hash", "algorithm_version", "locked_by", "locked_at",
        "heartbeat_at", "lease_token", "error_message",
    ):
        assert f'"{field}"' not in serialized


def _assert_public_result(body: dict[str, object]) -> None:
    serialized = json.dumps(body, sort_keys=True).lower()
    for field in (
        "organization_id", "idempotency_key", "polygon_wkt_snapshot",
        "locked_by", "locked_at", "heartbeat_at", "lease_token",
        "error_message", "credentials", "provider_response",
    ):
        assert f'"{field}"' not in serialized


def test_e2e_happy_path_replay_history_polling_and_cross_tenant_isolation():
    with _fixture() as fixture, _ephemeral_worker_login(
        fixture["owner_engine"]
    ) as worker_engine:
        account_a = fixture["a"]
        token_a = _authenticate(account_a)
        token_b = _authenticate(fixture["b"])
        key_a = f"p22e6-happy-{uuid4().hex}"
        payload_a = _submit_payload(account_a, key=key_a)
        submit_a = _submit(token_a, payload_a)
        job_a = int(submit_a["body"]["job_id"])
        fixture["job_ids"].append(job_a)

        queued_db = _job_snapshot(fixture["owner_engine"], job_a)
        queued_status = _get(token_a, f"/api/v1/satellite/jobs/{job_a}")
        queued_result = _get(token_a, f"/api/v1/satellite/jobs/{job_a}/result")

        running_evidence: dict[str, object] = {}

        def _capture_running():
            running_evidence["db"] = _job_snapshot(
                fixture["owner_engine"], job_a
            )["job"]
            running_evidence["status"] = _get(
                token_a, f"/api/v1/satellite/jobs/{job_a}"
            )
            running_evidence["result"] = _get(
                token_a, f"/api/v1/satellite/jobs/{job_a}/result"
            )

        adapter_a = _SuccessAdapter(values=(0.61, 0.67), on_execute=_capture_running)
        run_a = _worker(
            fixture,
            worker_engine,
            worker_id=f"p22e6-worker-a-{uuid4().hex[:8]}",
            adapter=adapter_a,
        ).run_once()
        success_a = _job_snapshot(fixture["owner_engine"], job_a)
        status_a = _get(token_a, f"/api/v1/satellite/jobs/{job_a}")
        result_a_before = _get(token_a, f"/api/v1/satellite/jobs/{job_a}/result")

        polls_before = _job_snapshot(fixture["owner_engine"], job_a)
        audits_before = _job_audit_count(fixture["owner_engine"], account_a, job_a)
        repeated = [
            _get(token_a, f"/api/v1/satellite/jobs/{job_a}"),
            _get(token_a, f"/api/v1/satellite/jobs/{job_a}/result"),
            _get(token_a, f"/api/v1/satellite/jobs/{job_a}"),
            _get(token_a, f"/api/v1/satellite/jobs/{job_a}/result"),
        ]
        polls_after = _job_snapshot(fixture["owner_engine"], job_a)
        audits_after = _job_audit_count(fixture["owner_engine"], account_a, job_a)

        replay_a = _submit(token_a, payload_a)
        replay_db = _job_snapshot(fixture["owner_engine"], job_a)

        payload_b = _submit_payload(
            account_a,
            key=f"p22e6-history-{uuid4().hex}",
            start_date="2026-07-02",
        )
        submit_b = _submit(token_a, payload_b)
        job_b = int(submit_b["body"]["job_id"])
        fixture["job_ids"].append(job_b)
        adapter_b = _SuccessAdapter(values=(0.81, 0.87))
        run_b = _worker(
            fixture,
            worker_engine,
            worker_id=f"p22e6-worker-b-{uuid4().hex[:8]}",
            adapter=adapter_b,
        ).run_once()
        result_a_after = _get(token_a, f"/api/v1/satellite/jobs/{job_a}/result")
        result_b = _get(token_a, f"/api/v1/satellite/jobs/{job_b}/result")
        success_b = _job_snapshot(fixture["owner_engine"], job_b)

        cross_status = _get(token_b, f"/api/v1/satellite/jobs/{job_a}")
        missing_status = _get(token_b, "/api/v1/satellite/jobs/2147483000")
        cross_result = _get(token_b, f"/api/v1/satellite/jobs/{job_a}/result")
        missing_result = _get(token_b, "/api/v1/satellite/jobs/2147483000/result")
        with fixture["runtime_engine"].begin() as conn:
            conn.execute(
                text(
                    "SELECT set_config('app.current_organization_id', "
                    ":organization_id, true)"
                ),
                {"organization_id": str(fixture["b"]["organization_id"])},
            )
            invisible_jobs = conn.execute(
                text("SELECT id FROM satellite_jobs WHERE id = :job_id"),
                {"job_id": job_a},
            ).scalars().all()
            invisible_results = conn.execute(
                text(
                    "SELECT satellite_job_id FROM satellite_job_results "
                    "WHERE satellite_job_id = :job_id"
                ),
                {"job_id": job_a},
            ).scalars().all()

    assert submit_a["status_code"] == 202
    assert submit_a["location"] == f"/api/v1/satellite/jobs/{job_a}"
    assert submit_a["body"]["status"] == "queued"
    _assert_public_status(submit_a["body"], "queued")
    assert queued_db["job"]["organization_id"] == account_a["organization_id"]
    assert queued_db["job"]["lote_id"] == account_a["lote_id"]
    assert queued_db["job"]["attempt_count"] == 0
    assert queued_db["results"] == []
    assert queued_status["status_code"] == 200
    _assert_public_status(queued_status["body"], "queued")
    assert queued_result["status_code"] == 409
    assert queued_result["body"]["status"] == "queued"

    running_db = running_evidence["db"]
    assert running_db["status"] == "running"
    assert running_db["attempt_count"] == 1
    assert running_db["started_at"] is not None
    assert running_db["locked_by"] is not None
    assert running_db["lease_token"] is not None
    assert running_evidence["status"]["status_code"] == 200
    _assert_public_status(running_evidence["status"]["body"], "running")
    assert running_evidence["result"]["status_code"] == 409
    assert running_evidence["result"]["body"]["status"] == "running"

    assert run_a.status is WorkerRunStatus.SUCCEEDED
    assert adapter_a.calls == 1
    assert success_a["job"]["status"] == "succeeded"
    assert success_a["job"]["attempt_count"] == 1
    assert success_a["job"]["started_at"] is not None
    assert success_a["job"]["finished_at"] is not None
    assert len(success_a["results"]) == 1
    assert len(success_a["observations"]) == 2
    snapshot = success_a["results"][0]
    assert snapshot["organization_id"] == account_a["organization_id"]
    assert snapshot["lote_id"] == account_a["lote_id"]
    assert snapshot["result_schema_version"] == NDVI_TIMESERIES_RESULT_SCHEMA_VERSION
    assert snapshot["geometry_hash"] == success_a["job"]["geometry_hash"]
    assert snapshot["algorithm_version"] == success_a["job"]["algorithm_version"]
    assert compute_satellite_job_result_payload_sha256(
        snapshot["result_payload"]
    ) == snapshot["payload_sha256"]
    assert status_a["status_code"] == 200
    _assert_public_status(status_a["body"], "succeeded")
    assert status_a["body"]["attempt_count"] == 1
    assert result_a_before["status_code"] == 200
    _assert_public_result(result_a_before["body"])
    assert result_a_before["body"]["total_observations"] == 2
    assert [item["ndvi_mean"] for item in result_a_before["body"]["observations"]] == [0.61, 0.67]

    assert [item["status_code"] for item in repeated] == [200, 200, 200, 200]
    assert polls_after == polls_before
    assert audits_after == audits_before
    assert audits_before >= 1
    assert replay_a["status_code"] == 200
    assert replay_a["body"]["job_id"] == job_a
    assert replay_a["location"] == submit_a["location"]
    assert replay_db["job"]["status"] == "succeeded"
    assert len(replay_db["results"]) == 1
    assert adapter_a.calls == 1

    assert run_b.status is WorkerRunStatus.SUCCEEDED
    assert len(success_b["results"]) == 1
    assert result_a_after["body"] == result_a_before["body"]
    assert result_b["status_code"] == 200
    assert [item["ndvi_mean"] for item in result_b["body"]["observations"]] == [0.81, 0.87]
    assert cross_status == missing_status
    assert cross_status["status_code"] == 404
    assert cross_result == missing_result
    assert cross_result["status_code"] == 404
    assert invisible_jobs == []
    assert invisible_results == []


def test_e2e_non_retryable_failure_is_terminal_and_publicly_sanitized():
    with _fixture() as fixture, _ephemeral_worker_login(
        fixture["owner_engine"]
    ) as worker_engine:
        account = fixture["a"]
        token = _authenticate(account)
        submit = _submit(
            token,
            _submit_payload(account, key=f"p22e6-failed-{uuid4().hex}"),
        )
        job_id = int(submit["body"]["job_id"])
        fixture["job_ids"].append(job_id)
        run = _worker(
            fixture,
            worker_engine,
            worker_id=f"p22e6-worker-failed-{uuid4().hex[:8]}",
            adapter=_FailureAdapter(retryable=False),
        ).run_once()
        persisted = _job_snapshot(fixture["owner_engine"], job_id)
        public_status = _get(token, f"/api/v1/satellite/jobs/{job_id}")
        public_result = _get(token, f"/api/v1/satellite/jobs/{job_id}/result")

    assert submit["status_code"] == 202
    assert run.status is WorkerRunStatus.FAILED
    assert persisted["job"]["status"] == "failed"
    assert persisted["job"]["attempt_count"] == 1
    assert persisted["job"]["finished_at"] is not None
    assert persisted["job"]["error_code"] == "invalid_job_payload"
    assert persisted["job"]["error_message"] is not None
    assert persisted["results"] == []
    assert persisted["observations"] == []
    assert public_status["status_code"] == 200
    _assert_public_status(public_status["body"], "failed")
    assert public_status["body"]["error_code"] == "invalid_job_payload"
    assert public_result["status_code"] == 409
    assert public_result["body"] == {
        "job_id": job_id,
        "status": "failed",
        "error_code": "invalid_job_payload",
        "detail": "El satellite job finalizo sin un resultado disponible.",
    }
    assert "private provider" not in json.dumps(public_result).lower()


def test_e2e_retryable_failure_requeues_then_succeeds_on_actual_reclaim():
    with _fixture() as fixture, _ephemeral_worker_login(
        fixture["owner_engine"]
    ) as worker_engine:
        account = fixture["a"]
        token = _authenticate(account)
        submit = _submit(
            token,
            _submit_payload(account, key=f"p22e6-retry-{uuid4().hex}"),
        )
        job_id = int(submit["body"]["job_id"])
        fixture["job_ids"].append(job_id)
        first_run = _worker(
            fixture,
            worker_engine,
            worker_id=f"p22e6-worker-retry-a-{uuid4().hex[:8]}",
            adapter=_FailureAdapter(retryable=True),
        ).run_once()
        retry_state = _job_snapshot(fixture["owner_engine"], job_id)
        retry_public = _get(token, f"/api/v1/satellite/jobs/{job_id}")
        retry_result = _get(token, f"/api/v1/satellite/jobs/{job_id}/result")

        with fixture["owner_engine"].begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE satellite_jobs
                    SET next_attempt_at = CURRENT_TIMESTAMP - INTERVAL '1 minute'
                    WHERE id = :job_id
                    """
                ),
                {"job_id": job_id},
            )
        second_run = _worker(
            fixture,
            worker_engine,
            worker_id=f"p22e6-worker-retry-b-{uuid4().hex[:8]}",
            adapter=_SuccessAdapter(values=(0.71, 0.76)),
        ).run_once()
        success = _job_snapshot(fixture["owner_engine"], job_id)
        final_status = _get(token, f"/api/v1/satellite/jobs/{job_id}")
        final_result = _get(token, f"/api/v1/satellite/jobs/{job_id}/result")

    assert submit["status_code"] == 202
    assert first_run.status is WorkerRunStatus.RETRY_SCHEDULED
    assert retry_state["job"]["status"] == "queued"
    assert retry_state["job"]["attempt_count"] == 1
    assert retry_state["job"]["next_attempt_at"] > retry_state["job"]["updated_at"]
    assert retry_state["job"]["locked_by"] is None
    assert retry_state["job"]["locked_at"] is None
    assert retry_state["job"]["heartbeat_at"] is None
    assert retry_state["job"]["lease_token"] is None
    assert retry_state["results"] == []
    assert retry_state["observations"] == []
    assert retry_public["status_code"] == 200
    _assert_public_status(retry_public["body"], "queued")
    assert retry_result["status_code"] == 409
    assert retry_result["body"]["status"] == "queued"
    assert "retry_scheduled" not in json.dumps(retry_public).lower()
    assert "retrying" not in json.dumps(retry_public).lower()

    assert second_run.status is WorkerRunStatus.SUCCEEDED
    assert success["job"]["status"] == "succeeded"
    assert success["job"]["attempt_count"] == 2
    assert len(success["results"]) == 1
    assert len(success["observations"]) == 2
    assert final_status["status_code"] == 200
    _assert_public_status(final_status["body"], "succeeded")
    assert final_status["body"]["attempt_count"] == 2
    assert final_result["status_code"] == 200
    _assert_public_result(final_result["body"])
    assert final_result["body"]["total_observations"] == 2

