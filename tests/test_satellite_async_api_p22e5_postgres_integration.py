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
    reason="P2.2E5 requires isolated PostgreSQL runtime and owner URLs.",
)


def _owner_engine():
    return create_engine(
        normalize_database_url(OWNER_URL),
        pool_pre_ping=True,
        hide_parameters=True,
    )


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
    username = f"p22e5_{label.lower()}_{suffix}"
    password = f"P22E5-{label}-{suffix}-Password!"
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
                "name": f"P22E5 {label} {suffix}",
                "slug": f"p22e5-{label.lower()}-{suffix}",
                "tax_id": f"87-{suffix[:8]}",
            },
        ).scalar_one()
        license_id = conn.execute(
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
        user_id = conn.execute(
            text(
                """
                INSERT INTO users (
                    organization_id, email, username, password_hash, role,
                    full_name, is_active
                ) VALUES (
                    :organization_id, :email, :username, :password_hash,
                    :role, :full_name, true
                ) RETURNING id
                """
            ),
            {
                "organization_id": organization_id,
                "email": f"{username}@example.com",
                "username": username,
                "password_hash": hash_password(password),
                "role": role,
                "full_name": f"P22E5 {label} User",
            },
        ).scalar_one()
        lote_id = conn.execute(
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
                "identificador": f"P22E5-{label}-{suffix}",
                "productor_id": f"49-{suffix[:8]}",
                "polygon": (
                    "POLYGON((-58.91 -27.46, -58.89 -27.46, "
                    "-58.89 -27.44, -58.91 -27.44, -58.91 -27.46))"
                ),
            },
        ).scalar_one()
    return {
        "organization_id": int(organization_id),
        "license_id": int(license_id),
        "user_id": int(user_id),
        "lote_id": int(lote_id),
        "username": username,
        "password": password,
    }


@contextmanager
def _fixture(owner_engine):
    accounts = [
        _create_account(owner_engine, label="A", role="manager"),
        _create_account(owner_engine, label="B", role="manager"),
        _create_account(owner_engine, label="C", role="cliente"),
    ]
    fixture = {
        "a": accounts[0],
        "b": accounts[1],
        "c": accounts[2],
        "organization_ids": [account["organization_id"] for account in accounts],
        "license_ids": [account["license_id"] for account in accounts],
        "user_ids": [account["user_id"] for account in accounts],
        "lote_ids": [account["lote_id"] for account in accounts],
        "job_ids": [],
        "observation_ids": [],
    }
    try:
        yield fixture
    finally:
        with owner_engine.begin() as conn:
            if fixture["job_ids"]:
                conn.execute(
                    text(
                        "DELETE FROM satellite_job_results "
                        "WHERE satellite_job_id = ANY(:ids)"
                    ),
                    {"ids": fixture["job_ids"]},
                )
            if fixture["observation_ids"]:
                conn.execute(
                    text(
                        "DELETE FROM satellite_ndvi_observations "
                        "WHERE id = ANY(:ids)"
                    ),
                    {"ids": fixture["observation_ids"]},
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


def _result_payload(
    *,
    job_id: int,
    lote_id: int,
    ndvi_mean: float,
) -> dict[str, object]:
    return {
        "schema_version": NDVI_TIMESERIES_RESULT_SCHEMA_VERSION,
        "job_id": job_id,
        "lote_id": lote_id,
        "geometry_hash": "f" * 64,
        "algorithm_version": "p22e5-postgres-test",
        "total_observations": 1,
        "observations": [
            {
                "observation_date": "2026-08-01",
                "ndvi_mean": ndvi_mean,
                "ndvi_min": ndvi_mean - 0.05,
                "ndvi_max": ndvi_mean + 0.05,
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


def _insert_job(
    owner_engine,
    fixture,
    account,
    *,
    job_status: str,
    with_result: bool = False,
    ndvi_mean: float = 0.62,
) -> int:
    now = datetime.now(timezone.utc)
    with owner_engine.begin() as conn:
        job_id = int(
            conn.execute(
                text(
                    """
                    INSERT INTO satellite_jobs (
                        organization_id, lote_id, job_type, status,
                        attempt_count, max_attempts, next_attempt_at,
                        started_at, finished_at, idempotency_key,
                        request_start_date, request_end_date, max_cloud_pct,
                        geometry_hash, algorithm_version, polygon_wkt_snapshot
                    ) VALUES (
                        :organization_id, :lote_id, 'ndvi_timeseries', :status,
                        :attempt_count, 3, :next_attempt_at, :started_at,
                        :finished_at, :idempotency_key, :start_date, :end_date,
                        20.0, :geometry_hash, 'p22e5-postgres-test', :polygon
                    ) RETURNING id
                    """
                ),
                {
                    "organization_id": account["organization_id"],
                    "lote_id": account["lote_id"],
                    "status": job_status,
                    "attempt_count": 0 if job_status == "queued" else 1,
                    "next_attempt_at": now,
                    "started_at": None if job_status == "queued" else now,
                    "finished_at": now if job_status == "succeeded" else None,
                    "idempotency_key": f"p22e5-{uuid4().hex}",
                    "start_date": date(2026, 7, 1),
                    "end_date": date(2026, 8, 1),
                    "geometry_hash": "f" * 64,
                    "polygon": (
                        "POLYGON((-58.91 -27.46, -58.89 -27.46, "
                        "-58.89 -27.44, -58.91 -27.44, -58.91 -27.46))"
                    ),
                },
            ).scalar_one()
        )
        if with_result:
            payload = _result_payload(
                job_id=job_id,
                lote_id=int(account["lote_id"]),
                ndvi_mean=ndvi_mean,
            )
            conn.execute(
                text(
                    """
                    INSERT INTO satellite_job_results (
                        satellite_job_id, organization_id, lote_id,
                        result_schema_version, geometry_hash, algorithm_version,
                        result_payload, payload_sha256
                    ) VALUES (
                        :job_id, :organization_id, :lote_id, :schema_version,
                        :geometry_hash, :algorithm_version,
                        CAST(:payload AS jsonb), :sha256
                    )
                    """
                ),
                {
                    "job_id": job_id,
                    "organization_id": account["organization_id"],
                    "lote_id": account["lote_id"],
                    "schema_version": NDVI_TIMESERIES_RESULT_SCHEMA_VERSION,
                    "geometry_hash": "f" * 64,
                    "algorithm_version": "p22e5-postgres-test",
                    "payload": json.dumps(payload),
                    "sha256": compute_satellite_job_result_payload_sha256(payload),
                },
            )
    fixture["job_ids"].append(job_id)
    return job_id


def _insert_canonical_observation(
    owner_engine,
    fixture,
    account,
    *,
    job_id: int,
    ndvi_mean: float,
) -> int:
    with owner_engine.begin() as conn:
        observation_id = int(
            conn.execute(
                text(
                    """
                    INSERT INTO satellite_ndvi_observations (
                        organization_id, lote_id, satellite_job_id,
                        observation_date, ndvi_mean, ndvi_min, ndvi_max,
                        ndvi_std, cloud_percentage, valid_pixel_count,
                        valid_pixel_percentage, satellite, collection,
                        geometry_hash, algorithm_version
                    ) VALUES (
                        :organization_id, :lote_id, :job_id, '2026-08-01',
                        :ndvi_mean, :ndvi_min, :ndvi_max, 0.03, 5.0, 10,
                        98.0, 'Sentinel-2', 'COPERNICUS/S2_SR_HARMONIZED',
                        :geometry_hash, 'p22e5-postgres-test'
                    ) RETURNING id
                    """
                ),
                {
                    "organization_id": account["organization_id"],
                    "lote_id": account["lote_id"],
                    "job_id": job_id,
                    "ndvi_mean": ndvi_mean,
                    "ndvi_min": ndvi_mean - 0.05,
                    "ndvi_max": ndvi_mean + 0.05,
                    "geometry_hash": "f" * 64,
                },
            ).scalar_one()
        )
    fixture["observation_ids"].append(observation_id)
    return observation_id


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
    token: str | None = None,
    payload: dict[str, object] | None = None,
) -> dict[str, object]:
    body = json.dumps(payload).encode("utf-8") if payload is not None else b""
    headers = [(b"host", b"testserver")]
    if token:
        headers.append((b"authorization", f"Bearer {token}".encode("utf-8")))
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
        "client": ("203.0.113.51", 50124),
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
        message for message in messages if message["type"] == "http.response.start"
    )
    response_body = b"".join(
        message.get("body", b"")
        for message in messages
        if message["type"] == "http.response.body"
    )
    return {
        "status_code": int(start["status"]),
        "body": json.loads(response_body.decode("utf-8")),
    }


def _submit_payload(lote_id: int) -> dict[str, object]:
    return {
        "lote_id": lote_id,
        "start_date": "2026-07-01",
        "end_date": "2026-08-01",
        "max_cloud_pct": 20.0,
    }


def _assert_no_fields(body: dict[str, object], forbidden: set[str]) -> None:
    serialized = json.dumps(body, sort_keys=True).lower()
    for field in forbidden:
        assert f'"{field}"' not in serialized


def test_postgres_cross_endpoint_auth_rbac_and_public_leakage_matrix(
    owner_engine,
    runtime_environment,
):
    with _fixture(owner_engine) as fixture:
        account_a = fixture["a"]
        job_id = _insert_job(
            owner_engine,
            fixture,
            account_a,
            job_status="succeeded",
            with_result=True,
        )
        routes = (
            ("POST", "/api/v1/satellite/jobs", _submit_payload(int(account_a["lote_id"]))),
            ("GET", f"/api/v1/satellite/jobs/{job_id}", None),
            ("GET", f"/api/v1/satellite/jobs/{job_id}/result", None),
        )
        unauthenticated = [
            asyncio.run(_asgi_request(method, path, payload=payload))
            for method, path, payload in routes
        ]

        denied_token = _authenticate(fixture["c"])
        denied = [
            asyncio.run(
                _asgi_request(method, path, token=denied_token, payload=payload)
            )
            for method, path, payload in routes
        ]

        token_a = _authenticate(account_a)
        allowed = [
            asyncio.run(_asgi_request(method, path, token=token_a, payload=payload))
            for method, path, payload in routes
        ]
        fixture["job_ids"].append(int(allowed[0]["body"]["job_id"]))

    assert [item["status_code"] for item in unauthenticated] == [401, 401, 401]
    assert [item["status_code"] for item in denied] == [403, 403, 403]
    assert [item["status_code"] for item in allowed] == [202, 200, 200]

    common_forbidden = {
        "organization_id", "idempotency_key", "polygon_wkt_snapshot",
        "locked_by", "locked_at", "heartbeat_at", "lease_token",
        "error_message", "credentials",
    }
    _assert_no_fields(
        allowed[0]["body"],
        common_forbidden | {"geometry_hash", "algorithm_version"},
    )
    _assert_no_fields(
        allowed[1]["body"],
        common_forbidden | {"geometry_hash", "algorithm_version"},
    )
    _assert_no_fields(allowed[2]["body"], common_forbidden)
    assert "geometry_hash" in allowed[2]["body"]
    assert "algorithm_version" in allowed[2]["body"]


def test_postgres_cross_endpoint_idor_is_indistinguishable_from_missing(
    owner_engine,
    runtime_environment,
):
    with _fixture(owner_engine) as fixture:
        account_a = fixture["a"]
        token_b = _authenticate(fixture["b"])
        job_id = _insert_job(
            owner_engine,
            fixture,
            account_a,
            job_status="succeeded",
            with_result=True,
        )
        cross_submit = asyncio.run(
            _asgi_request(
                "POST",
                "/api/v1/satellite/jobs",
                token=token_b,
                payload=_submit_payload(int(account_a["lote_id"])),
            )
        )
        missing_submit = asyncio.run(
            _asgi_request(
                "POST",
                "/api/v1/satellite/jobs",
                token=token_b,
                payload=_submit_payload(2_147_483_000),
            )
        )
        cross_status = asyncio.run(
            _asgi_request("GET", f"/api/v1/satellite/jobs/{job_id}", token=token_b)
        )
        missing_status = asyncio.run(
            _asgi_request("GET", "/api/v1/satellite/jobs/2147483000", token=token_b)
        )
        cross_result = asyncio.run(
            _asgi_request(
                "GET",
                f"/api/v1/satellite/jobs/{job_id}/result",
                token=token_b,
            )
        )
        missing_result = asyncio.run(
            _asgi_request(
                "GET",
                "/api/v1/satellite/jobs/2147483000/result",
                token=token_b,
            )
        )

    assert cross_submit == missing_submit == {
        "status_code": 404,
        "body": {"detail": "Lote no encontrado."},
    }
    assert cross_status == missing_status == {
        "status_code": 404,
        "body": {"detail": "Satellite job no encontrado."},
    }
    assert cross_result == missing_result == {
        "status_code": 404,
        "body": {"detail": "Satellite job no encontrado."},
    }


def test_postgres_historical_result_api_ignores_later_canonical_state(
    owner_engine,
    runtime_environment,
):
    with _fixture(owner_engine) as fixture:
        account = fixture["a"]
        job_a = _insert_job(
            owner_engine,
            fixture,
            account,
            job_status="succeeded",
            with_result=True,
            ndvi_mean=0.62,
        )
        observation_id = _insert_canonical_observation(
            owner_engine,
            fixture,
            account,
            job_id=job_a,
            ndvi_mean=0.62,
        )
        token = _authenticate(account)
        before = asyncio.run(
            _asgi_request(
                "GET",
                f"/api/v1/satellite/jobs/{job_a}/result",
                token=token,
            )
        )

        job_b = _insert_job(
            owner_engine,
            fixture,
            account,
            job_status="succeeded",
            with_result=True,
            ndvi_mean=0.91,
        )
        with owner_engine.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE satellite_ndvi_observations
                    SET satellite_job_id = :job_b, ndvi_mean = 0.91,
                        ndvi_min = 0.86, ndvi_max = 0.96,
                        algorithm_version = 'p22e5-later-canonical'
                    WHERE id = :observation_id
                    """
                ),
                {"job_b": job_b, "observation_id": observation_id},
            )
        after = asyncio.run(
            _asgi_request(
                "GET",
                f"/api/v1/satellite/jobs/{job_a}/result",
                token=token,
            )
        )
        with owner_engine.connect() as conn:
            canonical = dict(
                conn.execute(
                    text(
                        """
                        SELECT satellite_job_id, ndvi_mean, algorithm_version
                        FROM satellite_ndvi_observations
                        WHERE id = :observation_id
                        """
                    ),
                    {"observation_id": observation_id},
                ).mappings().one()
            )

    assert before["status_code"] == after["status_code"] == 200
    assert after["body"] == before["body"]
    assert after["body"]["observations"][0]["ndvi_mean"] == 0.62
    assert canonical == {
        "satellite_job_id": job_b,
        "ndvi_mean": 0.91,
        "algorithm_version": "p22e5-later-canonical",
    }
