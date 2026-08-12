from __future__ import annotations

import asyncio
import json

import pytest
from fastapi import status

from main import app
from litoral_trace.api import satellite as satellite_api
from litoral_trace.api.auth import UserTenantContext, get_current_tenant_user


ASYNC_ROUTES = (
    ("POST", "/api/v1/satellite/jobs"),
    ("GET", "/api/v1/satellite/jobs/1"),
    ("GET", "/api/v1/satellite/jobs/1/result"),
)


def _tenant_user(*, role: str) -> UserTenantContext:
    return UserTenantContext(
        user_id=100,
        username=f"p22e5_{role}",
        organization_id=200,
        organization_name="P22E5 Test Organization",
        organization_slug="p22e5-test",
        role=role,
        email=f"p22e5_{role}@example.com",
        session_id=300,
    )


async def _asgi_request(method: str, path: str) -> dict[str, object]:
    payload = None
    if method == "POST":
        payload = {
            "lote_id": 1,
            "start_date": "2026-07-01",
            "end_date": "2026-08-01",
        }
    body = json.dumps(payload).encode("utf-8") if payload is not None else b""
    headers = [(b"host", b"testserver")]
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
        "client": ("203.0.113.50", 50124),
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


def _serialized_schema(openapi: dict[str, object], name: str) -> str:
    return json.dumps(
        openapi["components"]["schemas"][name],
        sort_keys=True,
    ).lower()


def test_async_openapi_contract_and_public_schemas_are_security_scoped():
    openapi = app.openapi()
    paths = openapi["paths"]

    assert set(paths["/api/v1/satellite/jobs"]["post"]["responses"]) == {
        "200", "202", "401", "403", "404", "409", "422",
    }
    assert set(paths["/api/v1/satellite/jobs/{job_id}"]["get"]["responses"]) == {
        "200", "401", "403", "404", "422",
    }
    result_responses = paths[
        "/api/v1/satellite/jobs/{job_id}/result"
    ]["get"]["responses"]
    assert set(result_responses) == {
        "200", "401", "403", "404", "409", "422", "500",
    }
    assert "202" not in result_responses

    submit_schema = _serialized_schema(openapi, "SatelliteJobSubmitResponse")
    status_schema = _serialized_schema(openapi, "SatelliteJobStatusResponse")
    result_schema = _serialized_schema(openapi, "SatelliteJobResultResponse")
    pending_schema = _serialized_schema(
        openapi,
        "SatelliteJobResultPendingResponse",
    )
    failed_schema = _serialized_schema(openapi, "SatelliteJobResultFailedResponse")

    submit_forbidden = {
        "organization_id", "idempotency_key", "polygon_wkt_snapshot",
        "geometry_hash", "algorithm_version", "locked_by", "locked_at",
        "heartbeat_at", "lease_token", "error_message",
    }
    status_forbidden = submit_forbidden | {
        "request_start_date", "request_end_date", "max_cloud_pct",
    }
    result_forbidden = {
        "organization_id", "idempotency_key", "polygon_wkt_snapshot",
        "locked_by", "locked_at", "heartbeat_at", "lease_token",
        "error_message", "payload_sha256", "credentials",
    }

    for field in submit_forbidden:
        assert field not in submit_schema
    for field in status_forbidden:
        assert field not in status_schema
    for field in result_forbidden:
        assert field not in result_schema
        assert field not in pending_schema
        assert field not in failed_schema
    assert "geometry_hash" in result_schema
    assert "algorithm_version" in result_schema


def test_all_async_routes_require_authentication_and_satellite_permission():
    unauthenticated = [
        asyncio.run(_asgi_request(method, path))
        for method, path in ASYNC_ROUTES
    ]
    assert [item["status_code"] for item in unauthenticated] == [401, 401, 401]

    app.dependency_overrides[get_current_tenant_user] = lambda: _tenant_user(
        role="cliente"
    )
    try:
        denied = [
            asyncio.run(_asgi_request(method, path))
            for method, path in ASYNC_ROUTES
        ]
    finally:
        app.dependency_overrides.pop(get_current_tenant_user, None)

    assert [item["status_code"] for item in denied] == [403, 403, 403]


@pytest.mark.parametrize(
    "path",
    (
        "/api/v1/satellite/jobs/0",
        "/api/v1/satellite/jobs/-1",
        "/api/v1/satellite/jobs/0/result",
        "/api/v1/satellite/jobs/-1/result",
    ),
)
def test_invalid_job_paths_return_422_without_running_handlers(monkeypatch, path: str):
    def _unexpected_call(*_args, **_kwargs):
        raise AssertionError("An invalid path must not execute a database handler.")

    monkeypatch.setattr(satellite_api, "get_satellite_job", _unexpected_call)
    monkeypatch.setattr(
        satellite_api,
        "get_tenant_scoped_db_session",
        _unexpected_call,
    )
    app.dependency_overrides[get_current_tenant_user] = lambda: _tenant_user(
        role="manager"
    )
    try:
        response = asyncio.run(_asgi_request("GET", path))
    finally:
        app.dependency_overrides.pop(get_current_tenant_user, None)

    assert response["status_code"] == status.HTTP_422_UNPROCESSABLE_CONTENT
