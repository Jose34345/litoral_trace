"""UX10-G real HTTP acceptance for browser operations against PostgreSQL.

This test deliberately crosses the complete browser boundary:
Uvicorn TCP -> login CSRF -> auth cookies -> authenticated CSRF -> operations
router -> runtime service -> PostgreSQL/FORCE RLS -> ledger posting -> HTML.
"""
from __future__ import annotations

import http.client
import os
import re
import socket
import threading
import time
from datetime import datetime
from http.cookies import SimpleCookie
from urllib.parse import urlencode
from uuid import uuid4
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import create_engine, text

from litoral_trace.auth.passwords import hash_password, verify_password
from litoral_trace.auth.sessions import ACCESS_TOKEN_COOKIE_KEY, REFRESH_TOKEN_COOKIE_KEY
from litoral_trace.config.settings import normalize_database_url
from litoral_trace.db.engine import reset_engine_state
from litoral_trace.web.csrf import CSRF_BROWSER_COOKIE_KEY


ENABLED = (os.getenv("ENABLE_POSTGRES_TESTS", "").strip().lower() in {"1", "true", "yes", "on"})
RUNTIME_URL = os.getenv("TEST_POSTGRES_DATABASE_URL")
OWNER_URL = os.getenv("TEST_POSTGRES_MIGRATION_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not (ENABLED and RUNTIME_URL and OWNER_URL),
    reason="UX10-G HTTP acceptance requires the isolated PostgreSQL contract.",
)

_CSRF_RE = re.compile(r'name="csrf_token"\s+value="([^"]+)"')


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _merge_set_cookies(headers: list[tuple[str, str]], cookies: dict[str, str]) -> None:
    for name, value in headers:
        if name.lower() != "set-cookie":
            continue
        parsed = SimpleCookie()
        parsed.load(value)
        for cookie_name, morsel in parsed.items():
            if morsel.value:
                cookies[cookie_name] = morsel.value
            else:
                cookies.pop(cookie_name, None)


def _request(
    *,
    port: int,
    method: str,
    path: str,
    cookies: dict[str, str],
    form: dict[str, str] | None = None,
) -> tuple[int, dict[str, str], str]:
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    headers = {"User-Agent": "Litoral-Trace-UX10G-HTTP-Acceptance/1.0"}
    if cookies:
        headers["Cookie"] = "; ".join(f"{key}={value}" for key, value in cookies.items())

    body: str | None = None
    if form is not None:
        body = urlencode(form)
        headers["Content-Type"] = "application/x-www-form-urlencoded"
        headers["Content-Length"] = str(len(body.encode("utf-8")))

    connection.request(method, path, body=body, headers=headers)
    response = connection.getresponse()
    response_headers = response.getheaders()
    response_body = response.read().decode("utf-8", errors="replace")
    status_code = int(response.status)
    connection.close()

    _merge_set_cookies(response_headers, cookies)
    return status_code, {name.lower(): value for name, value in response_headers}, response_body


def _csrf_token(html: str) -> str:
    match = _CSRF_RE.search(html)
    assert match is not None, "Expected browser-bound csrf_token in rendered HTML."
    return match.group(1)


def test_operations_receipt_real_http_login_csrf_rls_and_posting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import uvicorn

    # tests/conftest.py intentionally forces ordinary pytest into SQLite/test mode.
    # This dedicated gate must instead exercise the real application runtime URL.
    monkeypatch.setenv("ENVIRONMENT", "development")
    monkeypatch.setenv("DATABASE_URL", RUNTIME_URL or "")
    monkeypatch.setenv("MIGRATION_DATABASE_URL", OWNER_URL or "")
    monkeypatch.delenv("TEST_DATABASE_URL", raising=False)
    reset_engine_state()

    owner_engine = create_engine(
        normalize_database_url(OWNER_URL or ""),
        pool_pre_ping=True,
        hide_parameters=True,
    )

    suffix = uuid4().hex[:10]
    username = f"ux10g_http_{suffix}"
    password = f"Ux10G-Http-{suffix}-A9!"
    source_identifier = f"RODAL-HTTP-{suffix}"
    denied_event_code = f"REC-DENIED-{suffix}"
    event_code = f"REC-HTTP-{suffix}"
    batch_code = f"MP-HTTP-{suffix}"
    organization_id: int | None = None

    server = None
    server_thread = None

    try:
        with owner_engine.begin() as connection:
            revision = connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
            assert revision == "020_add_traceability_evidence_links"

            organization_id = int(
                connection.execute(
                    text(
                        """
                        INSERT INTO organizations (name, slug, tax_id, tier, is_active)
                        VALUES (:name, :slug, :tax_id, 'pro', true)
                        RETURNING id
                        """
                    ),
                    {
                        "name": f"UX10-G HTTP Corrientes {suffix}",
                        "slug": f"ux10g-http-corrientes-{suffix}",
                        "tax_id": f"UX10G-HTTP-{suffix}",
                    },
                ).scalar_one()
            )
            connection.execute(
                text(
                    """
                    INSERT INTO licenses (
                        organization_id, plan_type, max_lotes,
                        max_volume_tons, max_batch_rows, is_active
                    ) VALUES (:organization_id, 'pro', 100, 5000.0, 500, true)
                    """
                ),
                {"organization_id": organization_id},
            )
            connection.execute(
                text(
                    """
                    INSERT INTO users (
                        organization_id, email, username, password_hash,
                        role, full_name, is_active
                    ) VALUES (
                        :organization_id, :email, :username, :password_hash,
                        'admin', 'UX10-G HTTP Operator', true
                    )
                    """
                ),
                {
                    "organization_id": organization_id,
                    "email": f"{username}@example.invalid",
                    "username": username,
                    "password_hash": hash_password(password),
                },
            )
            connection.execute(
                text(
                    """
                    INSERT INTO lotes (
                        organization_id, identificador, productor_id,
                        producto_forestal, hectareas, latitud, longitud,
                        estatus, volumen_ingresado_ton, volumen_exportar_ton
                    ) VALUES (
                        :organization_id, :identificador, :productor_id,
                        'Pino resinoso', 100.0, -28.05, -56.03,
                        'Verde', 0.0, 0.0
                    )
                    """
                ),
                {
                    "organization_id": organization_id,
                    "identificador": source_identifier,
                    "productor_id": f"PROV-HTTP-{suffix}",
                },
            )

        # Verify the exact runtime principal can bootstrap the exact HTTP user
        # before Uvicorn starts. This keeps RLS/auth diagnosis separate from HTTP.
        runtime_probe_engine = create_engine(
            normalize_database_url(RUNTIME_URL or ""),
            pool_pre_ping=True,
            hide_parameters=True,
        )
        try:
            with runtime_probe_engine.begin() as connection:
                bootstrap = connection.execute(
                    text(
                        "SELECT id, organization_id, password_hash, is_active "
                        "FROM public.bootstrap_auth_user_by_username(:username)"
                    ),
                    {"username": username},
                ).mappings().one_or_none()
            assert bootstrap is not None, "Runtime bootstrap function did not resolve seeded HTTP user."
            assert int(bootstrap["organization_id"]) == organization_id
            assert bool(bootstrap["is_active"]) is True
            assert verify_password(password, str(bootstrap["password_hash"])) is True
        finally:
            runtime_probe_engine.dispose()

        # Import only after the isolated PostgreSQL runtime environment and seed are ready.
        from main import app

        port = _free_port()
        config = uvicorn.Config(
            app,
            host="127.0.0.1",
            port=port,
            log_level="warning",
            access_log=False,
        )
        server = uvicorn.Server(config)
        server_thread = threading.Thread(target=server.run, daemon=True)
        server_thread.start()

        deadline = time.monotonic() + 15
        while not server.started and time.monotonic() < deadline:
            time.sleep(0.05)
        assert server.started, "UX10-G Uvicorn HTTP acceptance server did not start."

        cookies: dict[str, str] = {}

        status_code, _, login_html = _request(
            port=port,
            method="GET",
            path="/login",
            cookies=cookies,
        )
        assert status_code == 200
        assert CSRF_BROWSER_COOKIE_KEY in cookies
        anonymous_csrf = _csrf_token(login_html)

        status_code, headers, _ = _request(
            port=port,
            method="POST",
            path="/login",
            cookies=cookies,
            form={
                "csrf_token": anonymous_csrf,
                "username": username,
                "password": password,
            },
        )
        assert status_code == 303
        assert headers.get("location") == "/dashboard"
        assert ACCESS_TOKEN_COOKIE_KEY in cookies
        assert REFRESH_TOKEN_COOKIE_KEY in cookies
        assert CSRF_BROWSER_COOKIE_KEY in cookies

        status_code, _, operations_html = _request(
            port=port,
            method="GET",
            path="/operations",
            cookies=cookies,
        )
        assert status_code == 200
        assert source_identifier in operations_html
        authenticated_csrf = _csrf_token(operations_html)

        occurred_at = datetime.now(ZoneInfo("America/Argentina/Cordoba")).strftime("%Y-%m-%dT%H:%M")
        receipt_form = {
            "source_identifier": source_identifier,
            "event_code": event_code,
            "batch_code": batch_code,
            "product_name": "Madera rolliza demo",
            "quantity": "100",
            "unit": "M3",
            "occurred_at": occurred_at,
            "facility_reference": "Planta Demo Corrientes",
            "notes": "UX10-G HTTP acceptance",
            "commit_mode": "post",
        }

        # Browser-bound CSRF must fail closed before the mutation reaches the service.
        denied_form = dict(receipt_form)
        denied_form["event_code"] = denied_event_code
        denied_form["batch_code"] = f"MP-DENIED-{suffix}"
        status_code, _, denied_html = _request(
            port=port,
            method="POST",
            path="/operations/receipts",
            cookies=cookies,
            form=denied_form,
        )
        assert status_code == 403
        assert "Solicitud expirada" in denied_html
        with owner_engine.begin() as connection:
            denied_count = int(
                connection.execute(
                    text(
                        "SELECT count(*) FROM traceability_events "
                        "WHERE organization_id = :org_id AND event_code = :event_code"
                    ),
                    {"org_id": organization_id, "event_code": denied_event_code},
                ).scalar_one()
            )
        assert denied_count == 0

        receipt_form["csrf_token"] = authenticated_csrf
        status_code, headers, response_body = _request(
            port=port,
            method="POST",
            path="/operations/receipts",
            cookies=cookies,
            form=receipt_form,
        )
        assert status_code == 303, response_body
        assert headers.get("location") == "/operations?result=receipt-posted"
        assert "TRACEABILITY_SERVICE_UNAVAILABLE" not in response_body

        status_code, _, result_html = _request(
            port=port,
            method="GET",
            path=headers["location"],
            cookies=cookies,
        )
        assert status_code == 200
        assert "Recepción contabilizada" in result_html
        assert batch_code in result_html
        assert "100 M3" in result_html
        assert "TRACEABILITY_SERVICE_UNAVAILABLE" not in result_html

        with owner_engine.begin() as connection:
            row = connection.execute(
                text(
                    """
                    SELECT
                        e.status AS event_status,
                        b.status AS batch_status,
                        o.quantity AS quantity,
                        o.unit AS unit
                    FROM traceability_events AS e
                    JOIN traceability_event_outputs AS o
                      ON o.organization_id = e.organization_id
                     AND o.event_id = e.id
                    JOIN traceability_batches AS b
                      ON b.organization_id = o.organization_id
                     AND b.id = o.batch_id
                    WHERE e.organization_id = :org_id
                      AND e.event_code = :event_code
                      AND b.code = :batch_code
                    """
                ),
                {
                    "org_id": organization_id,
                    "event_code": event_code,
                    "batch_code": batch_code,
                },
            ).one()
        assert row.event_status == "POSTED"
        assert row.batch_status == "ACTIVE"
        assert str(row.quantity) == "100.000000"
        assert row.unit == "M3"
    finally:
        if server is not None:
            server.should_exit = True
        if server_thread is not None:
            server_thread.join(timeout=10)

        reset_engine_state()
        if organization_id is not None:
            with owner_engine.begin() as connection:
                params = {"org_id": organization_id}
                for table_name in (
                    "audit_logs",
                    "traceability_evidence_links",
                    "shipment_items",
                    "shipments",
                    "traceability_event_inputs",
                    "traceability_event_outputs",
                    "traceability_events",
                    "traceability_batches",
                    "user_sessions",
                ):
                    connection.execute(
                        text(f"DELETE FROM {table_name} WHERE organization_id = :org_id"),
                        params,
                    )
                connection.execute(text("DELETE FROM lotes WHERE organization_id = :org_id"), params)
                connection.execute(text("DELETE FROM users WHERE organization_id = :org_id"), params)
                connection.execute(text("DELETE FROM licenses WHERE organization_id = :org_id"), params)
                connection.execute(text("DELETE FROM organizations WHERE id = :org_id"), params)

        owner_engine.dispose()
        reset_engine_state()
