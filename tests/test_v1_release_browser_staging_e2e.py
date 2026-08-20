from __future__ import annotations

import os
import socket
import threading
import time
from uuid import uuid4

import pytest


def _truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}


ENABLED = _truthy(os.environ.get("ENABLE_V1_FINAL_STAGING_E2E"))
RUNTIME_URL = os.environ.get("TEST_POSTGRES_DATABASE_URL")
OWNER_URL = os.environ.get("TEST_POSTGRES_MIGRATION_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not (ENABLED and RUNTIME_URL and OWNER_URL),
    reason=(
        "V1 final browser staging E2E requires explicit enablement plus "
        "isolated PostgreSQL owner/runtime URLs."
    ),
)


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def test_release_browser_public_auth_and_operator_surfaces() -> None:
    """Prove the final V1 browser shell across public and operator surfaces.

    This is intentionally broader than the dedicated Satellite browser test:
    public pages, auth redirect/login, tenant lot rendering, Batch workspace and
    Vault workspace are all exercised against one real isolated PostgreSQL
    runtime. Data-heavy Satellite execution remains covered by the companion
    staging E2E in the same final-acceptance workflow.
    """

    from playwright.sync_api import expect, sync_playwright
    import uvicorn
    from sqlalchemy import create_engine, text

    from litoral_trace.auth.passwords import hash_password
    from litoral_trace.config.settings import normalize_database_url
    from litoral_trace.db.engine import reset_engine_state

    reset_engine_state()

    owner_engine = create_engine(
        normalize_database_url(OWNER_URL or ""),
        pool_pre_ping=True,
        hide_parameters=True,
    )
    runtime_engine = create_engine(
        normalize_database_url(RUNTIME_URL or ""),
        pool_size=4,
        max_overflow=0,
        pool_pre_ping=True,
        hide_parameters=True,
    )

    suffix = uuid4().hex[:10]
    username = f"v1_release_{suffix}"
    password = f"V1-Release-{suffix}-A9!"
    organization_id: int | None = None
    user_id: int | None = None
    license_id: int | None = None
    lote_id: int | None = None

    server = None
    server_thread = None

    try:
        with owner_engine.begin() as conn:
            organization_id = int(
                conn.execute(
                    text(
                        """
                        INSERT INTO organizations (name, slug, tax_id, tier, is_active)
                        VALUES (:name, :slug, :tax_id, 'pro', true)
                        RETURNING id
                        """
                    ),
                    {
                        "name": f"V1 Final Staging {suffix}",
                        "slug": f"v1-final-staging-{suffix}",
                        "tax_id": f"FINAL-{suffix}",
                    },
                ).scalar_one()
            )
            license_id = int(
                conn.execute(
                    text(
                        """
                        INSERT INTO licenses (
                            organization_id, plan_type, max_lotes,
                            max_volume_tons, max_batch_rows, is_active
                        ) VALUES (
                            :organization_id, 'pro', 100, 5000.0, 500, true
                        ) RETURNING id
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
                            organization_id, email, username, password_hash,
                            role, full_name, is_active
                        ) VALUES (
                            :organization_id, :email, :username, :password_hash,
                            'manager', :full_name, true
                        ) RETURNING id
                        """
                    ),
                    {
                        "organization_id": organization_id,
                        "email": f"{username}@example.invalid",
                        "username": username,
                        "password_hash": hash_password(password),
                        "full_name": "V1 Final Staging Operator",
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
                            'Madera Aserrada (Pino)', 12.0, -27.45, -58.90,
                            :polygon, 'Pendiente', 12.0, 3.0
                        ) RETURNING id
                        """
                    ),
                    {
                        "organization_id": organization_id,
                        "identificador": f"FINAL-LOT-{suffix}",
                        "productor_id": f"FINAL-PRODUCER-{suffix}",
                        "polygon": (
                            "POLYGON((-58.91 -27.46, -58.89 -27.46, "
                            "-58.89 -27.44, -58.91 -27.44, -58.91 -27.46))"
                        ),
                    },
                ).scalar_one()
            )

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
        assert server.started, "FastAPI final staging server did not start."

        base_url = f"http://127.0.0.1:{port}"

        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            page = browser.new_page()
            try:
                response = page.goto(base_url, wait_until="domcontentloaded")
                assert response is not None and response.ok
                expect(page.locator("body")).to_contain_text("Litoral Trace")

                response = page.goto(
                    f"{base_url}/regional-intelligence",
                    wait_until="domcontentloaded",
                )
                assert response is not None and response.ok
                assert page.url.rstrip("/").endswith("/regional-intelligence")

                page.goto(f"{base_url}/dashboard", wait_until="domcontentloaded")
                assert page.url.rstrip("/").endswith("/login")

                page.locator("#loginUsername").fill(username)
                page.locator("#loginPassword").fill(password)
                page.locator('form[action="/login"] button[type="submit"]').click()
                page.wait_for_url("**/dashboard", timeout=10_000)

                expect(page.locator("#selected-lote-name")).to_contain_text(
                    f"FINAL-LOT-{suffix}",
                    timeout=15_000,
                )

                response = page.goto(
                    f"{base_url}/imports",
                    wait_until="domcontentloaded",
                )
                assert response is not None and response.ok
                assert page.url.rstrip("/").endswith("/imports")
                assert "/login" not in page.url

                response = page.goto(
                    f"{base_url}/vault",
                    wait_until="domcontentloaded",
                )
                assert response is not None and response.ok
                assert page.url.rstrip("/").endswith("/vault")
                assert "/login" not in page.url

                page.goto(f"{base_url}/dashboard", wait_until="domcontentloaded")
                expect(page.locator("#selected-lote-name")).to_contain_text(
                    f"FINAL-LOT-{suffix}",
                    timeout=10_000,
                )
            finally:
                browser.close()
    finally:
        if server is not None:
            server.should_exit = True
        if server_thread is not None:
            server_thread.join(timeout=10)

        reset_engine_state()
        if organization_id is not None:
            with owner_engine.begin() as conn:
                conn.execute(
                    text("DELETE FROM audit_logs WHERE organization_id = :organization_id"),
                    {"organization_id": organization_id},
                )
                if user_id is not None:
                    conn.execute(
                        text("DELETE FROM user_sessions WHERE user_id = :user_id"),
                        {"user_id": user_id},
                    )
                if lote_id is not None:
                    conn.execute(
                        text("DELETE FROM lotes WHERE id = :lote_id"),
                        {"lote_id": lote_id},
                    )
                if user_id is not None:
                    conn.execute(
                        text("DELETE FROM users WHERE id = :user_id"),
                        {"user_id": user_id},
                    )
                if license_id is not None:
                    conn.execute(
                        text("DELETE FROM licenses WHERE id = :license_id"),
                        {"license_id": license_id},
                    )
                conn.execute(
                    text("DELETE FROM organizations WHERE id = :organization_id"),
                    {"organization_id": organization_id},
                )

        runtime_engine.dispose()
        owner_engine.dispose()
        reset_engine_state()
