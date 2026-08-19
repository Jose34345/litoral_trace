from __future__ import annotations

import os
import socket
import threading
import time
from datetime import date, datetime, timezone
from uuid import uuid4

import pytest


def _truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}


ENABLED = _truthy(os.environ.get("ENABLE_STAGING_BROWSER_E2E"))
RUNTIME_URL = os.environ.get("TEST_POSTGRES_DATABASE_URL")
OWNER_URL = os.environ.get("TEST_POSTGRES_MIGRATION_DATABASE_URL")
WORKER_URL = os.environ.get("TEST_POSTGRES_WORKER_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not (ENABLED and RUNTIME_URL and OWNER_URL and WORKER_URL),
    reason=(
        "V1 browser staging E2E requires explicit enablement plus isolated "
        "PostgreSQL owner/runtime/worker URLs."
    ),
)


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def test_browser_lot_to_satellite_worker_success_result() -> None:
    """Exercise browser -> API -> durable queue -> worker -> result in one stack.

    The external Earth Engine dependency is replaced by a deterministic adapter;
    queueing, PostgreSQL persistence, tenant access, worker leasing, result
    persistence, browser polling, and final UI rendering remain real.
    """

    from playwright.sync_api import expect, sync_playwright
    import uvicorn
    from sqlalchemy import create_engine, text
    from sqlalchemy.orm import sessionmaker

    from litoral_trace.auth.passwords import hash_password
    from litoral_trace.config.settings import normalize_database_url
    from litoral_trace.db.engine import reset_engine_state
    from litoral_trace.services.satellite_ndvi_processing import (
        NdviObservationRecord,
        NormalizedNdviExecutionResult,
    )
    from litoral_trace.workers.satellite_worker import (
        SatelliteWorker,
        WorkerRunStatus,
    )

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
    worker_engine = create_engine(
        normalize_database_url(WORKER_URL or ""),
        pool_size=2,
        max_overflow=0,
        pool_pre_ping=True,
        hide_parameters=True,
    )

    suffix = uuid4().hex[:10]
    username = f"v1_browser_{suffix}"
    password = f"V1-Browser-{suffix}-A9!"
    organization_id: int | None = None
    user_id: int | None = None
    license_id: int | None = None
    lote_id: int | None = None
    job_id: int | None = None

    server = None
    server_thread = None
    worker_thread = None
    worker_stop = threading.Event()
    adapter_started = threading.Event()
    adapter_release = threading.Event()
    worker_errors: list[str] = []

    def current_job() -> dict[str, object] | None:
        if organization_id is None:
            return None
        with owner_engine.connect() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT id, status, attempt_count
                    FROM satellite_jobs
                    WHERE organization_id = :organization_id
                    ORDER BY id DESC
                    LIMIT 1
                    """
                ),
                {"organization_id": organization_id},
            ).mappings().first()
        return dict(row) if row is not None else None

    def wait_for_job(timeout_seconds: float = 10.0) -> dict[str, object]:
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            snapshot = current_job()
            if snapshot is not None:
                return snapshot
            time.sleep(0.05)
        raise AssertionError("Satellite job was not queued by the browser workflow.")

    class DeterministicNdviAdapter:
        def execute(self, request):
            adapter_started.set()
            if not adapter_release.wait(timeout=15):
                raise RuntimeError("Staging adapter release timed out.")
            observation = NdviObservationRecord(
                observation_date=date(2026, 8, 1),
                ndvi_mean=0.7312,
                ndvi_min=0.68,
                ndvi_max=0.79,
                ndvi_std=0.03,
                scene_cloud_percentage=4.0,
                aoi_cloud_percentage=1.5,
                valid_pixel_count=321,
                valid_pixel_percentage=98.5,
                satellite="Sentinel-2",
                collection="COPERNICUS/S2_SR_HARMONIZED",
                geometry_hash=request.geometry_hash,
                algorithm_version=request.algorithm_version,
                processing_date=datetime.now(timezone.utc),
            )
            return NormalizedNdviExecutionResult(
                geometry_hash=request.geometry_hash,
                algorithm_version=request.algorithm_version,
                observations=(observation,),
            )

    def run_worker() -> None:
        worker = SatelliteWorker(
            worker_id=f"v1-browser-worker-{suffix}",
            heartbeat_seconds=60,
            stale_recovery_interval_seconds=None,
            claim_session_factory=sessionmaker(
                bind=worker_engine,
                autoflush=False,
                autocommit=False,
            ),
            tenant_session_factory=sessionmaker(
                bind=runtime_engine,
                autoflush=False,
                autocommit=False,
            ),
            gee_ndvi_adapter=DeterministicNdviAdapter(),
            retry_base_seconds=30,
            retry_max_seconds=900,
        )
        try:
            while not worker_stop.is_set():
                result = worker.run_once()
                if result.status == WorkerRunStatus.IDLE:
                    time.sleep(0.05)
                elif result.status == WorkerRunStatus.SUCCEEDED:
                    return
        except Exception as exc:  # surfaced to the main assertion path
            worker_errors.append(f"{type(exc).__name__}: {exc}")

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
                        "name": f"V1 Browser Staging {suffix}",
                        "slug": f"v1-browser-staging-{suffix}",
                        "tax_id": f"STG-{suffix}",
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
                        "full_name": "V1 Browser Staging Operator",
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
                        "identificador": f"STAGING-LOT-{suffix}",
                        "productor_id": f"STAGING-PRODUCER-{suffix}",
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
        assert server.started, "FastAPI staging server did not start."

        base_url = f"http://127.0.0.1:{port}"

        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            page = browser.new_page()
            try:
                response = page.goto(f"{base_url}/login", wait_until="domcontentloaded")
                assert response is not None and response.ok

                login_response_status: dict[str, int] = {}

                def remember_login_response(response) -> None:
                    if (
                        response.request.method == "POST"
                        and response.url.rstrip("/").endswith("/login")
                    ):
                        login_response_status["status"] = int(response.status)

                page.on("response", remember_login_response)
                page.locator("#loginUsername").fill(username)
                page.locator("#loginPassword").fill(password)
                page.locator('form[action="/login"] button[type="submit"]').click()
                try:
                    page.wait_for_url("**/dashboard", timeout=10_000)
                except Exception as exc:
                    alert = page.locator('[role="alert"]')
                    alert_text = alert.first.inner_text() if alert.count() else ""
                    body_excerpt = page.locator("body").inner_text()[:800]
                    raise AssertionError(
                        "Browser login did not reach dashboard; "
                        f"post_status={login_response_status.get('status')!r}; "
                        f"url={page.url!r}; alert={alert_text!r}; "
                        f"body={body_excerpt!r}"
                    ) from exc

                expect(page.locator("#selected-lote-name")).to_contain_text(
                    f"STAGING-LOT-{suffix}",
                    timeout=15_000,
                )
                page.wait_for_function(
                    "document.querySelector('#satellite-submit') && "
                    "!document.querySelector('#satellite-submit').disabled"
                )

                page.locator("#satellite-submit").click()
                queued = wait_for_job()
                job_id = int(queued["id"])
                assert queued["status"] == "queued"

                worker_thread = threading.Thread(target=run_worker, daemon=True)
                worker_thread.start()

                assert adapter_started.wait(timeout=10), (
                    "Worker did not claim and begin the queued satellite job."
                )
                running = current_job()
                assert running is not None
                assert int(running["id"]) == job_id
                assert running["status"] == "running"

                adapter_release.set()

                expect(page.locator("#satellite-result")).to_contain_text(
                    "Análisis completado",
                    timeout=20_000,
                )
                expect(page.locator("#satellite-result")).to_contain_text(
                    "NDVI medio",
                )
                expect(page.locator("#satellite-result")).not_to_contain_text(
                    "No fue posible completar el análisis"
                )

                with owner_engine.connect() as conn:
                    final_status = conn.execute(
                        text("SELECT status FROM satellite_jobs WHERE id = :job_id"),
                        {"job_id": job_id},
                    ).scalar_one()
                    result_count = int(
                        conn.execute(
                            text(
                                "SELECT count(*) FROM satellite_job_results "
                                "WHERE satellite_job_id = :job_id"
                            ),
                            {"job_id": job_id},
                        ).scalar_one()
                    )
                    observation_count = int(
                        conn.execute(
                            text(
                                "SELECT count(*) FROM satellite_ndvi_observations "
                                "WHERE satellite_job_id = :job_id"
                            ),
                            {"job_id": job_id},
                        ).scalar_one()
                    )

                assert final_status == "succeeded"
                assert result_count == 1
                assert observation_count == 1
                assert not worker_errors, worker_errors
            finally:
                browser.close()
    finally:
        adapter_release.set()
        worker_stop.set()
        if worker_thread is not None:
            worker_thread.join(timeout=5)
        if server is not None:
            server.should_exit = True
        if server_thread is not None:
            server_thread.join(timeout=10)

        reset_engine_state()
        if organization_id is not None:
            with owner_engine.begin() as conn:
                conn.execute(
                    text(
                        "DELETE FROM satellite_job_results "
                        "WHERE organization_id = :organization_id"
                    ),
                    {"organization_id": organization_id},
                )
                conn.execute(
                    text(
                        "DELETE FROM satellite_ndvi_observations "
                        "WHERE organization_id = :organization_id"
                    ),
                    {"organization_id": organization_id},
                )
                conn.execute(
                    text("DELETE FROM audit_logs WHERE organization_id = :organization_id"),
                    {"organization_id": organization_id},
                )
                if user_id is not None:
                    conn.execute(
                        text("DELETE FROM user_sessions WHERE user_id = :user_id"),
                        {"user_id": user_id},
                    )
                conn.execute(
                    text("DELETE FROM satellite_jobs WHERE organization_id = :organization_id"),
                    {"organization_id": organization_id},
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

        worker_engine.dispose()
        runtime_engine.dispose()
        owner_engine.dispose()
        reset_engine_state()
