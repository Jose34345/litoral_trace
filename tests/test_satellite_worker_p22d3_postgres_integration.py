from __future__ import annotations

import logging
import os
import secrets
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest
from psycopg import ClientCursor, sql
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import sessionmaker

import litoral_trace.workers.satellite_worker as satellite_worker_module
from litoral_trace.config.settings import normalize_database_url
from litoral_trace.db.worker import reset_worker_engine_state
from litoral_trace.services.gee import ALGORITHM_VERSION, generate_geometry_hash
from litoral_trace.services.satellite_jobs import (
    StaleRecoveryResult,
    recover_stale_satellite_jobs,
)
from litoral_trace.services.satellite_ndvi_processing import (
    NormalizedNdviExecutionResult,
    NdviObservationRecord,
    SatelliteJobLeaseLostError,
    update_satellite_job_heartbeat,
)
from litoral_trace.workers.satellite_worker import (
    SatelliteWorker,
    WorkerRunStatus,
)


def _truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}


POSTGRES_RLS_TESTS_ENABLED = _truthy(os.environ.get("ENABLE_POSTGRES_TESTS"))
RUNTIME_TEST_DATABASE_URL = os.environ.get("TEST_POSTGRES_DATABASE_URL")
MIGRATION_TEST_DATABASE_URL = os.environ.get("TEST_POSTGRES_MIGRATION_DATABASE_URL")
WORKER_ROLE = "litoral_trace_worker_executor"
RUNTIME_ROLE = "litoral_trace_app"
RECOVER_FUNCTION_SIGNATURE = "public.worker_recover_stale_satellite_jobs(integer)"
CLAIM_FUNCTION_SIGNATURE = "public.worker_claim_next_satellite_job(text)"
EXHAUSTED_ERROR_MESSAGE = (
    "Satellite job heartbeat expired before completion "
    "and max attempts were exhausted."
)
_POLYGON_WKT_SNAPSHOT = (
    "POLYGON((-58.91 -27.46, -58.89 -27.46, -58.89 -27.44, "
    "-58.91 -27.44, -58.91 -27.46))"
)
_POLYGON_GEOMETRY_HASH = generate_geometry_hash(_POLYGON_WKT_SNAPSHOT)

pytestmark = pytest.mark.skipif(
    not (
        POSTGRES_RLS_TESTS_ENABLED
        and RUNTIME_TEST_DATABASE_URL
        and MIGRATION_TEST_DATABASE_URL
    ),
    reason=(
        "PostgreSQL P2.2D-3 tests require ENABLE_POSTGRES_TESTS=1 plus isolated "
        "TEST_POSTGRES_DATABASE_URL and TEST_POSTGRES_MIGRATION_DATABASE_URL."
    ),
)


def _runtime_engine():
    return create_engine(
        normalize_database_url(RUNTIME_TEST_DATABASE_URL),
        pool_size=2,
        max_overflow=0,
        pool_pre_ping=True,
    )


def _owner_engine():
    return create_engine(
        normalize_database_url(MIGRATION_TEST_DATABASE_URL),
        pool_pre_ping=True,
        hide_parameters=True,
    )


def _runtime_session_factory(runtime_engine):
    return sessionmaker(
        bind=runtime_engine,
        autoflush=False,
        autocommit=False,
    )


def _worker_session_factory(worker_login_engine):
    return sessionmaker(
        bind=worker_login_engine,
        autoflush=False,
        autocommit=False,
    )


def _run_owner_cursor_statement(connection, statement, params=()) -> None:
    driver_connection = connection.connection.driver_connection
    with ClientCursor(driver_connection) as cursor:
        cursor.execute(statement, params)


def _cleanup_ephemeral_worker_role(
    owner_engine,
    *,
    role_name: str,
    membership_granted: bool,
    disposable_engines: list,
) -> None:
    for engine in disposable_engines:
        engine.dispose()

    reset_worker_engine_state()

    try:
        with owner_engine.connect() as conn:
            role_exists = conn.execute(
                text("SELECT 1 FROM pg_roles WHERE rolname = :role_name"),
                {"role_name": role_name},
            ).scalar_one_or_none()
        if role_exists is None:
            return

        with owner_engine.connect() as conn:
            transaction = conn.begin()
            try:
                if membership_granted:
                    _run_owner_cursor_statement(
                        conn,
                        sql.SQL("REVOKE {} FROM {}").format(
                            sql.Identifier(WORKER_ROLE),
                            sql.Identifier(role_name),
                        ),
                    )
                _run_owner_cursor_statement(
                    conn,
                    sql.SQL("DROP ROLE {}").format(sql.Identifier(role_name)),
                )
                transaction.commit()
            except Exception:
                transaction.rollback()
                raise
    except Exception:
        raise AssertionError(
            f"Failed to cleanup temporary worker role {role_name}"
        ) from None


def _build_ephemeral_worker_database_url(*, role_name: str, password: str) -> str:
    base_url = make_url(normalize_database_url(MIGRATION_TEST_DATABASE_URL))
    worker_url = base_url.set(username=role_name, password=password)
    return worker_url.render_as_string(hide_password=False)


@contextmanager
def _ephemeral_worker_login(owner_engine):
    role_name = f"litoral_trace_worker_test_{uuid4().hex[:16]}"
    password = secrets.token_urlsafe(24)
    worker_database_url = _build_ephemeral_worker_database_url(
        role_name=role_name,
        password=password,
    )
    disposable_engines = []
    membership_granted = False

    try:
        with owner_engine.connect() as conn:
            transaction = conn.begin()
            try:
                _run_owner_cursor_statement(
                    conn,
                    sql.SQL(
                        """
                        CREATE ROLE {}
                        LOGIN
                        INHERIT
                        NOSUPERUSER
                        NOCREATEDB
                        NOCREATEROLE
                        NOBYPASSRLS
                        PASSWORD %s
                        """
                    ).format(sql.Identifier(role_name)),
                    (password,),
                )
                _run_owner_cursor_statement(
                    conn,
                    sql.SQL("GRANT {} TO {}").format(
                        sql.Identifier(WORKER_ROLE),
                        sql.Identifier(role_name),
                    ),
                )
                membership_granted = True
                transaction.commit()
            except Exception:
                transaction.rollback()
                raise
    except Exception:
        _cleanup_ephemeral_worker_role(
            owner_engine,
            role_name=role_name,
            membership_granted=membership_granted,
            disposable_engines=disposable_engines,
        )
        raise RuntimeError(
            f"Failed to provision ephemeral worker role {role_name}"
        ) from None

    try:
        yield {
            "role_name": role_name,
            "worker_database_url": worker_database_url,
            "register_engine": disposable_engines.append,
        }
    finally:
        _cleanup_ephemeral_worker_role(
            owner_engine,
            role_name=role_name,
            membership_granted=membership_granted,
            disposable_engines=disposable_engines,
        )


def _assert_worker_table_access_denied(engine, statement: str) -> None:
    with pytest.raises(DBAPIError):
        with engine.begin() as conn:
            conn.execute(text(statement))


def _claim_once(engine, *, worker_id: str):
    with engine.begin() as conn:
        row = conn.execute(
            text(
                """
                SELECT *
                FROM public.worker_claim_next_satellite_job(
                    :requested_worker_id
                )
                """
            ),
            {"requested_worker_id": worker_id},
        ).mappings().one_or_none()
    return dict(row) if row is not None else None


def _recover_once(engine, *, requested_batch_size: int | None = 10) -> dict[str, int]:
    with engine.begin() as conn:
        if requested_batch_size is None:
            row = conn.execute(
                text(
                    """
                    SELECT requeued_count, failed_count
                    FROM public.worker_recover_stale_satellite_jobs()
                    """
                )
            ).mappings().one()
        else:
            row = conn.execute(
                text(
                    """
                    SELECT requeued_count, failed_count
                    FROM public.worker_recover_stale_satellite_jobs(
                        :requested_batch_size
                    )
                    """
                ),
                {"requested_batch_size": requested_batch_size},
            ).mappings().one()
    return {
        "requeued_count": int(row["requeued_count"]),
        "failed_count": int(row["failed_count"]),
    }


def _db_now(conn) -> datetime:
    return conn.execute(text("SELECT CURRENT_TIMESTAMP")).scalar_one()


def _insert_job(
    conn,
    *,
    organization_id: int,
    lote_id: int,
    status: str = "queued",
    attempt_count: int = 0,
    max_attempts: int = 3,
    next_attempt_at: datetime | None = None,
    locked_at: datetime | None = None,
    locked_by: str | None = None,
    heartbeat_at: datetime | None = None,
    lease_token: str | None = None,
    started_at: datetime | None = None,
    finished_at: datetime | None = None,
    error_code: str | None = None,
    error_message: str | None = None,
) -> dict[str, object]:
    db_now = _db_now(conn)
    effective_next_attempt_at = next_attempt_at or (db_now - timedelta(minutes=10))
    row = conn.execute(
        text(
            """
            INSERT INTO satellite_jobs (
                organization_id, lote_id, job_type, status, attempt_count, max_attempts,
                next_attempt_at, locked_at, locked_by, heartbeat_at, lease_token,
                request_start_date, request_end_date, max_cloud_pct, geometry_hash,
                algorithm_version, polygon_wkt_snapshot, started_at, finished_at,
                error_code, error_message
            )
            VALUES (
                :organization_id, :lote_id, 'ndvi_timeseries', :status, :attempt_count,
                :max_attempts, :next_attempt_at, :locked_at, :locked_by, :heartbeat_at,
                :lease_token, :request_start_date, :request_end_date, 20.0,
                :geometry_hash, :algorithm_version, :polygon_wkt_snapshot, :started_at,
                :finished_at, :error_code, :error_message
            )
            RETURNING id, status, attempt_count, max_attempts, next_attempt_at,
                      locked_at, locked_by, heartbeat_at, lease_token, started_at,
                      finished_at, error_code, error_message, updated_at
            """
        ),
        {
            "organization_id": organization_id,
            "lote_id": lote_id,
            "status": status,
            "attempt_count": attempt_count,
            "max_attempts": max_attempts,
            "next_attempt_at": effective_next_attempt_at,
            "locked_at": locked_at,
            "locked_by": locked_by,
            "heartbeat_at": heartbeat_at,
            "lease_token": lease_token,
            "request_start_date": (db_now - timedelta(days=30)).date(),
            "request_end_date": db_now.date(),
            "geometry_hash": _POLYGON_GEOMETRY_HASH,
            "algorithm_version": ALGORITHM_VERSION,
            "polygon_wkt_snapshot": _POLYGON_WKT_SNAPSHOT,
            "started_at": started_at,
            "finished_at": finished_at,
            "error_code": error_code,
            "error_message": error_message,
        },
    ).mappings().one()
    return dict(row)


def _mark_job_stale(conn, *, job_id: int) -> None:
    conn.execute(
        text(
            """
            UPDATE satellite_jobs
            SET heartbeat_at = CURRENT_TIMESTAMP - INTERVAL '91 seconds',
                updated_at = CURRENT_TIMESTAMP - INTERVAL '91 seconds'
            WHERE id = :job_id
            """
        ),
        {"job_id": job_id},
    )


def _fetch_job_row(owner_engine, *, job_id: int) -> dict[str, object]:
    with owner_engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT status, attempt_count, max_attempts, next_attempt_at,
                       locked_at, locked_by, heartbeat_at, lease_token,
                       started_at, finished_at, error_code, error_message, updated_at
                FROM satellite_jobs
                WHERE id = :job_id
                """
            ),
            {"job_id": job_id},
        ).mappings().one()
    return dict(row)


def _count_running_jobs(owner_engine, *, organization_id: int) -> int:
    with owner_engine.connect() as conn:
        return int(
            conn.execute(
                text(
                    """
                    SELECT count(*)
                    FROM satellite_jobs
                    WHERE organization_id = :organization_id
                      AND status = 'running'
                    """
                ),
                {"organization_id": organization_id},
            ).scalar_one()
        )


def _count_job_observations(owner_engine, *, job_id: int) -> int:
    with owner_engine.connect() as conn:
        return int(
            conn.execute(
                text(
                    """
                    SELECT count(*)
                    FROM satellite_ndvi_observations
                    WHERE satellite_job_id = :job_id
                    """
                ),
                {"job_id": job_id},
            ).scalar_one()
        )


def _sample_result(geometry_hash: str, algorithm_version: str):
    return NormalizedNdviExecutionResult(
        geometry_hash=geometry_hash,
        algorithm_version=algorithm_version,
        observations=(
            NdviObservationRecord(
                observation_date=date(2026, 8, 1),
                ndvi_mean=0.61,
                ndvi_min=0.55,
                ndvi_max=0.68,
                ndvi_std=0.03,
                scene_cloud_percentage=5.0,
                valid_pixel_count=10,
                valid_pixel_percentage=98.0,
                satellite="Sentinel-2",
                collection="COPERNICUS/S2_SR_HARMONIZED",
                geometry_hash=geometry_hash,
                algorithm_version=algorithm_version,
                processing_date=datetime.now(timezone.utc),
            ),
        ),
    )


class _BlockingGeeAdapter:
    def __init__(self, *, result=None, error: Exception | None = None):
        self.result = result
        self.error = error
        self.started = threading.Event()
        self.release = threading.Event()

    def execute(self, request):
        self.started.set()
        if not self.release.wait(10):
            raise RuntimeError("Timed out waiting to release blocking GEE adapter.")
        if self.error is not None:
            raise self.error
        return self.result


class _ObservingSatelliteWorker(SatelliteWorker):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.last_heartbeat_controller = None
        self.persist_success_calls = 0
        self.persist_failure_calls = 0

    def _create_heartbeat_controller(self, context):
        controller = super()._create_heartbeat_controller(context)
        self.last_heartbeat_controller = controller
        return controller

    def _persist_success(self, context, result):
        self.persist_success_calls += 1
        return super()._persist_success(context, result)

    def _persist_failure(self, context, *, error_code: str, error_message: str):
        self.persist_failure_calls += 1
        return super()._persist_failure(
            context,
            error_code=error_code,
            error_message=error_message,
        )


class _PausableHeartbeatController(
    satellite_worker_module._SatelliteJobHeartbeatController
):
    def __init__(self, *args, pause_after_first_success: bool = False, **kwargs):
        super().__init__(*args, **kwargs)
        self.pause_after_first_success = pause_after_first_success
        self.first_success_event = threading.Event()
        self.resume_after_pause_event = threading.Event()
        self._successful_heartbeats = 0

    def _run(self) -> None:
        while not self._stop_event.wait(self._heartbeat_seconds):
            tenant_session = self._tenant_session_factory()

            if tenant_session is None:
                self._log_generic_error(
                    RuntimeError(
                        "Servicio de base de datos tenant no disponible."
                    )
                )
                continue

            try:
                update_satellite_job_heartbeat(
                    tenant_session,
                    organization_id=self.organization_id,
                    job_id=self.job_id,
                    worker_id=self.worker_id,
                    lease_token=self._lease_token,
                )
                tenant_session.commit()
                self._successful_heartbeats += 1
                if self._successful_heartbeats == 1:
                    self.first_success_event.set()
                    if self.pause_after_first_success:
                        while not self._stop_event.is_set():
                            if self.resume_after_pause_event.wait(0.05):
                                break

            except SatelliteJobLeaseLostError:
                tenant_session.rollback()
                self._lease_lost_event.set()
                self._stop_event.set()
                break

            except Exception as exc:
                tenant_session.rollback()
                self._log_generic_error(exc)

            finally:
                tenant_session.close()


class _PausableObservingSatelliteWorker(_ObservingSatelliteWorker):
    def __init__(self, *args, pause_after_first_heartbeat: bool = False, **kwargs):
        super().__init__(*args, **kwargs)
        self.pause_after_first_heartbeat = pause_after_first_heartbeat

    def _create_heartbeat_controller(self, context):
        controller = _PausableHeartbeatController(
            organization_id=context.organization_id,
            job_id=context.job_id,
            job_type=context.job_type,
            worker_id=context.worker_id,
            lease_token=context.lease_token,
            heartbeat_seconds=self.heartbeat_seconds,
            tenant_session_factory=self._tenant_session_factory,
            pause_after_first_success=self.pause_after_first_heartbeat,
        )
        self.last_heartbeat_controller = controller
        return controller


@contextmanager
def _lease_fixture():
    runtime_engine = _runtime_engine()
    owner_engine = _owner_engine()
    suffix = uuid4().hex[:8]

    with owner_engine.begin() as conn:
        org_id = conn.execute(
            text(
                """
                INSERT INTO organizations (name, slug, tax_id, tier, is_active)
                VALUES (:name, :slug, :tax_id, 'pro', true)
                RETURNING id
                """
            ),
            {
                "name": f"P22D3 Org {suffix}",
                "slug": f"p22d3-org-{suffix}",
                "tax_id": f"94-{suffix}",
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
                    'Madera Aserrada (Pino)', 10.0, -27.45, -58.90,
                    :polygon_wkt, 'Pendiente', 10.0, 5.0
                )
                RETURNING id
                """
            ),
            {
                "organization_id": org_id,
                "identificador": f"P22D3-LOTE-{suffix}",
                "productor_id": f"52-{suffix}",
                "polygon_wkt": _POLYGON_WKT_SNAPSHOT,
            },
        ).scalar_one()

    fixture = {
        "runtime_engine": runtime_engine,
        "owner_engine": owner_engine,
        "organization_id": int(org_id),
        "lote_id": int(lote_id),
    }

    try:
        yield fixture
    finally:
        with owner_engine.begin() as conn:
            conn.execute(
                text(
                    "DELETE FROM satellite_ndvi_observations "
                    "WHERE organization_id = :organization_id"
                ),
                {"organization_id": org_id},
            )
            conn.execute(
                text("DELETE FROM satellite_jobs WHERE organization_id = :organization_id"),
                {"organization_id": org_id},
            )
            conn.execute(
                text("DELETE FROM lotes WHERE id = :lote_id"),
                {"lote_id": lote_id},
            )
            conn.execute(
                text("DELETE FROM organizations WHERE id = :organization_id"),
                {"organization_id": org_id},
            )
        runtime_engine.dispose()
        owner_engine.dispose()


def _wait_until(predicate, *, timeout_seconds: float, interval_seconds: float = 0.05) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(interval_seconds)
    raise AssertionError("Timed out waiting for condition.")


def _run_worker_in_background(worker: SatelliteWorker):
    result_holder: dict[str, object] = {}
    error_holder: dict[str, BaseException] = {}

    def _runner():
        try:
            result_holder["result"] = worker.run_once()
        except BaseException as exc:  # pragma: no cover - diagnostic path
            error_holder["error"] = exc

    thread = threading.Thread(target=_runner, daemon=True)
    thread.start()
    return thread, result_holder, error_holder


def test_function_index_and_worker_capability_contract_are_hardened_for_d3():
    with _lease_fixture() as fixture:
        with fixture["owner_engine"].connect() as conn:
            function_row = conn.execute(
                text(
                    """
                    SELECT
                        p.proname,
                        p.prosecdef,
                        pg_get_function_identity_arguments(p.oid) AS identity_args,
                        coalesce(array_to_string(p.proconfig, ','), '') AS proconfig,
                        has_function_privilege(:runtime_role, p.oid, 'EXECUTE') AS runtime_execute,
                        has_function_privilege(:worker_role, p.oid, 'EXECUTE') AS worker_execute
                    FROM pg_proc AS p
                    JOIN pg_namespace AS n
                        ON n.oid = p.pronamespace
                    WHERE n.nspname = 'public'
                      AND p.proname = 'worker_recover_stale_satellite_jobs'
                    """
                ),
                {
                    "runtime_role": RUNTIME_ROLE,
                    "worker_role": WORKER_ROLE,
                },
            ).mappings().one()
            index_row = conn.execute(
                text(
                    """
                    SELECT indexdef
                    FROM pg_indexes
                    WHERE schemaname = 'public'
                      AND tablename = 'satellite_jobs'
                      AND indexname = 'ix_satellite_jobs_running_heartbeat_at'
                    """
                )
            ).mappings().one()

        assert function_row["proname"] == "worker_recover_stale_satellite_jobs"
        assert function_row["prosecdef"] is True
        assert function_row["identity_args"] == "requested_batch_size integer"
        assert "search_path=public, pg_temp" in function_row["proconfig"]
        assert function_row["runtime_execute"] is False
        assert function_row["worker_execute"] is True
        assert "ix_satellite_jobs_running_heartbeat_at" in index_row["indexdef"]
        assert "status" in index_row["indexdef"]
        assert "'running'" in index_row["indexdef"]
        assert "heartbeat_at IS NOT NULL" in index_row["indexdef"]

        with _ephemeral_worker_login(fixture["owner_engine"]) as worker_auth:
            worker_login_engine = create_engine(
                normalize_database_url(worker_auth["worker_database_url"]),
                pool_size=2,
                max_overflow=0,
                pool_pre_ping=True,
            )
            worker_auth["register_engine"](worker_login_engine)

            with worker_login_engine.begin() as conn:
                role_row = conn.execute(
                    text(
                        """
                        SELECT
                            current_user,
                            has_table_privilege(current_user, 'public.satellite_jobs', 'SELECT') AS can_select,
                            has_table_privilege(current_user, 'public.satellite_jobs', 'INSERT') AS can_insert,
                            has_table_privilege(current_user, 'public.satellite_jobs', 'UPDATE') AS can_update,
                            has_table_privilege(current_user, 'public.satellite_jobs', 'DELETE') AS can_delete,
                            has_function_privilege(current_user, :claim_fn, 'EXECUTE') AS can_execute_claim,
                            has_function_privilege(current_user, :recover_fn, 'EXECUTE') AS can_execute_recover
                        """
                    ),
                    {
                        "claim_fn": CLAIM_FUNCTION_SIGNATURE,
                        "recover_fn": RECOVER_FUNCTION_SIGNATURE,
                    },
                ).mappings().one()

            assert role_row["current_user"] == worker_auth["role_name"]
            assert role_row["can_select"] is False
            assert role_row["can_insert"] is False
            assert role_row["can_update"] is False
            assert role_row["can_delete"] is False
            assert role_row["can_execute_claim"] is True
            assert role_row["can_execute_recover"] is True

            _assert_worker_table_access_denied(
                worker_login_engine,
                "SELECT 1 FROM public.satellite_jobs LIMIT 1",
            )
            _assert_worker_table_access_denied(
                worker_login_engine,
                "INSERT INTO public.satellite_jobs DEFAULT VALUES",
            )
            _assert_worker_table_access_denied(
                worker_login_engine,
                "UPDATE public.satellite_jobs SET status = 'queued' WHERE false",
            )
            _assert_worker_table_access_denied(
                worker_login_engine,
                "DELETE FROM public.satellite_jobs WHERE false",
            )


def test_runtime_role_cannot_execute_global_recovery():
    with _lease_fixture() as fixture:
        with fixture["runtime_engine"].begin() as conn:
            privilege_row = conn.execute(
                text(
                    """
                    SELECT has_function_privilege(
                        current_user,
                        :recover_fn,
                        'EXECUTE'
                    ) AS can_execute_recover
                    """
                ),
                {"recover_fn": RECOVER_FUNCTION_SIGNATURE},
            ).mappings().one()

            assert privilege_row["can_execute_recover"] is False

            with pytest.raises(DBAPIError):
                conn.execute(
                    text(
                        """
                        SELECT *
                        FROM public.worker_recover_stale_satellite_jobs(10)
                        """
                    )
                ).mappings().one()


def test_recoverable_stale_job_is_requeued_and_reclaimed_with_new_lease():
    with _lease_fixture() as fixture:
        with fixture["owner_engine"].begin() as conn:
            job = _insert_job(
                conn,
                organization_id=fixture["organization_id"],
                lote_id=fixture["lote_id"],
            )

        with _ephemeral_worker_login(fixture["owner_engine"]) as worker_auth:
            worker_login_engine = create_engine(
                normalize_database_url(worker_auth["worker_database_url"]),
                pool_size=2,
                max_overflow=0,
                pool_pre_ping=True,
            )
            worker_auth["register_engine"](worker_login_engine)

            claim_a = _claim_once(worker_login_engine, worker_id="p22d3-worker-a")
            assert claim_a is not None
            lease_a = str(claim_a["lease_token"])

            with fixture["owner_engine"].begin() as conn:
                _mark_job_stale(conn, job_id=int(job["id"]))

            recovery_result = _recover_once(
                worker_login_engine,
                requested_batch_size=10,
            )
            after_recovery = _fetch_job_row(
                fixture["owner_engine"],
                job_id=int(job["id"]),
            )

            claim_b = _claim_once(worker_login_engine, worker_id="p22d3-worker-b")
            assert claim_b is not None
            lease_b = str(claim_b["lease_token"])

    assert recovery_result == {"requeued_count": 1, "failed_count": 0}
    assert after_recovery["status"] == "queued"
    assert after_recovery["locked_at"] is None
    assert after_recovery["locked_by"] is None
    assert after_recovery["heartbeat_at"] is None
    assert after_recovery["lease_token"] is None
    assert after_recovery["finished_at"] is None
    assert after_recovery["attempt_count"] == 1
    assert after_recovery["started_at"] is not None
    assert after_recovery["error_code"] is None
    assert after_recovery["error_message"] is None
    assert claim_b["status"] == "running"
    assert claim_b["attempt_count"] == 2
    assert UUID(lease_a)
    assert UUID(lease_b)
    assert lease_a != lease_b


def test_fresh_running_job_is_not_recovered():
    with _lease_fixture() as fixture:
        with fixture["owner_engine"].begin() as conn:
            job = _insert_job(
                conn,
                organization_id=fixture["organization_id"],
                lote_id=fixture["lote_id"],
            )

        with _ephemeral_worker_login(fixture["owner_engine"]) as worker_auth:
            worker_login_engine = create_engine(
                normalize_database_url(worker_auth["worker_database_url"]),
                pool_size=1,
                max_overflow=0,
                pool_pre_ping=True,
            )
            worker_auth["register_engine"](worker_login_engine)

            claim_a = _claim_once(worker_login_engine, worker_id="p22d3-fresh-worker")
            assert claim_a is not None
            lease_a = str(claim_a["lease_token"])

            recovery_result = _recover_once(
                worker_login_engine,
                requested_batch_size=10,
            )
            final_row = _fetch_job_row(
                fixture["owner_engine"],
                job_id=int(job["id"]),
            )

    assert recovery_result == {"requeued_count": 0, "failed_count": 0}
    assert final_row["status"] == "running"
    assert final_row["locked_by"] == "p22d3-fresh-worker"
    assert final_row["heartbeat_at"] is not None
    assert str(final_row["lease_token"]) == lease_a
    assert final_row["attempt_count"] == 1


def test_exhausted_stale_job_fails_and_old_heartbeat_is_rejected():
    with _lease_fixture() as fixture:
        with fixture["owner_engine"].begin() as conn:
            job = _insert_job(
                conn,
                organization_id=fixture["organization_id"],
                lote_id=fixture["lote_id"],
                attempt_count=2,
                max_attempts=3,
            )

        with _ephemeral_worker_login(fixture["owner_engine"]) as worker_auth:
            worker_login_engine = create_engine(
                normalize_database_url(worker_auth["worker_database_url"]),
                pool_size=2,
                max_overflow=0,
                pool_pre_ping=True,
            )
            worker_auth["register_engine"](worker_login_engine)

            claim_a = _claim_once(worker_login_engine, worker_id="p22d3-exhausted-worker")
            assert claim_a is not None
            lease_a = str(claim_a["lease_token"])
            started_at = claim_a["started_at"]
            next_attempt_at = claim_a["next_attempt_at"]

            with fixture["owner_engine"].begin() as conn:
                _mark_job_stale(conn, job_id=int(job["id"]))

            recovery_result = _recover_once(
                worker_login_engine,
                requested_batch_size=10,
            )
            final_row = _fetch_job_row(
                fixture["owner_engine"],
                job_id=int(job["id"]),
            )

            runtime_session = _runtime_session_factory(fixture["runtime_engine"])()
            try:
                with pytest.raises(SatelliteJobLeaseLostError):
                    update_satellite_job_heartbeat(
                        runtime_session,
                        organization_id=fixture["organization_id"],
                        job_id=int(job["id"]),
                        worker_id="p22d3-exhausted-worker",
                        lease_token=lease_a,
                    )
                runtime_session.rollback()
            finally:
                runtime_session.close()

    assert recovery_result == {"requeued_count": 0, "failed_count": 1}
    assert final_row["status"] == "failed"
    assert final_row["locked_at"] is None
    assert final_row["locked_by"] is None
    assert final_row["heartbeat_at"] is None
    assert final_row["finished_at"] is not None
    assert final_row["error_code"] == "stale_recovery_exhausted"
    assert final_row["error_message"] == EXHAUSTED_ERROR_MESSAGE
    assert final_row["attempt_count"] == 3
    assert final_row["started_at"] == started_at
    assert final_row["next_attempt_at"] == next_attempt_at
    assert str(final_row["lease_token"]) == lease_a


def test_db_side_batch_bounds_are_enforced():
    with _lease_fixture() as fixture:
        with fixture["owner_engine"].begin() as conn:
            default_ids = [
                int(
                    _insert_job(
                        conn,
                        organization_id=fixture["organization_id"],
                        lote_id=fixture["lote_id"],
                        status="running",
                        attempt_count=1,
                        locked_at=_db_now(conn) - timedelta(minutes=3),
                        locked_by=f"default-worker-{index}",
                        heartbeat_at=_db_now(conn) - timedelta(minutes=2),
                        lease_token=str(uuid4()),
                        started_at=_db_now(conn) - timedelta(minutes=3),
                    )["id"]
                )
                for index in range(11)
            ]
            for job_id in default_ids:
                _mark_job_stale(conn, job_id=job_id)

        with _ephemeral_worker_login(fixture["owner_engine"]) as worker_auth:
            worker_login_engine = create_engine(
                normalize_database_url(worker_auth["worker_database_url"]),
                pool_size=2,
                max_overflow=0,
                pool_pre_ping=True,
            )
            worker_auth["register_engine"](worker_login_engine)

            default_result = _recover_once(
                worker_login_engine,
                requested_batch_size=None,
            )
            remaining_after_default = [
                _fetch_job_row(fixture["owner_engine"], job_id=job_id)["status"]
                for job_id in default_ids
            ]
            _recover_once(
                worker_login_engine,
                requested_batch_size=10,
            )

        with fixture["owner_engine"].begin() as conn:
            non_positive_ids = [
                int(
                    _insert_job(
                        conn,
                        organization_id=fixture["organization_id"],
                        lote_id=fixture["lote_id"],
                        status="running",
                        attempt_count=1,
                        locked_at=_db_now(conn) - timedelta(minutes=3),
                        locked_by=f"non-positive-{index}",
                        heartbeat_at=_db_now(conn) - timedelta(minutes=2),
                        lease_token=str(uuid4()),
                        started_at=_db_now(conn) - timedelta(minutes=3),
                    )["id"]
                )
                for index in range(2)
            ]
            for job_id in non_positive_ids:
                _mark_job_stale(conn, job_id=job_id)

        with _ephemeral_worker_login(fixture["owner_engine"]) as worker_auth:
            worker_login_engine = create_engine(
                normalize_database_url(worker_auth["worker_database_url"]),
                pool_size=2,
                max_overflow=0,
                pool_pre_ping=True,
            )
            worker_auth["register_engine"](worker_login_engine)

            non_positive_result = _recover_once(
                worker_login_engine,
                requested_batch_size=0,
            )
            non_positive_statuses = [
                _fetch_job_row(fixture["owner_engine"], job_id=job_id)["status"]
                for job_id in non_positive_ids
            ]
            _recover_once(
                worker_login_engine,
                requested_batch_size=10,
            )

        with fixture["owner_engine"].begin() as conn:
            over_limit_ids = [
                int(
                    _insert_job(
                        conn,
                        organization_id=fixture["organization_id"],
                        lote_id=fixture["lote_id"],
                        status="running",
                        attempt_count=1,
                        locked_at=_db_now(conn) - timedelta(minutes=3),
                        locked_by=f"over-limit-{index}",
                        heartbeat_at=_db_now(conn) - timedelta(minutes=2),
                        lease_token=str(uuid4()),
                        started_at=_db_now(conn) - timedelta(minutes=3),
                    )["id"]
                )
                for index in range(101)
            ]
            for job_id in over_limit_ids:
                _mark_job_stale(conn, job_id=job_id)

        with _ephemeral_worker_login(fixture["owner_engine"]) as worker_auth:
            worker_login_engine = create_engine(
                normalize_database_url(worker_auth["worker_database_url"]),
                pool_size=2,
                max_overflow=0,
                pool_pre_ping=True,
            )
            worker_auth["register_engine"](worker_login_engine)

            over_limit_result = _recover_once(
                worker_login_engine,
                requested_batch_size=999,
            )
            over_limit_statuses = [
                _fetch_job_row(fixture["owner_engine"], job_id=job_id)["status"]
                for job_id in over_limit_ids
            ]

    assert default_result == {"requeued_count": 10, "failed_count": 0}
    assert remaining_after_default.count("queued") == 10
    assert remaining_after_default.count("running") == 1
    assert non_positive_result == {"requeued_count": 1, "failed_count": 0}
    assert non_positive_statuses.count("queued") == 1
    assert non_positive_statuses.count("running") == 1
    assert over_limit_result == {"requeued_count": 100, "failed_count": 0}
    assert over_limit_statuses.count("queued") == 100
    assert over_limit_statuses.count("running") == 1


def test_two_concurrent_reapers_do_not_double_recover_jobs():
    with _lease_fixture() as fixture:
        with fixture["owner_engine"].begin() as conn:
            job_ids = [
                int(
                    _insert_job(
                        conn,
                        organization_id=fixture["organization_id"],
                        lote_id=fixture["lote_id"],
                        status="running",
                        attempt_count=1,
                        locked_at=_db_now(conn) - timedelta(minutes=3),
                        locked_by=f"concurrent-{index}",
                        heartbeat_at=_db_now(conn) - timedelta(minutes=2),
                        lease_token=str(uuid4()),
                        started_at=_db_now(conn) - timedelta(minutes=3),
                    )["id"]
                )
                for index in range(6)
            ]
            for job_id in job_ids:
                _mark_job_stale(conn, job_id=job_id)

        with _ephemeral_worker_login(fixture["owner_engine"]) as worker_auth:
            worker_login_engine = create_engine(
                normalize_database_url(worker_auth["worker_database_url"]),
                pool_size=4,
                max_overflow=0,
                pool_pre_ping=True,
            )
            worker_auth["register_engine"](worker_login_engine)
            barrier = threading.Barrier(2)

            def _runner():
                barrier.wait(timeout=5)
                return _recover_once(
                    worker_login_engine,
                    requested_batch_size=4,
                )

            with ThreadPoolExecutor(max_workers=2) as executor:
                result_a, result_b = list(executor.map(lambda _: _runner(), range(2)))

            final_rows = [
                _fetch_job_row(fixture["owner_engine"], job_id=job_id)
                for job_id in job_ids
            ]

    total_requeued = result_a["requeued_count"] + result_b["requeued_count"]
    total_failed = result_a["failed_count"] + result_b["failed_count"]
    assert total_requeued == 6
    assert total_failed == 0
    assert [row["status"] for row in final_rows].count("queued") == 6
    assert all(row["attempt_count"] == 1 for row in final_rows)


def test_heartbeat_wins_then_recovery_does_not_touch_running_job():
    with _lease_fixture() as fixture:
        with fixture["owner_engine"].begin() as conn:
            job = _insert_job(
                conn,
                organization_id=fixture["organization_id"],
                lote_id=fixture["lote_id"],
            )

        with _ephemeral_worker_login(fixture["owner_engine"]) as worker_auth:
            worker_login_engine = create_engine(
                normalize_database_url(worker_auth["worker_database_url"]),
                pool_size=1,
                max_overflow=0,
                pool_pre_ping=True,
            )
            worker_auth["register_engine"](worker_login_engine)

            claim_a = _claim_once(worker_login_engine, worker_id="p22d3-race-a")
            assert claim_a is not None
            lease_a = str(claim_a["lease_token"])

            with fixture["owner_engine"].begin() as conn:
                _mark_job_stale(conn, job_id=int(job["id"]))

            runtime_session = _runtime_session_factory(fixture["runtime_engine"])()
            try:
                update_satellite_job_heartbeat(
                    runtime_session,
                    organization_id=fixture["organization_id"],
                    job_id=int(job["id"]),
                    worker_id="p22d3-race-a",
                    lease_token=lease_a,
                )
                runtime_session.commit()
            finally:
                runtime_session.close()

            recovery_result = _recover_once(
                worker_login_engine,
                requested_batch_size=10,
            )
            final_row = _fetch_job_row(
                fixture["owner_engine"],
                job_id=int(job["id"]),
            )

    assert recovery_result == {"requeued_count": 0, "failed_count": 0}
    assert final_row["status"] == "running"
    assert final_row["locked_by"] == "p22d3-race-a"
    assert str(final_row["lease_token"]) == lease_a


def test_recovery_wins_then_old_heartbeat_loses_authority():
    with _lease_fixture() as fixture:
        with fixture["owner_engine"].begin() as conn:
            job = _insert_job(
                conn,
                organization_id=fixture["organization_id"],
                lote_id=fixture["lote_id"],
            )

        with _ephemeral_worker_login(fixture["owner_engine"]) as worker_auth:
            worker_login_engine = create_engine(
                normalize_database_url(worker_auth["worker_database_url"]),
                pool_size=1,
                max_overflow=0,
                pool_pre_ping=True,
            )
            worker_auth["register_engine"](worker_login_engine)

            claim_a = _claim_once(worker_login_engine, worker_id="p22d3-race-b")
            assert claim_a is not None
            lease_a = str(claim_a["lease_token"])

            with fixture["owner_engine"].begin() as conn:
                _mark_job_stale(conn, job_id=int(job["id"]))

            recovery_result = _recover_once(
                worker_login_engine,
                requested_batch_size=10,
            )

            runtime_session = _runtime_session_factory(fixture["runtime_engine"])()
            try:
                with pytest.raises(SatelliteJobLeaseLostError):
                    update_satellite_job_heartbeat(
                        runtime_session,
                        organization_id=fixture["organization_id"],
                        job_id=int(job["id"]),
                        worker_id="p22d3-race-b",
                        lease_token=lease_a,
                    )
                runtime_session.rollback()
            finally:
                runtime_session.close()

            final_row = _fetch_job_row(
                fixture["owner_engine"],
                job_id=int(job["id"]),
            )

    assert recovery_result == {"requeued_count": 1, "failed_count": 0}
    assert final_row["status"] == "queued"
    assert final_row["locked_by"] is None
    assert final_row["lease_token"] is None


def test_zombie_worker_cannot_persist_after_real_stale_recovery(monkeypatch):
    real_update_satellite_job_heartbeat = (
        satellite_worker_module.update_satellite_job_heartbeat
    )
    first_heartbeat_executed = threading.Event()
    second_heartbeat_entered = threading.Event()
    allow_second_heartbeat = threading.Event()
    heartbeat_call_count = 0

    def _gated_second_heartbeat(
        db_session,
        *,
        organization_id: int,
        job_id: int,
        worker_id: str,
        lease_token: str,
    ) -> datetime:
        nonlocal heartbeat_call_count
        heartbeat_call_count += 1

        if heartbeat_call_count == 2:
            second_heartbeat_entered.set()
            if not allow_second_heartbeat.wait(10):
                raise AssertionError(
                    "Timed out waiting to release the second heartbeat."
                )

        heartbeat_at = real_update_satellite_job_heartbeat(
            db_session,
            organization_id=organization_id,
            job_id=job_id,
            worker_id=worker_id,
            lease_token=lease_token,
        )

        if heartbeat_call_count == 1:
            first_heartbeat_executed.set()

        return heartbeat_at

    monkeypatch.setattr(
        satellite_worker_module,
        "update_satellite_job_heartbeat",
        _gated_second_heartbeat,
    )

    with _lease_fixture() as fixture:
        with fixture["owner_engine"].begin() as conn:
            queued_job = _insert_job(
                conn,
                organization_id=fixture["organization_id"],
                lote_id=fixture["lote_id"],
            )

        with _ephemeral_worker_login(fixture["owner_engine"]) as worker_auth:
            worker_login_engine = create_engine(
                normalize_database_url(worker_auth["worker_database_url"]),
                pool_size=3,
                max_overflow=0,
                pool_pre_ping=True,
            )
            worker_auth["register_engine"](worker_login_engine)

            worker = _ObservingSatelliteWorker(
                worker_id="p22d3-zombie-worker-a",
                heartbeat_seconds=1,
                stale_recovery_interval_seconds=None,
                claim_session_factory=_worker_session_factory(worker_login_engine),
                tenant_session_factory=_runtime_session_factory(
                    fixture["runtime_engine"]
                ),
                gee_ndvi_adapter=_BlockingGeeAdapter(
                    result=_sample_result(
                        _POLYGON_GEOMETRY_HASH,
                        ALGORITHM_VERSION,
                    )
                ),
            )

            worker_thread, result_holder, error_holder = _run_worker_in_background(worker)
            adapter = worker._gee_ndvi_adapter
            assert isinstance(adapter, _BlockingGeeAdapter)
            assert adapter.started.wait(10)

            claim_a_row = _fetch_job_row(
                fixture["owner_engine"],
                job_id=int(queued_job["id"]),
            )
            claim_a_heartbeat = claim_a_row["heartbeat_at"]
            lease_a = str(claim_a_row["lease_token"])
            assert claim_a_row["status"] == "running"

            assert worker.last_heartbeat_controller is not None
            assert first_heartbeat_executed.wait(10)

            _wait_until(
                lambda: (
                    _fetch_job_row(
                        fixture["owner_engine"],
                        job_id=int(queued_job["id"]),
                    )["heartbeat_at"]
                    > claim_a_heartbeat
                ),
                timeout_seconds=10,
            )

            assert second_heartbeat_entered.wait(10)

            with fixture["owner_engine"].begin() as conn:
                _mark_job_stale(conn, job_id=int(queued_job["id"]))

            recovery_result = _recover_once(
                worker_login_engine,
                requested_batch_size=10,
            )
            assert recovery_result == {"requeued_count": 1, "failed_count": 0}
            claim_b = _claim_once(
                worker_login_engine,
                worker_id="p22d3-zombie-worker-b",
            )
            assert claim_b is not None
            assert claim_b["status"] == "running"
            assert int(claim_b["attempt_count"]) == 2
            lease_b = str(claim_b["lease_token"])
            assert UUID(lease_a)
            assert UUID(lease_b)
            assert lease_a != lease_b

            allow_second_heartbeat.set()

            _wait_until(
                lambda: (
                    worker.last_heartbeat_controller is not None
                    and worker.last_heartbeat_controller.has_lease_lost() is True
                    and adapter.release.is_set() is False
                ),
                timeout_seconds=10,
            )

            adapter.release.set()
            worker_thread.join(timeout=10)

            if "error" in error_holder:
                raise error_holder["error"]

            run_result = result_holder["result"]
            final_row = _fetch_job_row(
                fixture["owner_engine"],
                job_id=int(queued_job["id"]),
            )
            observation_count = _count_job_observations(
                fixture["owner_engine"],
                job_id=int(queued_job["id"]),
            )

    assert recovery_result == {"requeued_count": 1, "failed_count": 0}
    assert run_result.status is WorkerRunStatus.LEASE_LOST
    assert run_result.error_code == "lease_lost"
    assert worker.last_heartbeat_controller is not None
    assert worker.last_heartbeat_controller.has_lease_lost() is True
    assert worker.last_heartbeat_controller.is_alive() is False
    assert worker.persist_success_calls == 0
    assert worker.persist_failure_calls == 0
    assert observation_count == 0
    assert claim_b["status"] == "running"
    assert final_row["status"] == "running"
    assert final_row["locked_by"] == "p22d3-zombie-worker-b"
    assert final_row["finished_at"] is None
    assert str(final_row["lease_token"]) == lease_b


def test_worker_run_once_performs_stale_recovery_before_claim_when_interval_enabled(caplog):
    with _lease_fixture() as fixture:
        with fixture["owner_engine"].begin() as conn:
            stale_job = _insert_job(
                conn,
                organization_id=fixture["organization_id"],
                lote_id=fixture["lote_id"],
                status="running",
                attempt_count=1,
                max_attempts=3,
                locked_at=_db_now(conn) - timedelta(minutes=3),
                locked_by="stale-before-claim",
                heartbeat_at=_db_now(conn) - timedelta(minutes=2),
                lease_token=str(uuid4()),
                started_at=_db_now(conn) - timedelta(minutes=3),
            )
            _mark_job_stale(conn, job_id=int(stale_job["id"]))

        with _ephemeral_worker_login(fixture["owner_engine"]) as worker_auth:
            worker_login_engine = create_engine(
                normalize_database_url(worker_auth["worker_database_url"]),
                pool_size=2,
                max_overflow=0,
                pool_pre_ping=True,
            )
            worker_auth["register_engine"](worker_login_engine)

            worker = SatelliteWorker(
                worker_id="p22d3-reaper-worker",
                heartbeat_seconds=1,
                stale_recovery_interval_seconds=30,
                claim_session_factory=_worker_session_factory(worker_login_engine),
                tenant_session_factory=_runtime_session_factory(
                    fixture["runtime_engine"]
                ),
                gee_ndvi_adapter=_BlockingGeeAdapter(
                    result=_sample_result(
                        _POLYGON_GEOMETRY_HASH,
                        ALGORITHM_VERSION,
                    )
                ),
            )
            adapter = worker._gee_ndvi_adapter
            assert isinstance(adapter, _BlockingGeeAdapter)
            adapter.release.set()

            with caplog.at_level(
                logging.INFO,
                logger="litoral_trace.workers.satellite_worker",
            ):
                run_result = worker.run_once()

            final_row = _fetch_job_row(
                fixture["owner_engine"],
                job_id=int(stale_job["id"]),
            )

    assert run_result.status is WorkerRunStatus.SUCCEEDED
    assert final_row["status"] == "succeeded"
    assert final_row["attempt_count"] == 2
    assert final_row["started_at"] is not None
    recovery_logs = [
        record
        for record in caplog.records
        if record.getMessage() == "satellite_worker_stale_recovery"
    ]
    assert len(recovery_logs) == 1
    assert recovery_logs[0].requeued_count == 1
    assert recovery_logs[0].failed_count == 0


def test_recovery_return_contract_exposes_only_counts():
    with _lease_fixture() as fixture:
        with _ephemeral_worker_login(fixture["owner_engine"]) as worker_auth:
            worker_login_engine = create_engine(
                normalize_database_url(worker_auth["worker_database_url"]),
                pool_size=1,
                max_overflow=0,
                pool_pre_ping=True,
            )
            worker_auth["register_engine"](worker_login_engine)

            result = _recover_once(
                worker_login_engine,
                requested_batch_size=10,
            )

    assert set(result.keys()) == {"requeued_count", "failed_count"}
