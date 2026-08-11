from __future__ import annotations

import os
import secrets
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest
from psycopg import ClientCursor, sql
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import sessionmaker

from litoral_trace.config.settings import normalize_database_url
from litoral_trace.db.worker import reset_worker_engine_state
from litoral_trace.services.gee import ALGORITHM_VERSION, generate_geometry_hash
from litoral_trace.services.satellite_ndvi_processing import (
    SatelliteJobLeaseLostError,
    schedule_satellite_job_retry,
)
from litoral_trace.workers.satellite_worker import (
    RetryDisposition,
    SatelliteWorker,
    SatelliteWorkerExecutionError,
    WorkerRunStatus,
)


def _truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}


POSTGRES_RLS_TESTS_ENABLED = _truthy(os.environ.get("ENABLE_POSTGRES_TESTS"))
RUNTIME_TEST_DATABASE_URL = os.environ.get("TEST_POSTGRES_DATABASE_URL")
MIGRATION_TEST_DATABASE_URL = os.environ.get("TEST_POSTGRES_MIGRATION_DATABASE_URL")
WORKER_ROLE = "litoral_trace_worker_executor"
RUNTIME_ROLE = "litoral_trace_app"
CLAIM_FUNCTION_SIGNATURE = "public.worker_claim_next_satellite_job(text)"
RECOVER_FUNCTION_SIGNATURE = "public.worker_recover_stale_satellite_jobs(integer)"
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
        "PostgreSQL P2.2D-4 tests require ENABLE_POSTGRES_TESTS=1 plus isolated "
        "TEST_POSTGRES_DATABASE_URL and TEST_POSTGRES_MIGRATION_DATABASE_URL."
    ),
)


def _runtime_engine():
    return create_engine(
        normalize_database_url(RUNTIME_TEST_DATABASE_URL),
        pool_size=4,
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


def _move_next_attempt_at_to_past(conn, *, job_id: int) -> None:
    conn.execute(
        text(
            """
            UPDATE satellite_jobs
            SET next_attempt_at = CURRENT_TIMESTAMP - INTERVAL '1 minute',
                updated_at = CURRENT_TIMESTAMP
            WHERE id = :job_id
            """
        ),
        {"job_id": job_id},
    )


def _requeue_job_for_reclaim(conn, *, job_id: int) -> None:
    conn.execute(
        text(
            """
            UPDATE satellite_jobs
            SET status = 'queued',
                next_attempt_at = CURRENT_TIMESTAMP - INTERVAL '1 minute',
                locked_at = NULL,
                locked_by = NULL,
                heartbeat_at = NULL,
                lease_token = NULL,
                finished_at = NULL,
                error_code = NULL,
                error_message = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = :job_id
            """
        ),
        {"job_id": job_id},
    )


def _assert_approx_seconds(
    delta: timedelta,
    *,
    expected_seconds: int,
    tolerance_seconds: float = 2.0,
) -> None:
    assert abs(delta.total_seconds() - expected_seconds) <= tolerance_seconds


def _wait_until(
    predicate,
    *,
    timeout_seconds: float,
    interval_seconds: float = 0.05,
) -> None:
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


class _RetryableErrorAdapter:
    def __init__(self, error_code: str = "gee_temporary_service_failure"):
        self.error_code = error_code

    def execute(self, request):
        raise SatelliteWorkerExecutionError(
            self.error_code,
            "temporary upstream failure",
            retry_disposition=RetryDisposition.RETRYABLE,
        )


class _NonRetryableErrorAdapter:
    def __init__(self, error_code: str = "invalid_job_payload"):
        self.error_code = error_code

    def execute(self, request):
        raise SatelliteWorkerExecutionError(
            self.error_code,
            "non retryable failure",
            retry_disposition=RetryDisposition.NON_RETRYABLE,
        )


class _BlockingRetryableAdapter:
    def __init__(self, error_code: str = "gee_temporary_service_failure"):
        self.error_code = error_code
        self.started = threading.Event()
        self.release = threading.Event()

    def execute(self, request):
        self.started.set()
        if not self.release.wait(10):
            raise AssertionError("Timed out waiting to release retryable adapter.")
        raise SatelliteWorkerExecutionError(
            self.error_code,
            "temporary upstream failure",
            retry_disposition=RetryDisposition.RETRYABLE,
        )


class _ObservingRetryWorker(SatelliteWorker):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.last_heartbeat_controller = None

    def _create_heartbeat_controller(self, context):
        controller = super()._create_heartbeat_controller(context)
        self.last_heartbeat_controller = controller
        return controller


class _OrderingRetryWorker(_ObservingRetryWorker):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.heartbeat_alive_when_retry_started: bool | None = None

    def _schedule_retry(self, context, *, retry_delay_seconds: int):
        assert self.last_heartbeat_controller is not None
        self.heartbeat_alive_when_retry_started = (
            self.last_heartbeat_controller.is_alive()
        )
        return super()._schedule_retry(
            context,
            retry_delay_seconds=retry_delay_seconds,
        )


@contextmanager
def _lease_fixture():
    runtime_engine = _runtime_engine()
    owner_engine = _owner_engine()
    suffix = uuid4().hex[:8]

    with owner_engine.begin() as conn:
        org_a_id = conn.execute(
            text(
                """
                INSERT INTO organizations (name, slug, tax_id, tier, is_active)
                VALUES (:name, :slug, :tax_id, 'pro', true)
                RETURNING id
                """
            ),
            {
                "name": f"P22D4 Org A {suffix}",
                "slug": f"p22d4-org-a-{suffix}",
                "tax_id": f"95-a-{suffix}",
            },
        ).scalar_one()
        org_b_id = conn.execute(
            text(
                """
                INSERT INTO organizations (name, slug, tax_id, tier, is_active)
                VALUES (:name, :slug, :tax_id, 'pro', true)
                RETURNING id
                """
            ),
            {
                "name": f"P22D4 Org B {suffix}",
                "slug": f"p22d4-org-b-{suffix}",
                "tax_id": f"95-b-{suffix}",
            },
        ).scalar_one()
        lote_a_id = conn.execute(
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
                "organization_id": org_a_id,
                "identificador": f"P22D4-LOTE-A-{suffix}",
                "productor_id": f"60-a-{suffix}",
                "polygon_wkt": _POLYGON_WKT_SNAPSHOT,
            },
        ).scalar_one()
        lote_b_id = conn.execute(
            text(
                """
                INSERT INTO lotes (
                    organization_id, identificador, productor_id, producto_forestal,
                    hectareas, latitud, longitud, polygon_wkt, estatus,
                    volumen_ingresado_ton, volumen_exportar_ton
                )
                VALUES (
                    :organization_id, :identificador, :productor_id,
                    'Madera Aserrada (Pino)', 11.0, -27.55, -58.80,
                    :polygon_wkt, 'Pendiente', 11.0, 4.0
                )
                RETURNING id
                """
            ),
            {
                "organization_id": org_b_id,
                "identificador": f"P22D4-LOTE-B-{suffix}",
                "productor_id": f"60-b-{suffix}",
                "polygon_wkt": (
                    "POLYGON((-58.81 -27.56, -58.79 -27.56, -58.79 -27.54, "
                    "-58.81 -27.54, -58.81 -27.56))"
                ),
            },
        ).scalar_one()

    fixture = {
        "runtime_engine": runtime_engine,
        "owner_engine": owner_engine,
        "organization_id": int(org_a_id),
        "organization_b_id": int(org_b_id),
        "lote_id": int(lote_a_id),
        "lote_b_id": int(lote_b_id),
    }

    try:
        yield fixture
    finally:
        with owner_engine.begin() as conn:
            conn.execute(
                text(
                    "DELETE FROM satellite_ndvi_observations "
                    "WHERE organization_id IN (:org_a_id, :org_b_id)"
                ),
                {"org_a_id": org_a_id, "org_b_id": org_b_id},
            )
            conn.execute(
                text(
                    "DELETE FROM satellite_jobs "
                    "WHERE organization_id IN (:org_a_id, :org_b_id)"
                ),
                {"org_a_id": org_a_id, "org_b_id": org_b_id},
            )
            conn.execute(
                text(
                    "DELETE FROM lotes WHERE id IN (:lote_a_id, :lote_b_id)"
                ),
                {"lote_a_id": lote_a_id, "lote_b_id": lote_b_id},
            )
            conn.execute(
                text(
                    "DELETE FROM organizations "
                    "WHERE id IN (:org_a_id, :org_b_id)"
                ),
                {"org_a_id": org_a_id, "org_b_id": org_b_id},
            )
        runtime_engine.dispose()
        owner_engine.dispose()


def test_runtime_role_and_worker_claim_privileges_remain_hardened_for_d4():
    with _lease_fixture() as fixture:
        with fixture["owner_engine"].connect() as conn:
            runtime_role_row = conn.execute(
                text(
                    """
                    SELECT rolbypassrls
                    FROM pg_roles
                    WHERE rolname = :role_name
                    """
                ),
                {"role_name": RUNTIME_ROLE},
            ).mappings().one()
            worker_role_row = conn.execute(
                text(
                    """
                    SELECT rolcanlogin, rolbypassrls
                    FROM pg_roles
                    WHERE rolname = :role_name
                    """
                ),
                {"role_name": WORKER_ROLE},
            ).mappings().one()
            retry_function_count = conn.execute(
                text(
                    """
                    SELECT count(*)
                    FROM pg_proc
                    WHERE pronamespace = 'public'::regnamespace
                      AND proname ILIKE '%retry%'
                    """
                )
            ).scalar_one()

        assert runtime_role_row["rolbypassrls"] is False
        assert worker_role_row["rolcanlogin"] is False
        assert worker_role_row["rolbypassrls"] is False
        assert int(retry_function_count) == 0

        with _ephemeral_worker_login(fixture["owner_engine"]) as worker_auth:
            worker_login_engine = create_engine(
                normalize_database_url(worker_auth["worker_database_url"]),
                pool_size=1,
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
                            has_function_privilege(
                                current_user,
                                :claim_signature,
                                'EXECUTE'
                            ) AS can_execute_claim,
                            has_function_privilege(
                                current_user,
                                :recover_signature,
                                'EXECUTE'
                            ) AS can_execute_recover
                        """
                    ),
                    {
                        "claim_signature": CLAIM_FUNCTION_SIGNATURE,
                        "recover_signature": RECOVER_FUNCTION_SIGNATURE,
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
                "UPDATE public.satellite_jobs SET status = 'queued' WHERE false",
            )


def test_retry_schedule_requeues_with_runtime_rls_and_blocks_immediate_reclaim():
    with _lease_fixture() as fixture:
        with _ephemeral_worker_login(fixture["owner_engine"]) as worker_auth:
            worker_login_engine = create_engine(
                normalize_database_url(worker_auth["worker_database_url"]),
                pool_size=2,
                max_overflow=0,
                pool_pre_ping=True,
            )
            worker_auth["register_engine"](worker_login_engine)

            with fixture["owner_engine"].begin() as conn:
                queued_job = _insert_job(
                    conn,
                    organization_id=fixture["organization_id"],
                    lote_id=fixture["lote_id"],
                    max_attempts=3,
                )

            claim_a = _claim_once(worker_login_engine, worker_id="p22d4-worker-a")
            assert claim_a is not None
            lease_a = str(claim_a["lease_token"])
            started_at_a = claim_a["started_at"]
            assert UUID(lease_a)
            assert claim_a["status"] == "running"
            assert int(claim_a["attempt_count"]) == 1

            runtime_session = _runtime_session_factory(fixture["runtime_engine"])()
            try:
                schedule_result = schedule_satellite_job_retry(
                    runtime_session,
                    organization_id=fixture["organization_id"],
                    job_id=int(queued_job["id"]),
                    worker_id="p22d4-worker-a",
                    lease_token=lease_a,
                    retry_delay_seconds=30,
                )
                runtime_session.commit()
            finally:
                runtime_session.close()

            scheduled_row = _fetch_job_row(
                fixture["owner_engine"],
                job_id=int(queued_job["id"]),
            )
            immediate_claim = _claim_once(
                worker_login_engine,
                worker_id="p22d4-worker-b",
            )

            with fixture["owner_engine"].begin() as conn:
                _move_next_attempt_at_to_past(
                    conn,
                    job_id=int(queued_job["id"]),
                )

            claim_b = _claim_once(worker_login_engine, worker_id="p22d4-worker-b")
            assert claim_b is not None
            lease_b = str(claim_b["lease_token"])
            assert UUID(lease_b)
            assert lease_b != lease_a

    assert schedule_result.next_attempt_at.tzinfo is not None
    assert scheduled_row["status"] == "queued"
    assert scheduled_row["attempt_count"] == 1
    assert scheduled_row["started_at"] == started_at_a
    assert scheduled_row["locked_at"] is None
    assert scheduled_row["locked_by"] is None
    assert scheduled_row["heartbeat_at"] is None
    assert scheduled_row["lease_token"] is None
    assert scheduled_row["finished_at"] is None
    assert scheduled_row["error_code"] is None
    assert scheduled_row["error_message"] is None
    assert immediate_claim is None
    assert claim_b["status"] == "running"
    assert int(claim_b["attempt_count"]) == 2
    assert claim_b["started_at"] == started_at_a


def test_retry_schedule_uses_statement_timestamp_and_db_clock_for_delay():
    with _lease_fixture() as fixture:
        worker_id = "p22d4-clock-worker"
        lease_token = str(uuid4())

        with fixture["owner_engine"].begin() as conn:
            job = _insert_job(
                conn,
                organization_id=fixture["organization_id"],
                lote_id=fixture["lote_id"],
                status="running",
                attempt_count=1,
                max_attempts=3,
                locked_at=_db_now(conn) - timedelta(minutes=2),
                locked_by=worker_id,
                heartbeat_at=_db_now(conn) - timedelta(minutes=1),
                lease_token=lease_token,
                started_at=_db_now(conn) - timedelta(minutes=2),
            )

        runtime_session = _runtime_session_factory(fixture["runtime_engine"])()
        try:
            tx_started_at = runtime_session.execute(
                text("SELECT transaction_timestamp()")
            ).scalar_one()
            runtime_session.execute(text("SELECT pg_sleep(1.1)"))
            schedule_satellite_job_retry(
                runtime_session,
                organization_id=fixture["organization_id"],
                job_id=int(job["id"]),
                worker_id=worker_id,
                lease_token=lease_token,
                retry_delay_seconds=30,
            )
            runtime_session.commit()
        finally:
            runtime_session.close()

        final_row = _fetch_job_row(fixture["owner_engine"], job_id=int(job["id"]))

    assert final_row["updated_at"] > tx_started_at + timedelta(seconds=0.9)
    assert final_row["next_attempt_at"] > final_row["updated_at"]
    _assert_approx_seconds(
        final_row["next_attempt_at"] - final_row["updated_at"],
        expected_seconds=30,
    )


def test_worker_run_once_retryable_returns_retry_scheduled_and_stops_heartbeat_before_persistence():
    with _lease_fixture() as fixture:
        with fixture["owner_engine"].begin() as conn:
            _insert_job(
                conn,
                organization_id=fixture["organization_id"],
                lote_id=fixture["lote_id"],
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

            worker = _OrderingRetryWorker(
                worker_id="p22d4-worker-retry",
                heartbeat_seconds=1,
                claim_session_factory=_worker_session_factory(worker_login_engine),
                tenant_session_factory=_runtime_session_factory(
                    fixture["runtime_engine"]
                ),
                gee_ndvi_adapter=_RetryableErrorAdapter(),
                retry_base_seconds=30,
                retry_max_seconds=900,
            )

            result = worker.run_once()
            with fixture["owner_engine"].connect() as conn:
                job_row = conn.execute(
                    text(
                        """
                        SELECT id, status, attempt_count, next_attempt_at, locked_at,
                               locked_by, heartbeat_at, lease_token, updated_at
                        FROM satellite_jobs
                        WHERE organization_id = :organization_id
                        ORDER BY id DESC
                        LIMIT 1
                        """
                    ),
                    {"organization_id": fixture["organization_id"]},
                ).mappings().one()
            observation_count = _count_job_observations(
                fixture["owner_engine"],
                job_id=int(job_row["id"]),
            )

    assert result.status is WorkerRunStatus.RETRY_SCHEDULED
    assert result.error_code == "gee_temporary_service_failure"
    assert worker.heartbeat_alive_when_retry_started is False
    assert job_row["status"] == "queued"
    assert int(job_row["attempt_count"]) == 1
    assert job_row["next_attempt_at"] > job_row["updated_at"]
    assert job_row["locked_at"] is None
    assert job_row["locked_by"] is None
    assert job_row["heartbeat_at"] is None
    assert job_row["lease_token"] is None
    assert observation_count == 0


def test_retryable_exhausted_is_terminal_and_retains_terminal_lease_and_error_code():
    with _lease_fixture() as fixture:
        with fixture["owner_engine"].begin() as conn:
            _insert_job(
                conn,
                organization_id=fixture["organization_id"],
                lote_id=fixture["lote_id"],
                max_attempts=1,
            )

        with _ephemeral_worker_login(fixture["owner_engine"]) as worker_auth:
            worker_login_engine = create_engine(
                normalize_database_url(worker_auth["worker_database_url"]),
                pool_size=2,
                max_overflow=0,
                pool_pre_ping=True,
            )
            worker_auth["register_engine"](worker_login_engine)

            worker = _ObservingRetryWorker(
                worker_id="p22d4-worker-exhausted",
                heartbeat_seconds=1,
                claim_session_factory=_worker_session_factory(worker_login_engine),
                tenant_session_factory=_runtime_session_factory(
                    fixture["runtime_engine"]
                ),
                gee_ndvi_adapter=_RetryableErrorAdapter(),
            )

            result = worker.run_once()
            with fixture["owner_engine"].connect() as conn:
                job_row = conn.execute(
                    text(
                        """
                        SELECT id, status, attempt_count, max_attempts, locked_at,
                               locked_by, heartbeat_at, lease_token, finished_at,
                               error_code, error_message
                        FROM satellite_jobs
                        WHERE organization_id = :organization_id
                        ORDER BY id DESC
                        LIMIT 1
                        """
                    ),
                    {"organization_id": fixture["organization_id"]},
                ).mappings().one()
            observation_count = _count_job_observations(
                fixture["owner_engine"],
                job_id=int(job_row["id"]),
            )

    assert result.status is WorkerRunStatus.FAILED
    assert result.error_code == "gee_temporary_service_failure"
    assert job_row["status"] == "failed"
    assert int(job_row["attempt_count"]) == int(job_row["max_attempts"]) == 1
    assert job_row["locked_at"] is None
    assert job_row["locked_by"] is None
    assert job_row["heartbeat_at"] is None
    assert job_row["finished_at"] is not None
    assert job_row["lease_token"] is not None
    assert UUID(str(job_row["lease_token"]))
    assert job_row["error_code"] == "gee_temporary_service_failure"
    assert "retry_exhausted" not in str(job_row["error_code"])
    assert job_row["error_message"] is not None
    assert observation_count == 0


def test_non_retryable_failure_is_terminal_without_requeue():
    with _lease_fixture() as fixture:
        with fixture["owner_engine"].begin() as conn:
            _insert_job(
                conn,
                organization_id=fixture["organization_id"],
                lote_id=fixture["lote_id"],
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

            worker = _ObservingRetryWorker(
                worker_id="p22d4-worker-non-retryable",
                heartbeat_seconds=1,
                claim_session_factory=_worker_session_factory(worker_login_engine),
                tenant_session_factory=_runtime_session_factory(
                    fixture["runtime_engine"]
                ),
                gee_ndvi_adapter=_NonRetryableErrorAdapter(),
            )

            result = worker.run_once()
            with fixture["owner_engine"].connect() as conn:
                job_row = conn.execute(
                    text(
                        """
                        SELECT id, status, attempt_count, locked_at, locked_by,
                               heartbeat_at, lease_token, finished_at,
                               error_code, error_message
                        FROM satellite_jobs
                        WHERE organization_id = :organization_id
                        ORDER BY id DESC
                        LIMIT 1
                        """
                    ),
                    {"organization_id": fixture["organization_id"]},
                ).mappings().one()
            observation_count = _count_job_observations(
                fixture["owner_engine"],
                job_id=int(job_row["id"]),
            )

    assert result.status is WorkerRunStatus.FAILED
    assert result.error_code == "invalid_job_payload"
    assert job_row["status"] == "failed"
    assert int(job_row["attempt_count"]) == 1
    assert job_row["locked_at"] is None
    assert job_row["locked_by"] is None
    assert job_row["heartbeat_at"] is None
    assert job_row["finished_at"] is not None
    assert job_row["lease_token"] is not None
    assert UUID(str(job_row["lease_token"]))
    assert job_row["error_code"] == "invalid_job_payload"
    assert job_row["error_message"] is not None
    assert observation_count == 0


def test_zombie_old_lease_cannot_schedule_retry_after_reclaim():
    with _lease_fixture() as fixture:
        with _ephemeral_worker_login(fixture["owner_engine"]) as worker_auth:
            worker_login_engine = create_engine(
                normalize_database_url(worker_auth["worker_database_url"]),
                pool_size=3,
                max_overflow=0,
                pool_pre_ping=True,
            )
            worker_auth["register_engine"](worker_login_engine)

            with fixture["owner_engine"].begin() as conn:
                queued_job = _insert_job(
                    conn,
                    organization_id=fixture["organization_id"],
                    lote_id=fixture["lote_id"],
                    max_attempts=3,
                )

            claim_a = _claim_once(worker_login_engine, worker_id="p22d4-zombie-a")
            assert claim_a is not None
            lease_a = str(claim_a["lease_token"])

            runtime_session = _runtime_session_factory(fixture["runtime_engine"])()
            try:
                schedule_satellite_job_retry(
                    runtime_session,
                    organization_id=fixture["organization_id"],
                    job_id=int(queued_job["id"]),
                    worker_id="p22d4-zombie-a",
                    lease_token=lease_a,
                    retry_delay_seconds=30,
                )
                runtime_session.commit()
            finally:
                runtime_session.close()

            with fixture["owner_engine"].begin() as conn:
                _move_next_attempt_at_to_past(conn, job_id=int(queued_job["id"]))

            claim_b = _claim_once(worker_login_engine, worker_id="p22d4-zombie-b")
            assert claim_b is not None
            lease_b = str(claim_b["lease_token"])
            assert lease_b != lease_a

            runtime_session = _runtime_session_factory(fixture["runtime_engine"])()
            try:
                with pytest.raises(SatelliteJobLeaseLostError):
                    schedule_satellite_job_retry(
                        runtime_session,
                        organization_id=fixture["organization_id"],
                        job_id=int(queued_job["id"]),
                        worker_id="p22d4-zombie-a",
                        lease_token=lease_a,
                        retry_delay_seconds=30,
                    )
                runtime_session.rollback()
            finally:
                runtime_session.close()

            final_row = _fetch_job_row(
                fixture["owner_engine"],
                job_id=int(queued_job["id"]),
            )

    assert final_row["status"] == "running"
    assert final_row["locked_by"] == "p22d4-zombie-b"
    assert str(final_row["lease_token"]) == lease_b
    assert final_row["finished_at"] is None
    assert int(final_row["attempt_count"]) == 2


@pytest.mark.parametrize(
    ("case_name", "mutator"),
    [
        (
            "wrong_worker",
            lambda fixture, job, lease: {
                "organization_id": fixture["organization_id"],
                "job_id": int(job["id"]),
                "worker_id": "p22d4-wrong-worker",
                "lease_token": lease,
            },
        ),
        (
            "wrong_lease",
            lambda fixture, job, lease: {
                "organization_id": fixture["organization_id"],
                "job_id": int(job["id"]),
                "worker_id": "p22d4-right-worker",
                "lease_token": str(uuid4()),
            },
        ),
        (
            "wrong_org",
            lambda fixture, job, lease: {
                "organization_id": fixture["organization_b_id"],
                "job_id": int(job["id"]),
                "worker_id": "p22d4-right-worker",
                "lease_token": lease,
            },
        ),
    ],
)
def test_retry_schedule_wrong_worker_wrong_lease_or_wrong_org_raise_lease_lost(
    case_name,
    mutator,
):
    with _lease_fixture() as fixture:
        worker_id = "p22d4-right-worker"
        lease_token = str(uuid4())

        with fixture["owner_engine"].begin() as conn:
            job = _insert_job(
                conn,
                organization_id=fixture["organization_id"],
                lote_id=fixture["lote_id"],
                status="running",
                attempt_count=1,
                max_attempts=3,
                locked_at=_db_now(conn) - timedelta(minutes=2),
                locked_by=worker_id,
                heartbeat_at=_db_now(conn) - timedelta(minutes=1),
                lease_token=lease_token,
                started_at=_db_now(conn) - timedelta(minutes=2),
            )

        runtime_session = _runtime_session_factory(fixture["runtime_engine"])()
        try:
            kwargs = mutator(fixture, job, lease_token)
            with pytest.raises(SatelliteJobLeaseLostError):
                schedule_satellite_job_retry(
                    runtime_session,
                    retry_delay_seconds=30,
                    **kwargs,
                )
            runtime_session.rollback()
        finally:
            runtime_session.close()

        final_row = _fetch_job_row(fixture["owner_engine"], job_id=int(job["id"]))

    assert final_row["status"] == "running", case_name
    assert final_row["locked_by"] == worker_id, case_name
    assert str(final_row["lease_token"]) == lease_token, case_name


def test_retry_schedule_non_running_status_raises_lease_lost():
    with _lease_fixture() as fixture:
        worker_id = "p22d4-terminal-worker"
        lease_token = str(uuid4())

        with fixture["owner_engine"].begin() as conn:
            job = _insert_job(
                conn,
                organization_id=fixture["organization_id"],
                lote_id=fixture["lote_id"],
                status="failed",
                attempt_count=1,
                max_attempts=3,
                locked_at=_db_now(conn) - timedelta(minutes=2),
                locked_by=worker_id,
                heartbeat_at=_db_now(conn) - timedelta(minutes=1),
                lease_token=lease_token,
                started_at=_db_now(conn) - timedelta(minutes=2),
                finished_at=_db_now(conn),
            )

        runtime_session = _runtime_session_factory(fixture["runtime_engine"])()
        try:
            with pytest.raises(SatelliteJobLeaseLostError):
                schedule_satellite_job_retry(
                    runtime_session,
                    organization_id=fixture["organization_id"],
                    job_id=int(job["id"]),
                    worker_id=worker_id,
                    lease_token=lease_token,
                    retry_delay_seconds=30,
                )
            runtime_session.rollback()
        finally:
            runtime_session.close()

        final_row = _fetch_job_row(fixture["owner_engine"], job_id=int(job["id"]))

    assert final_row["status"] == "failed"
    assert final_row["finished_at"] is not None
    assert str(final_row["lease_token"]) == lease_token


def test_two_concurrent_retry_schedulers_allow_exactly_one_success():
    with _lease_fixture() as fixture:
        worker_id = "p22d4-concurrent-worker"
        lease_token = str(uuid4())

        with fixture["owner_engine"].begin() as conn:
            job = _insert_job(
                conn,
                organization_id=fixture["organization_id"],
                lote_id=fixture["lote_id"],
                status="running",
                attempt_count=1,
                max_attempts=3,
                locked_at=_db_now(conn) - timedelta(minutes=2),
                locked_by=worker_id,
                heartbeat_at=_db_now(conn) - timedelta(minutes=1),
                lease_token=lease_token,
                started_at=_db_now(conn) - timedelta(minutes=2),
            )

        barrier = threading.Barrier(2)

        def _runner():
            runtime_session = _runtime_session_factory(fixture["runtime_engine"])()
            try:
                barrier.wait(timeout=5)
                schedule_satellite_job_retry(
                    runtime_session,
                    organization_id=fixture["organization_id"],
                    job_id=int(job["id"]),
                    worker_id=worker_id,
                    lease_token=lease_token,
                    retry_delay_seconds=30,
                )
                runtime_session.commit()
                return "scheduled"
            except SatelliteJobLeaseLostError:
                runtime_session.rollback()
                return "lease_lost"
            finally:
                runtime_session.close()

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(lambda _: _runner(), range(2)))

        final_row = _fetch_job_row(fixture["owner_engine"], job_id=int(job["id"]))

    assert sorted(results) == ["lease_lost", "scheduled"]
    assert final_row["status"] == "queued"
    assert int(final_row["attempt_count"]) == 1
    assert final_row["lease_token"] is None
    assert final_row["locked_by"] is None
    assert final_row["heartbeat_at"] is None
    assert final_row["next_attempt_at"] > final_row["updated_at"]


def test_backoff_progression_and_cap_are_persisted_by_postgresql():
    with _lease_fixture() as fixture:
        with _ephemeral_worker_login(fixture["owner_engine"]) as worker_auth:
            worker_login_engine = create_engine(
                normalize_database_url(worker_auth["worker_database_url"]),
                pool_size=2,
                max_overflow=0,
                pool_pre_ping=True,
            )
            worker_auth["register_engine"](worker_login_engine)

            with fixture["owner_engine"].begin() as conn:
                job = _insert_job(
                    conn,
                    organization_id=fixture["organization_id"],
                    lote_id=fixture["lote_id"],
                    max_attempts=5,
                )

            observed_deltas: list[float] = []
            expected_delays = [30, 60, 120]
            claim = None

            for worker_suffix, expected_delay in enumerate(expected_delays, start=1):
                claim = _claim_once(
                    worker_login_engine,
                    worker_id=f"p22d4-backoff-{worker_suffix}",
                )
                assert claim is not None

                runtime_session = _runtime_session_factory(fixture["runtime_engine"])()
                try:
                    schedule_satellite_job_retry(
                        runtime_session,
                        organization_id=fixture["organization_id"],
                        job_id=int(job["id"]),
                        worker_id=f"p22d4-backoff-{worker_suffix}",
                        lease_token=str(claim["lease_token"]),
                        retry_delay_seconds=expected_delay,
                    )
                    runtime_session.commit()
                finally:
                    runtime_session.close()

                scheduled_row = _fetch_job_row(
                    fixture["owner_engine"],
                    job_id=int(job["id"]),
                )
                observed_deltas.append(
                    (
                        scheduled_row["next_attempt_at"]
                        - scheduled_row["updated_at"]
                    ).total_seconds()
                )

                with fixture["owner_engine"].begin() as conn:
                    _move_next_attempt_at_to_past(conn, job_id=int(job["id"]))

            runtime_session = _runtime_session_factory(fixture["runtime_engine"])()
            try:
                cap_job = None
                with fixture["owner_engine"].begin() as conn:
                    cap_job = _insert_job(
                        conn,
                        organization_id=fixture["organization_id"],
                        lote_id=fixture["lote_id"],
                        status="running",
                        attempt_count=7,
                        max_attempts=10,
                        locked_at=_db_now(conn) - timedelta(minutes=2),
                        locked_by="p22d4-cap-worker",
                        heartbeat_at=_db_now(conn) - timedelta(minutes=1),
                        lease_token=str(uuid4()),
                        started_at=_db_now(conn) - timedelta(minutes=2),
                    )
                schedule_satellite_job_retry(
                    runtime_session,
                    organization_id=fixture["organization_id"],
                    job_id=int(cap_job["id"]),
                    worker_id="p22d4-cap-worker",
                    lease_token=str(cap_job["lease_token"]),
                    retry_delay_seconds=900,
                )
                runtime_session.commit()
            finally:
                runtime_session.close()

            cap_row = _fetch_job_row(
                fixture["owner_engine"],
                job_id=int(cap_job["id"]),
            )

    for observed, expected in zip(observed_deltas, expected_delays, strict=True):
        assert abs(observed - expected) <= 2.0
    _assert_approx_seconds(
        cap_row["next_attempt_at"] - cap_row["updated_at"],
        expected_seconds=900,
    )


def test_lease_loss_beats_retry_and_preserves_new_worker_authority():
    with _lease_fixture() as fixture:
        with fixture["owner_engine"].begin() as conn:
            queued_job = _insert_job(
                conn,
                organization_id=fixture["organization_id"],
                lote_id=fixture["lote_id"],
                max_attempts=3,
            )

        with _ephemeral_worker_login(fixture["owner_engine"]) as worker_auth:
            worker_login_engine = create_engine(
                normalize_database_url(worker_auth["worker_database_url"]),
                pool_size=3,
                max_overflow=0,
                pool_pre_ping=True,
            )
            worker_auth["register_engine"](worker_login_engine)

            adapter = _BlockingRetryableAdapter()
            worker = _ObservingRetryWorker(
                worker_id="p22d4-lease-lost-a",
                heartbeat_seconds=1,
                claim_session_factory=_worker_session_factory(worker_login_engine),
                tenant_session_factory=_runtime_session_factory(
                    fixture["runtime_engine"]
                ),
                gee_ndvi_adapter=adapter,
            )

            worker_thread, result_holder, error_holder = _run_worker_in_background(worker)
            assert adapter.started.wait(10)

            with fixture["owner_engine"].connect() as conn:
                claim_a_row = conn.execute(
                    text(
                        """
                        SELECT id, status, locked_by, lease_token
                        FROM satellite_jobs
                        WHERE id = :job_id
                        """
                    ),
                    {"job_id": int(queued_job["id"])},
                ).mappings().one()
            lease_a = str(claim_a_row["lease_token"])

            with fixture["owner_engine"].begin() as conn:
                _requeue_job_for_reclaim(conn, job_id=int(queued_job["id"]))

            claim_b = _claim_once(worker_login_engine, worker_id="p22d4-lease-lost-b")
            assert claim_b is not None
            lease_b = str(claim_b["lease_token"])
            assert lease_b != lease_a

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

    assert run_result.status is WorkerRunStatus.LEASE_LOST
    assert run_result.error_code == "lease_lost"
    assert final_row["status"] == "running"
    assert final_row["locked_by"] == "p22d4-lease-lost-b"
    assert str(final_row["lease_token"]) == lease_b
    assert final_row["finished_at"] is None
    assert observation_count == 0


def test_graceful_shutdown_during_retryable_job_still_schedules_retry_and_stops_future_claims():
    with _lease_fixture() as fixture:
        with fixture["owner_engine"].begin() as conn:
            queued_job = _insert_job(
                conn,
                organization_id=fixture["organization_id"],
                lote_id=fixture["lote_id"],
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

            adapter = _BlockingRetryableAdapter()
            worker = _ObservingRetryWorker(
                worker_id="p22d4-shutdown-worker",
                heartbeat_seconds=1,
                claim_session_factory=_worker_session_factory(worker_login_engine),
                tenant_session_factory=_runtime_session_factory(
                    fixture["runtime_engine"]
                ),
                gee_ndvi_adapter=adapter,
            )

            worker_thread, result_holder, error_holder = _run_worker_in_background(worker)
            assert adapter.started.wait(10)
            worker.request_shutdown()
            adapter.release.set()
            worker_thread.join(timeout=10)

            if "error" in error_holder:
                raise error_holder["error"]

            first_result = result_holder["result"]
            second_result = worker.run_once()
            final_row = _fetch_job_row(
                fixture["owner_engine"],
                job_id=int(queued_job["id"]),
            )

    assert first_result.status is WorkerRunStatus.RETRY_SCHEDULED
    assert second_result.status is WorkerRunStatus.STOPPED
    assert final_row["status"] == "queued"
    assert final_row["lease_token"] is None
    assert final_row["locked_by"] is None
