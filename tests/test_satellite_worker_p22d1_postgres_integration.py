from __future__ import annotations

import os
import secrets
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest
from psycopg import ClientCursor, sql
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import sessionmaker

from litoral_trace.config.settings import normalize_database_url
from litoral_trace.db.engine import reset_engine_state
from litoral_trace.db.worker import reset_worker_engine_state
from litoral_trace.services.gee import ALGORITHM_VERSION
from litoral_trace.services.satellite_ndvi_processing import (
    NormalizedNdviExecutionResult,
    NdviObservationRecord,
    SatelliteJobLeaseLostError,
    mark_satellite_job_failed,
    mark_satellite_job_succeeded,
    persist_ndvi_execution_result,
)


def _truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}


POSTGRES_RLS_TESTS_ENABLED = _truthy(os.environ.get("ENABLE_POSTGRES_TESTS"))
RUNTIME_TEST_DATABASE_URL = os.environ.get("TEST_POSTGRES_DATABASE_URL")
MIGRATION_TEST_DATABASE_URL = os.environ.get("TEST_POSTGRES_MIGRATION_DATABASE_URL")
WORKER_ROLE = "litoral_trace_worker_executor"
RUNTIME_ROLE = "litoral_trace_app"

pytestmark = pytest.mark.skipif(
    not (
        POSTGRES_RLS_TESTS_ENABLED
        and RUNTIME_TEST_DATABASE_URL
        and MIGRATION_TEST_DATABASE_URL
    ),
    reason=(
        "PostgreSQL P2.2D-1 tests require ENABLE_POSTGRES_TESTS=1 plus isolated "
        "TEST_POSTGRES_DATABASE_URL and TEST_POSTGRES_MIGRATION_DATABASE_URL."
    ),
)


def _runtime_engine():
    return create_engine(
        normalize_database_url(RUNTIME_TEST_DATABASE_URL),
        pool_size=1,
        max_overflow=0,
        pool_pre_ping=True,
    )


def _owner_engine():
    return create_engine(
        normalize_database_url(MIGRATION_TEST_DATABASE_URL),
        pool_pre_ping=True,
        hide_parameters=True,
    )


def _set_tenant_context(conn, organization_id: int) -> None:
    conn.execute(
        text(
            "SELECT set_config('app.current_organization_id', :organization_id, true)"
        ),
        {"organization_id": str(organization_id)},
    )


def _db_now(conn) -> datetime:
    return conn.execute(text("SELECT CURRENT_TIMESTAMP")).scalar_one()


def _build_ephemeral_worker_database_url(*, role_name: str, password: str) -> str:
    base_url = make_url(normalize_database_url(MIGRATION_TEST_DATABASE_URL))
    worker_url = base_url.set(username=role_name, password=password)
    return worker_url.render_as_string(hide_password=False)


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

        with owner_engine.connect() as conn:
            role_visible = conn.execute(
                text("SELECT 1 FROM pg_roles WHERE rolname = :role_name"),
                {"role_name": role_name},
            ).scalar_one_or_none()
        if role_visible != 1:
            raise RuntimeError("ephemeral role not visible after commit")
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


def _insert_job(
    conn,
    *,
    organization_id: int,
    lote_id: int,
    status: str = "running",
    attempt_count: int = 1,
    max_attempts: int = 3,
    locked_by: str | None = None,
    lease_token: str | None = None,
    started_at: datetime | None = None,
    finished_at: datetime | None = None,
    geometry_hash: str | None = None,
    algorithm_version: str = ALGORITHM_VERSION,
) -> dict[str, object]:
    db_now = _db_now(conn)
    next_attempt_at = db_now - timedelta(minutes=10)
    locked_at = db_now - timedelta(minutes=2) if locked_by is not None else None
    heartbeat_at = db_now - timedelta(minutes=1) if locked_by is not None else None
    resolved_geometry_hash = geometry_hash or (uuid4().hex + uuid4().hex)
    resolved_started_at = started_at
    if status == "running" and resolved_started_at is None:
        resolved_started_at = db_now - timedelta(minutes=2)

    row = conn.execute(
        text(
            """
            INSERT INTO satellite_jobs (
                organization_id, lote_id, job_type, status, attempt_count, max_attempts,
                next_attempt_at, locked_at, locked_by, heartbeat_at, lease_token,
                request_start_date, request_end_date, max_cloud_pct, geometry_hash,
                algorithm_version, polygon_wkt_snapshot, started_at, finished_at
            )
            VALUES (
                :organization_id, :lote_id, 'ndvi_timeseries', :status, :attempt_count,
                :max_attempts, :next_attempt_at, :locked_at, :locked_by, :heartbeat_at,
                :lease_token, :request_start_date, :request_end_date, 20.0,
                :geometry_hash, :algorithm_version, :polygon_wkt_snapshot, :started_at,
                :finished_at
            )
            RETURNING id, organization_id, lote_id, status, attempt_count, max_attempts,
                      locked_by, locked_at, heartbeat_at, lease_token, geometry_hash,
                      algorithm_version, started_at, finished_at
            """
        ),
        {
            "organization_id": organization_id,
            "lote_id": lote_id,
            "status": status,
            "attempt_count": attempt_count,
            "max_attempts": max_attempts,
            "next_attempt_at": next_attempt_at,
            "locked_at": locked_at,
            "locked_by": locked_by,
            "heartbeat_at": heartbeat_at,
            "lease_token": lease_token,
            "request_start_date": (db_now - timedelta(days=30)).date(),
            "request_end_date": db_now.date(),
            "geometry_hash": resolved_geometry_hash,
            "algorithm_version": algorithm_version,
            "polygon_wkt_snapshot": (
                "POLYGON((-58.91 -27.46, -58.89 -27.46, -58.89 -27.44, "
                "-58.91 -27.44, -58.91 -27.46))"
            ),
            "started_at": resolved_started_at,
            "finished_at": finished_at,
        },
    ).mappings().one()
    return dict(row)


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
                started_at = NULL,
                finished_at = NULL,
                error_code = NULL,
                error_message = NULL,
                updated_at = CURRENT_TIMESTAMP
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
                SELECT status, attempt_count, locked_by, locked_at, heartbeat_at,
                       lease_token, started_at, finished_at, error_code, error_message
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
                "name": f"P22D1 Org {suffix}",
                "slug": f"p22d1-org-{suffix}",
                "tax_id": f"92-{suffix}",
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
                "identificador": f"P22D1-LOTE-{suffix}",
                "productor_id": f"50-{suffix}",
                "polygon_wkt": (
                    "POLYGON((-58.91 -27.46, -58.89 -27.46, -58.89 -27.44, "
                    "-58.91 -27.44, -58.91 -27.46))"
                ),
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
                    "DELETE FROM satellite_job_results "
                    "WHERE organization_id = :organization_id"
                ),
                {"organization_id": org_id},
            )
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


def test_runtime_role_and_worker_claim_privileges_remain_hardened():
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

        assert runtime_role_row["rolbypassrls"] is False

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
                                'public.worker_claim_next_satellite_job(text)',
                                'EXECUTE'
                            ) AS can_execute_claim
                        """
                    )
                ).mappings().one()

            assert role_row["current_user"] == worker_auth["role_name"]
            assert role_row["can_select"] is False
            assert role_row["can_insert"] is False
            assert role_row["can_update"] is False
            assert role_row["can_delete"] is False
            assert role_row["can_execute_claim"] is True

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


def test_mark_satellite_job_succeeded_with_valid_worker_and_lease_succeeds_under_runtime_rls():
    with _lease_fixture() as fixture:
        worker_id = "p22d1-worker-success"
        lease_token = str(uuid4())

        with fixture["owner_engine"].begin() as conn:
            job = _insert_job(
                conn,
                organization_id=fixture["organization_id"],
                lote_id=fixture["lote_id"],
                status="running",
                attempt_count=1,
                locked_by=worker_id,
                lease_token=lease_token,
            )

        runtime_session = sessionmaker(
            bind=fixture["runtime_engine"],
            autoflush=False,
            autocommit=False,
        )()
        try:
            mark_satellite_job_succeeded(
                runtime_session,
                organization_id=fixture["organization_id"],
                job_id=int(job["id"]),
                worker_id=worker_id,
                lease_token=lease_token,
            )
            runtime_session.commit()
        finally:
            runtime_session.close()

        final_row = _fetch_job_row(fixture["owner_engine"], job_id=int(job["id"]))

    assert final_row["status"] == "succeeded"
    assert final_row["locked_at"] is None
    assert final_row["locked_by"] is None
    assert final_row["heartbeat_at"] is None
    assert final_row["finished_at"] is not None
    assert str(final_row["lease_token"]) == lease_token


def test_mark_satellite_job_succeeded_with_incorrect_lease_raises_and_preserves_state():
    with _lease_fixture() as fixture:
        worker_id = "p22d1-worker-bad-lease"
        lease_token = str(uuid4())

        with fixture["owner_engine"].begin() as conn:
            job = _insert_job(
                conn,
                organization_id=fixture["organization_id"],
                lote_id=fixture["lote_id"],
                status="running",
                attempt_count=1,
                locked_by=worker_id,
                lease_token=lease_token,
            )

        runtime_session = sessionmaker(
            bind=fixture["runtime_engine"],
            autoflush=False,
            autocommit=False,
        )()
        try:
            with pytest.raises(SatelliteJobLeaseLostError):
                mark_satellite_job_succeeded(
                    runtime_session,
                    organization_id=fixture["organization_id"],
                    job_id=int(job["id"]),
                    worker_id=worker_id,
                    lease_token=str(uuid4()),
                )
            runtime_session.rollback()
        finally:
            runtime_session.close()

        final_row = _fetch_job_row(fixture["owner_engine"], job_id=int(job["id"]))

    assert final_row["status"] == "running"
    assert final_row["locked_by"] == worker_id
    assert final_row["locked_at"] is not None
    assert final_row["heartbeat_at"] is not None
    assert str(final_row["lease_token"]) == lease_token
    assert final_row["finished_at"] is None


def test_mark_satellite_job_succeeded_with_incorrect_worker_raises_lease_lost():
    with _lease_fixture() as fixture:
        worker_id = "p22d1-worker-owner"
        lease_token = str(uuid4())

        with fixture["owner_engine"].begin() as conn:
            job = _insert_job(
                conn,
                organization_id=fixture["organization_id"],
                lote_id=fixture["lote_id"],
                status="running",
                attempt_count=1,
                locked_by=worker_id,
                lease_token=lease_token,
            )

        runtime_session = sessionmaker(
            bind=fixture["runtime_engine"],
            autoflush=False,
            autocommit=False,
        )()
        try:
            with pytest.raises(SatelliteJobLeaseLostError):
                mark_satellite_job_succeeded(
                    runtime_session,
                    organization_id=fixture["organization_id"],
                    job_id=int(job["id"]),
                    worker_id="p22d1-wrong-worker",
                    lease_token=lease_token,
                )
            runtime_session.rollback()
        finally:
            runtime_session.close()

        final_row = _fetch_job_row(fixture["owner_engine"], job_id=int(job["id"]))

    assert final_row["status"] == "running"
    assert final_row["locked_by"] == worker_id
    assert str(final_row["lease_token"]) == lease_token


def test_mark_satellite_job_succeeded_on_non_running_status_raises_lease_lost():
    with _lease_fixture() as fixture:
        worker_id = "p22d1-worker-not-running"
        lease_token = str(uuid4())

        with fixture["owner_engine"].begin() as conn:
            job = _insert_job(
                conn,
                organization_id=fixture["organization_id"],
                lote_id=fixture["lote_id"],
                status="running",
                attempt_count=1,
                locked_by=worker_id,
                lease_token=lease_token,
            )
            conn.execute(
                text(
                    """
                    UPDATE satellite_jobs
                    SET status = 'failed',
                        finished_at = CURRENT_TIMESTAMP,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = :job_id
                    """
                ),
                {"job_id": int(job["id"])},
            )

        runtime_session = sessionmaker(
            bind=fixture["runtime_engine"],
            autoflush=False,
            autocommit=False,
        )()
        try:
            with pytest.raises(SatelliteJobLeaseLostError):
                mark_satellite_job_succeeded(
                    runtime_session,
                    organization_id=fixture["organization_id"],
                    job_id=int(job["id"]),
                    worker_id=worker_id,
                    lease_token=lease_token,
                )
            runtime_session.rollback()
        finally:
            runtime_session.close()

        final_row = _fetch_job_row(fixture["owner_engine"], job_id=int(job["id"]))

    assert final_row["status"] == "failed"
    assert final_row["finished_at"] is not None
    assert final_row["locked_by"] == worker_id
    assert str(final_row["lease_token"]) == lease_token


def test_mark_satellite_job_failed_with_valid_worker_and_lease_succeeds_under_runtime_rls():
    with _lease_fixture() as fixture:
        worker_id = "p22d1-worker-fail"
        lease_token = str(uuid4())

        with fixture["owner_engine"].begin() as conn:
            job = _insert_job(
                conn,
                organization_id=fixture["organization_id"],
                lote_id=fixture["lote_id"],
                status="running",
                attempt_count=1,
                locked_by=worker_id,
                lease_token=lease_token,
            )

        runtime_session = sessionmaker(
            bind=fixture["runtime_engine"],
            autoflush=False,
            autocommit=False,
        )()
        try:
            mark_satellite_job_failed(
                runtime_session,
                organization_id=fixture["organization_id"],
                job_id=int(job["id"]),
                worker_id=worker_id,
                lease_token=lease_token,
                error_code="worker_execution_failed",
                error_message="controlled failure",
            )
            runtime_session.commit()
        finally:
            runtime_session.close()

        final_row = _fetch_job_row(fixture["owner_engine"], job_id=int(job["id"]))

    assert final_row["status"] == "failed"
    assert final_row["locked_at"] is None
    assert final_row["locked_by"] is None
    assert final_row["heartbeat_at"] is None
    assert final_row["finished_at"] is not None
    assert final_row["error_code"] == "worker_execution_failed"
    assert final_row["error_message"] == "controlled failure"
    assert str(final_row["lease_token"]) == lease_token


def test_mark_satellite_job_failed_with_incorrect_lease_raises_and_preserves_state():
    with _lease_fixture() as fixture:
        worker_id = "p22d1-worker-fail-bad-lease"
        lease_token = str(uuid4())

        with fixture["owner_engine"].begin() as conn:
            job = _insert_job(
                conn,
                organization_id=fixture["organization_id"],
                lote_id=fixture["lote_id"],
                status="running",
                attempt_count=1,
                locked_by=worker_id,
                lease_token=lease_token,
            )

        runtime_session = sessionmaker(
            bind=fixture["runtime_engine"],
            autoflush=False,
            autocommit=False,
        )()
        try:
            with pytest.raises(SatelliteJobLeaseLostError):
                mark_satellite_job_failed(
                    runtime_session,
                    organization_id=fixture["organization_id"],
                    job_id=int(job["id"]),
                    worker_id=worker_id,
                    lease_token=str(uuid4()),
                    error_code="worker_execution_failed",
                    error_message="controlled failure",
                )
            runtime_session.rollback()
        finally:
            runtime_session.close()

        final_row = _fetch_job_row(fixture["owner_engine"], job_id=int(job["id"]))

    assert final_row["status"] == "running"
    assert final_row["locked_by"] == worker_id
    assert final_row["finished_at"] is None
    assert str(final_row["lease_token"]) == lease_token


def test_zombie_worker_loses_old_lease_and_cannot_persist_observations_or_terminal_transition():
    with _lease_fixture() as fixture:
        with fixture["owner_engine"].begin() as conn:
            job = _insert_job(
                conn,
                organization_id=fixture["organization_id"],
                lote_id=fixture["lote_id"],
                status="queued",
                attempt_count=0,
                locked_by=None,
                lease_token=None,
                geometry_hash="c" * 64,
                algorithm_version=ALGORITHM_VERSION,
            )

        with _ephemeral_worker_login(fixture["owner_engine"]) as worker_auth:
            worker_login_engine = create_engine(
                normalize_database_url(worker_auth["worker_database_url"]),
                pool_size=1,
                max_overflow=0,
                pool_pre_ping=True,
            )
            worker_auth["register_engine"](worker_login_engine)

            first_claim = _claim_once(worker_login_engine, worker_id="p22d1-worker-a")
            assert first_claim is not None
            first_lease = str(first_claim["lease_token"])
            assert UUID(first_lease)

            with fixture["owner_engine"].begin() as conn:
                _requeue_job_for_reclaim(conn, job_id=int(job["id"]))

            second_claim = _claim_once(worker_login_engine, worker_id="p22d1-worker-b")
            assert second_claim is not None
            second_lease = str(second_claim["lease_token"])
            assert UUID(second_lease)
            assert second_lease != first_lease

        runtime_session = sessionmaker(
            bind=fixture["runtime_engine"],
            autoflush=False,
            autocommit=False,
        )()
        try:
            persist_ndvi_execution_result(
                runtime_session,
                organization_id=fixture["organization_id"],
                lote_id=fixture["lote_id"],
                satellite_job_id=int(job["id"]),
                result=_sample_result("c" * 64, ALGORITHM_VERSION),
            )

            with pytest.raises(SatelliteJobLeaseLostError):
                mark_satellite_job_succeeded(
                    runtime_session,
                    organization_id=fixture["organization_id"],
                    job_id=int(job["id"]),
                    worker_id="p22d1-worker-a",
                    lease_token=first_lease,
                )
            runtime_session.rollback()
        finally:
            runtime_session.close()

        final_row = _fetch_job_row(fixture["owner_engine"], job_id=int(job["id"]))
        observation_count = _count_job_observations(
            fixture["owner_engine"],
            job_id=int(job["id"]),
        )

    assert observation_count == 0
    assert final_row["status"] == "running"
    assert final_row["locked_by"] == "p22d1-worker-b"
    assert final_row["locked_at"] is not None
    assert final_row["heartbeat_at"] is not None
    assert final_row["finished_at"] is None
    assert str(final_row["lease_token"]) == second_lease
    assert final_row["attempt_count"] == 2
