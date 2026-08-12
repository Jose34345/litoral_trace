from __future__ import annotations

import os
import secrets
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest
from psycopg import ClientCursor, sql
from sqlalchemy.engine import make_url
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import sessionmaker

from litoral_trace.config.settings import normalize_database_url
from litoral_trace.db.engine import reset_engine_state
from litoral_trace.db.worker import reset_worker_engine_state
from litoral_trace.services.gee import ALGORITHM_VERSION
from litoral_trace.services.satellite_jobs import enqueue_satellite_ndvi_job
from litoral_trace.services.satellite_ndvi_processing import (
    NormalizedNdviExecutionResult,
    NdviObservationRecord,
    mark_satellite_job_succeeded,
    persist_ndvi_execution_result,
)
from litoral_trace.workers.satellite_worker import SatelliteWorker, WorkerRunStatus


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
        "PostgreSQL P2.2C tests require ENABLE_POSTGRES_TESTS=1 plus isolated "
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
def _postgres_runtime_worker_environment(worker_database_url: str):
    original_values = {
        "ENVIRONMENT": os.environ.get("ENVIRONMENT"),
        "DATABASE_URL": os.environ.get("DATABASE_URL"),
        "MIGRATION_DATABASE_URL": os.environ.get("MIGRATION_DATABASE_URL"),
        "TEST_DATABASE_URL": os.environ.get("TEST_DATABASE_URL"),
        "WORKER_DATABASE_URL": os.environ.get("WORKER_DATABASE_URL"),
    }

    os.environ["ENVIRONMENT"] = "development"
    os.environ["DATABASE_URL"] = RUNTIME_TEST_DATABASE_URL or ""
    os.environ["WORKER_DATABASE_URL"] = worker_database_url
    os.environ["MIGRATION_DATABASE_URL"] = (
        "postgresql://blocked_migration_guard:blocked_guard@127.0.0.1:1/"
        "blocked_migration_guard"
    )
    os.environ.pop("TEST_DATABASE_URL", None)
    reset_engine_state()
    reset_worker_engine_state()

    try:
        yield
    finally:
        reset_worker_engine_state()
        reset_engine_state()
        for variable_name, original_value in original_values.items():
            if original_value is None:
                os.environ.pop(variable_name, None)
            else:
                os.environ[variable_name] = original_value
        reset_worker_engine_state()
        reset_engine_state()


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


@contextmanager
def _worker_fixture():
    runtime_engine = _runtime_engine()
    owner_engine = _owner_engine()
    suffix = uuid4().hex[:8]
    now = datetime.now(timezone.utc)
    job_a_locked_by = "worker-1"
    job_a_lease_token = str(uuid4())

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
                "name": f"P22C Org A {suffix}",
                "slug": f"p22c-org-a-{suffix}",
                "tax_id": f"90-{suffix}",
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
                "name": f"P22C Org B {suffix}",
                "slug": f"p22c-org-b-{suffix}",
                "tax_id": f"91-{suffix}",
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
                    :organization_id, :identificador, :productor_id, 'Madera Aserrada (Pino)',
                    10.0, -27.45, -58.90, :polygon_wkt, 'Pendiente', 10.0, 5.0
                )
                RETURNING id
                """
            ),
            {
                "organization_id": org_a_id,
                "identificador": f"P22C-LOTE-A-{suffix}",
                "productor_id": f"40-A-{suffix}",
                "polygon_wkt": "POLYGON((-58.91 -27.46, -58.89 -27.46, -58.89 -27.44, -58.91 -27.44, -58.91 -27.46))",
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
                    :organization_id, :identificador, :productor_id, 'Madera Aserrada (Pino)',
                    11.0, -27.55, -58.80, :polygon_wkt, 'Pendiente', 12.0, 4.0
                )
                RETURNING id
                """
            ),
            {
                "organization_id": org_b_id,
                "identificador": f"P22C-LOTE-B-{suffix}",
                "productor_id": f"40-B-{suffix}",
                "polygon_wkt": "POLYGON((-58.81 -27.56, -58.79 -27.56, -58.79 -27.54, -58.81 -27.54, -58.81 -27.56))",
            },
        ).scalar_one()
        job_a_id = conn.execute(
            text(
                """
                INSERT INTO satellite_jobs (
                    organization_id, lote_id, job_type, status, attempt_count, max_attempts,
                    next_attempt_at, locked_at, locked_by, heartbeat_at, lease_token,
                    request_start_date, request_end_date, max_cloud_pct, geometry_hash,
                    algorithm_version, polygon_wkt_snapshot, started_at
                )
                VALUES (
                    :organization_id, :lote_id, 'ndvi_timeseries', 'running', 1, 3,
                    :next_attempt_at, :locked_at, :locked_by, :heartbeat_at, :lease_token,
                    '2020-12-31', '2026-08-09', 20.0, :geometry_hash,
                    '2.4.0-gee-sentinel2-scl-v2', :polygon_wkt_snapshot, :started_at
                )
                RETURNING id
                """
            ),
            {
                "organization_id": org_a_id,
                "lote_id": lote_a_id,
                "next_attempt_at": now - timedelta(minutes=10),
                "locked_at": now - timedelta(minutes=2),
                "locked_by": job_a_locked_by,
                "heartbeat_at": now - timedelta(minutes=2),
                "lease_token": job_a_lease_token,
                "geometry_hash": "a" * 64,
                "polygon_wkt_snapshot": "POLYGON((-58.91 -27.46, -58.89 -27.46, -58.89 -27.44, -58.91 -27.44, -58.91 -27.46))",
                "started_at": now - timedelta(minutes=2),
            },
        ).scalar_one()

    fixture = {
        "runtime_engine": runtime_engine,
        "owner_engine": owner_engine,
        "org_a_id": int(org_a_id),
        "org_b_id": int(org_b_id),
        "lote_a_id": int(lote_a_id),
        "lote_b_id": int(lote_b_id),
        "job_a_id": int(job_a_id),
        "job_a_locked_by": job_a_locked_by,
        "job_a_lease_token": job_a_lease_token,
    }

    try:
        yield fixture
    finally:
        with owner_engine.begin() as conn:
            conn.execute(
                text(
                    "DELETE FROM satellite_job_results "
                    "WHERE organization_id IN (:org_a_id, :org_b_id)"
                ),
                {"org_a_id": org_a_id, "org_b_id": org_b_id},
            )
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
                text("DELETE FROM lotes WHERE id IN (:lote_a_id, :lote_b_id)"),
                {"lote_a_id": lote_a_id, "lote_b_id": lote_b_id},
            )
            conn.execute(
                text("DELETE FROM organizations WHERE id IN (:org_a_id, :org_b_id)"),
                {"org_a_id": org_a_id, "org_b_id": org_b_id},
            )
        runtime_engine.dispose()
        owner_engine.dispose()


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


def test_worker_runtime_persistence_uses_runtime_session_and_links_satellite_job():
    with _worker_fixture() as fixture:
        runtime_session = sessionmaker(
            bind=fixture["runtime_engine"],
            autoflush=False,
            autocommit=False,
        )()
        result = _sample_result("a" * 64, "2.4.0-gee-sentinel2-scl-v2")
        try:
            persist_ndvi_execution_result(
                runtime_session,
                organization_id=fixture["org_a_id"],
                lote_id=fixture["lote_a_id"],
                satellite_job_id=fixture["job_a_id"],
                result=result,
            )
            mark_satellite_job_succeeded(
                runtime_session,
                organization_id=fixture["org_a_id"],
                job_id=fixture["job_a_id"],
                worker_id=fixture["job_a_locked_by"],
                lease_token=fixture["job_a_lease_token"],
            )
            runtime_session.commit()
        finally:
            runtime_session.close()

        with fixture["runtime_engine"].begin() as conn:
            current_user = conn.execute(text("SELECT current_user")).scalar_one()
            no_context_rows = conn.execute(
                text("SELECT id FROM satellite_ndvi_observations ORDER BY id")
            ).scalars().all()
            conn.execute(
                text(
                    "SELECT set_config('app.current_organization_id', :organization_id, true)"
                ),
                {"organization_id": str(fixture["org_a_id"])},
            )
            tenant_rows = conn.execute(
                text(
                    "SELECT satellite_job_id FROM satellite_ndvi_observations ORDER BY id"
                )
            ).scalars().all()

        with fixture["owner_engine"].connect() as conn:
            persisted_row = conn.execute(
                text(
                    """
                    SELECT organization_id, lote_id, satellite_job_id
                    FROM satellite_ndvi_observations
                    WHERE satellite_job_id = :job_id
                    """
                ),
                {"job_id": fixture["job_a_id"]},
            ).mappings().one()
            job_row = conn.execute(
                text(
                    "SELECT status, finished_at FROM satellite_jobs WHERE id = :job_id"
                ),
                {"job_id": fixture["job_a_id"]},
            ).mappings().one()

    assert current_user == "litoral_trace_app"
    assert no_context_rows == []
    assert tenant_rows == [fixture["job_a_id"]]
    assert persisted_row["organization_id"] == fixture["org_a_id"]
    assert persisted_row["lote_id"] == fixture["lote_a_id"]
    assert persisted_row["satellite_job_id"] == fixture["job_a_id"]
    assert job_row["status"] == "succeeded"
    assert job_row["finished_at"] is not None


def test_worker_success_persistence_is_atomic_under_runtime_rollback():
    with _worker_fixture() as fixture:
        runtime_session = sessionmaker(
            bind=fixture["runtime_engine"],
            autoflush=False,
            autocommit=False,
        )()
        result = _sample_result("a" * 64, "2.4.0-gee-sentinel2-scl-v2")
        try:
            persist_ndvi_execution_result(
                runtime_session,
                organization_id=fixture["org_a_id"],
                lote_id=fixture["lote_a_id"],
                satellite_job_id=fixture["job_a_id"],
                result=result,
            )
            mark_satellite_job_succeeded(
                runtime_session,
                organization_id=fixture["org_a_id"],
                job_id=fixture["job_a_id"],
                worker_id=fixture["job_a_locked_by"],
                lease_token=fixture["job_a_lease_token"],
            )
            raise RuntimeError("force rollback")
        except RuntimeError:
            runtime_session.rollback()
        finally:
            runtime_session.close()

        with fixture["owner_engine"].connect() as conn:
            observation_count = conn.execute(
                text(
                    """
                    SELECT count(*)
                    FROM satellite_ndvi_observations
                    WHERE satellite_job_id = :job_id
                    """
                ),
                {"job_id": fixture["job_a_id"]},
            ).scalar_one()
            job_row = conn.execute(
                text("SELECT status, finished_at FROM satellite_jobs WHERE id = :job_id"),
                {"job_id": fixture["job_a_id"]},
            ).mappings().one()

    assert observation_count == 0
    assert job_row["status"] == "running"
    assert job_row["finished_at"] is None


def test_satellite_worker_run_once_uses_ephemeral_worker_login_and_runtime_rls_persistence():
    with _worker_fixture() as fixture:
        with _ephemeral_worker_login(fixture["owner_engine"]) as worker_auth:
            with fixture["owner_engine"].connect() as conn:
                role_row = conn.execute(
                    text(
                        """
                        SELECT rolcanlogin, rolsuper, rolcreatedb, rolcreaterole,
                               rolinherit, rolbypassrls
                        FROM pg_roles
                        WHERE rolname = :role_name
                        """
                    ),
                    {"role_name": worker_auth["role_name"]},
                ).mappings().one()
                membership_rows = conn.execute(
                    text(
                        """
                        SELECT granted.rolname
                        FROM pg_roles AS member
                        JOIN pg_auth_members AS membership
                          ON membership.member = member.oid
                        JOIN pg_roles AS granted
                          ON granted.oid = membership.roleid
                        WHERE member.rolname = :role_name
                        ORDER BY granted.rolname
                        """
                    ),
                    {"role_name": worker_auth["role_name"]},
                ).scalars().all()
                direct_grants = conn.execute(
                    text(
                        """
                        SELECT
                            has_table_privilege(:role_name, 'public.satellite_jobs', 'SELECT') AS can_select,
                            has_table_privilege(:role_name, 'public.satellite_jobs', 'INSERT') AS can_insert,
                            has_table_privilege(:role_name, 'public.satellite_jobs', 'UPDATE') AS can_update,
                            has_table_privilege(:role_name, 'public.satellite_jobs', 'DELETE') AS can_delete,
                            has_function_privilege(
                                :role_name,
                                'public.worker_claim_next_satellite_job(text)',
                                'EXECUTE'
                            ) AS can_execute_claim
                        """
                    ),
                    {"role_name": worker_auth["role_name"]},
                ).mappings().one()

            assert role_row["rolcanlogin"] is True
            assert role_row["rolsuper"] is False
            assert role_row["rolcreatedb"] is False
            assert role_row["rolcreaterole"] is False
            assert role_row["rolinherit"] is True
            assert role_row["rolbypassrls"] is False
            assert membership_rows == [WORKER_ROLE]
            assert direct_grants["can_select"] is False
            assert direct_grants["can_insert"] is False
            assert direct_grants["can_update"] is False
            assert direct_grants["can_delete"] is False
            assert direct_grants["can_execute_claim"] is True

            with _postgres_runtime_worker_environment(worker_auth["worker_database_url"]):
                queued_job, created = enqueue_satellite_ndvi_job(
                    organization_id=fixture["org_a_id"],
                    lote_id=fixture["lote_a_id"],
                    start_date=date(2026, 7, 1),
                    end_date=date(2026, 8, 1),
                    max_cloud_pct=20.0,
                    idempotency_key=f"p22c-worker-e2e-{uuid4().hex}",
                )
                assert created is True

                adapter_claim_snapshot: dict[str, object] = {}
                worker_id = f"p22c-e2e-worker-{uuid4().hex[:8]}"

                class FakeGeeAdapter:
                    def execute(self, request):
                        with fixture["owner_engine"].connect() as conn:
                            adapter_claim_snapshot.update(
                                conn.execute(
                                    text(
                                        """
                                        SELECT status, attempt_count, locked_by, locked_at,
                                               heartbeat_at, lease_token,
                                               started_at, finished_at
                                        FROM satellite_jobs
                                        WHERE id = :job_id
                                        """
                                    ),
                                    {"job_id": queued_job.id},
                                ).mappings().one()
                            )

                        return _sample_result(
                            queued_job.geometry_hash,
                            ALGORITHM_VERSION,
                        )

                worker = SatelliteWorker(
                    worker_id=worker_id,
                    gee_ndvi_adapter=FakeGeeAdapter(),
                )
                run_result = worker.run_once()

            worker_login_engine = create_engine(
                normalize_database_url(worker_auth["worker_database_url"]),
                pool_size=1,
                max_overflow=0,
                pool_pre_ping=True,
            )
            worker_auth["register_engine"](worker_login_engine)

            with worker_login_engine.begin() as conn:
                connection_privileges = conn.execute(
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
                empty_claim = conn.execute(
                    text(
                        """
                        SELECT *
                        FROM public.worker_claim_next_satellite_job(
                            :requested_worker_id
                        )
                        """
                    ),
                    {"requested_worker_id": "p22c-e2e-empty-queue"},
                ).mappings().one_or_none()

            assert connection_privileges["current_user"] == worker_auth["role_name"]
            assert connection_privileges["can_select"] is False
            assert connection_privileges["can_insert"] is False
            assert connection_privileges["can_update"] is False
            assert connection_privileges["can_delete"] is False
            assert connection_privileges["can_execute_claim"] is True
            assert empty_claim is None

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

        with fixture["owner_engine"].connect() as conn:
            final_job_row = conn.execute(
                text(
                    """
                    SELECT organization_id, lote_id, status, attempt_count,
                           locked_by, locked_at, heartbeat_at, lease_token,
                           started_at, finished_at
                    FROM satellite_jobs
                    WHERE id = :job_id
                    """
                ),
                {"job_id": queued_job.id},
            ).mappings().one()
            persisted_row = conn.execute(
                text(
                    """
                    SELECT organization_id, lote_id, satellite_job_id, observation_date
                    FROM satellite_ndvi_observations
                    WHERE satellite_job_id = :job_id
                    ORDER BY observation_date
                    """
                ),
                {"job_id": queued_job.id},
            ).mappings().all()

        with fixture["runtime_engine"].begin() as conn:
            runtime_current_user = conn.execute(text("SELECT current_user")).scalar_one()
            no_context_rows = conn.execute(
                text("SELECT satellite_job_id FROM satellite_ndvi_observations ORDER BY id")
            ).scalars().all()
            conn.execute(
                text(
                    "SELECT set_config('app.current_organization_id', :organization_id, true)"
                ),
                {"organization_id": str(fixture["org_a_id"])},
            )
            tenant_a_rows = conn.execute(
                text(
                    """
                    SELECT satellite_job_id
                    FROM satellite_ndvi_observations
                    WHERE satellite_job_id = :job_id
                    ORDER BY id
                    """
                ),
                {"job_id": queued_job.id},
            ).scalars().all()

        with fixture["runtime_engine"].begin() as conn:
            conn.execute(
                text(
                    "SELECT set_config('app.current_organization_id', :organization_id, true)"
                ),
                {"organization_id": str(fixture["org_b_id"])},
            )
            tenant_b_rows = conn.execute(
                text(
                    """
                    SELECT satellite_job_id
                    FROM satellite_ndvi_observations
                    WHERE satellite_job_id = :job_id
                    ORDER BY id
                    """
                ),
                {"job_id": queued_job.id},
            ).scalars().all()

    # TODO(P2.2E/P3): satellite_ndvi_observations enforces canonical uniqueness on
    # organization_id/lote_id/observation_date/geometry_hash with only one nullable
    # satellite_job_id, so different jobs producing the same canonical observation
    # cannot yet preserve immutable per-job lineage.
    assert run_result.status is WorkerRunStatus.SUCCEEDED
    assert run_result.job_id == queued_job.id
    assert adapter_claim_snapshot["status"] == "running"
    assert adapter_claim_snapshot["attempt_count"] == 1
    assert adapter_claim_snapshot["locked_by"] == worker_id
    assert adapter_claim_snapshot["locked_at"] is not None
    assert adapter_claim_snapshot["heartbeat_at"] is not None
    assert adapter_claim_snapshot["lease_token"] is not None
    assert UUID(str(adapter_claim_snapshot["lease_token"]))
    assert adapter_claim_snapshot["started_at"] is not None
    assert adapter_claim_snapshot["finished_at"] is None
    assert final_job_row["organization_id"] == fixture["org_a_id"]
    assert final_job_row["lote_id"] == fixture["lote_a_id"]
    assert final_job_row["status"] == "succeeded"
    assert final_job_row["attempt_count"] == 1
    assert final_job_row["locked_by"] is None
    assert final_job_row["locked_at"] is None
    assert final_job_row["heartbeat_at"] is None
    assert final_job_row["lease_token"] is not None
    assert UUID(str(final_job_row["lease_token"]))
    assert final_job_row["lease_token"] == adapter_claim_snapshot["lease_token"]
    assert final_job_row["started_at"] == adapter_claim_snapshot["started_at"]
    assert final_job_row["finished_at"] is not None
    assert runtime_current_user == RUNTIME_ROLE
    assert no_context_rows == []
    assert tenant_a_rows == [queued_job.id]
    assert tenant_b_rows == []
    assert len(persisted_row) == 1
    assert persisted_row[0]["organization_id"] == fixture["org_a_id"]
    assert persisted_row[0]["lote_id"] == fixture["lote_a_id"]
    assert persisted_row[0]["satellite_job_id"] == queued_job.id
