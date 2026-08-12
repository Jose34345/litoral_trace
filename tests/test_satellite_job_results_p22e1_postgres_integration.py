from __future__ import annotations

import os
import secrets
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from uuid import uuid4

import pytest
from psycopg import ClientCursor, sql
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.orm import sessionmaker

from litoral_trace.config.settings import normalize_database_url
from litoral_trace.db.engine import get_db_session, reset_engine_state
from litoral_trace.db.worker import get_worker_db_session, reset_worker_engine_state
from litoral_trace.services.gee import ALGORITHM_VERSION, generate_geometry_hash
from litoral_trace.services.satellite_job_results import (
    NDVI_TIMESERIES_RESULT_SCHEMA_VERSION,
    build_satellite_job_result_snapshot,
    canonicalize_satellite_job_result_payload,
    compute_satellite_job_result_payload_sha256,
    persist_satellite_job_result,
)
from litoral_trace.services.satellite_ndvi_processing import (
    NormalizedNdviExecutionResult,
    NdviObservationRecord,
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
        "PostgreSQL P2.2E-1 tests require ENABLE_POSTGRES_TESTS=1 plus isolated "
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


def _polygon_wkt_snapshot() -> str:
    return (
        "POLYGON((-58.91 -27.46, -58.89 -27.46, -58.89 -27.44, "
        "-58.91 -27.44, -58.91 -27.46))"
    )


def _db_now(conn) -> datetime:
    return conn.execute(text("SELECT CURRENT_TIMESTAMP")).scalar_one()


def _run_owner_cursor_statement(connection, statement, params=()) -> None:
    driver_connection = connection.connection.driver_connection
    with ClientCursor(driver_connection) as cursor:
        cursor.execute(statement, params)


def _build_ephemeral_worker_database_url(*, role_name: str, password: str) -> str:
    base_url = make_url(normalize_database_url(MIGRATION_TEST_DATABASE_URL))
    worker_url = base_url.set(username=role_name, password=password)
    return worker_url.render_as_string(hide_password=False)


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
def _integration_fixture():
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
                "name": f"P22E1 Org A {suffix}",
                "slug": f"p22e1-org-a-{suffix}",
                "tax_id": f"96-a-{suffix}",
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
                "name": f"P22E1 Org B {suffix}",
                "slug": f"p22e1-org-b-{suffix}",
                "tax_id": f"96-b-{suffix}",
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
                "identificador": f"P22E1-LOTE-A-{suffix}",
                "productor_id": f"60-a-{suffix}",
                "polygon_wkt": (
                    "POLYGON((-58.91 -27.46, -58.89 -27.46, -58.89 -27.44, "
                    "-58.91 -27.44, -58.91 -27.46))"
                ),
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
                    :polygon_wkt, 'Pendiente', 12.0, 4.0
                )
                RETURNING id
                """
            ),
            {
                "organization_id": org_b_id,
                "identificador": f"P22E1-LOTE-B-{suffix}",
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
        "org_a_id": int(org_a_id),
        "org_b_id": int(org_b_id),
        "lote_a_id": int(lote_a_id),
        "lote_b_id": int(lote_b_id),
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
                {
                    "org_a_id": fixture["org_a_id"],
                    "org_b_id": fixture["org_b_id"],
                },
            )
            conn.execute(
                text(
                    "DELETE FROM satellite_ndvi_observations "
                    "WHERE organization_id IN (:org_a_id, :org_b_id)"
                ),
                {
                    "org_a_id": fixture["org_a_id"],
                    "org_b_id": fixture["org_b_id"],
                },
            )
            conn.execute(
                text(
                    "DELETE FROM satellite_jobs "
                    "WHERE organization_id IN (:org_a_id, :org_b_id)"
                ),
                {
                    "org_a_id": fixture["org_a_id"],
                    "org_b_id": fixture["org_b_id"],
                },
            )
            conn.execute(
                text("DELETE FROM lotes WHERE id IN (:lote_a_id, :lote_b_id)"),
                {
                    "lote_a_id": fixture["lote_a_id"],
                    "lote_b_id": fixture["lote_b_id"],
                },
            )
            conn.execute(
                text("DELETE FROM organizations WHERE id IN (:org_a_id, :org_b_id)"),
                {
                    "org_a_id": fixture["org_a_id"],
                    "org_b_id": fixture["org_b_id"],
                },
            )
        runtime_engine.dispose()
        owner_engine.dispose()


def _insert_job(
    conn,
    *,
    organization_id: int,
    lote_id: int,
    status: str = "queued",
    next_attempt_at: datetime | None = None,
    geometry_hash: str,
    algorithm_version: str = ALGORITHM_VERSION,
    polygon_wkt_snapshot: str,
) -> dict[str, object]:
    effective_next_attempt_at = next_attempt_at or (_db_now(conn) - timedelta(minutes=1))
    row = conn.execute(
        text(
            """
            INSERT INTO satellite_jobs (
                organization_id, lote_id, job_type, status, attempt_count, max_attempts,
                next_attempt_at, request_start_date, request_end_date, max_cloud_pct,
                geometry_hash, algorithm_version, polygon_wkt_snapshot
            )
            VALUES (
                :organization_id, :lote_id, 'ndvi_timeseries', :status, 0, 3,
                :next_attempt_at, '2026-07-01', '2026-08-01', 20.0,
                :geometry_hash, :algorithm_version, :polygon_wkt_snapshot
            )
            RETURNING id, organization_id, lote_id, status, geometry_hash,
                      algorithm_version, polygon_wkt_snapshot
            """
        ),
        {
            "organization_id": organization_id,
            "lote_id": lote_id,
            "status": status,
            "next_attempt_at": effective_next_attempt_at,
            "geometry_hash": geometry_hash,
            "algorithm_version": algorithm_version,
            "polygon_wkt_snapshot": polygon_wkt_snapshot,
        },
    ).mappings().one()
    return dict(row)


def _sample_result(
    *,
    geometry_hash: str,
    algorithm_version: str,
    ndvi_mean: float = 0.61,
    aoi_cloud_percentage: float | None = 1.0,
    observations: tuple[date, ...] = (date(2026, 8, 1),),
) -> NormalizedNdviExecutionResult:
    return NormalizedNdviExecutionResult(
        geometry_hash=geometry_hash,
        algorithm_version=algorithm_version,
        observations=tuple(
            NdviObservationRecord(
                observation_date=observation_date,
                ndvi_mean=ndvi_mean,
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
                aoi_cloud_percentage=aoi_cloud_percentage,
                processing_date=datetime(2026, 8, 1, 12, 0, tzinfo=timezone.utc),
            )
            for observation_date in observations
        ),
    )


class _FixedGeeAdapter:
    def __init__(self, result: NormalizedNdviExecutionResult, on_execute=None):
        self._result = result
        self._on_execute = on_execute

    def execute(self, request):
        if self._on_execute is not None:
            self._on_execute(request)
        return self._result


def _insert_runtime_snapshot(
    runtime_engine,
    *,
    organization_id: int,
    lote_id: int,
    job_id: int,
    result: NormalizedNdviExecutionResult,
) -> None:
    runtime_session = sessionmaker(
        bind=runtime_engine,
        autoflush=False,
        autocommit=False,
    )()
    try:
        persist_satellite_job_result(
            runtime_session,
            snapshot=build_satellite_job_result_snapshot(
                satellite_job_id=job_id,
                organization_id=organization_id,
                lote_id=lote_id,
                result=result,
            ),
        )
        runtime_session.commit()
    finally:
        runtime_session.close()


def _fetch_result_rows(owner_engine, *, job_id: int) -> list[dict[str, object]]:
    with owner_engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT satellite_job_id, organization_id, lote_id, result_schema_version,
                       geometry_hash, algorithm_version, result_payload, payload_sha256
                FROM satellite_job_results
                WHERE satellite_job_id = :job_id
                """
            ),
            {"job_id": job_id},
        ).mappings().all()
    return [dict(row) for row in rows]


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


def _fetch_job_row(owner_engine, *, job_id: int) -> dict[str, object]:
    with owner_engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT id, status, lote_id, organization_id, geometry_hash,
                       algorithm_version, locked_by, lease_token, finished_at
                FROM satellite_jobs
                WHERE id = :job_id
                """
            ),
            {"job_id": job_id},
        ).mappings().one()
    return dict(row)


def _canonical_payload_bytes(payload: dict[str, object]) -> bytes:
    return canonicalize_satellite_job_result_payload(payload)


def test_catalog_schema_rls_and_privileges_are_hardened_for_immutable_results():
    with _integration_fixture() as fixture:
        with fixture["owner_engine"].connect() as conn:
            columns = conn.execute(
                text(
                    """
                    SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod) AS type_name
                    FROM pg_attribute AS a
                    WHERE a.attrelid = 'public.satellite_job_results'::regclass
                      AND a.attnum > 0
                      AND NOT a.attisdropped
                    ORDER BY a.attnum
                    """
                )
            ).mappings().all()
            pk_columns = conn.execute(
                text(
                    """
                    SELECT a.attname
                    FROM pg_index AS i
                    JOIN pg_attribute AS a
                      ON a.attrelid = i.indrelid
                     AND a.attnum = ANY(i.indkey)
                    WHERE i.indrelid = 'public.satellite_job_results'::regclass
                      AND i.indisprimary
                    ORDER BY array_position(i.indkey, a.attnum)
                    """
                )
            ).scalars().all()
            fk_rows = conn.execute(
                text(
                    """
                    SELECT conname, confdeltype
                    FROM pg_constraint
                    WHERE conrelid = 'public.satellite_job_results'::regclass
                      AND contype = 'f'
                    ORDER BY conname
                    """
                )
            ).mappings().all()
            rls_row = conn.execute(
                text(
                    """
                    SELECT relrowsecurity, relforcerowsecurity
                    FROM pg_class
                    WHERE oid = 'public.satellite_job_results'::regclass
                    """
                )
            ).mappings().one()
            policies = conn.execute(
                text(
                    """
                    SELECT policyname, cmd, qual, with_check
                    FROM pg_policies
                    WHERE schemaname = 'public'
                      AND tablename = 'satellite_job_results'
                    ORDER BY policyname
                    """
                )
            ).mappings().all()
            runtime_privileges = conn.execute(
                text(
                    """
                    SELECT
                        has_table_privilege(:role_name, 'public.satellite_job_results', 'SELECT') AS can_select,
                        has_table_privilege(:role_name, 'public.satellite_job_results', 'INSERT') AS can_insert,
                        has_table_privilege(:role_name, 'public.satellite_job_results', 'UPDATE') AS can_update,
                        has_table_privilege(:role_name, 'public.satellite_job_results', 'DELETE') AS can_delete,
                        has_table_privilege(:role_name, 'public.satellite_job_results', 'TRUNCATE') AS can_truncate,
                        has_table_privilege(:role_name, 'public.satellite_job_results', 'REFERENCES') AS can_references,
                        has_table_privilege(:role_name, 'public.satellite_job_results', 'TRIGGER') AS can_trigger
                    """
                ),
                {"role_name": RUNTIME_ROLE},
            ).mappings().one()
            public_grants = conn.execute(
                text(
                    """
                    SELECT count(*)
                    FROM information_schema.role_table_grants
                    WHERE table_schema = 'public'
                      AND table_name = 'satellite_job_results'
                      AND grantee = 'PUBLIC'
                    """
                )
            ).scalar_one()
            worker_privileges = conn.execute(
                text(
                    """
                    SELECT
                        has_table_privilege(:role_name, 'public.satellite_job_results', 'SELECT') AS can_select,
                        has_table_privilege(:role_name, 'public.satellite_job_results', 'INSERT') AS can_insert,
                        has_table_privilege(:role_name, 'public.satellite_job_results', 'UPDATE') AS can_update,
                        has_table_privilege(:role_name, 'public.satellite_job_results', 'DELETE') AS can_delete
                    """
                ),
                {"role_name": WORKER_ROLE},
            ).mappings().one()

    assert [(row["attname"], row["type_name"]) for row in columns] == [
        ("satellite_job_id", "integer"),
        ("organization_id", "integer"),
        ("lote_id", "integer"),
        ("result_schema_version", "character varying(50)"),
        ("geometry_hash", "character varying(64)"),
        ("algorithm_version", "character varying(50)"),
        ("result_payload", "jsonb"),
        ("payload_sha256", "character varying(64)"),
        ("created_at", "timestamp with time zone"),
    ]
    assert pk_columns == ["satellite_job_id"]
    assert {row["conname"]: row["confdeltype"] for row in fk_rows} == {
        "fk_satellite_job_results_job_tenant": "r",
        "fk_satellite_job_results_lote_tenant": "r",
        "fk_satellite_job_results_organization_id": "r",
    }
    assert rls_row["relrowsecurity"] is True
    assert rls_row["relforcerowsecurity"] is True
    assert [(row["policyname"], row["cmd"]) for row in policies] == [
        ("satellite_job_results_tenant_insert", "INSERT"),
        ("satellite_job_results_tenant_select", "SELECT"),
    ]
    for row in policies:
        if row["cmd"] == "SELECT":
            assert "current_setting('app.current_organization_id'" in (row["qual"] or "")
            assert row["with_check"] is None
        if row["cmd"] == "INSERT":
            assert row["qual"] is None
            assert "current_setting('app.current_organization_id'" in (row["with_check"] or "")
    assert dict(runtime_privileges) == {
        "can_select": True,
        "can_insert": True,
        "can_update": False,
        "can_delete": False,
        "can_truncate": False,
        "can_references": False,
        "can_trigger": False,
    }
    assert int(public_grants) == 0
    assert dict(worker_privileges) == {
        "can_select": False,
        "can_insert": False,
        "can_update": False,
        "can_delete": False,
    }


def test_runtime_insert_select_and_hash_visibility_are_tenant_safe():
    with _integration_fixture() as fixture:
        polygon_wkt_snapshot = (
            "POLYGON((-58.91 -27.46, -58.89 -27.46, -58.89 -27.44, "
            "-58.91 -27.44, -58.91 -27.46))"
        )
        geometry_hash = "a" * 64
        with fixture["owner_engine"].begin() as conn:
            job = _insert_job(
                conn,
                organization_id=fixture["org_a_id"],
                lote_id=fixture["lote_a_id"],
                geometry_hash=geometry_hash,
                polygon_wkt_snapshot=polygon_wkt_snapshot,
            )

        result = _sample_result(
            geometry_hash=geometry_hash,
            algorithm_version=ALGORITHM_VERSION,
        )
        _insert_runtime_snapshot(
            fixture["runtime_engine"],
            organization_id=fixture["org_a_id"],
            lote_id=fixture["lote_a_id"],
            job_id=int(job["id"]),
            result=result,
        )

        with fixture["runtime_engine"].begin() as conn:
            _set_tenant_context(conn, fixture["org_a_id"])
            visible_rows = conn.execute(
                text(
                    """
                    SELECT result_schema_version, result_payload, payload_sha256
                    FROM satellite_job_results
                    WHERE satellite_job_id = :job_id
                    """
                ),
                {"job_id": int(job["id"])},
            ).mappings().all()

    assert len(visible_rows) == 1
    visible_row = dict(visible_rows[0])
    assert visible_row["result_schema_version"] == NDVI_TIMESERIES_RESULT_SCHEMA_VERSION
    assert len(visible_row["payload_sha256"]) == 64
    assert compute_satellite_job_result_payload_sha256(
        visible_row["result_payload"]
    ) == visible_row["payload_sha256"]
    assert visible_row["result_payload"]["observations"][0]["aoi_cloud_percentage"] == 1.0
    for forbidden_key in (
        "organization_id",
        "worker_id",
        "locked_by",
        "heartbeat_at",
        "lease_token",
        "polygon_wkt_snapshot",
        "error_code",
        "error_message",
        "credentials",
    ):
        assert forbidden_key not in visible_row["result_payload"]


def test_runtime_update_delete_are_denied_and_cross_tenant_access_is_blocked():
    with _integration_fixture() as fixture:
        polygon_wkt_snapshot = _polygon_wkt_snapshot()
        geometry_hash = generate_geometry_hash(polygon_wkt_snapshot)
        with fixture["owner_engine"].begin() as conn:
            job = _insert_job(
                conn,
                organization_id=fixture["org_a_id"],
                lote_id=fixture["lote_a_id"],
                geometry_hash=geometry_hash,
                polygon_wkt_snapshot=polygon_wkt_snapshot,
            )
            cross_tenant_job = _insert_job(
                conn,
                organization_id=fixture["org_a_id"],
                lote_id=fixture["lote_a_id"],
                geometry_hash=geometry_hash,
                polygon_wkt_snapshot=polygon_wkt_snapshot,
            )

        result = _sample_result(
            geometry_hash=geometry_hash,
            algorithm_version=ALGORITHM_VERSION,
        )
        _insert_runtime_snapshot(
            fixture["runtime_engine"],
            organization_id=fixture["org_a_id"],
            lote_id=fixture["lote_a_id"],
            job_id=int(job["id"]),
            result=result,
        )

        with pytest.raises(DBAPIError) as update_exc:
            with fixture["runtime_engine"].begin() as conn:
                _set_tenant_context(conn, fixture["org_a_id"])
                conn.execute(
                    text(
                        """
                        UPDATE satellite_job_results
                        SET payload_sha256 = :payload_sha256
                        WHERE satellite_job_id = :job_id
                        """
                    ),
                    {
                        "payload_sha256": "f" * 64,
                        "job_id": int(job["id"]),
                    },
                )
        assert "permission denied" in str(update_exc.value).lower()

        with pytest.raises(DBAPIError) as delete_exc:
            with fixture["runtime_engine"].begin() as conn:
                _set_tenant_context(conn, fixture["org_a_id"])
                conn.execute(
                    text(
                        """
                        DELETE FROM satellite_job_results
                        WHERE satellite_job_id = :job_id
                        """
                    ),
                    {"job_id": int(job["id"])},
                )
        assert "permission denied" in str(delete_exc.value).lower()

        with fixture["runtime_engine"].begin() as conn:
            _set_tenant_context(conn, fixture["org_b_id"])
            invisible_rows = conn.execute(
                text(
                    """
                    SELECT satellite_job_id
                    FROM satellite_job_results
                    WHERE satellite_job_id = :job_id
                    """
                ),
                {"job_id": int(job["id"])},
            ).scalars().all()

        with pytest.raises(DBAPIError):
            with fixture["runtime_engine"].begin() as conn:
                _set_tenant_context(conn, fixture["org_b_id"])
                conn.execute(
                    text(
                        """
                        INSERT INTO satellite_job_results (
                            satellite_job_id, organization_id, lote_id,
                            result_schema_version, geometry_hash, algorithm_version,
                            result_payload, payload_sha256
                        )
                        VALUES (
                            :satellite_job_id, :organization_id, :lote_id,
                            :result_schema_version, :geometry_hash, :algorithm_version,
                            CAST(:result_payload AS jsonb), :payload_sha256
                        )
                        """
                    ),
                    {
                        "satellite_job_id": int(cross_tenant_job["id"]),
                        "organization_id": fixture["org_a_id"],
                        "lote_id": fixture["lote_a_id"],
                        "result_schema_version": NDVI_TIMESERIES_RESULT_SCHEMA_VERSION,
                        "geometry_hash": geometry_hash,
                        "algorithm_version": ALGORITHM_VERSION,
                        "result_payload": '{"schema_version":"ndvi_timeseries.v1","job_id":1,"lote_id":1,"geometry_hash":"x","algorithm_version":"x","total_observations":0,"observations":[]}',
                        "payload_sha256": "0" * 64,
                    },
                )

    assert invisible_rows == []


def test_parent_delete_barriers_preserve_evidence_lineage():
    with _integration_fixture() as fixture:
        polygon_wkt_snapshot = _polygon_wkt_snapshot()
        geometry_hash = generate_geometry_hash(polygon_wkt_snapshot)
        with fixture["owner_engine"].begin() as conn:
            job = _insert_job(
                conn,
                organization_id=fixture["org_a_id"],
                lote_id=fixture["lote_a_id"],
                geometry_hash=geometry_hash,
                polygon_wkt_snapshot=polygon_wkt_snapshot,
            )

        result = _sample_result(
            geometry_hash=geometry_hash,
            algorithm_version=ALGORITHM_VERSION,
        )
        _insert_runtime_snapshot(
            fixture["runtime_engine"],
            organization_id=fixture["org_a_id"],
            lote_id=fixture["lote_a_id"],
            job_id=int(job["id"]),
            result=result,
        )

        with pytest.raises(DBAPIError) as job_delete_exc:
            with fixture["runtime_engine"].begin() as conn:
                _set_tenant_context(conn, fixture["org_a_id"])
                conn.execute(
                    text("DELETE FROM satellite_jobs WHERE id = :job_id"),
                    {"job_id": int(job["id"])},
                )
        assert "foreign key" in str(job_delete_exc.value).lower()

        with fixture["runtime_engine"].begin() as conn:
            lote_delete_privilege = conn.execute(
                text(
                    """
                    SELECT has_table_privilege(
                        current_user,
                        'public.lotes',
                        'DELETE'
                    )
                    """
                )
            ).scalar_one()

        if lote_delete_privilege:
            with pytest.raises(DBAPIError) as runtime_lote_delete_exc:
                with fixture["runtime_engine"].begin() as conn:
                    _set_tenant_context(conn, fixture["org_a_id"])
                    conn.execute(
                        text("DELETE FROM lotes WHERE id = :lote_id"),
                        {"lote_id": fixture["lote_a_id"]},
                    )
            assert "foreign key" in str(runtime_lote_delete_exc.value).lower()

        with pytest.raises(DBAPIError) as lote_delete_exc:
            with fixture["owner_engine"].begin() as conn:
                conn.execute(
                    text("DELETE FROM lotes WHERE id = :lote_id"),
                    {"lote_id": fixture["lote_a_id"]},
                )
        assert "foreign key" in str(lote_delete_exc.value).lower()

        with pytest.raises(DBAPIError) as org_delete_exc:
            with fixture["owner_engine"].begin() as conn:
                conn.execute(
                    text("DELETE FROM organizations WHERE id = :organization_id"),
                    {"organization_id": fixture["org_a_id"]},
                )
        assert "foreign key" in str(org_delete_exc.value).lower()

        remaining_rows = _fetch_result_rows(
            fixture["owner_engine"],
            job_id=int(job["id"]),
        )

    assert len(remaining_rows) == 1


def test_real_worker_success_persists_snapshot_and_canonical_rows():
    with _integration_fixture() as fixture:
        polygon_wkt_snapshot = _polygon_wkt_snapshot()
        geometry_hash = generate_geometry_hash(polygon_wkt_snapshot)
        with fixture["owner_engine"].begin() as conn:
            job = _insert_job(
                conn,
                organization_id=fixture["org_a_id"],
                lote_id=fixture["lote_a_id"],
                geometry_hash=geometry_hash,
                polygon_wkt_snapshot=polygon_wkt_snapshot,
            )

        result = _sample_result(
            geometry_hash=geometry_hash,
            algorithm_version=ALGORITHM_VERSION,
            aoi_cloud_percentage=1.25,
        )

        with _ephemeral_worker_login(fixture["owner_engine"]) as worker_auth:
            with _postgres_runtime_worker_environment(
                worker_auth["worker_database_url"]
            ):
                worker = SatelliteWorker(
                    worker_id="p22e1-worker-success",
                    heartbeat_seconds=60,
                    stale_recovery_interval_seconds=None,
                    gee_ndvi_adapter=_FixedGeeAdapter(result),
                )
                run_result = worker.run_once()

        job_row = _fetch_job_row(fixture["owner_engine"], job_id=int(job["id"]))
        result_rows = _fetch_result_rows(fixture["owner_engine"], job_id=int(job["id"]))
        observation_count = _count_job_observations(
            fixture["owner_engine"],
            job_id=int(job["id"]),
        )

    assert run_result.status is WorkerRunStatus.SUCCEEDED
    assert job_row["status"] == "succeeded"
    assert observation_count == 1
    assert len(result_rows) == 1
    persisted_result = result_rows[0]
    assert persisted_result["result_payload"]["observations"][0]["aoi_cloud_percentage"] == 1.25
    assert persisted_result["geometry_hash"] == job_row["geometry_hash"]
    assert persisted_result["algorithm_version"] == job_row["algorithm_version"]
    assert compute_satellite_job_result_payload_sha256(
        persisted_result["result_payload"]
    ) == persisted_result["payload_sha256"]


def test_worker_lease_loss_rolls_back_snapshot_and_canonical_persistence():
    with _integration_fixture() as fixture:
        polygon_wkt_snapshot = _polygon_wkt_snapshot()
        geometry_hash = generate_geometry_hash(polygon_wkt_snapshot)
        with fixture["owner_engine"].begin() as conn:
            job = _insert_job(
                conn,
                organization_id=fixture["org_a_id"],
                lote_id=fixture["lote_a_id"],
                geometry_hash=geometry_hash,
                polygon_wkt_snapshot=polygon_wkt_snapshot,
            )

        result = _sample_result(
            geometry_hash=geometry_hash,
            algorithm_version=ALGORITHM_VERSION,
        )

        def _steal_lease(_request) -> None:
            with fixture["owner_engine"].begin() as conn:
                conn.execute(
                    text(
                        """
                        UPDATE satellite_jobs
                        SET locked_by = 'p22e1-zombie-owner-b',
                            locked_at = CURRENT_TIMESTAMP,
                            heartbeat_at = CURRENT_TIMESTAMP,
                            lease_token = :lease_token
                        WHERE id = :job_id
                        """
                    ),
                    {
                        "job_id": int(job["id"]),
                        "lease_token": str(uuid4()),
                    },
                )

        with _ephemeral_worker_login(fixture["owner_engine"]) as worker_auth:
            with _postgres_runtime_worker_environment(
                worker_auth["worker_database_url"]
            ):
                worker = SatelliteWorker(
                    worker_id="p22e1-worker-zombie-a",
                    heartbeat_seconds=60,
                    stale_recovery_interval_seconds=None,
                    gee_ndvi_adapter=_FixedGeeAdapter(
                        result,
                        on_execute=_steal_lease,
                    ),
                )
                run_result = worker.run_once()

        result_rows = _fetch_result_rows(fixture["owner_engine"], job_id=int(job["id"]))
        observation_count = _count_job_observations(
            fixture["owner_engine"],
            job_id=int(job["id"]),
        )
        job_row = _fetch_job_row(fixture["owner_engine"], job_id=int(job["id"]))

    assert run_result.status is WorkerRunStatus.LEASE_LOST
    assert result_rows == []
    assert observation_count == 0
    assert job_row["status"] == "running"
    assert job_row["locked_by"] == "p22e1-zombie-owner-b"


def test_worker_zero_observation_success_still_creates_snapshot():
    with _integration_fixture() as fixture:
        polygon_wkt_snapshot = _polygon_wkt_snapshot()
        geometry_hash = generate_geometry_hash(polygon_wkt_snapshot)
        with fixture["owner_engine"].begin() as conn:
            job = _insert_job(
                conn,
                organization_id=fixture["org_a_id"],
                lote_id=fixture["lote_a_id"],
                geometry_hash=geometry_hash,
                polygon_wkt_snapshot=polygon_wkt_snapshot,
            )

        zero_result = NormalizedNdviExecutionResult(
            geometry_hash=geometry_hash,
            algorithm_version=ALGORITHM_VERSION,
            observations=(),
        )

        with _ephemeral_worker_login(fixture["owner_engine"]) as worker_auth:
            with _postgres_runtime_worker_environment(
                worker_auth["worker_database_url"]
            ):
                worker = SatelliteWorker(
                    worker_id="p22e1-worker-zero",
                    heartbeat_seconds=60,
                    stale_recovery_interval_seconds=None,
                    gee_ndvi_adapter=_FixedGeeAdapter(zero_result),
                )
                run_result = worker.run_once()

        result_rows = _fetch_result_rows(fixture["owner_engine"], job_id=int(job["id"]))
        observation_count = _count_job_observations(
            fixture["owner_engine"],
            job_id=int(job["id"]),
        )
        job_row = _fetch_job_row(fixture["owner_engine"], job_id=int(job["id"]))

    assert run_result.status is WorkerRunStatus.SUCCEEDED
    assert job_row["status"] == "succeeded"
    assert observation_count == 0
    assert len(result_rows) == 1
    assert result_rows[0]["result_payload"]["total_observations"] == 0
    assert result_rows[0]["result_payload"]["observations"] == []
    assert compute_satellite_job_result_payload_sha256(
        result_rows[0]["result_payload"]
    ) == result_rows[0]["payload_sha256"]


def test_job_a_and_job_b_snapshots_remain_independently_immutable():
    with _integration_fixture() as fixture:
        polygon_wkt_snapshot = _polygon_wkt_snapshot()
        geometry_hash = generate_geometry_hash(polygon_wkt_snapshot)
        with fixture["owner_engine"].begin() as conn:
            job_a = _insert_job(
                conn,
                organization_id=fixture["org_a_id"],
                lote_id=fixture["lote_a_id"],
                geometry_hash=geometry_hash,
                polygon_wkt_snapshot=polygon_wkt_snapshot,
            )
            job_b = _insert_job(
                conn,
                organization_id=fixture["org_a_id"],
                lote_id=fixture["lote_a_id"],
                geometry_hash=geometry_hash,
                polygon_wkt_snapshot=polygon_wkt_snapshot,
            )

        with _ephemeral_worker_login(fixture["owner_engine"]) as worker_auth:
            with _postgres_runtime_worker_environment(
                worker_auth["worker_database_url"]
            ):
                worker_a = SatelliteWorker(
                    worker_id="p22e1-worker-a",
                    heartbeat_seconds=60,
                    stale_recovery_interval_seconds=None,
                    gee_ndvi_adapter=_FixedGeeAdapter(
                        _sample_result(
                            geometry_hash=geometry_hash,
                            algorithm_version=ALGORITHM_VERSION,
                            ndvi_mean=0.61,
                        )
                    ),
                )
                first_run = worker_a.run_once()

                snapshot_a_before = _fetch_result_rows(
                    fixture["owner_engine"],
                    job_id=int(job_a["id"]),
                )[0]
                snapshot_a_bytes_before = _canonical_payload_bytes(
                    snapshot_a_before["result_payload"]
                )
                snapshot_a_hash_before = snapshot_a_before["payload_sha256"]

                worker_b = SatelliteWorker(
                    worker_id="p22e1-worker-b",
                    heartbeat_seconds=60,
                    stale_recovery_interval_seconds=None,
                    gee_ndvi_adapter=_FixedGeeAdapter(
                        _sample_result(
                            geometry_hash=geometry_hash,
                            algorithm_version=ALGORITHM_VERSION,
                            ndvi_mean=0.72,
                        )
                    ),
                )
                second_run = worker_b.run_once()

        snapshot_a_after = _fetch_result_rows(
            fixture["owner_engine"],
            job_id=int(job_a["id"]),
        )[0]
        snapshot_b = _fetch_result_rows(
            fixture["owner_engine"],
            job_id=int(job_b["id"]),
        )[0]
        canonical_job_ids = []
        with fixture["owner_engine"].connect() as conn:
            canonical_job_ids = conn.execute(
                text(
                    """
                    SELECT satellite_job_id
                    FROM satellite_ndvi_observations
                    WHERE organization_id = :organization_id
                    ORDER BY observation_date
                    """
                ),
                {"organization_id": fixture["org_a_id"]},
            ).scalars().all()

    assert first_run.status is WorkerRunStatus.SUCCEEDED
    assert second_run.status is WorkerRunStatus.SUCCEEDED
    assert _canonical_payload_bytes(snapshot_a_after["result_payload"]) == snapshot_a_bytes_before
    assert snapshot_a_after["payload_sha256"] == snapshot_a_hash_before
    assert snapshot_a_after["satellite_job_id"] == int(job_a["id"])
    assert snapshot_b["satellite_job_id"] == int(job_b["id"])
    assert snapshot_b["payload_sha256"] != snapshot_a_hash_before
    assert canonical_job_ids == [int(job_b["id"])]


def test_duplicate_snapshot_insert_fails_closed_at_database_pk():
    with _integration_fixture() as fixture:
        polygon_wkt_snapshot = _polygon_wkt_snapshot()
        geometry_hash = generate_geometry_hash(polygon_wkt_snapshot)
        with fixture["owner_engine"].begin() as conn:
            job = _insert_job(
                conn,
                organization_id=fixture["org_a_id"],
                lote_id=fixture["lote_a_id"],
                geometry_hash=geometry_hash,
                polygon_wkt_snapshot=polygon_wkt_snapshot,
            )

        result = _sample_result(
            geometry_hash=geometry_hash,
            algorithm_version=ALGORITHM_VERSION,
        )
        _insert_runtime_snapshot(
            fixture["runtime_engine"],
            organization_id=fixture["org_a_id"],
            lote_id=fixture["lote_a_id"],
            job_id=int(job["id"]),
            result=result,
        )

        duplicate_session = sessionmaker(
            bind=fixture["runtime_engine"],
            autoflush=False,
            autocommit=False,
        )()
        try:
            with pytest.raises((IntegrityError, DBAPIError)):
                duplicate_session.execute(
                    text(
                        """
                        INSERT INTO satellite_job_results (
                            satellite_job_id, organization_id, lote_id,
                            result_schema_version, geometry_hash, algorithm_version,
                            result_payload, payload_sha256
                        )
                        VALUES (
                            :satellite_job_id, :organization_id, :lote_id,
                            :result_schema_version, :geometry_hash, :algorithm_version,
                            CAST(:result_payload AS jsonb), :payload_sha256
                        )
                        """
                    ),
                    {
                        "satellite_job_id": int(job["id"]),
                        "organization_id": fixture["org_a_id"],
                        "lote_id": fixture["lote_a_id"],
                        "result_schema_version": NDVI_TIMESERIES_RESULT_SCHEMA_VERSION,
                        "geometry_hash": geometry_hash,
                        "algorithm_version": ALGORITHM_VERSION,
                        "result_payload": '{"schema_version":"ndvi_timeseries.v1","job_id":1,"lote_id":1,"geometry_hash":"x","algorithm_version":"x","total_observations":0,"observations":[]}',
                        "payload_sha256": "0" * 64,
                    },
                )
            duplicate_session.rollback()
        finally:
            duplicate_session.close()

        persisted_result = _fetch_result_rows(
            fixture["owner_engine"],
            job_id=int(job["id"]),
        )[0]

    assert compute_satellite_job_result_payload_sha256(
        persisted_result["result_payload"]
    ) == persisted_result["payload_sha256"]
