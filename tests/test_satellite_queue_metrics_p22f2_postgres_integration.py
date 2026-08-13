from __future__ import annotations

import os
import secrets
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from psycopg import ClientCursor, sql
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError

from litoral_trace.config.settings import normalize_database_url
from litoral_trace.services.gee import ALGORITHM_VERSION, generate_geometry_hash


def _truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}


ENABLED = _truthy(os.environ.get("ENABLE_POSTGRES_TESTS"))
RUNTIME_URL = os.environ.get("TEST_POSTGRES_DATABASE_URL")
OWNER_URL = os.environ.get("TEST_POSTGRES_MIGRATION_DATABASE_URL")
RUNTIME_ROLE = "litoral_trace_app"
WORKER_ROLE = "litoral_trace_worker_executor"
FUNCTION_SIGNATURE = "public.worker_get_satellite_queue_metrics()"

APPROVED_COLUMNS = {
    "snapshot_time",
    "queued_ready_count",
    "queued_delayed_count",
    "running_count",
    "running_stale_count",
    "running_invalid_count",
    "oldest_ready_age_seconds",
    "oldest_active_lease_age_seconds",
    "oldest_heartbeat_age_seconds",
    "next_delayed_ready_in_seconds",
}


pytestmark = pytest.mark.skipif(
    not (ENABLED and RUNTIME_URL and OWNER_URL),
    reason=(
        "PostgreSQL P2.2F2 tests require ENABLE_POSTGRES_TESTS=1 plus "
        "isolated runtime and owner test URLs."
    ),
)


def _owner_engine():
    return create_engine(
        normalize_database_url(OWNER_URL),
        pool_pre_ping=True,
        hide_parameters=True,
    )


def _runtime_engine():
    return create_engine(
        normalize_database_url(RUNTIME_URL),
        pool_pre_ping=True,
        hide_parameters=True,
    )


def _client_statement(connection, statement, params=()):
    driver = connection.connection.driver_connection

    with ClientCursor(driver) as cursor:
        cursor.execute(statement, params)


@contextmanager
def _worker_login(owner_engine):
    role_name = f"litoral_trace_f2_worker_{uuid4().hex[:16]}"
    password = secrets.token_urlsafe(24)

    owner_url = make_url(normalize_database_url(OWNER_URL))
    worker_url = owner_url.set(
        username=role_name,
        password=password,
    )

    worker_engine = None

    with owner_engine.begin() as conn:
        _client_statement(
            conn,
            sql.SQL(
                "CREATE ROLE {} LOGIN INHERIT NOSUPERUSER NOCREATEDB "
                "NOCREATEROLE NOBYPASSRLS PASSWORD %s"
            ).format(sql.Identifier(role_name)),
            (password,),
        )

        _client_statement(
            conn,
            sql.SQL("GRANT {} TO {}").format(
                sql.Identifier(WORKER_ROLE),
                sql.Identifier(role_name),
            ),
        )

    try:
        worker_engine = create_engine(
            worker_url.render_as_string(hide_password=False),
            pool_pre_ping=True,
            hide_parameters=True,
        )

        yield worker_engine

    finally:
        if worker_engine is not None:
            worker_engine.dispose()

        with owner_engine.begin() as conn:
            _client_statement(
                conn,
                sql.SQL("REVOKE {} FROM {}").format(
                    sql.Identifier(WORKER_ROLE),
                    sql.Identifier(role_name),
                ),
            )

            _client_statement(
                conn,
                sql.SQL("DROP ROLE {}").format(
                    sql.Identifier(role_name)
                ),
            )


def _polygon() -> str:
    return (
        "POLYGON((-58.91 -27.46, -58.89 -27.46, -58.89 -27.44, "
        "-58.91 -27.44, -58.91 -27.46))"
    )


def _snapshot(connection):
    return connection.execute(
        text(
            "SELECT * "
            "FROM public.worker_get_satellite_queue_metrics()"
        )
    ).mappings().one()


def _public_has_function_execute(connection) -> bool:
    """
    Return True when PostgreSQL PUBLIC has EXECUTE on the queue metrics
    SECURITY DEFINER function.

    PUBLIC is a pseudo-role rather than a normal pg_authid role, so
    has_function_privilege('PUBLIC', ...) must not be used for this check.

    In PostgreSQL ACLs:
        grantee = 0
    represents PUBLIC.
    """
    return bool(
        connection.execute(
            text(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_proc AS p
                    CROSS JOIN LATERAL aclexplode(
                        COALESCE(
                            p.proacl,
                            acldefault('f', p.proowner)
                        )
                    ) AS acl
                    WHERE p.oid = to_regprocedure(:signature)
                      AND acl.grantee = 0
                      AND acl.privilege_type = 'EXECUTE'
                )
                """
            ),
            {
                "signature": FUNCTION_SIGNATURE,
            },
        ).scalar_one()
    )


@contextmanager
def _queue_fixture(owner_engine):
    suffix = uuid4().hex[:10]
    now = datetime.now(timezone.utc)

    fixture: dict[str, object] = {
        "now": now,
    }

    polygon = _polygon()
    geometry_hash = generate_geometry_hash(polygon)

    with owner_engine.begin() as conn:
        baseline = _snapshot(conn)
        fixture["baseline"] = baseline

        for label in ("a", "b"):
            organization_id = conn.execute(
                text(
                    """
                    INSERT INTO organizations (
                        name,
                        slug,
                        tax_id,
                        tier,
                        description,
                        is_active
                    )
                    VALUES (
                        :name,
                        :slug,
                        :tax_id,
                        'pro',
                        'P2.2F2 test',
                        true
                    )
                    RETURNING id
                    """
                ),
                {
                    "name": f"P22F2 Org {label} {suffix}",
                    "slug": f"p22f2-{label}-{suffix}",
                    "tax_id": f"F2-{label}-{suffix}",
                },
            ).scalar_one()

            lote_id = conn.execute(
                text(
                    """
                    INSERT INTO lotes (
                        organization_id,
                        identificador,
                        productor_id,
                        producto_forestal,
                        hectareas,
                        latitud,
                        longitud,
                        polygon_wkt,
                        estatus,
                        volumen_ingresado_ton,
                        volumen_exportar_ton
                    )
                    VALUES (
                        :org,
                        :identifier,
                        :producer,
                        'Madera Aserrada (Pino)',
                        10,
                        -27.45,
                        -58.90,
                        :polygon,
                        'Pendiente',
                        20,
                        5
                    )
                    RETURNING id
                    """
                ),
                {
                    "org": organization_id,
                    "identifier": f"F2-{label}-{suffix}",
                    "producer": f"F2-P-{label}-{suffix}",
                    "polygon": polygon,
                },
            ).scalar_one()

            fixture[f"org_{label}"] = int(organization_id)
            fixture[f"lote_{label}"] = int(lote_id)

        jobs = (
            (
                "a",
                "queued",
                0,
                now - timedelta(seconds=10),
                None,
                None,
                None,
                None,
                now - timedelta(seconds=120),
                None,
            ),
            (
                "b",
                "queued",
                1,
                now + timedelta(seconds=60),
                None,
                None,
                None,
                None,
                now - timedelta(seconds=30),
                None,
            ),
            (
                "a",
                "running",
                1,
                now,
                now - timedelta(seconds=20),
                "worker-f2",
                now - timedelta(seconds=10),
                str(uuid4()),
                now - timedelta(seconds=20),
                None,
            ),
            (
                "b",
                "running",
                2,
                now,
                now - timedelta(seconds=180),
                "worker-f2",
                now - timedelta(seconds=180),
                str(uuid4()),
                now - timedelta(seconds=180),
                None,
            ),
            (
                "a",
                "running",
                1,
                now,
                None,
                None,
                None,
                None,
                now,
                None,
            ),
        )

        for index, job in enumerate(jobs):
            (
                label,
                status,
                attempt,
                next_attempt,
                locked_at,
                locked_by,
                heartbeat_at,
                lease_token,
                created_at,
                finished_at,
            ) = job

            conn.execute(
                text(
                    """
                    INSERT INTO satellite_jobs (
                        organization_id,
                        lote_id,
                        job_type,
                        status,
                        attempt_count,
                        max_attempts,
                        next_attempt_at,
                        locked_at,
                        locked_by,
                        heartbeat_at,
                        lease_token,
                        started_at,
                        finished_at,
                        request_start_date,
                        request_end_date,
                        max_cloud_pct,
                        geometry_hash,
                        algorithm_version,
                        polygon_wkt_snapshot,
                        created_at,
                        updated_at
                    )
                    VALUES (
                        :org,
                        :lote,
                        'ndvi_timeseries',
                        :status,
                        :attempt,
                        3,
                        :next_attempt,
                        :locked_at,
                        :locked_by,
                        :heartbeat_at,
                        :lease_token,
                        :started_at,
                        :finished_at,
                        '2026-07-01',
                        '2026-08-01',
                        20,
                        :geometry_hash,
                        :algorithm_version,
                        :polygon,
                        :created_at,
                        :now
                    )
                    """
                ),
                {
                    "org": fixture[f"org_{label}"],
                    "lote": fixture[f"lote_{label}"],
                    "status": status,
                    "attempt": attempt,
                    "next_attempt": next_attempt,
                    "locked_at": locked_at,
                    "locked_by": locked_by,
                    "heartbeat_at": heartbeat_at,
                    "lease_token": lease_token,
                    "started_at": now if index == 0 else locked_at,
                    "finished_at": finished_at,
                    "geometry_hash": geometry_hash,
                    "algorithm_version": ALGORITHM_VERSION,
                    "polygon": polygon,
                    "created_at": created_at,
                    "now": now,
                },
            )

    try:
        yield fixture

    finally:
        with owner_engine.begin() as conn:
            conn.execute(
                text(
                    "DELETE FROM organizations "
                    "WHERE id = ANY(:ids)"
                ),
                {
                    "ids": [
                        fixture["org_a"],
                        fixture["org_b"],
                    ],
                },
            )


def test_migration_revision_function_security_rls_and_index():
    owner = _owner_engine()

    try:
        with owner.connect() as conn:
            assert (
                conn.execute(
                    text(
                        "SELECT version_num "
                        "FROM alembic_version"
                    )
                ).scalar_one()
                == "015_add_satellite_queue_metrics"
            )

            proc = conn.execute(
                text(
                    """
                    SELECT
                        p.prosecdef,
                        p.provolatile,
                        p.proconfig
                    FROM pg_proc AS p
                    WHERE p.oid = to_regprocedure(:signature)
                    """
                ),
                {
                    "signature": FUNCTION_SIGNATURE,
                },
            ).mappings().one()

            assert proc["prosecdef"] is True
            assert proc["provolatile"] == "v"

            assert (
                "search_path=public, pg_temp"
                in (proc["proconfig"] or [])
            )

            # PUBLIC is represented by grantee OID 0 in PostgreSQL ACLs.
            assert _public_has_function_execute(conn) is False

            assert (
                conn.execute(
                    text(
                        "SELECT has_function_privilege("
                        ":role, :sig, 'EXECUTE'"
                        ")"
                    ),
                    {
                        "role": RUNTIME_ROLE,
                        "sig": FUNCTION_SIGNATURE,
                    },
                ).scalar_one()
                is False
            )

            assert (
                conn.execute(
                    text(
                        "SELECT has_function_privilege("
                        ":role, :sig, 'EXECUTE'"
                        ")"
                    ),
                    {
                        "role": WORKER_ROLE,
                        "sig": FUNCTION_SIGNATURE,
                    },
                ).scalar_one()
                is True
            )

            assert (
                conn.execute(
                    text(
                        "SELECT has_table_privilege("
                        ":role, "
                        "'public.satellite_jobs', "
                        "'SELECT'"
                        ")"
                    ),
                    {
                        "role": WORKER_ROLE,
                    },
                ).scalar_one()
                is False
            )

            assert (
                conn.execute(
                    text(
                        "SELECT relforcerowsecurity "
                        "FROM pg_class "
                        "WHERE oid = "
                        "'public.satellite_jobs'::regclass"
                    )
                ).scalar_one()
                is True
            )

            index = conn.execute(
                text(
                    """
                    SELECT indexdef
                    FROM pg_indexes
                    WHERE schemaname = 'public'
                      AND indexname =
                          'ix_satellite_jobs_running_locked_at'
                    """
                )
            ).scalar_one()

            assert "locked_at" in index
            assert (
                "status" in index
                and "finished_at IS NULL" in index
            )

            assert set(_snapshot(conn).keys()) == APPROVED_COLUMNS

    finally:
        owner.dispose()


def test_worker_capability_executes_aggregate_but_cannot_select_table():
    owner = _owner_engine()

    try:
        with _worker_login(owner) as worker:
            with worker.connect() as conn:
                assert (
                    set(_snapshot(conn).keys())
                    == APPROVED_COLUMNS
                )

            with pytest.raises(DBAPIError):
                with worker.begin() as conn:
                    conn.execute(
                        text(
                            "SELECT id "
                            "FROM public.satellite_jobs "
                            "LIMIT 1"
                        )
                    )

    finally:
        owner.dispose()


def test_runtime_cannot_execute_global_function_and_tenant_rls_remains_scoped():
    owner = _owner_engine()
    runtime = _runtime_engine()

    try:
        with _queue_fixture(owner) as fixture:
            with pytest.raises(DBAPIError):
                with runtime.begin() as conn:
                    _snapshot(conn)

            with runtime.begin() as conn:
                conn.execute(
                    text(
                        "SELECT set_config("
                        "'app.current_organization_id', "
                        ":org, "
                        "true"
                        ")"
                    ),
                    {
                        "org": str(fixture["org_a"]),
                    },
                )

                organizations = conn.execute(
                    text(
                        """
                        SELECT DISTINCT organization_id
                        FROM satellite_jobs
                        WHERE organization_id IN (
                            :org_a,
                            :org_b
                        )
                        """
                    ),
                    {
                        "org_a": fixture["org_a"],
                        "org_b": fixture["org_b"],
                    },
                ).scalars().all()

                assert organizations == [
                    fixture["org_a"],
                ]

    finally:
        runtime.dispose()
        owner.dispose()


def test_global_snapshot_matches_claim_and_stale_state_semantics():
    owner = _owner_engine()

    try:
        with _queue_fixture(owner) as fixture:
            with owner.connect() as conn:
                snapshot = _snapshot(conn)

            baseline = fixture["baseline"]

            assert (
                snapshot["queued_ready_count"]
                - baseline["queued_ready_count"]
                == 1
            )

            assert (
                snapshot["queued_delayed_count"]
                - baseline["queued_delayed_count"]
                == 1
            )

            assert (
                snapshot["running_count"]
                - baseline["running_count"]
                == 3
            )

            assert (
                snapshot["running_stale_count"]
                - baseline["running_stale_count"]
                == 1
            )

            assert (
                snapshot["running_invalid_count"]
                - baseline["running_invalid_count"]
                == 1
            )

            assert snapshot["oldest_ready_age_seconds"] >= 100

            assert (
                snapshot["oldest_active_lease_age_seconds"]
                >= 150
            )

            assert (
                snapshot["oldest_heartbeat_age_seconds"]
                >= 150
            )

            assert (
                0
                <= snapshot["next_delayed_ready_in_seconds"]
                <= 90
            )

            assert all(
                snapshot[field] is not None
                and snapshot[field] >= 0
                for field in APPROVED_COLUMNS
                if field != "snapshot_time"
            )

    finally:
        owner.dispose()