from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timedelta
from threading import Barrier
from uuid import UUID, uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError

from litoral_trace.config.settings import normalize_database_url


def _truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}


POSTGRES_RLS_TESTS_ENABLED = _truthy(os.environ.get("ENABLE_POSTGRES_TESTS"))
RUNTIME_TEST_DATABASE_URL = os.environ.get("TEST_POSTGRES_DATABASE_URL")
MIGRATION_TEST_DATABASE_URL = os.environ.get("TEST_POSTGRES_MIGRATION_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not (
        POSTGRES_RLS_TESTS_ENABLED
        and RUNTIME_TEST_DATABASE_URL
        and MIGRATION_TEST_DATABASE_URL
    ),
    reason=(
        "PostgreSQL P2.2B tests require ENABLE_POSTGRES_TESTS=1 plus isolated "
        "TEST_POSTGRES_DATABASE_URL and TEST_POSTGRES_MIGRATION_DATABASE_URL."
    ),
)


WORKER_ROLE = "litoral_trace_worker_executor"
RUNTIME_ROLE = "litoral_trace_app"
CLAIM_FUNCTION_NAME = "public.worker_claim_next_satellite_job"


def _runtime_engine():
    return create_engine(
        normalize_database_url(RUNTIME_TEST_DATABASE_URL),
        pool_size=8,
        max_overflow=0,
        pool_pre_ping=True,
    )


def _owner_engine():
    return create_engine(
        normalize_database_url(MIGRATION_TEST_DATABASE_URL),
        pool_pre_ping=True,
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
            {
                "requested_worker_id": worker_id,
            },
        ).mappings().one_or_none()
    return dict(row) if row is not None else None


def _concurrent_claims(engine, worker_ids: list[str]):
    barrier = Barrier(len(worker_ids))

    def _runner(worker_id: str):
        barrier.wait(timeout=5)
        return _claim_once(engine, worker_id=worker_id)

    with ThreadPoolExecutor(max_workers=len(worker_ids)) as executor:
        return list(executor.map(_runner, worker_ids))


def _insert_job(
    conn,
    *,
    organization_id: int,
    lote_id: int,
    status: str = "queued",
    attempt_count: int = 0,
    max_attempts: int = 3,
    next_attempt_at: datetime | None = None,
    created_at: datetime | None = None,
    locked_at: datetime | None = None,
    locked_by: str | None = None,
    heartbeat_at: datetime | None = None,
    lease_token: str | None = None,
    finished_at: datetime | None = None,
) -> int:
    db_now = _db_now(conn)
    eligible_now = db_now - timedelta(minutes=10)
    request_start_date = (db_now - timedelta(days=30)).date()
    request_end_date = db_now.date()
    row = conn.execute(
        text(
            """
            INSERT INTO satellite_jobs (
                organization_id, lote_id, job_type, status, attempt_count, max_attempts,
                next_attempt_at, locked_at, locked_by, heartbeat_at, finished_at,
                lease_token, request_start_date, request_end_date, max_cloud_pct,
                geometry_hash, algorithm_version, polygon_wkt_snapshot, created_at, updated_at
            )
            VALUES (
                :organization_id, :lote_id, 'ndvi_timeseries', :status, :attempt_count,
                :max_attempts, :next_attempt_at, :locked_at, :locked_by, :heartbeat_at,
                :finished_at, :lease_token, :request_start_date, :request_end_date, 20.0,
                :geometry_hash, 'algo-v1', :polygon_wkt_snapshot, :created_at, :updated_at
            )
            RETURNING id
            """
        ),
        {
            "organization_id": organization_id,
            "lote_id": lote_id,
            "status": status,
            "attempt_count": attempt_count,
            "max_attempts": max_attempts,
            "next_attempt_at": next_attempt_at or eligible_now,
            "locked_at": locked_at,
            "locked_by": locked_by,
            "heartbeat_at": heartbeat_at,
            "finished_at": finished_at,
            "lease_token": lease_token,
            "request_start_date": request_start_date,
            "request_end_date": request_end_date,
            "geometry_hash": uuid4().hex + uuid4().hex,
            "polygon_wkt_snapshot": "POLYGON((-58.91 -27.46, -58.89 -27.46, -58.89 -27.44, -58.91 -27.44, -58.91 -27.46))",
            "created_at": created_at or eligible_now,
            "updated_at": created_at or eligible_now,
        },
    ).scalar_one()
    return int(row)


@contextmanager
def _claim_fixture():
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
                "name": f"P22B Org A {suffix}",
                "slug": f"p22b-org-a-{suffix}",
                "tax_id": f"70-{suffix}",
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
                "name": f"P22B Org B {suffix}",
                "slug": f"p22b-org-b-{suffix}",
                "tax_id": f"71-{suffix}",
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
                "identificador": f"P22B-LOTE-A-{suffix}",
                "productor_id": f"21-A-{suffix}",
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
                "identificador": f"P22B-LOTE-B-{suffix}",
                "productor_id": f"21-B-{suffix}",
                "polygon_wkt": "POLYGON((-58.81 -27.56, -58.79 -27.56, -58.79 -27.54, -58.81 -27.54, -58.81 -27.56))",
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
                text(
                    "DELETE FROM organizations WHERE id IN (:org_a_id, :org_b_id)"
                ),
                {"org_a_id": org_a_id, "org_b_id": org_b_id},
            )
        runtime_engine.dispose()
        owner_engine.dispose()


def test_claim_function_is_security_definer_and_not_public_or_runtime_executable():
    with _claim_fixture() as fixture:
        with fixture["owner_engine"].connect() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT
                        p.proname,
                        pg_get_userbyid(p.proowner) AS owner_name,
                        p.prosecdef,
                        pg_get_function_identity_arguments(p.oid) AS identity_args,
                        coalesce(array_to_string(p.proconfig, ','), '') AS proconfig,
                        coalesce(p.proacl::text, '') AS acl_text,
                        has_function_privilege(:runtime_role, p.oid, 'EXECUTE') AS runtime_execute,
                        has_function_privilege(:worker_role, p.oid, 'EXECUTE') AS worker_execute
                    FROM pg_proc AS p
                    JOIN pg_namespace AS n
                        ON n.oid = p.pronamespace
                    WHERE n.nspname = 'public'
                      AND p.proname = 'worker_claim_next_satellite_job'
                    """
                ),
                {
                    "runtime_role": RUNTIME_ROLE,
                    "worker_role": WORKER_ROLE,
                },
            ).mappings().one()

    assert row["proname"] == "worker_claim_next_satellite_job"
    assert row["owner_name"] != RUNTIME_ROLE
    assert row["prosecdef"] is True
    assert row["identity_args"] == "requested_worker_id text"
    assert "search_path=public, pg_temp" in row["proconfig"]
    assert "{=X/" not in row["acl_text"]
    assert ",=X/" not in row["acl_text"]
    assert row["runtime_execute"] is False
    assert row["worker_execute"] is True


def test_worker_role_is_nologin_and_has_no_direct_satellite_job_privileges():
    with _claim_fixture() as fixture:
        with fixture["owner_engine"].connect() as conn:
            role_row = conn.execute(
                text(
                    """
                    SELECT rolcanlogin, rolsuper, rolcreatedb, rolcreaterole,
                           rolinherit, rolbypassrls
                    FROM pg_roles
                    WHERE rolname = :worker_role
                    """
                ),
                {"worker_role": WORKER_ROLE},
            ).mappings().one()
            grants_row = conn.execute(
                text(
                    """
                    SELECT
                        has_table_privilege(:worker_role, 'public.satellite_jobs', 'SELECT') AS can_select,
                        has_table_privilege(:worker_role, 'public.satellite_jobs', 'INSERT') AS can_insert,
                        has_table_privilege(:worker_role, 'public.satellite_jobs', 'UPDATE') AS can_update,
                        has_table_privilege(:worker_role, 'public.satellite_jobs', 'DELETE') AS can_delete,
                        has_sequence_privilege(:worker_role, 'public.satellite_jobs_id_seq', 'USAGE') AS can_use_seq,
                        has_sequence_privilege(:worker_role, 'public.satellite_jobs_id_seq', 'SELECT') AS can_select_seq
                    """
                ),
                {"worker_role": WORKER_ROLE},
            ).mappings().one()

    assert role_row["rolcanlogin"] is False
    assert role_row["rolsuper"] is False
    assert role_row["rolcreatedb"] is False
    assert role_row["rolcreaterole"] is False
    assert role_row["rolinherit"] is False
    assert role_row["rolbypassrls"] is False
    assert grants_row["can_select"] is False
    assert grants_row["can_insert"] is False
    assert grants_row["can_update"] is False
    assert grants_row["can_delete"] is False
    assert grants_row["can_use_seq"] is False
    assert grants_row["can_select_seq"] is False


def test_runtime_role_cannot_execute_global_claim_function():
    with _claim_fixture() as fixture:
        with fixture["runtime_engine"].begin() as conn:
            with pytest.raises(DBAPIError):
                conn.execute(
                    text(
                        """
                        SELECT *
                        FROM public.worker_claim_next_satellite_job(
                            :requested_worker_id
                        )
                        """
                    ),
                    {
                        "requested_worker_id": "runtime-worker",
                    },
                ).mappings().one_or_none()


def test_old_two_argument_claim_signature_is_not_available():
    with _claim_fixture() as fixture:
        with fixture["owner_engine"].begin() as conn:
            with pytest.raises(DBAPIError):
                conn.execute(
                    text(
                        """
                        SELECT *
                        FROM public.worker_claim_next_satellite_job(
                            :requested_worker_id,
                            :requested_lease_token
                        )
                        """
                    ),
                    {
                        "requested_worker_id": "worker-invalid",
                        "requested_lease_token": str(uuid4()),
                    },
                ).mappings().one_or_none()


def test_claim_empty_queue_returns_none_cleanly():
    with _claim_fixture() as fixture:
        claimed = _claim_once(fixture["owner_engine"], worker_id="worker-empty")

    assert claimed is None


def test_claim_transitions_job_to_running_and_rls_visibility_remains_intact():
    with _claim_fixture() as fixture:
        with fixture["owner_engine"].begin() as conn:
            job_id = _insert_job(
                conn,
                organization_id=fixture["org_a_id"],
                lote_id=fixture["lote_a_id"],
            )

        claimed = _claim_once(fixture["owner_engine"], worker_id="worker-claim-1")

        with fixture["owner_engine"].connect() as conn:
            persisted = conn.execute(
                text(
                    """
                    SELECT status, attempt_count, locked_by, locked_at,
                           heartbeat_at, lease_token, started_at
                    FROM satellite_jobs
                    WHERE id = :job_id
                    """
                ),
                {"job_id": job_id},
            ).mappings().one()

        with fixture["runtime_engine"].begin() as conn:
            no_context_rows = conn.execute(
                text("SELECT id FROM satellite_jobs ORDER BY id")
            ).scalars().all()
            _set_tenant_context(conn, fixture["org_a_id"])
            tenant_a_rows = conn.execute(
                text("SELECT id FROM satellite_jobs ORDER BY id")
            ).scalars().all()

        with fixture["runtime_engine"].begin() as conn:
            _set_tenant_context(conn, fixture["org_b_id"])
            tenant_b_rows = conn.execute(
                text("SELECT id FROM satellite_jobs ORDER BY id")
            ).scalars().all()

    assert claimed is not None
    assert claimed["id"] == job_id
    assert claimed["status"] == "running"
    assert claimed["attempt_count"] == 1
    assert claimed["locked_by"] == "worker-claim-1"
    assert claimed["locked_at"] is not None
    assert claimed["heartbeat_at"] is not None
    assert claimed["lease_token"] is not None
    assert UUID(str(claimed["lease_token"]))
    assert claimed["started_at"] is not None
    assert persisted["status"] == "running"
    assert persisted["attempt_count"] == 1
    assert persisted["locked_by"] == "worker-claim-1"
    assert persisted["locked_at"] is not None
    assert persisted["heartbeat_at"] is not None
    assert persisted["lease_token"] is not None
    assert persisted["started_at"] is not None
    assert no_context_rows == []
    assert tenant_a_rows == [job_id]
    assert tenant_b_rows == []


def test_claim_ignores_future_exhausted_and_terminal_jobs():
    with _claim_fixture() as fixture:
        with fixture["owner_engine"].begin() as conn:
            db_now = _db_now(conn)
            _insert_job(
                conn,
                organization_id=fixture["org_a_id"],
                lote_id=fixture["lote_a_id"],
                next_attempt_at=db_now + timedelta(hours=1),
            )
            _insert_job(
                conn,
                organization_id=fixture["org_a_id"],
                lote_id=fixture["lote_a_id"],
                attempt_count=3,
                max_attempts=3,
            )
            _insert_job(
                conn,
                organization_id=fixture["org_a_id"],
                lote_id=fixture["lote_a_id"],
                status="running",
                attempt_count=1,
                locked_at=db_now,
                locked_by="running-worker",
                heartbeat_at=db_now,
                lease_token=str(uuid4()),
            )
            _insert_job(
                conn,
                organization_id=fixture["org_a_id"],
                lote_id=fixture["lote_a_id"],
                status="succeeded",
                finished_at=db_now,
            )
            _insert_job(
                conn,
                organization_id=fixture["org_a_id"],
                lote_id=fixture["lote_a_id"],
                status="failed",
                finished_at=db_now,
            )

        claimed = _claim_once(fixture["owner_engine"], worker_id="worker-none")

    assert claimed is None


def test_claim_order_is_deterministic_by_next_attempt_created_at_and_id():
    with _claim_fixture() as fixture:
        with fixture["owner_engine"].begin() as conn:
            db_now = _db_now(conn)
            base_time = db_now - timedelta(minutes=30)
            _insert_job(
                conn,
                organization_id=fixture["org_a_id"],
                lote_id=fixture["lote_a_id"],
                next_attempt_at=base_time + timedelta(minutes=10),
                created_at=base_time + timedelta(minutes=10),
            )
            expected_job_id = _insert_job(
                conn,
                organization_id=fixture["org_a_id"],
                lote_id=fixture["lote_a_id"],
                next_attempt_at=base_time,
                created_at=base_time,
            )
            _insert_job(
                conn,
                organization_id=fixture["org_a_id"],
                lote_id=fixture["lote_a_id"],
                next_attempt_at=base_time,
                created_at=base_time + timedelta(minutes=1),
            )

        claimed = _claim_once(fixture["owner_engine"], worker_id="worker-order")

    assert claimed is not None
    assert claimed["id"] == expected_job_id


def test_claim_from_max_minus_one_stops_at_max_attempts_without_overflow():
    with _claim_fixture() as fixture:
        with fixture["owner_engine"].begin() as conn:
            job_id = _insert_job(
                conn,
                organization_id=fixture["org_a_id"],
                lote_id=fixture["lote_a_id"],
                attempt_count=2,
                max_attempts=3,
            )

        claimed = _claim_once(fixture["owner_engine"], worker_id="worker-max")

        with fixture["owner_engine"].connect() as conn:
            persisted = conn.execute(
                text(
                    "SELECT attempt_count, max_attempts, status "
                    "FROM satellite_jobs WHERE id = :job_id"
                ),
                {"job_id": job_id},
            ).mappings().one()

    assert claimed is not None
    assert claimed["attempt_count"] == 3
    assert persisted["attempt_count"] == 3
    assert persisted["max_attempts"] == 3
    assert persisted["status"] == "running"


def test_claim_one_job_two_workers_returns_single_winner():
    with _claim_fixture() as fixture:
        with fixture["owner_engine"].begin() as conn:
            job_id = _insert_job(
                conn,
                organization_id=fixture["org_a_id"],
                lote_id=fixture["lote_a_id"],
            )

        results = _concurrent_claims(
            fixture["owner_engine"],
            ["worker-a", "worker-b"],
        )

    claimed_rows = [row for row in results if row is not None]
    claimed_ids = [row["id"] for row in claimed_rows]
    claimed_leases = [str(row["lease_token"]) for row in claimed_rows]
    successful_claim_count = len(claimed_rows)
    assert successful_claim_count == 1
    assert claimed_ids == [job_id]
    assert sum(row is None for row in results) == 1
    assert len(claimed_leases) == 1
    assert UUID(claimed_leases[0])


def test_claim_one_job_many_workers_never_duplicates_job_ownership():
    with _claim_fixture() as fixture:
        with fixture["owner_engine"].begin() as conn:
            job_id = _insert_job(
                conn,
                organization_id=fixture["org_a_id"],
                lote_id=fixture["lote_a_id"],
            )

        worker_ids = [f"worker-{index}" for index in range(6)]
        results = _concurrent_claims(fixture["owner_engine"], worker_ids)

    claimed_rows = [row for row in results if row is not None]
    claimed_ids = [row["id"] for row in claimed_rows]
    claimed_leases = [str(row["lease_token"]) for row in claimed_rows]
    successful_claim_count = len(claimed_rows)
    assert successful_claim_count == 1
    assert claimed_ids == [job_id]
    assert len(claimed_leases) == 1
    assert UUID(claimed_leases[0])


def test_claim_multiple_jobs_many_workers_claims_each_job_at_most_once_with_unique_leases():
    with _claim_fixture() as fixture:
        with fixture["owner_engine"].begin() as conn:
            expected_ids = {
                _insert_job(
                    conn,
                    organization_id=fixture["org_a_id"],
                    lote_id=fixture["lote_a_id"],
                ),
                _insert_job(
                    conn,
                    organization_id=fixture["org_a_id"],
                    lote_id=fixture["lote_a_id"],
                ),
                _insert_job(
                    conn,
                    organization_id=fixture["org_b_id"],
                    lote_id=fixture["lote_b_id"],
                ),
            }

        worker_ids = [f"worker-{index}" for index in range(8)]
        results = _concurrent_claims(fixture["owner_engine"], worker_ids)

    claimed_rows = [row for row in results if row is not None]
    claimed_ids = [row["id"] for row in claimed_rows]
    lease_tokens = [str(row["lease_token"]) for row in claimed_rows]
    successful_claim_count = len(claimed_rows)
    assert successful_claim_count == len(expected_ids)
    assert set(claimed_ids) == expected_ids
    assert len(claimed_ids) == len(set(claimed_ids))
    assert len(claimed_ids) == len(expected_ids)
    assert len(lease_tokens) == len(set(lease_tokens))
    for lease_token in lease_tokens:
        assert UUID(lease_token)


def test_two_successful_claims_return_distinct_database_generated_leases():
    with _claim_fixture() as fixture:
        with fixture["owner_engine"].begin() as conn:
            _insert_job(
                conn,
                organization_id=fixture["org_a_id"],
                lote_id=fixture["lote_a_id"],
            )
            _insert_job(
                conn,
                organization_id=fixture["org_b_id"],
                lote_id=fixture["lote_b_id"],
            )

        first_claim = _claim_once(fixture["owner_engine"], worker_id="worker-first")
        second_claim = _claim_once(fixture["owner_engine"], worker_id="worker-second")

    assert first_claim is not None
    assert second_claim is not None
    assert UUID(str(first_claim["lease_token"]))
    assert UUID(str(second_claim["lease_token"]))
    assert str(first_claim["lease_token"]) != str(second_claim["lease_token"])


def test_lease_token_unique_partial_index_exists():
    with _claim_fixture() as fixture:
        with fixture["owner_engine"].connect() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT indexdef
                    FROM pg_indexes
                    WHERE schemaname = 'public'
                      AND tablename = 'satellite_jobs'
                      AND indexname = 'uq_satellite_jobs_lease_token_non_null'
                    """
                )
            ).mappings().one()

    assert "CREATE UNIQUE INDEX" in row["indexdef"]
    assert "WHERE (lease_token IS NOT NULL)" in row["indexdef"]
