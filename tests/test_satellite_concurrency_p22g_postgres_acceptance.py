from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from threading import Barrier
from uuid import UUID, uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from litoral_trace.config.settings import normalize_database_url
from litoral_trace.services.satellite_ndvi_processing import (
    SatelliteJobLeaseLostError,
    update_satellite_job_heartbeat,
)


ROOT_DIR = Path(__file__).resolve().parents[1]
INTEGRATION_ENV_PATH = ROOT_DIR / ".env.integration"

EXPECTED_REVISION = "015_add_satellite_queue_metrics"
EXPECTED_RUNTIME_ROLE = "litoral_trace_app"
EXPECTED_WORKER_LOGIN = "litoral_trace_worker_integration"

CLAIM_FUNCTION = "public.worker_claim_next_satellite_job"
RECOVERY_FUNCTION = "public.worker_recover_stale_satellite_jobs"


def _truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _read_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}

    if not path.exists():
        return values

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()

        if (
            not line
            or line.startswith("#")
            or "=" not in line
        ):
            continue

        name, value = line.split("=", 1)
        values[name.strip()] = value.strip()

    return values


INTEGRATION_ENV = _read_env_file(INTEGRATION_ENV_PATH)

POSTGRES_TESTS_ENABLED = _truthy(
    INTEGRATION_ENV.get("ENABLE_POSTGRES_TESTS")
)
RUNTIME_DATABASE_URL = INTEGRATION_ENV.get(
    "TEST_POSTGRES_DATABASE_URL"
)
OWNER_DATABASE_URL = INTEGRATION_ENV.get(
    "TEST_POSTGRES_MIGRATION_DATABASE_URL"
)
WORKER_DATABASE_URL = INTEGRATION_ENV.get(
    "TEST_POSTGRES_WORKER_DATABASE_URL"
)

pytestmark = pytest.mark.skipif(
    not (
        POSTGRES_TESTS_ENABLED
        and RUNTIME_DATABASE_URL
        and OWNER_DATABASE_URL
        and WORKER_DATABASE_URL
    ),
    reason=(
        "P2.2G PostgreSQL concurrency acceptance requires "
        "ENABLE_POSTGRES_TESTS=1 plus isolated runtime, owner, "
        "and worker integration database URLs."
    ),
)


def _engine(
    url: str,
    *,
    pool_size: int,
):
    return create_engine(
        normalize_database_url(url),
        pool_size=pool_size,
        max_overflow=0,
        pool_pre_ping=True,
        hide_parameters=True,
    )


def _database_identity(engine) -> tuple[str, str]:
    with engine.connect() as connection:
        row = connection.execute(
            text(
                """
                SELECT
                    current_database()::text AS database_name,
                    current_user::text AS current_user
                """
            )
        ).mappings().one()

    return (
        str(row["database_name"]),
        str(row["current_user"]),
    )


def _claim_once(
    worker_engine,
    *,
    worker_id: str,
):
    with worker_engine.begin() as connection:
        row = connection.execute(
            text(
                f"""
                SELECT *
                FROM {CLAIM_FUNCTION}(
                    :requested_worker_id
                )
                """
            ),
            {
                "requested_worker_id": worker_id,
            },
        ).mappings().one_or_none()

    return dict(row) if row is not None else None


def _recover_once(
    worker_engine,
    *,
    requested_batch_size: int,
) -> dict[str, int]:
    with worker_engine.begin() as connection:
        row = connection.execute(
            text(
                f"""
                SELECT *
                FROM {RECOVERY_FUNCTION}(
                    :requested_batch_size
                )
                """
            ),
            {
                "requested_batch_size": requested_batch_size,
            },
        ).mappings().one()

    return {
        "requeued_count": int(row["requeued_count"]),
        "failed_count": int(row["failed_count"]),
    }


def _concurrent_claims(
    worker_engine,
    worker_ids: list[str],
):
    barrier = Barrier(len(worker_ids))

    def _runner(worker_id: str):
        barrier.wait(timeout=30)
        return _claim_once(
            worker_engine,
            worker_id=worker_id,
        )

    with ThreadPoolExecutor(
        max_workers=len(worker_ids)
    ) as executor:
        return list(
            executor.map(
                _runner,
                worker_ids,
            )
        )


def _concurrent_recovery(
    worker_engine,
    *,
    reaper_count: int,
    batch_size: int,
) -> list[dict[str, int]]:
    barrier = Barrier(reaper_count)

    def _runner(_index: int):
        barrier.wait(timeout=30)
        return _recover_once(
            worker_engine,
            requested_batch_size=batch_size,
        )

    with ThreadPoolExecutor(
        max_workers=reaper_count
    ) as executor:
        return list(
            executor.map(
                _runner,
                range(reaper_count),
            )
        )


def _sum_recovery_results(
    results: list[dict[str, int]],
) -> tuple[int, int]:
    return (
        sum(
            result["requeued_count"]
            for result in results
        ),
        sum(
            result["failed_count"]
            for result in results
        ),
    )


def _insert_job(
    connection,
    *,
    organization_id: int,
    lote_id: int,
    status: str = "queued",
    attempt_count: int = 0,
    max_attempts: int = 3,
    ready: bool = True,
    locked_by: str | None = None,
    lease_token: str | None = None,
) -> int:
    if status == "running":
        if not locked_by:
            raise ValueError(
                "running jobs require locked_by"
            )
        if not lease_token:
            raise ValueError(
                "running jobs require lease_token"
            )

    next_attempt_sql = (
        "CURRENT_TIMESTAMP - interval '10 minutes'"
        if ready
        else "CURRENT_TIMESTAMP + interval '2 hours'"
    )

    locked_at_sql = (
        "CURRENT_TIMESTAMP - interval '3 minutes'"
        if status == "running"
        else "NULL"
    )
    heartbeat_sql = (
        "CURRENT_TIMESTAMP - interval '2 minutes'"
        if status == "running"
        else "NULL"
    )
    started_at_sql = (
        "CURRENT_TIMESTAMP - interval '3 minutes'"
        if status == "running"
        else "NULL"
    )

    row = connection.execute(
        text(
            f"""
            INSERT INTO public.satellite_jobs (
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
                error_code,
                error_message,
                idempotency_key,
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
                :organization_id,
                :lote_id,
                'ndvi_timeseries',
                :status,
                :attempt_count,
                :max_attempts,
                {next_attempt_sql},
                {locked_at_sql},
                :locked_by,
                {heartbeat_sql},
                :lease_token,
                {started_at_sql},
                NULL,
                NULL,
                NULL,
                :idempotency_key,
                CURRENT_DATE - 30,
                CURRENT_DATE,
                20.0,
                :geometry_hash,
                'p22g-acceptance-v1',
                :polygon_wkt_snapshot,
                CURRENT_TIMESTAMP - interval '10 minutes',
                CURRENT_TIMESTAMP - interval '10 minutes'
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
            "locked_by": locked_by,
            "lease_token": lease_token,
            "idempotency_key": f"p22g-{uuid4().hex}",
            "geometry_hash": (
                uuid4().hex + uuid4().hex
            ),
            "polygon_wkt_snapshot": (
                "POLYGON(("
                "-58.91 -27.46, "
                "-58.89 -27.46, "
                "-58.89 -27.44, "
                "-58.91 -27.44, "
                "-58.91 -27.46"
                "))"
            ),
        },
    ).scalar_one()

    return int(row)


def _mark_job_stale(
    connection,
    *,
    job_id: int,
) -> None:
    connection.execute(
        text(
            """
            UPDATE public.satellite_jobs
            SET
                locked_at = CURRENT_TIMESTAMP - interval '3 minutes',
                heartbeat_at = CURRENT_TIMESTAMP - interval '2 minutes',
                updated_at = CURRENT_TIMESTAMP - interval '2 minutes'
            WHERE id = :job_id
            """
        ),
        {
            "job_id": job_id,
        },
    )


def _fetch_jobs(
    owner_engine,
    *,
    organization_ids: tuple[int, int],
) -> list[dict[str, object]]:
    with owner_engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT
                    id,
                    organization_id,
                    status,
                    attempt_count,
                    max_attempts,
                    next_attempt_at,
                    locked_by,
                    locked_at,
                    heartbeat_at,
                    lease_token,
                    started_at,
                    finished_at,
                    error_code
                FROM public.satellite_jobs
                WHERE organization_id IN (
                    :org_a_id,
                    :org_b_id
                )
                ORDER BY id
                """
            ),
            {
                "org_a_id": organization_ids[0],
                "org_b_id": organization_ids[1],
            },
        ).mappings().all()

    return [
        dict(row)
        for row in rows
    ]


def _fetch_job(
    owner_engine,
    *,
    job_id: int,
) -> dict[str, object]:
    with owner_engine.connect() as connection:
        row = connection.execute(
            text(
                """
                SELECT
                    id,
                    organization_id,
                    status,
                    attempt_count,
                    max_attempts,
                    locked_by,
                    locked_at,
                    heartbeat_at,
                    lease_token,
                    started_at,
                    finished_at,
                    error_code
                FROM public.satellite_jobs
                WHERE id = :job_id
                """
            ),
            {
                "job_id": job_id,
            },
        ).mappings().one()

    return dict(row)


@contextmanager
def _concurrency_fixture():
    owner_engine = _engine(
        OWNER_DATABASE_URL,
        pool_size=4,
    )
    runtime_engine = _engine(
        RUNTIME_DATABASE_URL,
        pool_size=4,
    )
    worker_engine = _engine(
        WORKER_DATABASE_URL,
        pool_size=12,
    )

    suffix = uuid4().hex[:10]

    with owner_engine.begin() as connection:
        revision = connection.execute(
            text(
                "SELECT version_num "
                "FROM alembic_version"
            )
        ).scalar_one()

        if revision != EXPECTED_REVISION:
            raise RuntimeError(
                "P2.2G requires integration database at "
                f"{EXPECTED_REVISION}"
            )

        org_a_id = int(
            connection.execute(
                text(
                    """
                    INSERT INTO public.organizations (
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
                        'P2.2G concurrency acceptance',
                        true
                    )
                    RETURNING id
                    """
                ),
                {
                    "name": f"P22G Org A {suffix}",
                    "slug": f"p22g-org-a-{suffix}",
                    "tax_id": f"G-A-{suffix}",
                },
            ).scalar_one()
        )

        org_b_id = int(
            connection.execute(
                text(
                    """
                    INSERT INTO public.organizations (
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
                        'P2.2G concurrency acceptance',
                        true
                    )
                    RETURNING id
                    """
                ),
                {
                    "name": f"P22G Org B {suffix}",
                    "slug": f"p22g-org-b-{suffix}",
                    "tax_id": f"G-B-{suffix}",
                },
            ).scalar_one()
        )

        lote_a_id = int(
            connection.execute(
                text(
                    """
                    INSERT INTO public.lotes (
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
                        :organization_id,
                        :identificador,
                        :productor_id,
                        'Madera Aserrada (Pino)',
                        10.0,
                        -27.45,
                        -58.90,
                        :polygon_wkt,
                        'Pendiente',
                        10.0,
                        5.0
                    )
                    RETURNING id
                    """
                ),
                {
                    "organization_id": org_a_id,
                    "identificador": f"P22G-A-{suffix}",
                    "productor_id": f"G-A-{suffix}",
                    "polygon_wkt": (
                        "POLYGON(("
                        "-58.91 -27.46, "
                        "-58.89 -27.46, "
                        "-58.89 -27.44, "
                        "-58.91 -27.44, "
                        "-58.91 -27.46"
                        "))"
                    ),
                },
            ).scalar_one()
        )

        lote_b_id = int(
            connection.execute(
                text(
                    """
                    INSERT INTO public.lotes (
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
                        :organization_id,
                        :identificador,
                        :productor_id,
                        'Madera Aserrada (Pino)',
                        11.0,
                        -27.55,
                        -58.80,
                        :polygon_wkt,
                        'Pendiente',
                        12.0,
                        4.0
                    )
                    RETURNING id
                    """
                ),
                {
                    "organization_id": org_b_id,
                    "identificador": f"P22G-B-{suffix}",
                    "productor_id": f"G-B-{suffix}",
                    "polygon_wkt": (
                        "POLYGON(("
                        "-58.81 -27.56, "
                        "-58.79 -27.56, "
                        "-58.79 -27.54, "
                        "-58.81 -27.54, "
                        "-58.81 -27.56"
                        "))"
                    ),
                },
            ).scalar_one()
        )

    try:
        yield {
            "owner_engine": owner_engine,
            "runtime_engine": runtime_engine,
            "worker_engine": worker_engine,
            "org_a_id": org_a_id,
            "org_b_id": org_b_id,
            "lote_a_id": lote_a_id,
            "lote_b_id": lote_b_id,
        }

    finally:
        with owner_engine.begin() as connection:
            connection.execute(
                text(
                    """
                    DELETE FROM public.satellite_job_results
                    WHERE organization_id IN (
                        :org_a_id,
                        :org_b_id
                    )
                    """
                ),
                {
                    "org_a_id": org_a_id,
                    "org_b_id": org_b_id,
                },
            )
            connection.execute(
                text(
                    """
                    DELETE FROM public.satellite_ndvi_observations
                    WHERE organization_id IN (
                        :org_a_id,
                        :org_b_id
                    )
                    """
                ),
                {
                    "org_a_id": org_a_id,
                    "org_b_id": org_b_id,
                },
            )
            connection.execute(
                text(
                    """
                    DELETE FROM public.audit_logs
                    WHERE organization_id IN (
                        :org_a_id,
                        :org_b_id
                    )
                    """
                ),
                {
                    "org_a_id": org_a_id,
                    "org_b_id": org_b_id,
                },
            )
            connection.execute(
                text(
                    """
                    DELETE FROM public.satellite_jobs
                    WHERE organization_id IN (
                        :org_a_id,
                        :org_b_id
                    )
                    """
                ),
                {
                    "org_a_id": org_a_id,
                    "org_b_id": org_b_id,
                },
            )
            connection.execute(
                text(
                    """
                    DELETE FROM public.lotes
                    WHERE id IN (
                        :lote_a_id,
                        :lote_b_id
                    )
                    """
                ),
                {
                    "lote_a_id": lote_a_id,
                    "lote_b_id": lote_b_id,
                },
            )
            connection.execute(
                text(
                    """
                    DELETE FROM public.organizations
                    WHERE id IN (
                        :org_a_id,
                        :org_b_id
                    )
                    """
                ),
                {
                    "org_a_id": org_a_id,
                    "org_b_id": org_b_id,
                },
            )

        worker_engine.dispose()
        runtime_engine.dispose()
        owner_engine.dispose()


def test_g_preflight_uses_real_distinct_runtime_owner_and_worker_principals():
    with _concurrency_fixture() as fixture:
        owner_database, owner_user = _database_identity(
            fixture["owner_engine"]
        )
        runtime_database, runtime_user = _database_identity(
            fixture["runtime_engine"]
        )
        worker_database, worker_user = _database_identity(
            fixture["worker_engine"]
        )

    assert owner_database == runtime_database == worker_database

    assert runtime_user == EXPECTED_RUNTIME_ROLE
    assert worker_user == EXPECTED_WORKER_LOGIN

    assert owner_user != runtime_user
    assert owner_user != worker_user
    assert runtime_user != worker_user


def test_g_many_workers_claim_many_jobs_exactly_once_across_tenants():
    with _concurrency_fixture() as fixture:
        expected_ids: set[int] = set()

        with fixture["owner_engine"].begin() as connection:
            for index in range(8):
                use_org_a = index % 2 == 0

                expected_ids.add(
                    _insert_job(
                        connection,
                        organization_id=(
                            fixture["org_a_id"]
                            if use_org_a
                            else fixture["org_b_id"]
                        ),
                        lote_id=(
                            fixture["lote_a_id"]
                            if use_org_a
                            else fixture["lote_b_id"]
                        ),
                    )
                )

        worker_ids = [
            f"p22g-claim-{index}"
            for index in range(12)
        ]
        results = _concurrent_claims(
            fixture["worker_engine"],
            worker_ids,
        )

        persisted = _fetch_jobs(
            fixture["owner_engine"],
            organization_ids=(
                fixture["org_a_id"],
                fixture["org_b_id"],
            ),
        )

    claimed = [
        result
        for result in results
        if result is not None
    ]

    claimed_ids = [
        int(result["id"])
        for result in claimed
    ]
    lease_tokens = [
        str(result["lease_token"])
        for result in claimed
    ]

    assert len(claimed) == 8
    assert sum(
        result is None
        for result in results
    ) == 4

    assert set(claimed_ids) == expected_ids
    assert len(claimed_ids) == len(
        set(claimed_ids)
    )

    assert len(lease_tokens) == len(
        set(lease_tokens)
    )
    for lease_token in lease_tokens:
        assert UUID(lease_token)

    assert len(persisted) == 8
    assert all(
        row["status"] == "running"
        for row in persisted
    )
    assert all(
        int(row["attempt_count"]) == 1
        for row in persisted
    )
    assert all(
        row["lease_token"] is not None
        for row in persisted
    )


def test_g_contention_claims_only_ready_eligible_jobs():
    with _concurrency_fixture() as fixture:
        ready_ids: set[int] = set()
        delayed_ids: set[int] = set()
        exhausted_ids: set[int] = set()
        existing_running_ids: set[int] = set()

        with fixture["owner_engine"].begin() as connection:
            for index in range(4):
                use_org_a = index % 2 == 0
                ready_ids.add(
                    _insert_job(
                        connection,
                        organization_id=(
                            fixture["org_a_id"]
                            if use_org_a
                            else fixture["org_b_id"]
                        ),
                        lote_id=(
                            fixture["lote_a_id"]
                            if use_org_a
                            else fixture["lote_b_id"]
                        ),
                    )
                )

            for index in range(2):
                delayed_ids.add(
                    _insert_job(
                        connection,
                        organization_id=fixture["org_a_id"],
                        lote_id=fixture["lote_a_id"],
                        ready=False,
                    )
                )

                exhausted_ids.add(
                    _insert_job(
                        connection,
                        organization_id=fixture["org_b_id"],
                        lote_id=fixture["lote_b_id"],
                        attempt_count=3,
                        max_attempts=3,
                    )
                )

                existing_running_ids.add(
                    _insert_job(
                        connection,
                        organization_id=fixture["org_a_id"],
                        lote_id=fixture["lote_a_id"],
                        status="running",
                        attempt_count=1,
                        max_attempts=3,
                        locked_by=f"preexisting-{index}",
                        lease_token=str(uuid4()),
                    )
                )

        results = _concurrent_claims(
            fixture["worker_engine"],
            [
                f"p22g-eligibility-{index}"
                for index in range(10)
            ],
        )

        persisted = _fetch_jobs(
            fixture["owner_engine"],
            organization_ids=(
                fixture["org_a_id"],
                fixture["org_b_id"],
            ),
        )

    claimed = [
        result
        for result in results
        if result is not None
    ]
    claimed_ids = {
        int(result["id"])
        for result in claimed
    }

    assert claimed_ids == ready_ids
    assert len(claimed) == len(ready_ids)

    by_id = {
        int(row["id"]): row
        for row in persisted
    }

    for job_id in delayed_ids:
        assert by_id[job_id]["status"] == "queued"
        assert int(
            by_id[job_id]["attempt_count"]
        ) == 0

    for job_id in exhausted_ids:
        assert by_id[job_id]["status"] == "queued"
        assert int(
            by_id[job_id]["attempt_count"]
        ) == 3

    for job_id in existing_running_ids:
        assert by_id[job_id]["status"] == "running"
        assert int(
            by_id[job_id]["attempt_count"]
        ) == 1


def test_g_concurrent_reapers_recover_each_stale_job_once():
    with _concurrency_fixture() as fixture:
        stale_ids: list[int] = []

        with fixture["owner_engine"].begin() as connection:
            for index in range(9):
                use_org_a = index % 2 == 0

                stale_ids.append(
                    _insert_job(
                        connection,
                        organization_id=(
                            fixture["org_a_id"]
                            if use_org_a
                            else fixture["org_b_id"]
                        ),
                        lote_id=(
                            fixture["lote_a_id"]
                            if use_org_a
                            else fixture["lote_b_id"]
                        ),
                        status="running",
                        attempt_count=1,
                        max_attempts=3,
                        locked_by=f"stale-{index}",
                        lease_token=str(uuid4()),
                    )
                )

            for job_id in stale_ids:
                _mark_job_stale(
                    connection,
                    job_id=job_id,
                )

        recovery_results = _concurrent_recovery(
            fixture["worker_engine"],
            reaper_count=3,
            batch_size=4,
        )

        persisted = _fetch_jobs(
            fixture["owner_engine"],
            organization_ids=(
                fixture["org_a_id"],
                fixture["org_b_id"],
            ),
        )

    total_requeued, total_failed = (
        _sum_recovery_results(
            recovery_results
        )
    )

    assert total_requeued == 9
    assert total_failed == 0

    assert len(persisted) == 9
    assert all(
        row["status"] == "queued"
        for row in persisted
    )
    assert all(
        int(row["attempt_count"]) == 1
        for row in persisted
    )
    assert all(
        row["locked_by"] is None
        and row["locked_at"] is None
        and row["heartbeat_at"] is None
        and row["lease_token"] is None
        for row in persisted
    )


def test_g_recovery_then_concurrent_reclaim_rotates_leases_and_fences_old_owner():
    with _concurrency_fixture() as fixture:
        old_authority: dict[
            int,
            tuple[str, str],
        ] = {}

        with fixture["owner_engine"].begin() as connection:
            for index in range(5):
                use_org_a = index % 2 == 0
                old_worker = f"old-worker-{index}"
                old_lease = str(uuid4())

                job_id = _insert_job(
                    connection,
                    organization_id=(
                        fixture["org_a_id"]
                        if use_org_a
                        else fixture["org_b_id"]
                    ),
                    lote_id=(
                        fixture["lote_a_id"]
                        if use_org_a
                        else fixture["lote_b_id"]
                    ),
                    status="running",
                    attempt_count=1,
                    max_attempts=3,
                    locked_by=old_worker,
                    lease_token=old_lease,
                )
                _mark_job_stale(
                    connection,
                    job_id=job_id,
                )

                old_authority[job_id] = (
                    old_worker,
                    old_lease,
                )

        recovery_results = _concurrent_recovery(
            fixture["worker_engine"],
            reaper_count=2,
            batch_size=3,
        )
        total_requeued, total_failed = (
            _sum_recovery_results(
                recovery_results
            )
        )

        assert total_requeued == 5
        assert total_failed == 0

        claim_results = _concurrent_claims(
            fixture["worker_engine"],
            [
                f"new-worker-{index}"
                for index in range(8)
            ],
        )

        claimed = [
            result
            for result in claim_results
            if result is not None
        ]

        assert len(claimed) == 5

        new_by_id = {
            int(result["id"]): result
            for result in claimed
        }

        assert set(new_by_id) == set(
            old_authority
        )

        new_leases = {
            str(result["lease_token"])
            for result in claimed
        }
        assert len(new_leases) == 5

        for job_id, (
            _old_worker,
            old_lease,
        ) in old_authority.items():
            assert (
                str(
                    new_by_id[job_id][
                        "lease_token"
                    ]
                )
                != old_lease
            )
            assert int(
                new_by_id[job_id][
                    "attempt_count"
                ]
            ) == 2

        fenced_job_id = min(
            old_authority
        )
        old_worker, old_lease = (
            old_authority[fenced_job_id]
        )

        runtime_session_factory = sessionmaker(
            bind=fixture["runtime_engine"],
            autoflush=False,
            autocommit=False,
        )
        runtime_session = (
            runtime_session_factory()
        )

        try:
            with pytest.raises(
                SatelliteJobLeaseLostError
            ):
                update_satellite_job_heartbeat(
                    runtime_session,
                    organization_id=int(
                        new_by_id[
                            fenced_job_id
                        ][
                            "organization_id"
                        ]
                    ),
                    job_id=fenced_job_id,
                    worker_id=old_worker,
                    lease_token=old_lease,
                )
            runtime_session.rollback()
        finally:
            runtime_session.close()

        final_job = _fetch_job(
            fixture["owner_engine"],
            job_id=fenced_job_id,
        )

    assert final_job["status"] == "running"
    assert (
        str(final_job["lease_token"])
        == str(
            new_by_id[fenced_job_id][
                "lease_token"
            ]
        )
    )


def test_g_max_attempts_remains_bounded_under_claim_and_recovery_contention():
    with _concurrency_fixture() as fixture:
        job_ids: list[int] = []

        with fixture["owner_engine"].begin() as connection:
            for index in range(4):
                use_org_a = index % 2 == 0

                job_ids.append(
                    _insert_job(
                        connection,
                        organization_id=(
                            fixture["org_a_id"]
                            if use_org_a
                            else fixture["org_b_id"]
                        ),
                        lote_id=(
                            fixture["lote_a_id"]
                            if use_org_a
                            else fixture["lote_b_id"]
                        ),
                        attempt_count=2,
                        max_attempts=3,
                    )
                )

        initial_claims = _concurrent_claims(
            fixture["worker_engine"],
            [
                f"max-worker-{index}"
                for index in range(8)
            ],
        )
        initial_winners = [
            result
            for result in initial_claims
            if result is not None
        ]

        assert len(initial_winners) == 4
        assert {
            int(result["id"])
            for result in initial_winners
        } == set(job_ids)
        assert all(
            int(
                result["attempt_count"]
            )
            == 3
            for result in initial_winners
        )

        with fixture["owner_engine"].begin() as connection:
            for job_id in job_ids:
                _mark_job_stale(
                    connection,
                    job_id=job_id,
                )

        recovery_results = _concurrent_recovery(
            fixture["worker_engine"],
            reaper_count=2,
            batch_size=3,
        )
        total_requeued, total_failed = (
            _sum_recovery_results(
                recovery_results
            )
        )

        final_claims = _concurrent_claims(
            fixture["worker_engine"],
            [
                f"post-fail-worker-{index}"
                for index in range(6)
            ],
        )

        persisted = _fetch_jobs(
            fixture["owner_engine"],
            organization_ids=(
                fixture["org_a_id"],
                fixture["org_b_id"],
            ),
        )

    assert total_requeued == 0
    assert total_failed == 4
    assert all(
        result is None
        for result in final_claims
    )

    assert len(persisted) == 4
    assert all(
        row["status"] == "failed"
        for row in persisted
    )
    assert all(
        int(row["attempt_count"]) == 3
        for row in persisted
    )
    assert all(
        int(row["attempt_count"])
        <= int(row["max_attempts"])
        for row in persisted
    )
    assert all(
        row["error_code"]
        == "stale_recovery_exhausted"
        for row in persisted
    )