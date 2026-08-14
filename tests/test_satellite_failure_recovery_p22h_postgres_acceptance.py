from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from litoral_trace.config.settings import normalize_database_url
from litoral_trace.services.satellite_ndvi_processing import (
    SatelliteJobLeaseLostError,
    mark_satellite_job_failed,
    mark_satellite_job_succeeded,
    schedule_satellite_job_retry,
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
        "P2.2H PostgreSQL failure/restart/recovery acceptance requires "
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


def _runtime_session_factory(runtime_engine):
    return sessionmaker(
        bind=runtime_engine,
        autoflush=False,
        autocommit=False,
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
    requested_batch_size: int = 10,
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


def _insert_queued_job(
    connection,
    *,
    organization_id: int,
    lote_id: int,
    attempt_count: int = 0,
    max_attempts: int = 3,
) -> int:
    row = connection.execute(
        text(
            """
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
                'queued',
                :attempt_count,
                :max_attempts,
                CURRENT_TIMESTAMP - interval '10 minutes',
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                :idempotency_key,
                CURRENT_DATE - 30,
                CURRENT_DATE,
                20.0,
                :geometry_hash,
                'p22h-acceptance-v1',
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
            "attempt_count": attempt_count,
            "max_attempts": max_attempts,
            "idempotency_key": f"p22h-{uuid4().hex}",
            "geometry_hash": uuid4().hex + uuid4().hex,
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


def _age_running_job_to_stale(
    owner_engine,
    *,
    job_id: int,
) -> None:
    with owner_engine.begin() as connection:
        result = connection.execute(
            text(
                """
                UPDATE public.satellite_jobs
                SET
                    locked_at = CURRENT_TIMESTAMP - interval '3 minutes',
                    heartbeat_at = CURRENT_TIMESTAMP - interval '2 minutes',
                    updated_at = CURRENT_TIMESTAMP - interval '2 minutes'
                WHERE id = :job_id
                  AND status = 'running'
                  AND lease_token IS NOT NULL
                """
            ),
            {
                "job_id": job_id,
            },
        )

    assert result.rowcount == 1


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
                    lote_id,
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
                    created_at,
                    updated_at
                FROM public.satellite_jobs
                WHERE id = :job_id
                """
            ),
            {
                "job_id": job_id,
            },
        ).mappings().one()

    return dict(row)


def _assert_clean_requeued_state(
    row: dict[str, object],
    *,
    expected_attempt_count: int,
) -> None:
    assert row["status"] == "queued"
    assert int(row["attempt_count"]) == expected_attempt_count
    assert row["locked_at"] is None
    assert row["locked_by"] is None
    assert row["heartbeat_at"] is None
    assert row["lease_token"] is None
    assert row["finished_at"] is None
    assert row["error_code"] is None
    assert row["error_message"] is None


def _assert_zombie_operations_rejected(
    runtime_engine,
    *,
    organization_id: int,
    job_id: int,
    worker_id: str,
    lease_token: str,
) -> None:
    operations = (
        (
            "heartbeat",
            lambda session: update_satellite_job_heartbeat(
                session,
                organization_id=organization_id,
                job_id=job_id,
                worker_id=worker_id,
                lease_token=lease_token,
            ),
        ),
        (
            "succeeded",
            lambda session: mark_satellite_job_succeeded(
                session,
                organization_id=organization_id,
                job_id=job_id,
                worker_id=worker_id,
                lease_token=lease_token,
            ),
        ),
        (
            "failed",
            lambda session: mark_satellite_job_failed(
                session,
                organization_id=organization_id,
                job_id=job_id,
                worker_id=worker_id,
                lease_token=lease_token,
                error_code="p22h_zombie_failure",
                error_message="stale worker must not finalize",
            ),
        ),
        (
            "retry_scheduled",
            lambda session: schedule_satellite_job_retry(
                session,
                organization_id=organization_id,
                job_id=job_id,
                worker_id=worker_id,
                lease_token=lease_token,
                retry_delay_seconds=30,
            ),
        ),
    )

    SessionFactory = _runtime_session_factory(runtime_engine)

    for expected_operation, operation in operations:
        session = SessionFactory()
        try:
            with pytest.raises(
                SatelliteJobLeaseLostError
            ) as exc_info:
                operation(session)

            assert exc_info.value.job_id == job_id
            assert (
                exc_info.value.organization_id
                == organization_id
            )
            assert (
                exc_info.value.worker_id
                == worker_id
            )
            assert (
                exc_info.value.operation
                == expected_operation
            )
            session.rollback()
        finally:
            session.close()


@contextmanager
def _failure_recovery_fixture():
    owner_engine = _engine(
        OWNER_DATABASE_URL,
        pool_size=3,
    )
    runtime_engine = _engine(
        RUNTIME_DATABASE_URL,
        pool_size=4,
    )
    worker_engine = _engine(
        WORKER_DATABASE_URL,
        pool_size=6,
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
                "P2.2H requires integration database at "
                f"{EXPECTED_REVISION}"
            )

        org_id = int(
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
                        'P2.2H failure/restart/recovery acceptance',
                        true
                    )
                    RETURNING id
                    """
                ),
                {
                    "name": f"P22H Org {suffix}",
                    "slug": f"p22h-org-{suffix}",
                    "tax_id": f"H-{suffix}",
                },
            ).scalar_one()
        )

        lote_id = int(
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
                    "organization_id": org_id,
                    "identificador": f"P22H-{suffix}",
                    "productor_id": f"H-{suffix}",
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

    try:
        yield {
            "owner_engine": owner_engine,
            "runtime_engine": runtime_engine,
            "worker_engine": worker_engine,
            "organization_id": org_id,
            "lote_id": lote_id,
        }

    finally:
        with owner_engine.begin() as connection:
            connection.execute(
                text(
                    """
                    DELETE FROM public.satellite_job_results
                    WHERE organization_id = :organization_id
                    """
                ),
                {
                    "organization_id": org_id,
                },
            )
            connection.execute(
                text(
                    """
                    DELETE FROM public.satellite_ndvi_observations
                    WHERE organization_id = :organization_id
                    """
                ),
                {
                    "organization_id": org_id,
                },
            )
            connection.execute(
                text(
                    """
                    DELETE FROM public.audit_logs
                    WHERE organization_id = :organization_id
                    """
                ),
                {
                    "organization_id": org_id,
                },
            )
            connection.execute(
                text(
                    """
                    DELETE FROM public.satellite_jobs
                    WHERE organization_id = :organization_id
                    """
                ),
                {
                    "organization_id": org_id,
                },
            )
            connection.execute(
                text(
                    """
                    DELETE FROM public.lotes
                    WHERE id = :lote_id
                    """
                ),
                {
                    "lote_id": lote_id,
                },
            )
            connection.execute(
                text(
                    """
                    DELETE FROM public.organizations
                    WHERE id = :organization_id
                    """
                ),
                {
                    "organization_id": org_id,
                },
            )

        worker_engine.dispose()
        runtime_engine.dispose()
        owner_engine.dispose()


def test_h_preflight_uses_real_distinct_runtime_owner_and_worker_principals():
    with _failure_recovery_fixture() as fixture:
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


def test_h_crashed_worker_is_recovered_to_clean_queue_without_attempt_increment():
    with _failure_recovery_fixture() as fixture:
        with fixture["owner_engine"].begin() as connection:
            job_id = _insert_queued_job(
                connection,
                organization_id=fixture["organization_id"],
                lote_id=fixture["lote_id"],
            )

        claim = _claim_once(
            fixture["worker_engine"],
            worker_id="p22h-crash-a",
        )

        assert claim is not None
        assert int(claim["id"]) == job_id
        assert int(claim["attempt_count"]) == 1
        old_lease = str(claim["lease_token"])
        assert UUID(old_lease)

        _age_running_job_to_stale(
            fixture["owner_engine"],
            job_id=job_id,
        )

        recovery = _recover_once(
            fixture["worker_engine"]
        )
        recovered = _fetch_job(
            fixture["owner_engine"],
            job_id=job_id,
        )

    assert recovery == {
        "requeued_count": 1,
        "failed_count": 0,
    }
    _assert_clean_requeued_state(
        recovered,
        expected_attempt_count=1,
    )


def test_h_restart_reclaim_rotates_lease_and_fences_every_old_worker_transition():
    with _failure_recovery_fixture() as fixture:
        with fixture["owner_engine"].begin() as connection:
            job_id = _insert_queued_job(
                connection,
                organization_id=fixture["organization_id"],
                lote_id=fixture["lote_id"],
            )

        first_claim = _claim_once(
            fixture["worker_engine"],
            worker_id="p22h-old-worker",
        )
        assert first_claim is not None

        old_lease = str(
            first_claim["lease_token"]
        )
        assert UUID(old_lease)

        _age_running_job_to_stale(
            fixture["owner_engine"],
            job_id=job_id,
        )

        recovery = _recover_once(
            fixture["worker_engine"]
        )
        assert recovery == {
            "requeued_count": 1,
            "failed_count": 0,
        }

        second_claim = _claim_once(
            fixture["worker_engine"],
            worker_id="p22h-new-worker",
        )
        assert second_claim is not None
        assert int(second_claim["id"]) == job_id
        assert int(
            second_claim["attempt_count"]
        ) == 2

        new_lease = str(
            second_claim["lease_token"]
        )
        assert UUID(new_lease)
        assert new_lease != old_lease

        before_zombie = _fetch_job(
            fixture["owner_engine"],
            job_id=job_id,
        )

        _assert_zombie_operations_rejected(
            fixture["runtime_engine"],
            organization_id=fixture["organization_id"],
            job_id=job_id,
            worker_id="p22h-old-worker",
            lease_token=old_lease,
        )

        after_zombie = _fetch_job(
            fixture["owner_engine"],
            job_id=job_id,
        )

    assert after_zombie["status"] == "running"
    assert after_zombie["locked_by"] == "p22h-new-worker"
    assert (
        str(after_zombie["lease_token"])
        == new_lease
    )
    assert int(
        after_zombie["attempt_count"]
    ) == 2

    for field in (
        "status",
        "attempt_count",
        "max_attempts",
        "locked_at",
        "locked_by",
        "heartbeat_at",
        "lease_token",
        "started_at",
        "finished_at",
        "error_code",
        "error_message",
    ):
        assert (
            after_zombie[field]
            == before_zombie[field]
        )


def test_h_fresh_worker_heartbeat_blocks_false_stale_recovery():
    with _failure_recovery_fixture() as fixture:
        with fixture["owner_engine"].begin() as connection:
            job_id = _insert_queued_job(
                connection,
                organization_id=fixture["organization_id"],
                lote_id=fixture["lote_id"],
            )

        claim = _claim_once(
            fixture["worker_engine"],
            worker_id="p22h-fresh-worker",
        )
        assert claim is not None

        lease_token = str(
            claim["lease_token"]
        )

        SessionFactory = _runtime_session_factory(
            fixture["runtime_engine"]
        )
        session = SessionFactory()

        try:
            heartbeat_at = (
                update_satellite_job_heartbeat(
                    session,
                    organization_id=fixture[
                        "organization_id"
                    ],
                    job_id=job_id,
                    worker_id="p22h-fresh-worker",
                    lease_token=lease_token,
                )
            )
            session.commit()
        finally:
            session.close()

        recovery = _recover_once(
            fixture["worker_engine"]
        )
        persisted = _fetch_job(
            fixture["owner_engine"],
            job_id=job_id,
        )

    assert heartbeat_at is not None
    assert recovery == {
        "requeued_count": 0,
        "failed_count": 0,
    }
    assert persisted["status"] == "running"
    assert persisted["locked_by"] == "p22h-fresh-worker"
    assert str(
        persisted["lease_token"]
    ) == lease_token
    assert int(
        persisted["attempt_count"]
    ) == 1


def test_h_repeated_crash_restart_cycles_stop_exactly_at_max_attempts():
    with _failure_recovery_fixture() as fixture:
        with fixture["owner_engine"].begin() as connection:
            job_id = _insert_queued_job(
                connection,
                organization_id=fixture["organization_id"],
                lote_id=fixture["lote_id"],
                attempt_count=0,
                max_attempts=3,
            )

        leases: list[str] = []

        first_claim = _claim_once(
            fixture["worker_engine"],
            worker_id="p22h-cycle-1",
        )
        assert first_claim is not None
        assert int(
            first_claim["attempt_count"]
        ) == 1
        leases.append(
            str(first_claim["lease_token"])
        )

        _age_running_job_to_stale(
            fixture["owner_engine"],
            job_id=job_id,
        )
        assert _recover_once(
            fixture["worker_engine"]
        ) == {
            "requeued_count": 1,
            "failed_count": 0,
        }

        second_claim = _claim_once(
            fixture["worker_engine"],
            worker_id="p22h-cycle-2",
        )
        assert second_claim is not None
        assert int(
            second_claim["attempt_count"]
        ) == 2
        leases.append(
            str(second_claim["lease_token"])
        )

        _age_running_job_to_stale(
            fixture["owner_engine"],
            job_id=job_id,
        )
        assert _recover_once(
            fixture["worker_engine"]
        ) == {
            "requeued_count": 1,
            "failed_count": 0,
        }

        third_claim = _claim_once(
            fixture["worker_engine"],
            worker_id="p22h-cycle-3",
        )
        assert third_claim is not None
        assert int(
            third_claim["attempt_count"]
        ) == 3
        leases.append(
            str(third_claim["lease_token"])
        )

        _age_running_job_to_stale(
            fixture["owner_engine"],
            job_id=job_id,
        )
        terminal_recovery = _recover_once(
            fixture["worker_engine"]
        )

        post_terminal_claim = _claim_once(
            fixture["worker_engine"],
            worker_id="p22h-cycle-4",
        )

        final_row = _fetch_job(
            fixture["owner_engine"],
            job_id=job_id,
        )

    assert len(set(leases)) == 3
    for lease_token in leases:
        assert UUID(lease_token)

    assert terminal_recovery == {
        "requeued_count": 0,
        "failed_count": 1,
    }
    assert post_terminal_claim is None

    assert final_row["status"] == "failed"
    assert int(
        final_row["attempt_count"]
    ) == 3
    assert int(
        final_row["max_attempts"]
    ) == 3
    assert int(
        final_row["attempt_count"]
    ) <= int(
        final_row["max_attempts"]
    )
    assert final_row["locked_at"] is None
    assert final_row["locked_by"] is None
    assert final_row["heartbeat_at"] is None
    assert final_row["finished_at"] is not None
    assert (
        final_row["error_code"]
        == "stale_recovery_exhausted"
    )


def test_h_terminal_success_is_not_recovered_or_overwritten_by_zombie_worker():
    with _failure_recovery_fixture() as fixture:
        with fixture["owner_engine"].begin() as connection:
            job_id = _insert_queued_job(
                connection,
                organization_id=fixture["organization_id"],
                lote_id=fixture["lote_id"],
            )

        claim = _claim_once(
            fixture["worker_engine"],
            worker_id="p22h-terminal-owner",
        )
        assert claim is not None

        lease_token = str(
            claim["lease_token"]
        )
        assert UUID(lease_token)

        SessionFactory = _runtime_session_factory(
            fixture["runtime_engine"]
        )
        session = SessionFactory()

        try:
            succeeded_job = (
                mark_satellite_job_succeeded(
                    session,
                    organization_id=fixture[
                        "organization_id"
                    ],
                    job_id=job_id,
                    worker_id="p22h-terminal-owner",
                    lease_token=lease_token,
                )
            )
            assert (
                succeeded_job.status
                == "succeeded"
            )
            session.commit()
        finally:
            session.close()

        before_recovery = _fetch_job(
            fixture["owner_engine"],
            job_id=job_id,
        )

        recovery = _recover_once(
            fixture["worker_engine"]
        )

        after_recovery = _fetch_job(
            fixture["owner_engine"],
            job_id=job_id,
        )

        post_terminal_claim = _claim_once(
            fixture["worker_engine"],
            worker_id="p22h-after-success",
        )

        _assert_zombie_operations_rejected(
            fixture["runtime_engine"],
            organization_id=fixture["organization_id"],
            job_id=job_id,
            worker_id="p22h-terminal-owner",
            lease_token=lease_token,
        )

        final_row = _fetch_job(
            fixture["owner_engine"],
            job_id=job_id,
        )

    assert recovery == {
        "requeued_count": 0,
        "failed_count": 0,
    }
    assert post_terminal_claim is None

    assert before_recovery["status"] == "succeeded"
    assert after_recovery["status"] == "succeeded"
    assert final_row["status"] == "succeeded"

    for field in (
        "status",
        "attempt_count",
        "max_attempts",
        "locked_at",
        "locked_by",
        "heartbeat_at",
        "lease_token",
        "started_at",
        "finished_at",
        "error_code",
        "error_message",
    ):
        assert (
            final_row[field]
            == before_recovery[field]
        )


def test_h_exhausted_stale_failure_retains_fencing_token_and_rejects_old_owner():
    with _failure_recovery_fixture() as fixture:
        with fixture["owner_engine"].begin() as connection:
            job_id = _insert_queued_job(
                connection,
                organization_id=fixture["organization_id"],
                lote_id=fixture["lote_id"],
                attempt_count=2,
                max_attempts=3,
            )

        claim = _claim_once(
            fixture["worker_engine"],
            worker_id="p22h-exhausted-owner",
        )
        assert claim is not None
        assert int(
            claim["attempt_count"]
        ) == 3

        terminal_lease = str(
            claim["lease_token"]
        )
        assert UUID(terminal_lease)

        _age_running_job_to_stale(
            fixture["owner_engine"],
            job_id=job_id,
        )

        recovery = _recover_once(
            fixture["worker_engine"]
        )
        failed_row = _fetch_job(
            fixture["owner_engine"],
            job_id=job_id,
        )

        _assert_zombie_operations_rejected(
            fixture["runtime_engine"],
            organization_id=fixture["organization_id"],
            job_id=job_id,
            worker_id="p22h-exhausted-owner",
            lease_token=terminal_lease,
        )

        final_row = _fetch_job(
            fixture["owner_engine"],
            job_id=job_id,
        )

    assert recovery == {
        "requeued_count": 0,
        "failed_count": 1,
    }
    assert failed_row["status"] == "failed"
    assert int(
        failed_row["attempt_count"]
    ) == 3
    assert str(
        failed_row["lease_token"]
    ) == terminal_lease
    assert failed_row["finished_at"] is not None
    assert (
        failed_row["error_code"]
        == "stale_recovery_exhausted"
    )

    for field in (
        "status",
        "attempt_count",
        "max_attempts",
        "locked_at",
        "locked_by",
        "heartbeat_at",
        "lease_token",
        "started_at",
        "finished_at",
        "error_code",
        "error_message",
    ):
        assert (
            final_row[field]
            == failed_row[field]
        )