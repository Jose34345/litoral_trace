from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import sessionmaker

from litoral_trace.config.settings import normalize_database_url
from litoral_trace.workers.satellite_worker import (
    check_satellite_worker_readiness,
)


ROOT_DIR = Path(__file__).resolve().parents[1]
INTEGRATION_ENV_PATH = ROOT_DIR / ".env.integration"

EXPECTED_REVISION = "015_add_satellite_queue_metrics"
EXPECTED_RUNTIME_ROLE = "litoral_trace_app"
EXPECTED_WORKER_LOGIN = "litoral_trace_worker_integration"
EXPECTED_WORKER_CAPABILITY = "litoral_trace_worker_executor"

MUTATION_SENSITIVE_TABLES = (
    "satellite_jobs",
    "satellite_job_results",
    "satellite_ndvi_observations",
    "audit_logs",
)


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
        "P2.2F3 PostgreSQL readiness acceptance requires "
        "ENABLE_POSTGRES_TESTS=1 plus isolated runtime, owner, "
        "and worker integration database URLs."
    ),
)


def _engine(url: str):
    return create_engine(
        normalize_database_url(url),
        pool_pre_ping=True,
        hide_parameters=True,
    )


@pytest.fixture
def owner_engine():
    engine = _engine(OWNER_DATABASE_URL)
    try:
        yield engine
    finally:
        engine.dispose()


@pytest.fixture
def runtime_engine():
    engine = _engine(RUNTIME_DATABASE_URL)
    try:
        yield engine
    finally:
        engine.dispose()


@pytest.fixture
def worker_engine():
    engine = _engine(WORKER_DATABASE_URL)
    try:
        yield engine
    finally:
        engine.dispose()


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


def _table_fingerprint(owner_engine, table_name: str) -> tuple[int, str]:
    if table_name not in MUTATION_SENSITIVE_TABLES:
        raise ValueError("Unsupported fingerprint table.")

    # Do not assume every durable table exposes a generic `id` column.
    # satellite_job_results, for example, is keyed by satellite_job_id.
    # Sorting the complete JSON representation of each row gives us a
    # deterministic, schema-agnostic fingerprint while preserving duplicates.
    statement = text(
        f"""
        SELECT
            count(*)::bigint AS row_count,
            md5(
                COALESCE(
                    string_agg(
                        to_jsonb(t)::text,
                        E'\\n'
                        ORDER BY to_jsonb(t)::text
                    ),
                    ''
                )
            ) AS row_hash
        FROM public.{table_name} AS t
        """
    )

    with owner_engine.connect() as connection:
        row = connection.execute(statement).mappings().one()

    return (
        int(row["row_count"]),
        str(row["row_hash"]),
    )


def _durable_fingerprint(owner_engine) -> dict[str, tuple[int, str]]:
    return {
        table_name: _table_fingerprint(
            owner_engine,
            table_name,
        )
        for table_name in MUTATION_SENSITIVE_TABLES
    }


def _subprocess_environment() -> dict[str, str]:
    env = os.environ.copy()

    # Run the CLI as production runtime wiring, not as pytest test wiring.
    env["ENVIRONMENT"] = "production"
    env["DATABASE_URL"] = RUNTIME_DATABASE_URL
    env["WORKER_DATABASE_URL"] = WORKER_DATABASE_URL

    # A syntactically strong non-production JWT value is enough for settings
    # construction. It is never used for authentication in --check.
    env["JWT_SECRET_KEY"] = (
        "p22f3-readiness-only-jwt-secret-"
        "0123456789abcdef"
    )
    env.setdefault("JWT_ALGORITHM", "HS256")
    env.setdefault("LOG_LEVEL", "INFO")

    # The worker healthcheck must not receive owner/test credentials.
    for name in (
        "MIGRATION_DATABASE_URL",
        "TEST_DATABASE_URL",
        "TEST_POSTGRES_DATABASE_URL",
        "TEST_POSTGRES_MIGRATION_DATABASE_URL",
        "TEST_POSTGRES_WORKER_DATABASE_URL",
        "ENABLE_POSTGRES_TESTS",
        "POSTGRES_URL",
        "DB_URL",
        "RUNTIME_DATABASE_URL",
    ):
        env.pop(name, None)

    src_dir = str(ROOT_DIR / "src")
    existing_pythonpath = env.get("PYTHONPATH")

    env["PYTHONPATH"] = (
        src_dir
        if not existing_pythonpath
        else os.pathsep.join(
            (src_dir, existing_pythonpath)
        )
    )

    return env


def test_integration_database_is_at_expected_revision(owner_engine):
    with owner_engine.connect() as connection:
        revision = connection.execute(
            text(
                "SELECT version_num "
                "FROM alembic_version"
            )
        ).scalar_one()

    assert revision == EXPECTED_REVISION


def test_runtime_worker_and_owner_target_same_database_with_distinct_principals(
    owner_engine,
    runtime_engine,
    worker_engine,
):
    owner_database, owner_user = _database_identity(
        owner_engine
    )
    runtime_database, runtime_user = _database_identity(
        runtime_engine
    )
    worker_database, worker_user = _database_identity(
        worker_engine
    )

    assert owner_database == runtime_database == worker_database

    assert runtime_user == EXPECTED_RUNTIME_ROLE
    assert worker_user == EXPECTED_WORKER_LOGIN

    assert owner_user != runtime_user
    assert owner_user != worker_user
    assert runtime_user != worker_user


def test_worker_login_has_narrow_capability_without_direct_job_table_access(
    worker_engine,
):
    with worker_engine.connect() as connection:
        row = connection.execute(
            text(
                """
                SELECT
                    current_user::text AS current_user,
                    rolsuper,
                    rolcreatedb,
                    rolcreaterole,
                    rolreplication,
                    rolbypassrls,
                    pg_has_role(
                        current_user,
                        :worker_capability,
                        'MEMBER'
                    ) AS capability_member,
                    has_function_privilege(
                        current_user,
                        'public.worker_claim_next_satellite_job(text)',
                        'EXECUTE'
                    ) AS claim_execute,
                    has_function_privilege(
                        current_user,
                        'public.worker_get_satellite_queue_metrics()',
                        'EXECUTE'
                    ) AS metrics_execute,
                    has_table_privilege(
                        current_user,
                        'public.satellite_jobs',
                        'SELECT'
                    ) AS jobs_select,
                    has_table_privilege(
                        current_user,
                        'public.satellite_jobs',
                        'INSERT'
                    ) AS jobs_insert,
                    has_table_privilege(
                        current_user,
                        'public.satellite_jobs',
                        'UPDATE'
                    ) AS jobs_update,
                    has_table_privilege(
                        current_user,
                        'public.satellite_jobs',
                        'DELETE'
                    ) AS jobs_delete
                FROM pg_roles
                WHERE rolname = current_user
                """
            ),
            {
                "worker_capability": (
                    EXPECTED_WORKER_CAPABILITY
                )
            },
        ).mappings().one()

    assert row["current_user"] == EXPECTED_WORKER_LOGIN
    assert row["rolsuper"] is False
    assert row["rolcreatedb"] is False
    assert row["rolcreaterole"] is False
    assert row["rolreplication"] is False
    assert row["rolbypassrls"] is False
    assert row["capability_member"] is True
    assert row["claim_execute"] is True
    assert row["metrics_execute"] is True

    assert row["jobs_select"] is False
    assert row["jobs_insert"] is False
    assert row["jobs_update"] is False
    assert row["jobs_delete"] is False


def test_worker_login_cannot_directly_select_satellite_jobs(worker_engine):
    with worker_engine.connect() as connection:
        with pytest.raises(DBAPIError):
            connection.execute(
                text(
                    "SELECT count(*) "
                    "FROM public.satellite_jobs"
                )
            )


def test_runtime_role_cannot_execute_global_worker_queue_metrics(
    runtime_engine,
):
    with runtime_engine.connect() as connection:
        allowed = connection.execute(
            text(
                """
                SELECT has_function_privilege(
                    current_user,
                    'public.worker_get_satellite_queue_metrics()',
                    'EXECUTE'
                )
                """
            )
        ).scalar_one()

    assert allowed is False


def test_real_readiness_check_succeeds_and_is_durable_state_read_only(
    owner_engine,
    runtime_engine,
    worker_engine,
):
    before = _durable_fingerprint(owner_engine)

    WorkerSession = sessionmaker(
        bind=worker_engine,
        autoflush=False,
        autocommit=False,
    )
    RuntimeSession = sessionmaker(
        bind=runtime_engine,
        autoflush=False,
        autocommit=False,
    )

    result = check_satellite_worker_readiness(
        worker_session_factory=WorkerSession,
        runtime_session_factory=RuntimeSession,
    )

    after = _durable_fingerprint(owner_engine)

    assert result is True
    assert after == before


def test_real_worker_queue_metrics_function_returns_valid_aggregate(
    worker_engine,
):
    with worker_engine.connect() as connection:
        row = connection.execute(
            text(
                """
                SELECT
                    snapshot_time,
                    queued_ready_count,
                    queued_delayed_count,
                    running_count,
                    running_stale_count,
                    running_invalid_count,
                    oldest_ready_age_seconds,
                    oldest_active_lease_age_seconds,
                    oldest_heartbeat_age_seconds,
                    next_delayed_ready_in_seconds
                FROM public.worker_get_satellite_queue_metrics()
                """
            )
        ).mappings().one()

    assert row["snapshot_time"] is not None

    for field in (
        "queued_ready_count",
        "queued_delayed_count",
        "running_count",
        "running_stale_count",
        "running_invalid_count",
    ):
        assert int(row[field]) >= 0

    for field in (
        "oldest_ready_age_seconds",
        "oldest_active_lease_age_seconds",
        "oldest_heartbeat_age_seconds",
        "next_delayed_ready_in_seconds",
    ):
        if row[field] is not None:
            assert float(row[field]) >= 0.0


def test_cli_check_uses_real_runtime_and_worker_credentials_without_mutation(
    owner_engine,
):
    before = _durable_fingerprint(owner_engine)

    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "litoral_trace.workers.satellite_worker",
            "--check",
        ],
        cwd=ROOT_DIR,
        env=_subprocess_environment(),
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )

    after = _durable_fingerprint(owner_engine)

    combined_output = (
        (completed.stdout or "")
        + "\n"
        + (completed.stderr or "")
    ).lower()

    assert completed.returncode == 0, (
        "worker --check failed; output intentionally "
        "not echoed to avoid possible credential disclosure"
    )
    assert after == before

    # Successful healthchecks should not emit credential-bearing material.
    assert "postgresql://" not in combined_output
    assert "postgresql+psycopg://" not in combined_output
    assert "migration_database_url" not in combined_output
    assert "worker_database_url" not in combined_output
    assert "jwt_secret_key" not in combined_output


def test_cli_check_environment_does_not_receive_migration_owner_url():
    env = _subprocess_environment()

    assert "DATABASE_URL" in env
    assert "WORKER_DATABASE_URL" in env
    assert "MIGRATION_DATABASE_URL" not in env
    assert "TEST_POSTGRES_MIGRATION_DATABASE_URL" not in env
    assert "TEST_POSTGRES_DATABASE_URL" not in env
    assert "TEST_POSTGRES_WORKER_DATABASE_URL" not in env