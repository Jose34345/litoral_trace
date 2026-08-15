from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import threading
from uuid import uuid4

import pandas as pd
import pytest
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.orm import sessionmaker

from litoral_trace.config.settings import normalize_database_url
from litoral_trace.db.models import BatchImport, Lote
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.services.audit import build_audit_actor
from litoral_trace.services.batch import (
    BATCH_COLUMNAS,
    BatchWorkbook,
)
from litoral_trace.services.batch_imports import (
    BatchImportConflictError,
    BatchImportIdempotencyConflictError,
    BatchImportService,
)


ROOT_DIR = Path(__file__).resolve().parents[1]
INTEGRATION_ENV_PATH = ROOT_DIR / ".env.integration"
EXPECTED_REVISION = "017_add_batch_import_idempotency"


def _truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _read_env_file(
    path: Path,
) -> dict[str, str]:
    values: dict[str, str] = {}

    if not path.exists():
        return values

    for raw_line in path.read_text(
        encoding="utf-8-sig"
    ).splitlines():
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


INTEGRATION_ENV = _read_env_file(
    INTEGRATION_ENV_PATH
)
POSTGRES_TESTS_ENABLED = _truthy(
    INTEGRATION_ENV.get(
        "ENABLE_POSTGRES_TESTS"
    )
)
RUNTIME_DATABASE_URL = INTEGRATION_ENV.get(
    "TEST_POSTGRES_DATABASE_URL"
)
OWNER_DATABASE_URL = INTEGRATION_ENV.get(
    "TEST_POSTGRES_MIGRATION_DATABASE_URL"
)

pytestmark = pytest.mark.skipif(
    not (
        POSTGRES_TESTS_ENABLED
        and RUNTIME_DATABASE_URL
        and OWNER_DATABASE_URL
    ),
    reason=(
        "P2.4D PostgreSQL tests require ENABLE_POSTGRES_TESTS=1 "
        "plus isolated runtime and migration-owner integration URLs."
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


def _row(
    *,
    identificador: str,
    proveedor: str,
) -> dict[str, object]:
    return {
        "Identificador_Lote": identificador,
        "ID_Proveedor": proveedor,
        "Producto_Forestal": "Madera Aserrada (Pino)",
        "Hectareas": 50.0,
        "Latitud": -27.45,
        "Longitud": -58.90,
        "Volumen_Ingresado_Ton": 100.0,
        "Volumen_Exportar_Ton": 45.0,
    }


def _workbook(
    *rows: dict[str, object],
    sha256: str = "d" * 64,
) -> BatchWorkbook:
    dataframe = pd.DataFrame(
        list(rows),
        columns=BATCH_COLUMNAS,
    )

    return BatchWorkbook(
        filename="P24D_Import.xlsx",
        sha256=sha256,
        sheet_name="Plantilla_LitoralTrace",
        row_count=len(dataframe.index),
        dataframe=dataframe,
        source_row_numbers=tuple(
            range(
                2,
                len(dataframe.index) + 2,
            )
        ),
    )


def _actor(
    organization_id: int,
):
    return build_audit_actor(
        organization_id=organization_id,
        user_id=None,
        username=f"p24d-{organization_id}",
        role="admin",
    )


@pytest.fixture()
def pg_batch_runtime():
    owner_engine = _engine(
        OWNER_DATABASE_URL,
        pool_size=4,
    )
    runtime_engine = _engine(
        RUNTIME_DATABASE_URL,
        pool_size=8,
    )
    RuntimeSession = sessionmaker(
        bind=runtime_engine,
        autoflush=False,
        expire_on_commit=False,
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
                "P2.4D requires integration database at "
                f"{EXPECTED_REVISION}; found {revision!r}."
            )

        org_ids: list[int] = []

        for label in ("A", "B"):
            org_ids.append(
                int(
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
                                'P2.4D batch integration',
                                true
                            )
                            RETURNING id
                            """
                        ),
                        {
                            "name": (
                                f"P24D Org {label} {suffix}"
                            ),
                            "slug": (
                                f"p24d-org-"
                                f"{label.lower()}-{suffix}"
                            ),
                            "tax_id": (
                                f"P24D-{label}-{suffix}"
                            ),
                        },
                    ).scalar_one()
                )
            )

    service = BatchImportService(
        session_factory=RuntimeSession
    )

    try:
        yield {
            "owner_engine": owner_engine,
            "runtime_engine": runtime_engine,
            "RuntimeSession": RuntimeSession,
            "service": service,
            "org_a_id": org_ids[0],
            "org_b_id": org_ids[1],
        }

    finally:
        with owner_engine.begin() as connection:
            connection.execute(
                text(
                    "DELETE FROM public.audit_logs "
                    "WHERE organization_id IN (:a, :b)"
                ),
                {
                    "a": org_ids[0],
                    "b": org_ids[1],
                },
            )
            connection.execute(
                text(
                    "DELETE FROM public.batch_imports "
                    "WHERE organization_id IN (:a, :b)"
                ),
                {
                    "a": org_ids[0],
                    "b": org_ids[1],
                },
            )
            connection.execute(
                text(
                    "DELETE FROM public.lotes "
                    "WHERE organization_id IN (:a, :b)"
                ),
                {
                    "a": org_ids[0],
                    "b": org_ids[1],
                },
            )
            connection.execute(
                text(
                    "DELETE FROM public.organizations "
                    "WHERE id IN (:a, :b)"
                ),
                {
                    "a": org_ids[0],
                    "b": org_ids[1],
                },
            )

        runtime_engine.dispose()
        owner_engine.dispose()


def _tenant_count(
    RuntimeSession,
    model,
    organization_id: int,
) -> int:
    session = RuntimeSession()

    try:
        set_tenant_db_context(
            session,
            organization_id,
        )
        return int(
            session.execute(
                select(
                    func.count(model.id)
                ).where(
                    model.organization_id
                    == organization_id
                )
            ).scalar_one()
        )
    finally:
        session.rollback()
        session.close()


def test_migration_enforces_rls_privileges_and_lote_unique_index(
    pg_batch_runtime,
):
    owner_engine = pg_batch_runtime[
        "owner_engine"
    ]

    with owner_engine.connect() as connection:
        row_security = connection.execute(
            text(
                """
                SELECT relrowsecurity, relforcerowsecurity
                FROM pg_class
                WHERE oid = 'public.batch_imports'::regclass
                """
            )
        ).one()

        assert row_security.relrowsecurity is True
        assert row_security.relforcerowsecurity is True

        runtime_select = connection.execute(
            text(
                """
                SELECT has_table_privilege(
                    'litoral_trace_app',
                    'public.batch_imports',
                    'SELECT'
                )
                """
            )
        ).scalar_one()
        runtime_insert = connection.execute(
            text(
                """
                SELECT has_table_privilege(
                    'litoral_trace_app',
                    'public.batch_imports',
                    'INSERT'
                )
                """
            )
        ).scalar_one()
        runtime_delete = connection.execute(
            text(
                """
                SELECT has_table_privilege(
                    'litoral_trace_app',
                    'public.batch_imports',
                    'DELETE'
                )
                """
            )
        ).scalar_one()
        worker_privileges = connection.execute(
            text(
                """
                SELECT
                    has_table_privilege(
                        'litoral_trace_worker_executor',
                        'public.batch_imports',
                        'SELECT'
                    ) AS can_select,
                    has_table_privilege(
                        'litoral_trace_worker_executor',
                        'public.batch_imports',
                        'INSERT'
                    ) AS can_insert,
                    has_table_privilege(
                        'litoral_trace_worker_executor',
                        'public.batch_imports',
                        'UPDATE'
                    ) AS can_update,
                    has_table_privilege(
                        'litoral_trace_worker_executor',
                        'public.batch_imports',
                        'DELETE'
                    ) AS can_delete
                """
            )
        ).one()

        assert runtime_select is True
        assert runtime_insert is True
        assert runtime_delete is False
        assert worker_privileges.can_select is False
        assert worker_privileges.can_insert is False
        assert worker_privileges.can_update is False
        assert worker_privileges.can_delete is False

        unique_index = connection.execute(
            text(
                """
                SELECT indisunique
                FROM pg_index
                WHERE indexrelid = (
                    SELECT oid
                    FROM pg_class
                    WHERE relname = 'uq_lotes_tenant_identificador_ci'
                )
                """
            )
        ).scalar_one()

        assert unique_index is True


def test_same_key_and_sha_replays_one_committed_import(
    pg_batch_runtime,
):
    org_id = pg_batch_runtime["org_a_id"]
    service = pg_batch_runtime["service"]
    workbook = _workbook(
        _row(
            identificador="P24D-REPLAY-001",
            proveedor="30-11111111-1",
        )
    )

    first = service.import_workbook(
        workbook,
        organization_id=org_id,
        actor=_actor(org_id),
        idempotency_key="p24d-replay",
    )
    second = service.import_workbook(
        workbook,
        organization_id=org_id,
        actor=_actor(org_id),
        idempotency_key="p24d-replay",
    )

    assert first.replayed is False
    assert second.replayed is True
    assert first.import_public_id == second.import_public_id
    assert first.lote_ids == second.lote_ids

    RuntimeSession = pg_batch_runtime[
        "RuntimeSession"
    ]
    assert _tenant_count(
        RuntimeSession,
        Lote,
        org_id,
    ) == 1
    assert _tenant_count(
        RuntimeSession,
        BatchImport,
        org_id,
    ) == 1


def test_same_key_with_different_sha_conflicts_without_new_rows(
    pg_batch_runtime,
):
    org_id = pg_batch_runtime["org_a_id"]
    service = pg_batch_runtime["service"]

    service.import_workbook(
        _workbook(
            _row(
                identificador="P24D-SHA-001",
                proveedor="30-11111111-1",
            ),
            sha256="a" * 64,
        ),
        organization_id=org_id,
        actor=_actor(org_id),
        idempotency_key="p24d-sha",
    )

    with pytest.raises(
        BatchImportIdempotencyConflictError
    ):
        service.import_workbook(
            _workbook(
                _row(
                    identificador="P24D-SHA-002",
                    proveedor="30-22222222-2",
                ),
                sha256="b" * 64,
            ),
            organization_id=org_id,
            actor=_actor(org_id),
            idempotency_key="p24d-sha",
        )

    RuntimeSession = pg_batch_runtime[
        "RuntimeSession"
    ]
    assert _tenant_count(
        RuntimeSession,
        Lote,
        org_id,
    ) == 1
    assert _tenant_count(
        RuntimeSession,
        BatchImport,
        org_id,
    ) == 1


def test_same_key_and_identifier_are_independent_between_tenants(
    pg_batch_runtime,
):
    org_a = pg_batch_runtime["org_a_id"]
    org_b = pg_batch_runtime["org_b_id"]
    service = pg_batch_runtime["service"]
    workbook = _workbook(
        _row(
            identificador="P24D-SHARED",
            proveedor="30-11111111-1",
        )
    )

    result_a = service.import_workbook(
        workbook,
        organization_id=org_a,
        actor=_actor(org_a),
        idempotency_key="same-key",
    )
    result_b = service.import_workbook(
        workbook,
        organization_id=org_b,
        actor=_actor(org_b),
        idempotency_key="same-key",
    )

    assert result_a.import_public_id != result_b.import_public_id

    RuntimeSession = pg_batch_runtime[
        "RuntimeSession"
    ]
    assert _tenant_count(
        RuntimeSession,
        Lote,
        org_a,
    ) == 1
    assert _tenant_count(
        RuntimeSession,
        Lote,
        org_b,
    ) == 1

    session_b = RuntimeSession()
    try:
        set_tenant_db_context(
            session_b,
            org_b,
        )

        hidden = session_b.execute(
            select(BatchImport).where(
                BatchImport.public_id
                == result_a.import_public_id
            )
        ).scalar_one_or_none()

        assert hidden is None
    finally:
        session_b.rollback()
        session_b.close()


def test_concurrent_same_key_commits_exactly_one_and_replays_winner(
    pg_batch_runtime,
):
    org_id = pg_batch_runtime["org_a_id"]
    service = pg_batch_runtime["service"]
    workbook = _workbook(
        _row(
            identificador="P24D-CONCURRENT-001",
            proveedor="30-11111111-1",
        )
    )
    barrier = threading.Barrier(2)

    def run_one():
        barrier.wait()
        return service.import_workbook(
            workbook,
            organization_id=org_id,
            actor=_actor(org_id),
            idempotency_key="p24d-concurrent-same",
        )

    with ThreadPoolExecutor(
        max_workers=2
    ) as executor:
        futures = [
            executor.submit(run_one)
            for _ in range(2)
        ]
        results = [
            future.result(timeout=30)
            for future in futures
        ]

    assert {
        result.import_public_id
        for result in results
    }.__len__() == 1
    assert sorted(
        result.replayed
        for result in results
    ) == [False, True]

    RuntimeSession = pg_batch_runtime[
        "RuntimeSession"
    ]
    assert _tenant_count(
        RuntimeSession,
        Lote,
        org_id,
    ) == 1
    assert _tenant_count(
        RuntimeSession,
        BatchImport,
        org_id,
    ) == 1


def test_concurrent_different_keys_same_identifier_allow_only_one_commit(
    pg_batch_runtime,
):
    org_id = pg_batch_runtime["org_a_id"]
    service = pg_batch_runtime["service"]
    workbook = _workbook(
        _row(
            identificador="P24D-CONCURRENT-DUP",
            proveedor="30-11111111-1",
        )
    )
    barrier = threading.Barrier(2)

    def run_one(key: str):
        barrier.wait()
        try:
            result = service.import_workbook(
                workbook,
                organization_id=org_id,
                actor=_actor(org_id),
                idempotency_key=key,
            )
            return ("success", result)
        except BatchImportConflictError as exc:
            return ("conflict", exc)

    with ThreadPoolExecutor(
        max_workers=2
    ) as executor:
        futures = [
            executor.submit(
                run_one,
                "p24d-key-a",
            ),
            executor.submit(
                run_one,
                "p24d-key-b",
            ),
        ]
        outcomes = [
            future.result(timeout=30)
            for future in futures
        ]

    assert sorted(
        outcome[0]
        for outcome in outcomes
    ) == [
        "conflict",
        "success",
    ]

    RuntimeSession = pg_batch_runtime[
        "RuntimeSession"
    ]
    assert _tenant_count(
        RuntimeSession,
        Lote,
        org_id,
    ) == 1
    assert _tenant_count(
        RuntimeSession,
        BatchImport,
        org_id,
    ) == 1

