from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pandas as pd
import pytest
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.orm import sessionmaker

import litoral_trace.services.batch_imports as batch_imports
from litoral_trace.config.settings import normalize_database_url
from litoral_trace.db.models import AuditLog, Lote
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.services.audit import build_audit_actor
from litoral_trace.services.batch import (
    BATCH_COLUMNAS,
    BatchSemanticValidationError,
    BatchWorkbook,
)
from litoral_trace.services.batch_imports import (
    BatchImportConflictError,
    BatchImportPersistenceError,
    BatchImportService,
)


ROOT_DIR = Path(__file__).resolve().parents[1]
INTEGRATION_ENV_PATH = ROOT_DIR / ".env.integration"
EXPECTED_REVISION = "016_add_vault_documents"


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
        "P2.4C PostgreSQL tests require ENABLE_POSTGRES_TESTS=1 "
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
) -> BatchWorkbook:
    dataframe = pd.DataFrame(
        list(rows),
        columns=BATCH_COLUMNAS,
    )

    return BatchWorkbook(
        filename="P24C_Import.xlsx",
        sha256="b" * 64,
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
        username=f"p24c-{organization_id}",
        role="admin",
    )


@pytest.fixture()
def pg_batch_runtime():
    owner_engine = _engine(
        OWNER_DATABASE_URL,
        pool_size=3,
    )
    runtime_engine = _engine(
        RUNTIME_DATABASE_URL,
        pool_size=4,
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
                "P2.4C requires integration database at "
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
                                'P2.4C batch integration',
                                true
                            )
                            RETURNING id
                            """
                        ),
                        {
                            "name": (
                                f"P24C Org {label} {suffix}"
                            ),
                            "slug": (
                                f"p24c-org-"
                                f"{label.lower()}-{suffix}"
                            ),
                            "tax_id": (
                                f"P24C-{label}-{suffix}"
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


def _tenant_lote_count(
    RuntimeSession,
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
                    func.count(Lote.id)
                ).where(
                    Lote.organization_id
                    == organization_id
                )
            ).scalar_one()
        )
    finally:
        session.rollback()
        session.close()


def test_real_runtime_import_commits_all_rows_and_audit(
    pg_batch_runtime,
):
    org_id = pg_batch_runtime["org_a_id"]
    service = pg_batch_runtime["service"]

    result = service.import_workbook(
        _workbook(
            _row(
                identificador="P24C-A-001",
                proveedor="30-11111111-1",
            ),
            _row(
                identificador="P24C-A-002",
                proveedor="30-22222222-2",
            ),
        ),
        organization_id=org_id,
        actor=_actor(org_id),
    )

    assert result.inserted_rows == 2
    assert len(result.lote_ids) == 2

    RuntimeSession = pg_batch_runtime[
        "RuntimeSession"
    ]
    session = RuntimeSession()

    try:
        set_tenant_db_context(
            session,
            org_id,
        )

        lotes = session.execute(
            select(Lote)
            .where(
                Lote.organization_id == org_id
            )
            .order_by(Lote.id)
        ).scalars().all()

        assert [
            lote.identificador
            for lote in lotes
        ] == [
            "P24C-A-001",
            "P24C-A-002",
        ]

        audits = session.execute(
            select(AuditLog).where(
                AuditLog.organization_id == org_id,
                AuditLog.action.in_(
                    [
                        "lote.create",
                        "lote.batch_upload",
                    ]
                ),
            )
        ).scalars().all()

        assert len(audits) == 3

    finally:
        session.rollback()
        session.close()


def test_existing_identifier_conflict_produces_zero_new_rows(
    pg_batch_runtime,
):
    org_id = pg_batch_runtime["org_a_id"]
    service = pg_batch_runtime["service"]

    service.import_workbook(
        _workbook(
            _row(
                identificador="P24C-DUP-001",
                proveedor="30-11111111-1",
            )
        ),
        organization_id=org_id,
        actor=_actor(org_id),
    )

    before = _tenant_lote_count(
        pg_batch_runtime["RuntimeSession"],
        org_id,
    )

    with pytest.raises(
        BatchImportConflictError
    ):
        service.import_workbook(
            _workbook(
                _row(
                    identificador="p24c-dup-001",
                    proveedor="30-22222222-2",
                ),
                _row(
                    identificador="P24C-NEW-002",
                    proveedor="30-33333333-3",
                ),
            ),
            organization_id=org_id,
            actor=_actor(org_id),
        )

    after = _tenant_lote_count(
        pg_batch_runtime["RuntimeSession"],
        org_id,
    )

    assert after == before


def test_force_rls_isolates_batch_rows_between_tenants(
    pg_batch_runtime,
):
    org_a = pg_batch_runtime["org_a_id"]
    org_b = pg_batch_runtime["org_b_id"]
    service = pg_batch_runtime["service"]

    result_a = service.import_workbook(
        _workbook(
            _row(
                identificador="P24C-SHARED",
                proveedor="30-11111111-1",
            )
        ),
        organization_id=org_a,
        actor=_actor(org_a),
    )

    RuntimeSession = pg_batch_runtime[
        "RuntimeSession"
    ]
    session_b = RuntimeSession()

    try:
        set_tenant_db_context(
            session_b,
            org_b,
        )

        invisible = session_b.execute(
            select(Lote).where(
                Lote.id == result_a.lote_ids[0]
            )
        ).scalar_one_or_none()

        assert invisible is None

    finally:
        session_b.rollback()
        session_b.close()

    result_b = service.import_workbook(
        _workbook(
            _row(
                identificador="P24C-SHARED",
                proveedor="30-99999999-9",
            )
        ),
        organization_id=org_b,
        actor=_actor(org_b),
    )

    assert result_b.inserted_rows == 1

    assert _tenant_lote_count(
        RuntimeSession,
        org_a,
    ) == 1
    assert _tenant_lote_count(
        RuntimeSession,
        org_b,
    ) == 1


def test_real_postgres_audit_failure_rolls_back_flushed_lotes(
    pg_batch_runtime,
    monkeypatch,
):
    org_id = pg_batch_runtime["org_a_id"]
    service = pg_batch_runtime["service"]

    def fail_audit(*args, **kwargs):
        raise RuntimeError(
            "synthetic audit failure"
        )

    monkeypatch.setattr(
        batch_imports,
        "record_audit_event",
        fail_audit,
    )

    with pytest.raises(
        BatchImportPersistenceError
    ) as exc_info:
        service.import_workbook(
            _workbook(
                _row(
                    identificador="P24C-ROLLBACK-001",
                    proveedor="30-11111111-1",
                ),
                _row(
                    identificador="P24C-ROLLBACK-002",
                    proveedor="30-22222222-2",
                ),
            ),
            organization_id=org_id,
            actor=_actor(org_id),
        )

    assert "synthetic" not in str(
        exc_info.value
    ).lower()

    assert _tenant_lote_count(
        pg_batch_runtime["RuntimeSession"],
        org_id,
    ) == 0


def test_semantic_failure_writes_nothing_to_real_postgres(
    pg_batch_runtime,
):
    org_id = pg_batch_runtime["org_a_id"]
    service = pg_batch_runtime["service"]

    invalid_workbook = _workbook(
        {
            **_row(
                identificador="P24C-INVALID-001",
                proveedor="30-11111111-1",
            ),
            "Latitud": 999.0,
        }
    )

    with pytest.raises(
        BatchSemanticValidationError
    ) as exc_info:
        service.import_workbook(
            invalid_workbook,
            organization_id=org_id,
            actor=_actor(org_id),
        )

    assert exc_info.value.result.invalid_rows == 1

    assert _tenant_lote_count(
        pg_batch_runtime["RuntimeSession"],
        org_id,
    ) == 0