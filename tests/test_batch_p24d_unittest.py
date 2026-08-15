from __future__ import annotations

import pandas as pd
import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

import litoral_trace.services.batch_imports as batch_imports
from litoral_trace.db.models import BatchImport, Lote
from litoral_trace.services.audit import build_audit_actor
from litoral_trace.services.batch import (
    BATCH_COLUMNAS,
    BatchWorkbook,
)
from litoral_trace.services.batch_imports import (
    BatchImportConflictError,
    BatchImportIdempotencyConflictError,
    BatchImportService,
    normalize_idempotency_key,
)


def _row(
    *,
    identificador="RODAL-001",
    proveedor="30-12345678-9",
):
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
    *rows,
    sha256="a" * 64,
):
    dataframe = pd.DataFrame(
        list(rows),
        columns=BATCH_COLUMNAS,
    )
    return BatchWorkbook(
        filename="batch.xlsx",
        sha256=sha256,
        sheet_name="Plantilla_LitoralTrace",
        row_count=len(dataframe.index),
        dataframe=dataframe,
        source_row_numbers=tuple(
            range(2, len(dataframe.index) + 2)
        ),
    )


def _actor(org_id=10):
    return build_audit_actor(
        organization_id=org_id,
        user_id=None,
        username="batch-admin",
        role="admin",
    )


@pytest.fixture()
def sqlite_runtime(monkeypatch):
    engine = create_engine(
        "sqlite://",
        connect_args={
            "check_same_thread": False,
        },
        poolclass=StaticPool,
    )

    Lote.__table__.create(
        engine
    )
    BatchImport.__table__.create(
        engine
    )

    SessionLocal = sessionmaker(
        bind=engine,
        autoflush=False,
        expire_on_commit=False,
    )

    monkeypatch.setattr(
        batch_imports,
        "record_audit_event",
        lambda *args, **kwargs: None,
    )

    try:
        yield SessionLocal
    finally:
        engine.dispose()


def test_idempotency_key_normalization_is_bounded_and_fail_closed():
    assert normalize_idempotency_key(
        "  request-001  "
    ) == "request-001"

    for invalid in (
        "",
        " ",
        "x\nkey",
        "x" * 256,
    ):
        with pytest.raises(
            BatchImportIdempotencyConflictError
        ):
            normalize_idempotency_key(
                invalid
            )


def test_first_keyed_import_persists_one_completed_identity(
    sqlite_runtime,
):
    service = BatchImportService(
        session_factory=sqlite_runtime
    )

    result = service.import_workbook(
        _workbook(
            _row(
                identificador="RODAL-001",
            ),
            _row(
                identificador="RODAL-002",
                proveedor="30-22222222-2",
            ),
        ),
        organization_id=10,
        actor=_actor(10),
        idempotency_key="request-001",
    )

    assert result.replayed is False
    assert result.import_public_id is not None
    assert result.inserted_rows == 2

    session = sqlite_runtime()
    try:
        record = session.execute(
            select(BatchImport)
        ).scalar_one()

        assert record.status == "completed"
        assert record.inserted_rows == 2
        assert record.lote_ids == list(
            result.lote_ids
        )
    finally:
        session.close()


def test_same_key_and_sha_replays_without_duplicate_lotes(
    sqlite_runtime,
):
    service = BatchImportService(
        session_factory=sqlite_runtime
    )
    workbook = _workbook(
        _row(
            identificador="RODAL-REPLAY",
        )
    )

    first = service.import_workbook(
        workbook,
        organization_id=10,
        actor=_actor(10),
        idempotency_key="request-replay",
    )
    second = service.import_workbook(
        workbook,
        organization_id=10,
        actor=_actor(10),
        idempotency_key="request-replay",
    )

    assert first.replayed is False
    assert second.replayed is True
    assert (
        second.import_public_id
        == first.import_public_id
    )
    assert second.lote_ids == first.lote_ids

    session = sqlite_runtime()
    try:
        assert session.execute(
            select(
                func.count(Lote.id)
            )
        ).scalar_one() == 1
        assert session.execute(
            select(
                func.count(BatchImport.id)
            )
        ).scalar_one() == 1
    finally:
        session.close()


def test_same_key_with_different_sha_is_conflict(
    sqlite_runtime,
):
    service = BatchImportService(
        session_factory=sqlite_runtime
    )

    first = _workbook(
        _row(
            identificador="RODAL-SHA",
        ),
        sha256="a" * 64,
    )
    different_source = _workbook(
        _row(
            identificador="RODAL-SHA-OTHER",
        ),
        sha256="b" * 64,
    )

    service.import_workbook(
        first,
        organization_id=10,
        actor=_actor(10),
        idempotency_key="request-sha",
    )

    with pytest.raises(
        BatchImportIdempotencyConflictError
    ):
        service.import_workbook(
            different_source,
            organization_id=10,
            actor=_actor(10),
            idempotency_key="request-sha",
        )


def test_different_key_cannot_duplicate_existing_tenant_identifier(
    sqlite_runtime,
):
    service = BatchImportService(
        session_factory=sqlite_runtime
    )

    workbook = _workbook(
        _row(
            identificador="RODAL-DUP",
        )
    )

    service.import_workbook(
        workbook,
        organization_id=10,
        actor=_actor(10),
        idempotency_key="request-a",
    )

    with pytest.raises(
        BatchImportConflictError
    ):
        service.import_workbook(
            workbook,
            organization_id=10,
            actor=_actor(10),
            idempotency_key="request-b",
        )


def test_legacy_unkeyed_path_remains_backward_compatible(
    sqlite_runtime,
):
    service = BatchImportService(
        session_factory=sqlite_runtime
    )

    result = service.import_workbook(
        _workbook(
            _row(
                identificador="RODAL-LEGACY",
            )
        ),
        organization_id=10,
        actor=_actor(10),
    )

    assert result.replayed is False
    assert result.import_public_id is None

    session = sqlite_runtime()
    try:
        assert session.execute(
            select(
                func.count(BatchImport.id)
            )
        ).scalar_one() == 0
    finally:
        session.close()


def test_case_insensitive_db_uniqueness_exists_in_model_metadata():
    index = next(
        index
        for index in Lote.__table__.indexes
        if index.name
        == "uq_lotes_tenant_identificador_ci"
    )

    assert index.unique is True


def test_batch_import_model_has_tenant_idempotency_unique_constraint():
    names = {
        constraint.name
        for constraint
        in BatchImport.__table__.constraints
    }

    assert (
        "uq_batch_imports_tenant_idempotency_key"
        in names
    )
