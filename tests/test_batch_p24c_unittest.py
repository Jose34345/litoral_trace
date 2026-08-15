from __future__ import annotations

from dataclasses import replace

import pandas as pd
import pytest

import litoral_trace.services.batch_imports as batch_imports
from litoral_trace.services.audit import build_audit_actor
from litoral_trace.services.batch import (
    BATCH_COLUMNAS,
    BatchSemanticValidationError,
    BatchValidationResult,
    BatchWorkbook,
    validar_dataframe_lotes,
)
from litoral_trace.services.batch_imports import (
    BatchImportAuthorizationError,
    BatchImportConflictError,
    BatchImportPersistenceError,
    BatchImportService,
)


class _ScalarResult:
    def __init__(self, values):
        self._values = list(values)

    def all(self):
        return list(self._values)


class _ExecuteResult:
    def __init__(self, values):
        self._values = values

    def scalars(self):
        return _ScalarResult(
            self._values
        )


class FakeSession:
    def __init__(
        self,
        *,
        existing_identifiers=(),
        fail_flush=False,
        fail_commit=False,
    ):
        self.existing_identifiers = tuple(
            existing_identifiers
        )
        self.fail_flush = fail_flush
        self.fail_commit = fail_commit
        self.added = []
        self.execute_calls = 0
        self.flush_calls = 0
        self.commit_calls = 0
        self.rollback_calls = 0
        self.close_calls = 0

    def get_bind(self):
        class _Dialect:
            name = "sqlite"

        class _Bind:
            dialect = _Dialect()

        return _Bind()

    def execute(self, statement):
        self.execute_calls += 1
        return _ExecuteResult(
            self.existing_identifiers
        )

    def add_all(self, entities):
        self.added.extend(
            entities
        )

    def flush(self):
        self.flush_calls += 1

        if self.fail_flush:
            from sqlalchemy.exc import SQLAlchemyError
            raise SQLAlchemyError(
                "synthetic flush failure"
            )

        for index, entity in enumerate(
            self.added,
            start=101,
        ):
            if getattr(entity, "id", None) is None:
                entity.id = index

    def commit(self):
        self.commit_calls += 1
        if self.fail_commit:
            from sqlalchemy.exc import SQLAlchemyError
            raise SQLAlchemyError(
                "synthetic commit failure"
            )

    def rollback(self):
        self.rollback_calls += 1

    def close(self):
        self.close_calls += 1


def _df(*rows):
    return pd.DataFrame(
        list(rows),
        columns=BATCH_COLUMNAS,
    )


def _row(
    *,
    identificador="RODAL-001",
    proveedor="30-12345678-9",
    producto="Madera Aserrada (Pino)",
    hectareas=50.0,
    latitud=-27.45,
    longitud=-58.90,
    volumen_ingresado=100.0,
    volumen_exportar=45.0,
):
    return {
        "Identificador_Lote": identificador,
        "ID_Proveedor": proveedor,
        "Producto_Forestal": producto,
        "Hectareas": hectareas,
        "Latitud": latitud,
        "Longitud": longitud,
        "Volumen_Ingresado_Ton": volumen_ingresado,
        "Volumen_Exportar_Ton": volumen_exportar,
    }


def _workbook(*rows):
    dataframe = _df(*rows)
    return BatchWorkbook(
        filename="batch.xlsx",
        sha256="a" * 64,
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
        user_id=7,
        username="batch-admin",
        role="admin",
    )


def test_semantic_failure_happens_before_session_creation():
    calls = {"factory": 0}

    def factory():
        calls["factory"] += 1
        return FakeSession()

    service = BatchImportService(
        session_factory=factory
    )

    with pytest.raises(
        BatchSemanticValidationError
    ):
        service.import_workbook(
            _workbook(
                _row(
                    latitud=999.0,
                )
            ),
            organization_id=10,
            actor=_actor(10),
        )

    assert calls["factory"] == 0


def test_actor_tenant_mismatch_fails_before_session_creation():
    calls = {"factory": 0}

    def factory():
        calls["factory"] += 1
        return FakeSession()

    service = BatchImportService(
        session_factory=factory
    )

    with pytest.raises(
        BatchImportAuthorizationError
    ):
        service.import_workbook(
            _workbook(_row()),
            organization_id=11,
            actor=_actor(10),
        )

    assert calls["factory"] == 0


def test_existing_tenant_identifier_conflicts_before_any_insert(monkeypatch):
    session = FakeSession(
        existing_identifiers=("RODAL-001",)
    )
    service = BatchImportService(
        session_factory=lambda: session
    )

    monkeypatch.setattr(
        batch_imports,
        "record_audit_event",
        lambda *args, **kwargs: None,
    )

    with pytest.raises(
        BatchImportConflictError
    ) as exc_info:
        service.import_workbook(
            _workbook(_row()),
            organization_id=10,
            actor=_actor(10),
        )

    assert exc_info.value.identifiers == (
        "RODAL-001",
    )
    assert session.added == []
    assert session.commit_calls == 0
    assert session.rollback_calls >= 1
    assert session.close_calls == 1


def test_valid_import_uses_trusted_tenant_and_one_commit(monkeypatch):
    session = FakeSession()
    service = BatchImportService(
        session_factory=lambda: session
    )
    audit_calls = []

    def fake_audit(db_session, **kwargs):
        audit_calls.append(
            kwargs
        )
        return None

    monkeypatch.setattr(
        batch_imports,
        "record_audit_event",
        fake_audit,
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
    )

    assert result.organization_id == 10
    assert result.total_rows == 2
    assert result.inserted_rows == 2
    assert result.lote_ids == (101, 102)
    assert result.identifiers == (
        "RODAL-001",
        "RODAL-002",
    )

    assert len(session.added) == 2
    assert {
        lote.organization_id
        for lote in session.added
    } == {10}
    assert session.flush_calls == 1
    assert session.commit_calls == 1
    assert session.rollback_calls == 0
    assert session.close_calls == 1

    # Two lote.create events plus one batch summary event.
    assert len(audit_calls) == 3
    assert audit_calls[-1]["entity_type"] == "lote_batch_import"


def test_flush_failure_rolls_back_and_never_commits(monkeypatch):
    session = FakeSession(
        fail_flush=True
    )
    service = BatchImportService(
        session_factory=lambda: session
    )

    monkeypatch.setattr(
        batch_imports,
        "record_audit_event",
        lambda *args, **kwargs: None,
    )

    with pytest.raises(
        BatchImportPersistenceError
    ) as exc_info:
        service.import_workbook(
            _workbook(_row()),
            organization_id=10,
            actor=_actor(10),
        )

    assert "synthetic" not in str(
        exc_info.value
    ).lower()
    assert session.commit_calls == 0
    assert session.rollback_calls == 1
    assert session.close_calls == 1


def test_audit_failure_rolls_back_all_insert_candidates(monkeypatch):
    session = FakeSession()
    service = BatchImportService(
        session_factory=lambda: session
    )

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
                    identificador="RODAL-001",
                ),
                _row(
                    identificador="RODAL-002",
                    proveedor="30-22222222-2",
                ),
            ),
            organization_id=10,
            actor=_actor(10),
        )

    assert "synthetic" not in str(
        exc_info.value
    ).lower()
    assert len(session.added) == 2
    assert session.commit_calls == 0
    assert session.rollback_calls == 1
    assert session.close_calls == 1


def test_unavailable_database_fails_closed(monkeypatch):
    service = BatchImportService(
        session_factory=lambda: None
    )

    with pytest.raises(
        BatchImportPersistenceError
    ) as exc_info:
        service.import_workbook(
            _workbook(_row()),
            organization_id=10,
            actor=_actor(10),
        )

    assert (
        str(exc_info.value)
        == "Servicio de base de datos no disponible."
    )


def test_import_validated_rejects_inconsistent_invalid_result():
    valid = validar_dataframe_lotes(
        _df(_row())
    )
    inconsistent = replace(
        valid,
        valid=False,
        invalid_rows=1,
    )

    service = BatchImportService(
        session_factory=lambda: FakeSession()
    )

    with pytest.raises(
        BatchSemanticValidationError
    ):
        service.import_validated(
            inconsistent,
            organization_id=10,
            actor=_actor(10),
        )


def test_canonical_rows_never_accept_organization_from_excel():
    validation = validar_dataframe_lotes(
        _df(_row())
    )
    canonical = validation.canonical_rows[0]

    assert "organization_id" not in canonical.as_lote_payload()