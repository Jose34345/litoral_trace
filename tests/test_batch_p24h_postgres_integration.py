from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID, uuid4

import pandas as pd
import pytest
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

import litoral_trace.services.batch_imports as batch_imports_module
from litoral_trace.config.settings import normalize_database_url
from litoral_trace.db.models import (
    AuditLog,
    BatchEvidenceLink,
    BatchImport,
    Lote,
)
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.services.audit import (
    AuditAction,
    build_audit_actor,
    record_audit_event as real_record_audit_event,
)
from litoral_trace.services.batch import (
    BATCH_COLUMNAS,
    BatchWorkbook,
)
from litoral_trace.services.batch_evidence import (
    BatchEvidenceConflictError,
    BatchEvidenceService,
)
from litoral_trace.services.batch_imports import (
    BatchImportPersistenceError,
    BatchImportService,
)
from litoral_trace.services.batch_queries import (
    BatchImportQueryService,
)


ROOT_DIR = Path(__file__).resolve().parents[1]
INTEGRATION_ENV_PATH = ROOT_DIR / ".env.integration"
EXPECTED_REVISION = "018_add_batch_evidence_links"
RUNTIME_ROLE = "litoral_trace_app"
WORKER_ROLE = "litoral_trace_worker_executor"


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
        "P2.4H PostgreSQL tests require ENABLE_POSTGRES_TESTS=1 "
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
    filename: str = "P24H_Import.xlsx",
    sha256: str = "h" * 64,
) -> BatchWorkbook:
    dataframe = pd.DataFrame(
        list(rows),
        columns=BATCH_COLUMNAS,
    )

    return BatchWorkbook(
        filename=filename,
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
        username=f"p24h-{organization_id}",
        role="admin",
    )


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


@pytest.fixture()
def pg_batch_acceptance():
    owner_engine = _engine(
        OWNER_DATABASE_URL,
        pool_size=4,
    )
    runtime_engine = _engine(
        RUNTIME_DATABASE_URL,
        pool_size=6,
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
                "P2.4H requires integration database at "
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
                                'P2.4H acceptance integration',
                                true
                            )
                            RETURNING id
                            """
                        ),
                        {
                            "name": (
                                f"P24H Org {label} {suffix}"
                            ),
                            "slug": (
                                f"p24h-org-"
                                f"{label.lower()}-{suffix}"
                            ),
                            "tax_id": (
                                f"P24H-{label}-{suffix}"
                            ),
                        },
                    ).scalar_one()
                )
            )

    def create_batch_import(
        *,
        organization_id: int,
        source_sha256: str,
        idempotency_key: str,
        source_filename: str = "P24H_Source.xlsx",
        identifiers: list[str] | None = None,
        inserted_rows: int = 1,
    ) -> tuple[int, UUID]:
        lote_ids = list(
            range(
                1,
                inserted_rows + 1,
            )
        )
        names = identifiers or [
            f"P24H-{organization_id}-{index}"
            for index in range(inserted_rows)
        ]

        with owner_engine.begin() as connection:
            row = connection.execute(
                text(
                    """
                    INSERT INTO public.batch_imports (
                        organization_id,
                        created_by_user_id,
                        idempotency_key,
                        source_sha256,
                        source_filename,
                        status,
                        total_rows,
                        inserted_rows,
                        lote_ids,
                        identifiers,
                        completed_at
                    )
                    VALUES (
                        :organization_id,
                        NULL,
                        :idempotency_key,
                        :source_sha256,
                        :source_filename,
                        'completed',
                        :inserted_rows,
                        :inserted_rows,
                        CAST(:lote_ids AS json),
                        CAST(:identifiers AS json),
                        CURRENT_TIMESTAMP
                    )
                    RETURNING id, public_id
                    """
                ),
                {
                    "organization_id": organization_id,
                    "idempotency_key": idempotency_key,
                    "source_sha256": source_sha256,
                    "source_filename": source_filename,
                    "inserted_rows": inserted_rows,
                    "lote_ids": json.dumps(lote_ids),
                    "identifiers": json.dumps(names),
                },
            ).one()

        return int(row.id), UUID(str(row.public_id))

    def create_vault_document(
        *,
        organization_id: int,
        filename: str,
        content_type: str,
        document_type: str,
        sha256: str,
        object_suffix: str,
        status: str = "available",
    ) -> tuple[int, UUID]:
        with owner_engine.begin() as connection:
            row = connection.execute(
                text(
                    """
                    INSERT INTO public.vault_documents (
                        organization_id,
                        created_by_user_id,
                        original_filename,
                        content_type,
                        size_bytes,
                        sha256,
                        object_key,
                        storage_backend,
                        storage_bucket,
                        document_type,
                        status,
                        deleted_at
                    )
                    VALUES (
                        :organization_id,
                        NULL,
                        :filename,
                        :content_type,
                        128,
                        :sha256,
                        :object_key,
                        's3',
                        'p24h-integration-bucket',
                        :document_type,
                        :status,
                        :deleted_at
                    )
                    RETURNING id, public_id
                    """
                ),
                {
                    "organization_id": organization_id,
                    "filename": filename,
                    "content_type": content_type,
                    "sha256": sha256,
                    "object_key": (
                        "p24h/integration/"
                        f"{suffix}/{object_suffix}"
                    ),
                    "document_type": document_type,
                    "status": status,
                    "deleted_at": (
                        datetime.now(timezone.utc)
                        if status == "deleted"
                        else None
                    ),
                },
            ).one()

        return int(row.id), UUID(str(row.public_id))

    fixture = {
        "owner_engine": owner_engine,
        "runtime_engine": runtime_engine,
        "RuntimeSession": RuntimeSession,
        "import_service": BatchImportService(
            session_factory=RuntimeSession
        ),
        "query_service": BatchImportQueryService(
            session_factory=RuntimeSession
        ),
        "evidence_service": BatchEvidenceService(
            session_factory=RuntimeSession
        ),
        "org_a_id": org_ids[0],
        "org_b_id": org_ids[1],
        "create_batch_import": create_batch_import,
        "create_vault_document": create_vault_document,
    }

    try:
        yield fixture
    finally:
        with owner_engine.begin() as connection:
            connection.execute(
                text(
                    """
                    DELETE FROM public.audit_logs
                    WHERE organization_id IN (:a, :b)
                    """
                ),
                {
                    "a": org_ids[0],
                    "b": org_ids[1],
                },
            )
            connection.execute(
                text(
                    """
                    DELETE FROM public.batch_evidence_links
                    WHERE organization_id IN (:a, :b)
                    """
                ),
                {
                    "a": org_ids[0],
                    "b": org_ids[1],
                },
            )
            connection.execute(
                text(
                    """
                    DELETE FROM public.batch_imports
                    WHERE organization_id IN (:a, :b)
                    """
                ),
                {
                    "a": org_ids[0],
                    "b": org_ids[1],
                },
            )
            connection.execute(
                text(
                    """
                    DELETE FROM public.vault_documents
                    WHERE organization_id IN (:a, :b)
                    """
                ),
                {
                    "a": org_ids[0],
                    "b": org_ids[1],
                },
            )
            connection.execute(
                text(
                    """
                    DELETE FROM public.lotes
                    WHERE organization_id IN (:a, :b)
                    """
                ),
                {
                    "a": org_ids[0],
                    "b": org_ids[1],
                },
            )
            connection.execute(
                text(
                    """
                    DELETE FROM public.organizations
                    WHERE id IN (:a, :b)
                    """
                ),
                {
                    "a": org_ids[0],
                    "b": org_ids[1],
                },
            )

        runtime_engine.dispose()
        owner_engine.dispose()


def test_p24h_catalog_confirms_batch_policies_indexes_and_public_grants(
    pg_batch_acceptance,
):
    owner_engine = pg_batch_acceptance[
        "owner_engine"
    ]

    with owner_engine.connect() as connection:
        revision = connection.execute(
            text(
                "SELECT version_num "
                "FROM alembic_version"
            )
        ).scalar_one()
        assert revision == EXPECTED_REVISION

        row_security = connection.execute(
            text(
                """
                SELECT relname, relrowsecurity, relforcerowsecurity
                FROM pg_class
                WHERE oid IN (
                    'public.batch_imports'::regclass,
                    'public.batch_evidence_links'::regclass
                )
                ORDER BY relname
                """
            )
        ).mappings().all()

        assert row_security == [
            {
                "relname": "batch_evidence_links",
                "relrowsecurity": True,
                "relforcerowsecurity": True,
            },
            {
                "relname": "batch_imports",
                "relrowsecurity": True,
                "relforcerowsecurity": True,
            },
        ]

        policies = connection.execute(
            text(
                """
                SELECT tablename, policyname
                FROM pg_policies
                WHERE schemaname = 'public'
                  AND tablename IN (
                    'batch_imports',
                    'batch_evidence_links'
                  )
                ORDER BY tablename, policyname
                """
            )
        ).all()

        assert policies == [
            (
                "batch_evidence_links",
                "batch_evidence_links_tenant_insert",
            ),
            (
                "batch_evidence_links",
                "batch_evidence_links_tenant_select",
            ),
            (
                "batch_evidence_links",
                "batch_evidence_links_tenant_update",
            ),
            (
                "batch_imports",
                "batch_imports_tenant_insert",
            ),
            (
                "batch_imports",
                "batch_imports_tenant_select",
            ),
            (
                "batch_imports",
                "batch_imports_tenant_update",
            ),
        ]

        indexes = connection.execute(
            text(
                """
                SELECT
                    c.relname AS index_name,
                    i.indisunique,
                    pg_get_expr(i.indpred, i.indrelid) AS predicate
                FROM pg_index i
                JOIN pg_class c
                  ON c.oid = i.indexrelid
                WHERE c.relname IN (
                    'uq_batch_evidence_links_active_pair',
                    'uq_batch_evidence_links_active_source'
                )
                ORDER BY c.relname
                """
            )
        ).mappings().all()

        assert len(indexes) == 2
        assert indexes[0]["index_name"] == (
            "uq_batch_evidence_links_active_pair"
        )
        assert indexes[0]["indisunique"] is True
        assert "unlinked_at IS NULL" in str(
            indexes[0]["predicate"]
        )
        assert indexes[1]["index_name"] == (
            "uq_batch_evidence_links_active_source"
        )
        assert indexes[1]["indisunique"] is True
        assert "unlinked_at IS NULL" in str(
            indexes[1]["predicate"]
        )
        assert "SOURCE_WORKBOOK" in str(
            indexes[1]["predicate"]
        )

        privilege_rows = connection.execute(
            text(
                f"""
                SELECT *
                FROM (
                    SELECT
                        'batch_imports' AS object_name,
                        has_table_privilege(
                            'public',
                            'public.batch_imports',
                            'SELECT'
                        ) AS public_select,
                        has_table_privilege(
                            'public',
                            'public.batch_imports',
                            'INSERT'
                        ) AS public_insert,
                        has_table_privilege(
                            'public',
                            'public.batch_imports',
                            'UPDATE'
                        ) AS public_update,
                        has_table_privilege(
                            '{RUNTIME_ROLE}',
                            'public.batch_imports',
                            'SELECT'
                        ) AS runtime_select,
                        has_table_privilege(
                            '{RUNTIME_ROLE}',
                            'public.batch_imports',
                            'INSERT'
                        ) AS runtime_insert,
                        has_table_privilege(
                            '{RUNTIME_ROLE}',
                            'public.batch_imports',
                            'UPDATE'
                        ) AS runtime_update,
                        has_table_privilege(
                            '{RUNTIME_ROLE}',
                            'public.batch_imports',
                            'DELETE'
                        ) AS runtime_delete,
                        has_table_privilege(
                            '{WORKER_ROLE}',
                            'public.batch_imports',
                            'SELECT'
                        ) AS worker_select,
                        has_table_privilege(
                            '{WORKER_ROLE}',
                            'public.batch_imports',
                            'INSERT'
                        ) AS worker_insert,
                        has_table_privilege(
                            '{WORKER_ROLE}',
                            'public.batch_imports',
                            'UPDATE'
                        ) AS worker_update,
                        has_table_privilege(
                            '{WORKER_ROLE}',
                            'public.batch_imports',
                            'DELETE'
                        ) AS worker_delete,
                        has_sequence_privilege(
                            'public',
                            'public.batch_imports_id_seq',
                            'USAGE'
                        ) AS public_seq_usage,
                        has_sequence_privilege(
                            '{RUNTIME_ROLE}',
                            'public.batch_imports_id_seq',
                            'USAGE'
                        ) AS runtime_seq_usage,
                        has_sequence_privilege(
                            '{RUNTIME_ROLE}',
                            'public.batch_imports_id_seq',
                            'SELECT'
                        ) AS runtime_seq_select,
                        has_sequence_privilege(
                            '{WORKER_ROLE}',
                            'public.batch_imports_id_seq',
                            'USAGE'
                        ) AS worker_seq_usage
                    UNION ALL
                    SELECT
                        'batch_evidence_links' AS object_name,
                        has_table_privilege(
                            'public',
                            'public.batch_evidence_links',
                            'SELECT'
                        ) AS public_select,
                        has_table_privilege(
                            'public',
                            'public.batch_evidence_links',
                            'INSERT'
                        ) AS public_insert,
                        has_table_privilege(
                            'public',
                            'public.batch_evidence_links',
                            'UPDATE'
                        ) AS public_update,
                        has_table_privilege(
                            '{RUNTIME_ROLE}',
                            'public.batch_evidence_links',
                            'SELECT'
                        ) AS runtime_select,
                        has_table_privilege(
                            '{RUNTIME_ROLE}',
                            'public.batch_evidence_links',
                            'INSERT'
                        ) AS runtime_insert,
                        has_table_privilege(
                            '{RUNTIME_ROLE}',
                            'public.batch_evidence_links',
                            'UPDATE'
                        ) AS runtime_update,
                        has_table_privilege(
                            '{RUNTIME_ROLE}',
                            'public.batch_evidence_links',
                            'DELETE'
                        ) AS runtime_delete,
                        has_table_privilege(
                            '{WORKER_ROLE}',
                            'public.batch_evidence_links',
                            'SELECT'
                        ) AS worker_select,
                        has_table_privilege(
                            '{WORKER_ROLE}',
                            'public.batch_evidence_links',
                            'INSERT'
                        ) AS worker_insert,
                        has_table_privilege(
                            '{WORKER_ROLE}',
                            'public.batch_evidence_links',
                            'UPDATE'
                        ) AS worker_update,
                        has_table_privilege(
                            '{WORKER_ROLE}',
                            'public.batch_evidence_links',
                            'DELETE'
                        ) AS worker_delete,
                        has_sequence_privilege(
                            'public',
                            'public.batch_evidence_links_id_seq',
                            'USAGE'
                        ) AS public_seq_usage,
                        has_sequence_privilege(
                            '{RUNTIME_ROLE}',
                            'public.batch_evidence_links_id_seq',
                            'USAGE'
                        ) AS runtime_seq_usage,
                        has_sequence_privilege(
                            '{RUNTIME_ROLE}',
                            'public.batch_evidence_links_id_seq',
                            'SELECT'
                        ) AS runtime_seq_select,
                        has_sequence_privilege(
                            '{WORKER_ROLE}',
                            'public.batch_evidence_links_id_seq',
                            'USAGE'
                        ) AS worker_seq_usage
                ) AS privileges
                ORDER BY object_name
                """
            )
        ).mappings().all()

        for row in privilege_rows:
            assert row["public_select"] is False
            assert row["public_insert"] is False
            assert row["public_update"] is False
            assert row["runtime_select"] is True
            assert row["runtime_insert"] is True
            assert row["runtime_update"] is True
            assert row["runtime_delete"] is False
            assert row["worker_select"] is False
            assert row["worker_insert"] is False
            assert row["worker_update"] is False
            assert row["worker_delete"] is False
            assert row["public_seq_usage"] is False
            assert row["runtime_seq_usage"] is True
            assert row["runtime_seq_select"] is True
            assert row["worker_seq_usage"] is False


def test_p24h_keyed_audit_failure_rolls_back_claim_and_allows_retry(
    pg_batch_acceptance,
    monkeypatch,
):
    org_id = pg_batch_acceptance["org_a_id"]
    service = pg_batch_acceptance["import_service"]
    RuntimeSession = pg_batch_acceptance[
        "RuntimeSession"
    ]
    workbook = _workbook(
        _row(
            identificador="P24H-ROLLBACK-001",
            proveedor="30-11111111-1",
        ),
        _row(
            identificador="P24H-ROLLBACK-002",
            proveedor="30-22222222-2",
        ),
        sha256="1" * 64,
    )
    idem_key = f"p24h-keyed-rollback-{uuid4().hex}"

    def fail_audit(*args, **kwargs):
        raise RuntimeError(
            "synthetic audit failure"
        )

    monkeypatch.setattr(
        batch_imports_module,
        "record_audit_event",
        fail_audit,
    )

    with pytest.raises(
        BatchImportPersistenceError
    ):
        service.import_workbook(
            workbook,
            organization_id=org_id,
            actor=_actor(org_id),
            idempotency_key=idem_key,
        )

    assert _tenant_count(
        RuntimeSession,
        Lote,
        org_id,
    ) == 0
    assert _tenant_count(
        RuntimeSession,
        BatchImport,
        org_id,
    ) == 0

    monkeypatch.setattr(
        batch_imports_module,
        "record_audit_event",
        real_record_audit_event,
    )

    retried = service.import_workbook(
        workbook,
        organization_id=org_id,
        actor=_actor(org_id),
        idempotency_key=idem_key,
    )

    assert retried.replayed is False
    assert retried.inserted_rows == 2
    assert retried.import_public_id is not None
    assert _tenant_count(
        RuntimeSession,
        Lote,
        org_id,
    ) == 2
    assert _tenant_count(
        RuntimeSession,
        BatchImport,
        org_id,
    ) == 1


def test_p24h_replay_does_not_duplicate_audit_rows(
    pg_batch_acceptance,
):
    org_id = pg_batch_acceptance["org_a_id"]
    service = pg_batch_acceptance["import_service"]
    RuntimeSession = pg_batch_acceptance[
        "RuntimeSession"
    ]
    workbook = _workbook(
        _row(
            identificador="P24H-REPLAY-001",
            proveedor="30-11111111-1",
        ),
        sha256="2" * 64,
    )
    idem_key = f"p24h-audit-replay-{uuid4().hex}"

    first = service.import_workbook(
        workbook,
        organization_id=org_id,
        actor=_actor(org_id),
        idempotency_key=idem_key,
    )

    session = RuntimeSession()
    try:
        set_tenant_db_context(
            session,
            org_id,
        )
        audit_count_before = int(
            session.execute(
                select(
                    func.count(AuditLog.id)
                ).where(
                    AuditLog.organization_id == org_id,
                    AuditLog.action.in_(
                        [
                            AuditAction.LOTE_CREATE.value,
                            AuditAction.LOTE_BATCH_UPLOAD.value,
                        ]
                    ),
                )
            ).scalar_one()
        )
    finally:
        session.rollback()
        session.close()

    second = service.import_workbook(
        workbook,
        organization_id=org_id,
        actor=_actor(org_id),
        idempotency_key=idem_key,
    )

    assert second.replayed is True
    assert second.import_public_id == first.import_public_id
    assert second.lote_ids == first.lote_ids
    assert _tenant_count(
        RuntimeSession,
        Lote,
        org_id,
    ) == 1

    session = RuntimeSession()
    try:
        set_tenant_db_context(
            session,
            org_id,
        )
        audit_count_after = int(
            session.execute(
                select(
                    func.count(AuditLog.id)
                ).where(
                    AuditLog.organization_id == org_id,
                    AuditLog.action.in_(
                        [
                            AuditAction.LOTE_CREATE.value,
                            AuditAction.LOTE_BATCH_UPLOAD.value,
                        ]
                    ),
                )
            ).scalar_one()
        )
    finally:
        session.rollback()
        session.close()

    assert audit_count_after == audit_count_before


def test_p24h_runtime_rls_hides_cross_tenant_evidence_links(
    pg_batch_acceptance,
):
    org_a = pg_batch_acceptance["org_a_id"]
    org_b = pg_batch_acceptance["org_b_id"]
    service = pg_batch_acceptance["evidence_service"]
    RuntimeSession = pg_batch_acceptance[
        "RuntimeSession"
    ]

    batch_a_id, batch_a_public_id = (
        pg_batch_acceptance["create_batch_import"](
            organization_id=org_a,
            source_sha256="3" * 64,
            idempotency_key=f"p24h-evidence-a-{uuid4().hex}",
        )
    )
    _, source_a_public_id = (
        pg_batch_acceptance["create_vault_document"](
            organization_id=org_a,
            filename="P24H_Source.xlsx",
            content_type=(
                "application/vnd.openxmlformats-officedocument."
                "spreadsheetml.sheet"
            ),
            document_type="REMITO_EXCEL",
            sha256="3" * 64,
            object_suffix="source-a",
        )
    )

    linked = service.link_evidence(
        organization_id=org_a,
        batch_import_id=batch_a_public_id,
        vault_document_id=source_a_public_id,
        evidence_type="SOURCE_WORKBOOK",
        actor=_actor(org_a),
    )

    session_a = RuntimeSession()
    try:
        set_tenant_db_context(
            session_a,
            org_a,
        )
        visible = session_a.execute(
            select(BatchEvidenceLink).where(
                BatchEvidenceLink.id
                == linked.evidence.link_internal_id
            )
        ).scalar_one_or_none()
        assert visible is not None
        assert visible.batch_import_id == batch_a_id
    finally:
        session_a.rollback()
        session_a.close()

    session_b = RuntimeSession()
    try:
        set_tenant_db_context(
            session_b,
            org_b,
        )
        hidden = session_b.execute(
            select(BatchEvidenceLink).where(
                BatchEvidenceLink.id
                == linked.evidence.link_internal_id
            )
        ).scalar_one_or_none()
        assert hidden is None
    finally:
        session_b.rollback()
        session_b.close()


def test_p24h_composite_fk_rejects_vault_side_cross_tenant_pair(
    pg_batch_acceptance,
):
    org_a = pg_batch_acceptance["org_a_id"]
    org_b = pg_batch_acceptance["org_b_id"]

    batch_a_id, _ = pg_batch_acceptance[
        "create_batch_import"
    ](
        organization_id=org_a,
        source_sha256="4" * 64,
        idempotency_key=f"p24h-fk-a-{uuid4().hex}",
    )
    vault_b_id, _ = pg_batch_acceptance[
        "create_vault_document"
    ](
        organization_id=org_b,
        filename="P24H_Other_Tenant.pdf",
        content_type="application/pdf",
        document_type="PDF_CERTIFICADO",
        sha256="5" * 64,
        object_suffix="support-b",
    )

    with pytest.raises(
        IntegrityError
    ):
        with pg_batch_acceptance[
            "owner_engine"
        ].begin() as connection:
            connection.execute(
                text(
                    """
                    INSERT INTO public.batch_evidence_links (
                        organization_id,
                        batch_import_id,
                        vault_document_id,
                        evidence_type
                    )
                    VALUES (
                        :organization_id,
                        :batch_import_id,
                        :vault_document_id,
                        'SUPPORTING_EVIDENCE'
                    )
                    """
                ),
                {
                    "organization_id": org_a,
                    "batch_import_id": batch_a_id,
                    "vault_document_id": vault_b_id,
                },
            )


def test_p24h_source_workbook_rejects_wrong_document_type(
    pg_batch_acceptance,
):
    org_id = pg_batch_acceptance["org_a_id"]
    service = pg_batch_acceptance["evidence_service"]
    RuntimeSession = pg_batch_acceptance[
        "RuntimeSession"
    ]

    _, batch_public_id = pg_batch_acceptance[
        "create_batch_import"
    ](
        organization_id=org_id,
        source_sha256="6" * 64,
        idempotency_key=f"p24h-source-type-{uuid4().hex}",
    )
    _, wrong_doc_public_id = pg_batch_acceptance[
        "create_vault_document"
    ](
        organization_id=org_id,
        filename="P24H_Wrong_Type.pdf",
        content_type="application/pdf",
        document_type="PDF_CERTIFICADO",
        sha256="6" * 64,
        object_suffix="wrong-type",
    )

    with pytest.raises(
        BatchEvidenceConflictError
    ) as exc_info:
        service.link_evidence(
            organization_id=org_id,
            batch_import_id=batch_public_id,
            vault_document_id=wrong_doc_public_id,
            evidence_type="SOURCE_WORKBOOK",
            actor=_actor(org_id),
        )

    assert exc_info.value.code == (
        "SOURCE_WORKBOOK_REQUIRES_REMITO_EXCEL"
    )

    session = RuntimeSession()
    try:
        set_tenant_db_context(
            session,
            org_id,
        )
        active_links = int(
            session.execute(
                select(
                    func.count(BatchEvidenceLink.id)
                ).where(
                    BatchEvidenceLink.organization_id == org_id,
                    BatchEvidenceLink.unlinked_at.is_(None),
                )
            ).scalar_one()
        )
    finally:
        session.rollback()
        session.close()

    assert active_links == 0


def test_p24h_status_snapshot_returns_counts_and_identifiers(
    pg_batch_acceptance,
):
    org_id = pg_batch_acceptance["org_a_id"]
    import_service = pg_batch_acceptance[
        "import_service"
    ]
    query_service = pg_batch_acceptance[
        "query_service"
    ]
    workbook = _workbook(
        _row(
            identificador="P24H-STATUS-001",
            proveedor="30-11111111-1",
        ),
        _row(
            identificador="P24H-STATUS-002",
            proveedor="30-22222222-2",
        ),
        filename="P24H_Status.xlsx",
        sha256="7" * 64,
    )
    idem_key = f"p24h-status-{uuid4().hex}"

    result = import_service.import_workbook(
        workbook,
        organization_id=org_id,
        actor=_actor(org_id),
        idempotency_key=idem_key,
    )

    snapshot = query_service.get_by_public_id(
        organization_id=org_id,
        public_id=result.import_public_id,
    )

    assert snapshot is not None
    assert snapshot.public_id == result.import_public_id
    assert snapshot.status == "completed"
    assert snapshot.total_rows == 2
    assert snapshot.inserted_rows == 2
    assert snapshot.identifiers == (
        "P24H-STATUS-001",
        "P24H-STATUS-002",
    )
    assert snapshot.source_filename == "P24H_Status.xlsx"
    assert snapshot.source_sha256 == "7" * 64
    assert snapshot.completed_at is not None
