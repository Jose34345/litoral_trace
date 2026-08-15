from __future__ import annotations

import asyncio
import json
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from sqlalchemy import (
    create_engine,
    func,
    select,
    text,
)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker
from starlette.requests import Request

import litoral_trace.api.batch_evidence as evidence_api
from litoral_trace.api.auth import UserTenantContext
from litoral_trace.config.settings import normalize_database_url
from litoral_trace.db.models import (
    AuditLog,
    BatchEvidenceLink,
)
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.services.audit import (
    AuditAction,
    build_audit_actor,
)
from litoral_trace.services.batch_evidence import (
    BatchEvidenceConflictError,
    BatchEvidenceNotFoundError,
    BatchEvidenceService,
)


ROOT_DIR = Path(__file__).resolve().parents[1]
INTEGRATION_ENV_PATH = (
    ROOT_DIR / ".env.integration"
)
EXPECTED_REVISION = (
    "018_add_batch_evidence_links"
)


def _truthy(
    value: str | None,
) -> bool:
    return (
        value
        or ""
    ).strip().lower() in {
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

        name, value = line.split(
            "=",
            1,
        )
        values[
            name.strip()
        ] = value.strip()

    return values


INTEGRATION_ENV = _read_env_file(
    INTEGRATION_ENV_PATH
)
POSTGRES_TESTS_ENABLED = _truthy(
    INTEGRATION_ENV.get(
        "ENABLE_POSTGRES_TESTS"
    )
)
RUNTIME_DATABASE_URL = (
    INTEGRATION_ENV.get(
        "TEST_POSTGRES_DATABASE_URL"
    )
)
OWNER_DATABASE_URL = (
    INTEGRATION_ENV.get(
        "TEST_POSTGRES_MIGRATION_DATABASE_URL"
    )
)

pytestmark = pytest.mark.skipif(
    not (
        POSTGRES_TESTS_ENABLED
        and RUNTIME_DATABASE_URL
        and OWNER_DATABASE_URL
    ),
    reason=(
        "P2.4F PostgreSQL tests require "
        "ENABLE_POSTGRES_TESTS=1 plus isolated "
        "runtime and migration-owner integration URLs."
    ),
)


def _engine(
    url: str,
    *,
    pool_size: int,
):
    return create_engine(
        normalize_database_url(
            url
        ),
        pool_size=pool_size,
        max_overflow=0,
        pool_pre_ping=True,
        hide_parameters=True,
    )


def _user(
    organization_id: int,
) -> UserTenantContext:
    return UserTenantContext(
        user_id=None,
        username=(
            f"p24f-{organization_id}"
        ),
        organization_id=organization_id,
        organization_name=(
            f"P24F Org {organization_id}"
        ),
        organization_slug=(
            f"p24f-{organization_id}"
        ),
        role="admin",
        email=(
            f"p24f-{organization_id}"
            "@example.com"
        ),
    )


def _request(
    *,
    method: str,
    path: str,
) -> Request:
    return Request(
        {
            "type": "http",
            "http_version": "1.1",
            "method": method,
            "scheme": "https",
            "path": path,
            "raw_path": path.encode(),
            "query_string": b"",
            "headers": [
                (
                    b"x-request-id",
                    b"p24f-pg-request",
                ),
                (
                    b"user-agent",
                    b"p24f-pg/1.0",
                ),
            ],
            "client": (
                "203.0.113.60",
                50000,
            ),
            "server": (
                "test",
                443,
            ),
            "root_path": "",
        }
    )


def _body(
    response,
):
    return json.loads(
        response.body.decode()
    )


@pytest.fixture()
def pg_evidence(
    monkeypatch,
):
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
    source_sha = "a" * 64

    with owner_engine.begin() as connection:
        revision = connection.execute(
            text(
                "SELECT version_num "
                "FROM alembic_version"
            )
        ).scalar_one()

        if revision != EXPECTED_REVISION:
            raise RuntimeError(
                "P2.4F requires integration database at "
                f"{EXPECTED_REVISION}; found {revision!r}."
            )

        org_a_id = int(
            connection.execute(
                text(
                    """
                    INSERT INTO public.organizations (
                        name, slug, tax_id, tier, description, is_active
                    )
                    VALUES (
                        :name, :slug, :tax_id, 'pro',
                        'P2.4F evidence integration A', true
                    )
                    RETURNING id
                    """
                ),
                {
                    "name": f"P24F Org A {suffix}",
                    "slug": f"p24f-org-a-{suffix}",
                    "tax_id": f"P24F-A-{suffix}",
                },
            ).scalar_one()
        )
        org_b_id = int(
            connection.execute(
                text(
                    """
                    INSERT INTO public.organizations (
                        name, slug, tax_id, tier, description, is_active
                    )
                    VALUES (
                        :name, :slug, :tax_id, 'pro',
                        'P2.4F evidence integration B', true
                    )
                    RETURNING id
                    """
                ),
                {
                    "name": f"P24F Org B {suffix}",
                    "slug": f"p24f-org-b-{suffix}",
                    "tax_id": f"P24F-B-{suffix}",
                },
            ).scalar_one()
        )

        def create_import(
            org_id: int,
            sha: str,
            key: str,
        ):
            return connection.execute(
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
                        :org_id,
                        NULL,
                        :key,
                        :sha,
                        'P24F_Source.xlsx',
                        'completed',
                        1,
                        1,
                        '[1]'::json,
                        '["P24F-1"]'::json,
                        CURRENT_TIMESTAMP
                    )
                    RETURNING id, public_id
                    """
                ),
                {
                    "org_id": org_id,
                    "key": key,
                    "sha": sha,
                },
            ).one()

        import_a = create_import(
            org_a_id,
            source_sha,
            f"p24f-import-a-{suffix}",
        )
        import_b = create_import(
            org_b_id,
            "b" * 64,
            f"p24f-import-b-{suffix}",
        )

        def create_document(
            *,
            org_id: int,
            filename: str,
            content_type: str,
            document_type: str,
            sha: str,
            object_suffix: str,
        ):
            return connection.execute(
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
                        status
                    )
                    VALUES (
                        :org_id,
                        NULL,
                        :filename,
                        :content_type,
                        128,
                        :sha,
                        :object_key,
                        's3',
                        'p24f-integration-bucket',
                        :document_type,
                        'available'
                    )
                    RETURNING id, public_id
                    """
                ),
                {
                    "org_id": org_id,
                    "filename": filename,
                    "content_type": content_type,
                    "sha": sha,
                    "object_key": (
                        "p24f/integration/"
                        f"{suffix}/{object_suffix}"
                    ),
                    "document_type": document_type,
                },
            ).one()

        source_good = create_document(
            org_id=org_a_id,
            filename="P24F_Source.xlsx",
            content_type=(
                "application/vnd.openxmlformats-officedocument."
                "spreadsheetml.sheet"
            ),
            document_type="REMITO_EXCEL",
            sha=source_sha,
            object_suffix="source-good",
        )
        source_duplicate = create_document(
            org_id=org_a_id,
            filename="P24F_Source_Copy.xlsx",
            content_type=(
                "application/vnd.openxmlformats-officedocument."
                "spreadsheetml.sheet"
            ),
            document_type="REMITO_EXCEL",
            sha=source_sha,
            object_suffix="source-duplicate",
        )
        source_mismatch = create_document(
            org_id=org_a_id,
            filename="P24F_Wrong.xlsx",
            content_type=(
                "application/vnd.openxmlformats-officedocument."
                "spreadsheetml.sheet"
            ),
            document_type="REMITO_EXCEL",
            sha="d" * 64,
            object_suffix="source-mismatch",
        )
        support_a = create_document(
            org_id=org_a_id,
            filename="P24F_Support.pdf",
            content_type="application/pdf",
            document_type="PDF_CERTIFICADO",
            sha="c" * 64,
            object_suffix="support-a",
        )
        support_b = create_document(
            org_id=org_b_id,
            filename="P24F_Other_Tenant.pdf",
            content_type="application/pdf",
            document_type="PDF_CERTIFICADO",
            sha="e" * 64,
            object_suffix="support-b",
        )

    service = BatchEvidenceService(
        session_factory=RuntimeSession
    )
    monkeypatch.setattr(
        evidence_api,
        "_new_batch_evidence_service",
        lambda: service,
    )

    fixture = {
        "owner_engine": owner_engine,
        "runtime_engine": runtime_engine,
        "RuntimeSession": RuntimeSession,
        "service": service,
        "org_a_id": org_a_id,
        "org_b_id": org_b_id,
        "import_a_id": int(
            import_a.id
        ),
        "import_a_public_id": UUID(
            str(
                import_a.public_id
            )
        ),
        "import_b_id": int(
            import_b.id
        ),
        "import_b_public_id": UUID(
            str(
                import_b.public_id
            )
        ),
        "source_good_id": int(
            source_good.id
        ),
        "source_good_public_id": UUID(
            str(
                source_good.public_id
            )
        ),
        "source_duplicate_id": int(
            source_duplicate.id
        ),
        "source_duplicate_public_id": UUID(
            str(
                source_duplicate.public_id
            )
        ),
        "source_mismatch_public_id": UUID(
            str(
                source_mismatch.public_id
            )
        ),
        "support_a_id": int(
            support_a.id
        ),
        "support_a_public_id": UUID(
            str(
                support_a.public_id
            )
        ),
        "support_b_id": int(
            support_b.id
        ),
        "support_b_public_id": UUID(
            str(
                support_b.public_id
            )
        ),
        "source_sha": source_sha,
    }

    yield fixture

    with owner_engine.begin() as connection:
        connection.execute(
            text(
                """
                DELETE FROM public.audit_logs
                WHERE organization_id IN (:org_a, :org_b)
                """
            ),
            {
                "org_a": org_a_id,
                "org_b": org_b_id,
            },
        )
        connection.execute(
            text(
                """
                DELETE FROM public.batch_evidence_links
                WHERE organization_id IN (:org_a, :org_b)
                """
            ),
            {
                "org_a": org_a_id,
                "org_b": org_b_id,
            },
        )
        connection.execute(
            text(
                """
                DELETE FROM public.batch_imports
                WHERE organization_id IN (:org_a, :org_b)
                """
            ),
            {
                "org_a": org_a_id,
                "org_b": org_b_id,
            },
        )
        connection.execute(
            text(
                """
                DELETE FROM public.vault_documents
                WHERE organization_id IN (:org_a, :org_b)
                """
            ),
            {
                "org_a": org_a_id,
                "org_b": org_b_id,
            },
        )
        connection.execute(
            text(
                """
                DELETE FROM public.organizations
                WHERE id IN (:org_a, :org_b)
                """
            ),
            {
                "org_a": org_a_id,
                "org_b": org_b_id,
            },
        )

    runtime_engine.dispose()
    owner_engine.dispose()


def _actor(
    org_id: int,
):
    return build_audit_actor(
        organization_id=org_id,
        user_id=None,
        username="p24f",
        role="admin",
    )


def test_p24f_schema_rls_grants_and_revision(
    pg_evidence,
):
    owner_engine = pg_evidence[
        "owner_engine"
    ]

    with owner_engine.connect() as connection:
        revision = connection.execute(
            text(
                "SELECT version_num "
                "FROM alembic_version"
            )
        ).scalar_one()
        assert (
            revision
            == EXPECTED_REVISION
        )

        rls = connection.execute(
            text(
                """
                SELECT relrowsecurity, relforcerowsecurity
                FROM pg_class
                WHERE oid = 'public.batch_evidence_links'::regclass
                """
            )
        ).one()
        assert rls.relrowsecurity is True
        assert rls.relforcerowsecurity is True

        runtime_privs = connection.execute(
            text(
                """
                SELECT
                    has_table_privilege(
                        'litoral_trace_app',
                        'public.batch_evidence_links',
                        'SELECT'
                    ) AS can_select,
                    has_table_privilege(
                        'litoral_trace_app',
                        'public.batch_evidence_links',
                        'INSERT'
                    ) AS can_insert,
                    has_table_privilege(
                        'litoral_trace_app',
                        'public.batch_evidence_links',
                        'UPDATE'
                    ) AS can_update,
                    has_table_privilege(
                        'litoral_trace_app',
                        'public.batch_evidence_links',
                        'DELETE'
                    ) AS can_delete
                """
            )
        ).one()

        assert runtime_privs.can_select is True
        assert runtime_privs.can_insert is True
        assert runtime_privs.can_update is True
        assert runtime_privs.can_delete is False

        worker_any = connection.execute(
            text(
                """
                SELECT (
                    has_table_privilege(
                        'litoral_trace_worker_executor',
                        'public.batch_evidence_links',
                        'SELECT'
                    )
                    OR has_table_privilege(
                        'litoral_trace_worker_executor',
                        'public.batch_evidence_links',
                        'INSERT'
                    )
                    OR has_table_privilege(
                        'litoral_trace_worker_executor',
                        'public.batch_evidence_links',
                        'UPDATE'
                    )
                    OR has_table_privilege(
                        'litoral_trace_worker_executor',
                        'public.batch_evidence_links',
                        'DELETE'
                    )
                )
                """
            )
        ).scalar_one()
        assert worker_any is False


def test_p24f_source_workbook_links_replays_and_audits(
    pg_evidence,
):
    service = pg_evidence[
        "service"
    ]
    org_id = pg_evidence[
        "org_a_id"
    ]

    first = service.link_evidence(
        organization_id=org_id,
        batch_import_id=(
            pg_evidence[
                "import_a_public_id"
            ]
        ),
        vault_document_id=(
            pg_evidence[
                "source_good_public_id"
            ]
        ),
        evidence_type="SOURCE_WORKBOOK",
        actor=_actor(
            org_id
        ),
    )
    assert first.replayed is False
    assert (
        first.evidence.document_sha256
        == pg_evidence["source_sha"]
    )

    replay = service.link_evidence(
        organization_id=org_id,
        batch_import_id=(
            pg_evidence[
                "import_a_public_id"
            ]
        ),
        vault_document_id=(
            pg_evidence[
                "source_good_public_id"
            ]
        ),
        evidence_type="SOURCE_WORKBOOK",
        actor=_actor(
            org_id
        ),
    )
    assert replay.replayed is True
    assert (
        replay.evidence.link_public_id
        == first.evidence.link_public_id
    )

    session = pg_evidence[
        "RuntimeSession"
    ]()
    try:
        set_tenant_db_context(
            session,
            org_id,
        )
        links = session.scalar(
            select(
                func.count(
                    BatchEvidenceLink.id
                )
            ).where(
                BatchEvidenceLink.organization_id
                == org_id,
                BatchEvidenceLink.unlinked_at.is_(
                    None
                ),
            )
        )
        assert links == 1

        audits = session.scalars(
            select(
                AuditLog
            ).where(
                AuditLog.organization_id
                == org_id,
                AuditLog.action
                == (
                    AuditAction
                    .LOTE_BATCH_EVIDENCE_LINK
                    .value
                ),
            )
        ).all()
        assert len(audits) == 1
    finally:
        session.close()


def test_p24f_source_workbook_hash_and_single_source_are_enforced(
    pg_evidence,
):
    service = pg_evidence[
        "service"
    ]
    org_id = pg_evidence[
        "org_a_id"
    ]

    with pytest.raises(
        BatchEvidenceConflictError
    ) as mismatch:
        service.link_evidence(
            organization_id=org_id,
            batch_import_id=(
                pg_evidence[
                    "import_a_public_id"
                ]
            ),
            vault_document_id=(
                pg_evidence[
                    "source_mismatch_public_id"
                ]
            ),
            evidence_type=(
                "SOURCE_WORKBOOK"
            ),
            actor=_actor(
                org_id
            ),
        )

    assert (
        mismatch.value.code
        == "SOURCE_WORKBOOK_HASH_MISMATCH"
    )

    service.link_evidence(
        organization_id=org_id,
        batch_import_id=(
            pg_evidence[
                "import_a_public_id"
            ]
        ),
        vault_document_id=(
            pg_evidence[
                "source_good_public_id"
            ]
        ),
        evidence_type="SOURCE_WORKBOOK",
        actor=_actor(
            org_id
        ),
    )

    with pytest.raises(
        BatchEvidenceConflictError
    ) as duplicate:
        service.link_evidence(
            organization_id=org_id,
            batch_import_id=(
                pg_evidence[
                    "import_a_public_id"
                ]
            ),
            vault_document_id=(
                pg_evidence[
                    "source_duplicate_public_id"
                ]
            ),
            evidence_type=(
                "SOURCE_WORKBOOK"
            ),
            actor=_actor(
                org_id
            ),
        )

    assert (
        duplicate.value.code
        == "SOURCE_WORKBOOK_ALREADY_LINKED"
    )


def test_p24f_supporting_evidence_unlink_keeps_history_and_allows_relink(
    pg_evidence,
):
    service = pg_evidence[
        "service"
    ]
    org_id = pg_evidence[
        "org_a_id"
    ]
    batch_id = pg_evidence[
        "import_a_public_id"
    ]
    document_id = pg_evidence[
        "support_a_public_id"
    ]

    first = service.link_evidence(
        organization_id=org_id,
        batch_import_id=batch_id,
        vault_document_id=document_id,
        evidence_type=(
            "SUPPORTING_EVIDENCE"
        ),
        actor=_actor(
            org_id
        ),
    )

    listed = service.list_evidence(
        organization_id=org_id,
        batch_import_id=batch_id,
    )
    assert len(listed) == 1

    service.unlink_evidence(
        organization_id=org_id,
        batch_import_id=batch_id,
        vault_document_id=document_id,
        actor=_actor(
            org_id
        ),
    )

    assert (
        service.list_evidence(
            organization_id=org_id,
            batch_import_id=batch_id,
        )
        == ()
    )

    second = service.link_evidence(
        organization_id=org_id,
        batch_import_id=batch_id,
        vault_document_id=document_id,
        evidence_type=(
            "SUPPORTING_EVIDENCE"
        ),
        actor=_actor(
            org_id
        ),
    )
    assert second.replayed is False
    assert (
        second.evidence.link_public_id
        != first.evidence.link_public_id
    )

    session = pg_evidence[
        "RuntimeSession"
    ]()
    try:
        set_tenant_db_context(
            session,
            org_id,
        )
        all_rows = session.scalars(
            select(
                BatchEvidenceLink
            ).where(
                BatchEvidenceLink.organization_id
                == org_id,
                BatchEvidenceLink.vault_document_id
                == pg_evidence[
                    "support_a_id"
                ],
            ).order_by(
                BatchEvidenceLink.id
            )
        ).all()

        assert len(all_rows) == 2
        assert (
            all_rows[0].unlinked_at
            is not None
        )
        assert (
            all_rows[1].unlinked_at
            is None
        )

        unlink_audits = session.scalar(
            select(
                func.count(
                    AuditLog.id
                )
            ).where(
                AuditLog.organization_id
                == org_id,
                AuditLog.action
                == (
                    AuditAction
                    .LOTE_BATCH_EVIDENCE_UNLINK
                    .value
                ),
            )
        )
        assert unlink_audits == 1
    finally:
        session.close()


def test_p24f_cross_tenant_document_is_not_visible(
    pg_evidence,
):
    service = pg_evidence[
        "service"
    ]
    org_id = pg_evidence[
        "org_a_id"
    ]

    with pytest.raises(
        BatchEvidenceNotFoundError
    ) as exc_info:
        service.link_evidence(
            organization_id=org_id,
            batch_import_id=(
                pg_evidence[
                    "import_a_public_id"
                ]
            ),
            vault_document_id=(
                pg_evidence[
                    "support_b_public_id"
                ]
            ),
            evidence_type=(
                "SUPPORTING_EVIDENCE"
            ),
            actor=_actor(
                org_id
            ),
        )

    assert (
        exc_info.value.code
        == "VAULT_DOCUMENT_NOT_FOUND"
    )


def test_p24f_deleted_linked_document_is_explicit_tombstone(
    pg_evidence,
):
    service = pg_evidence[
        "service"
    ]
    org_id = pg_evidence[
        "org_a_id"
    ]
    batch_id = pg_evidence[
        "import_a_public_id"
    ]
    document_id = pg_evidence[
        "support_a_public_id"
    ]

    service.link_evidence(
        organization_id=org_id,
        batch_import_id=batch_id,
        vault_document_id=document_id,
        evidence_type=(
            "SUPPORTING_EVIDENCE"
        ),
        actor=_actor(
            org_id
        ),
    )

    with pg_evidence[
        "owner_engine"
    ].begin() as connection:
        connection.execute(
            text(
                """
                UPDATE public.vault_documents
                SET status = 'deleted',
                    deleted_at = CURRENT_TIMESTAMP
                WHERE id = :document_id
                """
            ),
            {
                "document_id": (
                    pg_evidence[
                        "support_a_id"
                    ]
                ),
            },
        )

    listed = service.list_evidence(
        organization_id=org_id,
        batch_import_id=batch_id,
    )
    assert len(listed) == 1
    assert (
        listed[0].document_status
        == "deleted"
    )
    assert (
        listed[0].document_available
        is False
    )

    service.unlink_evidence(
        organization_id=org_id,
        batch_import_id=batch_id,
        vault_document_id=document_id,
        actor=_actor(
            org_id
        ),
    )

    with pytest.raises(
        BatchEvidenceNotFoundError
    ):
        service.link_evidence(
            organization_id=org_id,
            batch_import_id=batch_id,
            vault_document_id=document_id,
            evidence_type=(
                "SUPPORTING_EVIDENCE"
            ),
            actor=_actor(
                org_id
            ),
        )


def test_p24f_composite_fk_rejects_cross_tenant_parent_pair(
    pg_evidence,
):
    owner_engine = pg_evidence[
        "owner_engine"
    ]

    with pytest.raises(
        IntegrityError
    ):
        with owner_engine.begin() as connection:
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
                        :org_a,
                        :batch_b,
                        :vault_a,
                        'SUPPORTING_EVIDENCE'
                    )
                    """
                ),
                {
                    "org_a": pg_evidence[
                        "org_a_id"
                    ],
                    "batch_b": pg_evidence[
                        "import_b_id"
                    ],
                    "vault_a": pg_evidence[
                        "support_a_id"
                    ],
                },
            )


def test_p24f_api_real_db_link_list_and_unlink(
    pg_evidence,
):
    org_id = pg_evidence[
        "org_a_id"
    ]
    user = _user(
        org_id
    )
    batch_id = pg_evidence[
        "import_a_public_id"
    ]
    document_id = pg_evidence[
        "support_a_public_id"
    ]

    link_response = asyncio.run(
        evidence_api.vincular_evidencia_batch_endpoint(
            import_id=batch_id,
            payload=(
                evidence_api.BatchEvidenceLinkRequest(
                    document_id=document_id,
                    evidence_type=(
                        "COMPLIANCE_EVIDENCE"
                    ),
                )
            ),
            request=_request(
                method="POST",
                path=(
                    "/api/v1/batch/imports/"
                    f"{batch_id}/evidence"
                ),
            ),
            user=user,
        )
    )
    assert (
        link_response.status_code
        == 201
    )

    list_response = asyncio.run(
        evidence_api.listar_evidencia_batch_endpoint(
            import_id=batch_id,
            user=user,
        )
    )
    payload = _body(
        list_response
    )
    assert (
        payload["evidence_count"]
        == 1
    )
    assert (
        payload["evidence"][0][
            "document"
        ]["id"]
        == str(
            document_id
        )
    )

    delete_response = asyncio.run(
        evidence_api.desvincular_evidencia_batch_endpoint(
            import_id=batch_id,
            document_id=document_id,
            request=_request(
                method="DELETE",
                path=(
                    "/api/v1/batch/imports/"
                    f"{batch_id}/evidence/"
                    f"{document_id}"
                ),
            ),
            user=user,
        )
    )
    assert (
        delete_response.status_code
        == 204
    )
