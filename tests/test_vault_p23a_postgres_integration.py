from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError

from litoral_trace.config.settings import normalize_database_url


ROOT_DIR = Path(__file__).resolve().parents[1]
INTEGRATION_ENV_PATH = ROOT_DIR / ".env.integration"

EXPECTED_REVISION = "016_add_vault_documents"
EXPECTED_RUNTIME_ROLE = "litoral_trace_app"
WORKER_EXECUTOR_ROLE = "litoral_trace_worker_executor"


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

pytestmark = pytest.mark.skipif(
    not (
        POSTGRES_TESTS_ENABLED
        and RUNTIME_DATABASE_URL
        and OWNER_DATABASE_URL
    ),
    reason=(
        "P2.3A PostgreSQL tests require ENABLE_POSTGRES_TESTS=1 "
        "plus isolated runtime and migration-owner integration URLs."
    ),
)


def _engine(url: str, *, pool_size: int):
    return create_engine(
        normalize_database_url(url),
        pool_size=pool_size,
        max_overflow=0,
        pool_pre_ping=True,
        hide_parameters=True,
    )


def _set_tenant_context(connection, organization_id: int) -> None:
    connection.execute(
        text(
            "SELECT set_config("
            "'app.current_organization_id', "
            ":organization_id, "
            "true"
            ")"
        ),
        {
            "organization_id": str(organization_id),
        },
    )


def _insert_runtime_document(
    runtime_engine,
    *,
    organization_id: int,
    idempotency_key: str,
) -> dict[str, object]:
    with runtime_engine.begin() as connection:
        _set_tenant_context(
            connection,
            organization_id,
        )

        row = connection.execute(
            text(
                """
                INSERT INTO public.vault_documents (
                    organization_id,
                    original_filename,
                    content_type,
                    size_bytes,
                    sha256,
                    object_key,
                    storage_backend,
                    storage_bucket,
                    document_type,
                    status,
                    idempotency_key
                )
                VALUES (
                    :organization_id,
                    :original_filename,
                    'application/pdf',
                    128,
                    :sha256,
                    :object_key,
                    's3',
                    'p23a-private-bucket',
                    'PDF_CERTIFICADO',
                    'pending_upload',
                    :idempotency_key
                )
                RETURNING
                    id,
                    public_id,
                    organization_id,
                    status
                """
            ),
            {
                "organization_id": organization_id,
                "original_filename": "evidence.pdf",
                "sha256": "a" * 64,
                "object_key": (
                    f"p23a/{organization_id}/{uuid4().hex}"
                ),
                "idempotency_key": idempotency_key,
            },
        ).mappings().one()

    return dict(row)


@contextmanager
def _vault_fixture():
    owner_engine = _engine(
        OWNER_DATABASE_URL,
        pool_size=3,
    )
    runtime_engine = _engine(
        RUNTIME_DATABASE_URL,
        pool_size=4,
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
                "P2.3A requires integration database at "
                f"{EXPECTED_REVISION}; found {revision!r}."
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
                        'P2.3A Vault RLS acceptance',
                        true
                    )
                    RETURNING id
                    """
                ),
                {
                    "name": f"P23A Org A {suffix}",
                    "slug": f"p23a-org-a-{suffix}",
                    "tax_id": f"P23A-A-{suffix}",
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
                        'P2.3A Vault RLS acceptance',
                        true
                    )
                    RETURNING id
                    """
                ),
                {
                    "name": f"P23A Org B {suffix}",
                    "slug": f"p23a-org-b-{suffix}",
                    "tax_id": f"P23A-B-{suffix}",
                },
            ).scalar_one()
        )

    try:
        yield {
            "owner_engine": owner_engine,
            "runtime_engine": runtime_engine,
            "org_a_id": org_a_id,
            "org_b_id": org_b_id,
        }

    finally:
        with owner_engine.begin() as connection:
            connection.execute(
                text(
                    """
                    DELETE FROM public.vault_documents
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

        runtime_engine.dispose()
        owner_engine.dispose()


def test_p23a_schema_is_at_revision_016_and_runtime_is_not_table_owner():
    with _vault_fixture() as fixture:
        with fixture["owner_engine"].connect() as connection:
            row = connection.execute(
                text(
                    """
                    SELECT
                        c.relrowsecurity,
                        c.relforcerowsecurity,
                        pg_get_userbyid(c.relowner) AS owner_name
                    FROM pg_class AS c
                    JOIN pg_namespace AS n
                        ON n.oid = c.relnamespace
                    WHERE n.nspname = 'public'
                      AND c.relname = 'vault_documents'
                    """
                )
            ).mappings().one()

        with fixture["runtime_engine"].connect() as connection:
            runtime_user = connection.execute(
                text("SELECT current_user")
            ).scalar_one()

    assert row["relrowsecurity"] is True
    assert row["relforcerowsecurity"] is True
    assert row["owner_name"] != EXPECTED_RUNTIME_ROLE
    assert runtime_user == EXPECTED_RUNTIME_ROLE


def test_p23a_runtime_grants_are_select_insert_update_without_delete():
    with _vault_fixture() as fixture:
        with fixture["owner_engine"].connect() as connection:
            row = connection.execute(
                text(
                    """
                    SELECT
                        has_table_privilege(
                            :runtime_role,
                            'public.vault_documents',
                            'SELECT'
                        ) AS can_select,
                        has_table_privilege(
                            :runtime_role,
                            'public.vault_documents',
                            'INSERT'
                        ) AS can_insert,
                        has_table_privilege(
                            :runtime_role,
                            'public.vault_documents',
                            'UPDATE'
                        ) AS can_update,
                        has_table_privilege(
                            :runtime_role,
                            'public.vault_documents',
                            'DELETE'
                        ) AS can_delete,
                        has_sequence_privilege(
                            :runtime_role,
                            'public.vault_documents_id_seq',
                            'USAGE'
                        ) AS can_use_sequence,
                        has_sequence_privilege(
                            :runtime_role,
                            'public.vault_documents_id_seq',
                            'SELECT'
                        ) AS can_select_sequence
                    """
                ),
                {
                    "runtime_role": EXPECTED_RUNTIME_ROLE,
                },
            ).mappings().one()

    assert row["can_select"] is True
    assert row["can_insert"] is True
    assert row["can_update"] is True
    assert row["can_delete"] is False
    assert row["can_use_sequence"] is True
    assert row["can_select_sequence"] is True


def test_p23a_worker_capability_has_no_vault_table_or_sequence_privileges():
    with _vault_fixture() as fixture:
        with fixture["owner_engine"].connect() as connection:
            row = connection.execute(
                text(
                    """
                    SELECT
                        has_table_privilege(
                            :worker_role,
                            'public.vault_documents',
                            'SELECT'
                        ) AS can_select,
                        has_table_privilege(
                            :worker_role,
                            'public.vault_documents',
                            'INSERT'
                        ) AS can_insert,
                        has_table_privilege(
                            :worker_role,
                            'public.vault_documents',
                            'UPDATE'
                        ) AS can_update,
                        has_table_privilege(
                            :worker_role,
                            'public.vault_documents',
                            'DELETE'
                        ) AS can_delete,
                        has_sequence_privilege(
                            :worker_role,
                            'public.vault_documents_id_seq',
                            'USAGE'
                        ) AS can_use_sequence
                    """
                ),
                {
                    "worker_role": WORKER_EXECUTOR_ROLE,
                },
            ).mappings().one()

    assert row["can_select"] is False
    assert row["can_insert"] is False
    assert row["can_update"] is False
    assert row["can_delete"] is False
    assert row["can_use_sequence"] is False


def test_p23a_rls_policies_exist_without_runtime_delete_policy():
    with _vault_fixture() as fixture:
        with fixture["owner_engine"].connect() as connection:
            rows = connection.execute(
                text(
                    """
                    SELECT
                        policyname,
                        cmd
                    FROM pg_policies
                    WHERE schemaname = 'public'
                      AND tablename = 'vault_documents'
                    ORDER BY policyname
                    """
                )
            ).mappings().all()

    observed = {
        (
            str(row["policyname"]),
            str(row["cmd"]),
        )
        for row in rows
    }

    assert observed == {
        (
            "vault_documents_tenant_insert",
            "INSERT",
        ),
        (
            "vault_documents_tenant_select",
            "SELECT",
        ),
        (
            "vault_documents_tenant_update",
            "UPDATE",
        ),
    }


def test_p23a_runtime_insert_generates_opaque_uuid_and_rls_isolates_tenants():
    with _vault_fixture() as fixture:
        inserted = _insert_runtime_document(
            fixture["runtime_engine"],
            organization_id=fixture["org_a_id"],
            idempotency_key=f"p23a-{uuid4().hex}",
        )

        with fixture["runtime_engine"].begin() as connection:
            _set_tenant_context(
                connection,
                fixture["org_a_id"],
            )
            tenant_a_rows = connection.execute(
                text(
                    """
                    SELECT
                        id,
                        public_id,
                        organization_id,
                        status
                    FROM public.vault_documents
                    ORDER BY id
                    """
                )
            ).mappings().all()

        with fixture["runtime_engine"].begin() as connection:
            _set_tenant_context(
                connection,
                fixture["org_b_id"],
            )
            tenant_b_rows = connection.execute(
                text(
                    """
                    SELECT
                        id,
                        public_id,
                        organization_id,
                        status
                    FROM public.vault_documents
                    ORDER BY id
                    """
                )
            ).mappings().all()

    assert int(inserted["organization_id"]) == fixture["org_a_id"]
    assert inserted["status"] == "pending_upload"
    assert UUID(str(inserted["public_id"]))

    assert len(tenant_a_rows) == 1
    assert int(tenant_a_rows[0]["id"]) == int(inserted["id"])
    assert tenant_b_rows == []


def test_p23a_rls_rejects_cross_tenant_insert_and_cross_tenant_update():
    with _vault_fixture() as fixture:
        inserted = _insert_runtime_document(
            fixture["runtime_engine"],
            organization_id=fixture["org_a_id"],
            idempotency_key=f"p23a-{uuid4().hex}",
        )

        with fixture["runtime_engine"].begin() as connection:
            _set_tenant_context(
                connection,
                fixture["org_b_id"],
            )

            update_result = connection.execute(
                text(
                    """
                    UPDATE public.vault_documents
                    SET status = 'available'
                    WHERE public_id = :public_id
                    """
                ),
                {
                    "public_id": inserted["public_id"],
                },
            )

            assert update_result.rowcount == 0

        with pytest.raises(DBAPIError):
            with fixture["runtime_engine"].begin() as connection:
                _set_tenant_context(
                    connection,
                    fixture["org_a_id"],
                )

                connection.execute(
                    text(
                        """
                        INSERT INTO public.vault_documents (
                            organization_id,
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
                            :foreign_organization_id,
                            'cross-tenant.pdf',
                            'application/pdf',
                            128,
                            :sha256,
                            :object_key,
                            's3',
                            'p23a-private-bucket',
                            'PDF_CERTIFICADO',
                            'pending_upload'
                        )
                        """
                    ),
                    {
                        "foreign_organization_id": fixture["org_b_id"],
                        "sha256": "b" * 64,
                        "object_key": f"p23a/cross/{uuid4().hex}",
                    },
                )


def test_p23a_runtime_cannot_hard_delete_vault_documents():
    with _vault_fixture() as fixture:
        inserted = _insert_runtime_document(
            fixture["runtime_engine"],
            organization_id=fixture["org_a_id"],
            idempotency_key=f"p23a-{uuid4().hex}",
        )

        with pytest.raises(DBAPIError):
            with fixture["runtime_engine"].begin() as connection:
                _set_tenant_context(
                    connection,
                    fixture["org_a_id"],
                )
                connection.execute(
                    text(
                        """
                        DELETE FROM public.vault_documents
                        WHERE id = :document_id
                        """
                    ),
                    {
                        "document_id": inserted["id"],
                    },
                )