from __future__ import annotations

import io
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import sessionmaker

from litoral_trace.config.settings import StorageSettings, normalize_database_url
from litoral_trace.db.models import VaultDocument
from litoral_trace.services.vault import (
    PDF_CONTENT_TYPE,
    VaultNotFoundError,
    VaultService,
    VaultStorageOperationError,
)
from litoral_trace.storage import (
    ObjectDeleteResult,
    ObjectHead,
    ObjectStorageError,
    ObjectStorageNotFoundError,
    ObjectStorageStream,
    ObjectWriteResult,
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


def _read_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
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
        "P2.3C PostgreSQL tests require ENABLE_POSTGRES_TESTS=1 "
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


def _pdf_bytes() -> bytes:
    return b"%PDF-1.4\n1 0 obj\n<<>>\nendobj\n%%EOF\n"


class MemoryObjectStorage:
    def __init__(self):
        self.objects: dict[str, dict[str, object]] = {}
        self.put_count = 0
        self.delete_count = 0
        self.fail_put = False

    def put_object(
        self,
        *,
        key,
        body,
        content_type,
        content_length,
        metadata=None,
    ):
        if self.fail_put:
            raise ObjectStorageError("put_object")
        payload = body if isinstance(body, bytes) else body.read()
        self.put_count += 1
        self.objects[key] = {
            "payload": payload,
            "content_type": content_type,
            "metadata": dict(metadata or {}),
            "etag": f"etag-{self.put_count}",
            "version_id": f"version-{self.put_count}",
        }
        return ObjectWriteResult(
            etag=f"etag-{self.put_count}",
            version_id=f"version-{self.put_count}",
        )

    def head_object(self, *, key, version_id=None):
        stored = self.objects.get(key)
        if stored is None:
            raise ObjectStorageNotFoundError("head_object")
        return ObjectHead(
            size_bytes=len(stored["payload"]),
            content_type=stored["content_type"],
            etag=stored["etag"],
            version_id=stored["version_id"],
            metadata=stored["metadata"],
        )

    def get_object_stream(self, *, key, version_id=None):
        stored = self.objects.get(key)
        if stored is None:
            raise ObjectStorageNotFoundError("get_object")
        return ObjectStorageStream(
            body=io.BytesIO(stored["payload"]),
            head=self.head_object(key=key, version_id=version_id),
        )

    def delete_object(self, *, key, version_id=None):
        self.delete_count += 1
        self.objects.pop(key, None)
        return ObjectDeleteResult(
            delete_marker=False,
            version_id=version_id,
        )

    def object_exists(self, *, key, version_id=None):
        return key in self.objects

    def health_check(self):
        return True


@pytest.fixture()
def pg_vault_runtime():
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
        expire_on_commit=True,
    )

    suffix = uuid4().hex[:10]

    with owner_engine.begin() as connection:
        revision = connection.execute(
            text("SELECT version_num FROM alembic_version")
        ).scalar_one()
        if revision != EXPECTED_REVISION:
            raise RuntimeError(
                "P2.3C requires integration database at "
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
                        'P2.3C Vault service integration', true
                    )
                    RETURNING id
                    """
                ),
                {
                    "name": f"P23C Org A {suffix}",
                    "slug": f"p23c-org-a-{suffix}",
                    "tax_id": f"P23C-A-{suffix}",
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
                        'P2.3C Vault service integration', true
                    )
                    RETURNING id
                    """
                ),
                {
                    "name": f"P23C Org B {suffix}",
                    "slug": f"p23c-org-b-{suffix}",
                    "tax_id": f"P23C-B-{suffix}",
                },
            ).scalar_one()
        )

    storage = MemoryObjectStorage()
    settings = StorageSettings(
        backend="s3",
        bucket_name="p23c-private-vault",
        key_prefix="vault",
    )
    service = VaultService(
        storage_settings=settings,
        storage=storage,
        session_factory=RuntimeSession,
    )

    try:
        yield {
            "owner_engine": owner_engine,
            "runtime_engine": runtime_engine,
            "RuntimeSession": RuntimeSession,
            "storage": storage,
            "service": service,
            "org_a_id": org_a_id,
            "org_b_id": org_b_id,
        }
    finally:
        with owner_engine.begin() as connection:
            connection.execute(
                text(
                    """
                    DELETE FROM public.vault_documents
                    WHERE organization_id IN (:org_a_id, :org_b_id)
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
                    WHERE id IN (:org_a_id, :org_b_id)
                    """
                ),
                {
                    "org_a_id": org_a_id,
                    "org_b_id": org_b_id,
                },
            )

        runtime_engine.dispose()
        owner_engine.dispose()


def test_p23c_real_runtime_upload_reaches_available_with_private_metadata(
    pg_vault_runtime,
):
    service = pg_vault_runtime["service"]

    created = service.upload_document(
        organization_id=pg_vault_runtime["org_a_id"],
        created_by_user_id=None,
        filename="certificate.pdf",
        document_type="PDF_CERTIFICADO",
        content_type=PDF_CONTENT_TYPE,
        content=_pdf_bytes(),
        idempotency_key="p23c-real-upload",
    )

    assert created.status == "available"
    assert pg_vault_runtime["storage"].put_count == 1

    with pg_vault_runtime["owner_engine"].connect() as connection:
        row = connection.execute(
            text(
                """
                SELECT
                    organization_id,
                    status,
                    object_key,
                    storage_bucket,
                    storage_etag,
                    storage_version_id,
                    sha256
                FROM public.vault_documents
                WHERE public_id = :public_id
                """
            ),
            {"public_id": created.public_id},
        ).mappings().one()

    assert row["organization_id"] == pg_vault_runtime["org_a_id"]
    assert row["status"] == "available"
    assert row["storage_bucket"] == "p23c-private-vault"
    assert row["storage_etag"] == "etag-1"
    assert row["storage_version_id"] == "version-1"
    assert "certificate" not in row["object_key"]


def test_p23c_runtime_service_and_rls_hide_cross_tenant_document(
    pg_vault_runtime,
):
    service = pg_vault_runtime["service"]

    created = service.upload_document(
        organization_id=pg_vault_runtime["org_a_id"],
        created_by_user_id=None,
        filename="tenant-a.pdf",
        document_type="PDF_CERTIFICADO",
        content_type=PDF_CONTENT_TYPE,
        content=_pdf_bytes(),
    )

    assert len(
        service.list_documents(
            organization_id=pg_vault_runtime["org_a_id"],
        )
    ) == 1
    assert service.list_documents(
        organization_id=pg_vault_runtime["org_b_id"],
    ) == []

    with pytest.raises(VaultNotFoundError):
        service.get_document(
            organization_id=pg_vault_runtime["org_b_id"],
            document_id=created.public_id,
        )


def test_p23c_real_postgres_idempotency_reuses_one_row_and_one_object(
    pg_vault_runtime,
):
    service = pg_vault_runtime["service"]
    kwargs = {
        "organization_id": pg_vault_runtime["org_a_id"],
        "created_by_user_id": None,
        "filename": "certificate.pdf",
        "document_type": "PDF_CERTIFICADO",
        "content_type": PDF_CONTENT_TYPE,
        "content": _pdf_bytes(),
        "idempotency_key": "p23c-idempotent",
    }

    first = service.upload_document(**kwargs)
    second = service.upload_document(**kwargs)

    assert first.public_id == second.public_id
    assert pg_vault_runtime["storage"].put_count == 1

    with pg_vault_runtime["owner_engine"].connect() as connection:
        count = connection.execute(
            text(
                """
                SELECT count(*)
                FROM public.vault_documents
                WHERE organization_id = :organization_id
                  AND idempotency_key = 'p23c-idempotent'
                """
            ),
            {
                "organization_id": pg_vault_runtime["org_a_id"],
            },
        ).scalar_one()

    assert count == 1


def test_p23c_storage_failure_is_durable_and_recoverable_in_real_postgres(
    pg_vault_runtime,
):
    service = pg_vault_runtime["service"]
    storage = pg_vault_runtime["storage"]
    storage.fail_put = True

    kwargs = {
        "organization_id": pg_vault_runtime["org_a_id"],
        "created_by_user_id": None,
        "filename": "certificate.pdf",
        "document_type": "PDF_CERTIFICADO",
        "content_type": PDF_CONTENT_TYPE,
        "content": _pdf_bytes(),
        "idempotency_key": "p23c-storage-recovery",
    }

    with pytest.raises(VaultStorageOperationError):
        service.upload_document(**kwargs)

    with pg_vault_runtime["owner_engine"].connect() as connection:
        failed = connection.execute(
            text(
                """
                SELECT public_id, status, last_error_code
                FROM public.vault_documents
                WHERE organization_id = :organization_id
                  AND idempotency_key = 'p23c-storage-recovery'
                """
            ),
            {
                "organization_id": pg_vault_runtime["org_a_id"],
            },
        ).mappings().one()

    assert failed["status"] == "upload_failed"
    assert failed["last_error_code"] == "STORAGE_UPLOAD_FAILED"

    storage.fail_put = False
    recovered = service.upload_document(**kwargs)
    assert str(recovered.public_id) == str(failed["public_id"])
    assert recovered.status == "available"


def test_p23c_real_postgres_delete_is_lifecycle_update_not_hard_delete(
    pg_vault_runtime,
):
    service = pg_vault_runtime["service"]

    created = service.upload_document(
        organization_id=pg_vault_runtime["org_a_id"],
        created_by_user_id=None,
        filename="certificate.pdf",
        document_type="PDF_CERTIFICADO",
        content_type=PDF_CONTENT_TYPE,
        content=_pdf_bytes(),
    )

    deleted = service.delete_document(
        organization_id=pg_vault_runtime["org_a_id"],
        document_id=created.public_id,
    )

    assert deleted.status == "deleted"
    assert deleted.deleted_at is not None
    assert pg_vault_runtime["storage"].objects == {}

    with pg_vault_runtime["owner_engine"].connect() as connection:
        row = connection.execute(
            text(
                """
                SELECT status, deleted_at
                FROM public.vault_documents
                WHERE public_id = :public_id
                """
            ),
            {"public_id": created.public_id},
        ).mappings().one()

    assert row["status"] == "deleted"
    assert row["deleted_at"] is not None