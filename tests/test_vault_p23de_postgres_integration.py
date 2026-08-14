from __future__ import annotations

import asyncio
import io
import json
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from starlette.datastructures import UploadFile

import litoral_trace.api.vault as vault_api
from litoral_trace.api.auth import UserTenantContext
from litoral_trace.config.settings import StorageSettings, normalize_database_url
from litoral_trace.services.vault import VaultService
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
        "P2.3D/E PostgreSQL tests require ENABLE_POSTGRES_TESTS=1 "
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

    def put_object(
        self,
        *,
        key,
        body,
        content_type,
        content_length,
        metadata=None,
    ):
        payload = body if isinstance(body, bytes) else body.read()
        if len(payload) != content_length:
            raise ObjectStorageError("put_object")
        self.put_count += 1
        version_id = f"v-{self.put_count}"
        etag = f"etag-{self.put_count}"
        self.objects[key] = {
            "payload": payload,
            "content_type": content_type,
            "metadata": dict(metadata or {}),
            "etag": etag,
            "version_id": version_id,
        }
        return ObjectWriteResult(etag=etag, version_id=version_id)

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
        self.objects.pop(key, None)
        return ObjectDeleteResult(
            delete_marker=False,
            version_id=version_id,
        )

    def object_exists(self, *, key, version_id=None):
        return key in self.objects

    def health_check(self):
        return True


def _context(org_id: int, role: str = "admin") -> UserTenantContext:
    return UserTenantContext(
        user_id=None,
        username=f"p23de-{org_id}",
        organization_id=org_id,
        organization_name=f"P23DE Org {org_id}",
        organization_slug=f"p23de-{org_id}",
        role=role,
        email=f"p23de-{org_id}@example.com",
    )


@pytest.fixture()
def pg_api_runtime(monkeypatch):
    owner_engine = _engine(OWNER_DATABASE_URL, pool_size=3)
    runtime_engine = _engine(RUNTIME_DATABASE_URL, pool_size=4)
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
                "P2.3D/E requires integration database at "
                f"{EXPECTED_REVISION}; found {revision!r}."
            )

        ids = []
        for label in ("A", "B"):
            ids.append(
                int(
                    connection.execute(
                        text(
                            """
                            INSERT INTO public.organizations (
                                name, slug, tax_id, tier, description, is_active
                            )
                            VALUES (
                                :name, :slug, :tax_id, 'pro',
                                'P2.3D/E Vault API integration', true
                            )
                            RETURNING id
                            """
                        ),
                        {
                            "name": f"P23DE Org {label} {suffix}",
                            "slug": f"p23de-org-{label.lower()}-{suffix}",
                            "tax_id": f"P23DE-{label}-{suffix}",
                        },
                    ).scalar_one()
                )
            )

    storage = MemoryObjectStorage()
    settings = StorageSettings(
        backend="s3",
        bucket_name="p23de-private-vault",
        key_prefix="vault",
    )
    service = VaultService(
        storage_settings=settings,
        storage=storage,
        session_factory=RuntimeSession,
    )

    monkeypatch.setattr(vault_api, "_new_vault_service", lambda: service)
    monkeypatch.setattr(
        vault_api,
        "record_audit_event_now",
        lambda **kwargs: True,
    )

    try:
        yield {
            "owner_engine": owner_engine,
            "runtime_engine": runtime_engine,
            "service": service,
            "storage": storage,
            "org_a_id": ids[0],
            "org_b_id": ids[1],
        }
    finally:
        with owner_engine.begin() as connection:
            connection.execute(
                text(
                    "DELETE FROM public.vault_documents "
                    "WHERE organization_id IN (:a, :b)"
                ),
                {"a": ids[0], "b": ids[1]},
            )
            connection.execute(
                text(
                    "DELETE FROM public.organizations "
                    "WHERE id IN (:a, :b)"
                ),
                {"a": ids[0], "b": ids[1]},
            )
        runtime_engine.dispose()
        owner_engine.dispose()


def _upload(org_id: int, *, idem: str | None = None):
    upload = UploadFile(
        filename="evidence.pdf",
        file=io.BytesIO(_pdf_bytes()),
        headers={"content-type": "application/pdf"},
    )
    return asyncio.run(
        vault_api.subir_documento_boveda(
            file=upload,
            document_type="PDF_CERTIFICADO",
            idempotency_key=idem,
            user=_context(org_id),
        )
    )


def test_p23de_real_runtime_api_upload_list_detail_and_soft_delete(pg_api_runtime):
    org_a_id = pg_api_runtime["org_a_id"]

    created = _upload(org_a_id)
    created_body = json.loads(created.body.decode())
    document_id = created_body["public_id"]

    listed = asyncio.run(
        vault_api.consultar_documentos_boveda(
            q=None,
            type=None,
            user=_context(org_a_id),
        )
    )
    listed_body = json.loads(listed.body.decode())
    assert listed_body["total_documents"] == 1
    assert listed_body["documents"][0]["public_id"] == document_id

    detail = asyncio.run(
        vault_api.obtener_documento_boveda(
            document_id=document_id,
            user=_context(org_a_id),
        )
    )
    assert detail.status_code == 200

    deleted = asyncio.run(
        vault_api.eliminar_documento_boveda(
            document_id=document_id,
            user=_context(org_a_id),
        )
    )
    assert deleted.status_code == 204

    listed_after = asyncio.run(
        vault_api.consultar_documentos_boveda(
            q=None,
            type=None,
            user=_context(org_a_id),
        )
    )
    assert json.loads(listed_after.body.decode())["total_documents"] == 0


def test_p23de_cross_tenant_detail_and_download_are_404(pg_api_runtime):
    org_a_id = pg_api_runtime["org_a_id"]
    org_b_id = pg_api_runtime["org_b_id"]
    created = _upload(org_a_id)
    document_id = json.loads(created.body.decode())["public_id"]

    with pytest.raises(Exception) as detail_exc:
        asyncio.run(
            vault_api.obtener_documento_boveda(
                document_id=document_id,
                user=_context(org_b_id),
            )
        )
    assert getattr(detail_exc.value, "status_code", None) == 404

    with pytest.raises(Exception) as download_exc:
        asyncio.run(
            vault_api.descargar_documento_vault(
                document_id=document_id,
                user=_context(org_b_id),
            )
        )
    assert getattr(download_exc.value, "status_code", None) == 404


def test_p23de_api_idempotency_reuses_same_document(pg_api_runtime):
    org_a_id = pg_api_runtime["org_a_id"]
    idem = f"p23de-{uuid4().hex}"

    first = json.loads(_upload(org_a_id, idem=idem).body.decode())
    second = json.loads(_upload(org_a_id, idem=idem).body.decode())

    assert first["public_id"] == second["public_id"]
    assert pg_api_runtime["storage"].put_count == 1