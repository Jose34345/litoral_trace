from __future__ import annotations

from io import BytesIO

from openpyxl import Workbook
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from litoral_trace.assurance.ingestion import AssuranceIngestionService
from litoral_trace.config.settings import StorageSettings
from litoral_trace.db.models import AssuranceDocument, Organization, VaultDocument
from litoral_trace.storage import ObjectDeleteResult, ObjectHead, ObjectWriteResult


class FakeObjectStorage:
    def __init__(self) -> None:
        self.objects: dict[str, dict[str, object]] = {}
        self.put_calls = 0

    def put_object(self, *, key, body, content_type, content_length, metadata=None):
        payload = body if isinstance(body, bytes) else body.read()
        assert len(payload) == content_length
        self.put_calls += 1
        self.objects[key] = {
            "body": payload,
            "content_type": content_type,
            "metadata": dict(metadata or {}),
        }
        return ObjectWriteResult(etag=f"etag-{self.put_calls}", version_id=None)

    def delete_object(self, *, key, version_id=None):
        self.objects.pop(key, None)
        return ObjectDeleteResult(delete_marker=False, version_id=version_id)

    def head_object(self, *, key, version_id=None):
        item = self.objects[key]
        return ObjectHead(
            size_bytes=len(item["body"]),
            content_type=item["content_type"],
            etag="etag",
            version_id=version_id,
            metadata=item["metadata"],
        )

    def object_exists(self, *, key, version_id=None):
        return key in self.objects

    def health_check(self):
        return True

    def get_object_stream(self, *, key, version_id=None):
        raise NotImplementedError


def _xlsx_bytes() -> bytes:
    workbook = Workbook()
    sheet = workbook.active
    sheet.append(["Proveedor", "Cantidad"])
    sheet.append(["Forestal Norte", 10])
    buffer = BytesIO()
    workbook.save(buffer)
    workbook.close()
    return buffer.getvalue()


def _engine():
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Organization.__table__.create(engine)
    VaultDocument.__table__.create(engine)
    AssuranceDocument.__table__.create(engine)
    return engine


def test_identical_content_is_stored_once_and_reused_by_sha256():
    engine = _engine()
    with Session(engine) as session:
        organization = Organization(name="Tenant A", slug="tenant-a", tier="pro", is_active=True)
        session.add(organization)
        session.commit()
        organization_id = organization.id

    storage = FakeObjectStorage()
    settings = StorageSettings(
        backend="s3",
        bucket_name="assurance-test-bucket",
        max_upload_bytes=5 * 1024 * 1024,
    )

    def session_factory():
        return Session(engine)

    service = AssuranceIngestionService(
        storage_settings=settings,
        storage=storage,
        session_factory=session_factory,
    )
    payload = _xlsx_bytes()

    first = service.ingest(
        organization_id=organization_id,
        created_by_user_id=None,
        filename="operacion.xlsx",
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        content=payload,
    )
    second = service.ingest(
        organization_id=organization_id,
        created_by_user_id=None,
        filename="copia-renombrada.xlsx",
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        content=payload,
    )

    assert first.duplicate is False
    assert second.duplicate is True
    assert second.vault_public_id == first.vault_public_id
    assert second.assurance_public_id == first.assurance_public_id
    assert second.sha256 == first.sha256
    assert storage.put_calls == 1
    assert len(storage.objects) == 1

    with Session(engine) as session:
        vault_count = session.scalar(select(func.count()).select_from(VaultDocument))
        assurance_count = session.scalar(select(func.count()).select_from(AssuranceDocument))
        vault = session.scalar(select(VaultDocument))
        assurance = session.scalar(select(AssuranceDocument))

    assert vault_count == 1
    assert assurance_count == 1
    assert vault.status == "available"
    assert vault.sha256 == first.sha256
    assert assurance.vault_document_id == vault.id
