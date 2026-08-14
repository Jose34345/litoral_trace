from __future__ import annotations

import io
import json
import zipfile
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from litoral_trace.config.settings import StorageSettings
from litoral_trace.db.models import Organization, User, VaultDocument
from litoral_trace.services.vault import (
    JSON_CONTENT_TYPE,
    PDF_CONTENT_TYPE,
    XLSX_CONTENT_TYPE,
    VaultConflictError,
    VaultIntegrityError,
    VaultPersistenceError,
    VaultService,
    VaultStorageOperationError,
    VaultValidationError,
    sanitize_vault_filename,
    validate_vault_upload,
)
from litoral_trace.storage import (
    ObjectDeleteResult,
    ObjectHead,
    ObjectStorageError,
    ObjectStorageNotFoundError,
    ObjectStorageStream,
    ObjectWriteResult,
)


def _pdf_bytes() -> bytes:
    return b"%PDF-1.4\n1 0 obj\n<<>>\nendobj\n%%EOF\n"


def _json_bytes() -> bytes:
    return json.dumps(
        {
            "reference_number": "DDS-001",
            "status": "COMPLIANT",
        }
    ).encode("utf-8")


def _xlsx_bytes() -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as workbook:
        workbook.writestr(
            "[Content_Types].xml",
            "<Types></Types>",
        )
        workbook.writestr(
            "_rels/.rels",
            "<Relationships></Relationships>",
        )
        workbook.writestr(
            "xl/workbook.xml",
            "<workbook></workbook>",
        )
    return buffer.getvalue()


class MemoryObjectStorage:
    def __init__(self):
        self.objects: dict[str, dict[str, object]] = {}
        self.put_count = 0
        self.delete_count = 0
        self.fail_put = False
        self.fail_delete = False

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
        assert len(payload) == content_length
        self.put_count += 1
        version_id = f"version-{self.put_count}"
        self.objects[key] = {
            "payload": payload,
            "content_type": content_type,
            "metadata": dict(metadata or {}),
            "etag": f"etag-{self.put_count}",
            "version_id": version_id,
        }
        return ObjectWriteResult(
            etag=f"etag-{self.put_count}",
            version_id=version_id,
        )

    def head_object(self, *, key, version_id=None):
        stored = self.objects.get(key)
        if stored is None:
            raise ObjectStorageNotFoundError("head_object")
        payload = stored["payload"]
        return ObjectHead(
            size_bytes=len(payload),
            content_type=stored["content_type"],
            etag=stored["etag"],
            version_id=stored["version_id"],
            metadata=stored["metadata"],
        )

    def get_object_stream(self, *, key, version_id=None):
        stored = self.objects.get(key)
        if stored is None:
            raise ObjectStorageNotFoundError("get_object")
        head = self.head_object(
            key=key,
            version_id=version_id,
        )
        return ObjectStorageStream(
            body=io.BytesIO(stored["payload"]),
            head=head,
        )

    def delete_object(self, *, key, version_id=None):
        if self.fail_delete:
            raise ObjectStorageError("delete_object")
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
def vault_runtime():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    Organization.__table__.create(engine)
    User.__table__.create(engine)
    VaultDocument.__table__.create(engine)

    SessionLocal = sessionmaker(
        bind=engine,
        autoflush=False,
        expire_on_commit=False,
    )

    with SessionLocal() as session:
        organization_a = Organization(
            name="P23C Org A",
            slug=f"p23c-a-{uuid4().hex[:8]}",
            tax_id=f"A-{uuid4().hex[:8]}",
            tier="pro",
            is_active=True,
        )
        organization_b = Organization(
            name="P23C Org B",
            slug=f"p23c-b-{uuid4().hex[:8]}",
            tax_id=f"B-{uuid4().hex[:8]}",
            tier="pro",
            is_active=True,
        )
        session.add_all(
            [organization_a, organization_b]
        )
        session.commit()
        org_a_id = organization_a.id
        org_b_id = organization_b.id

    storage_settings = StorageSettings(
        backend="s3",
        bucket_name="p23c-private-vault",
        key_prefix="vault",
    )
    storage = MemoryObjectStorage()
    service = VaultService(
        storage_settings=storage_settings,
        storage=storage,
        session_factory=SessionLocal,
    )

    try:
        yield {
            "engine": engine,
            "SessionLocal": SessionLocal,
            "settings": storage_settings,
            "storage": storage,
            "service": service,
            "org_a_id": org_a_id,
            "org_b_id": org_b_id,
        }
    finally:
        engine.dispose()


def test_filename_normalization_rejects_paths_and_normalizes_whitespace():
    assert sanitize_vault_filename(
        "  certificado   final.pdf  "
    ) == "certificado final.pdf"

    with pytest.raises(VaultValidationError):
        sanitize_vault_filename("../secret.pdf")

    with pytest.raises(VaultValidationError):
        sanitize_vault_filename("folder\\secret.pdf")


def test_content_validation_accepts_pdf_json_and_xlsx(vault_runtime):
    settings = vault_runtime["settings"]

    pdf = validate_vault_upload(
        filename="certificate.pdf",
        document_type="PDF_CERTIFICADO",
        content_type=PDF_CONTENT_TYPE,
        content=_pdf_bytes(),
        settings=settings,
    )
    assert pdf.sha256
    assert pdf.size_bytes == len(_pdf_bytes())

    json_upload = validate_vault_upload(
        filename="dds.json",
        document_type="DDS_JSON_TRACES",
        content_type="application/json; charset=utf-8",
        content=_json_bytes(),
        settings=settings,
    )
    assert json_upload.content_type == JSON_CONTENT_TYPE

    xlsx = validate_vault_upload(
        filename="remito.xlsx",
        document_type="REMITO_EXCEL",
        content_type=XLSX_CONTENT_TYPE,
        content=_xlsx_bytes(),
        settings=settings,
    )
    assert xlsx.content_type == XLSX_CONTENT_TYPE


def test_content_validation_rejects_zero_size_extension_mime_and_signature_mismatch(
    vault_runtime,
):
    settings = vault_runtime["settings"]

    with pytest.raises(VaultValidationError):
        validate_vault_upload(
            filename="empty.pdf",
            document_type="PDF_CERTIFICADO",
            content_type=PDF_CONTENT_TYPE,
            content=b"",
            settings=settings,
        )

    with pytest.raises(VaultValidationError):
        validate_vault_upload(
            filename="wrong.json",
            document_type="PDF_CERTIFICADO",
            content_type=PDF_CONTENT_TYPE,
            content=_pdf_bytes(),
            settings=settings,
        )

    with pytest.raises(VaultValidationError):
        validate_vault_upload(
            filename="fake.pdf",
            document_type="PDF_CERTIFICADO",
            content_type=PDF_CONTENT_TYPE,
            content=b"not-a-pdf",
            settings=settings,
        )


def test_upload_persists_pending_then_available_and_never_uses_filename_as_key(
    vault_runtime,
):
    service = vault_runtime["service"]
    storage = vault_runtime["storage"]

    document = service.upload_document(
        organization_id=vault_runtime["org_a_id"],
        created_by_user_id=None,
        filename="sensitive customer name.pdf",
        document_type="PDF_CERTIFICADO",
        content_type=PDF_CONTENT_TYPE,
        content=_pdf_bytes(),
        idempotency_key="upload-001",
    )

    assert document.status == "available"
    assert document.filename == "sensitive customer name.pdf"
    assert storage.put_count == 1

    only_key = next(iter(storage.objects))
    assert "sensitive" not in only_key
    assert "customer" not in only_key
    assert only_key.startswith(
        f"vault/tenants/{vault_runtime['org_a_id']}/objects/"
    )

    with vault_runtime["SessionLocal"]() as session:
        row = session.scalar(
            select(VaultDocument).where(
                VaultDocument.public_id == document.public_id
            )
        )
        assert row is not None
        assert row.status == "available"
        assert row.object_key == only_key
        assert row.storage_etag == "etag-1"
        assert row.storage_version_id == "version-1"


def test_idempotent_retry_returns_same_available_document_without_second_put(
    vault_runtime,
):
    service = vault_runtime["service"]
    kwargs = {
        "organization_id": vault_runtime["org_a_id"],
        "created_by_user_id": None,
        "filename": "certificate.pdf",
        "document_type": "PDF_CERTIFICADO",
        "content_type": PDF_CONTENT_TYPE,
        "content": _pdf_bytes(),
        "idempotency_key": "same-request",
    }

    first = service.upload_document(**kwargs)
    second = service.upload_document(**kwargs)

    assert first.public_id == second.public_id
    assert vault_runtime["storage"].put_count == 1


def test_idempotency_key_conflicts_when_payload_changes(vault_runtime):
    service = vault_runtime["service"]

    service.upload_document(
        organization_id=vault_runtime["org_a_id"],
        created_by_user_id=None,
        filename="certificate.pdf",
        document_type="PDF_CERTIFICADO",
        content_type=PDF_CONTENT_TYPE,
        content=_pdf_bytes(),
        idempotency_key="conflict-key",
    )

    with pytest.raises(VaultConflictError):
        service.upload_document(
            organization_id=vault_runtime["org_a_id"],
            created_by_user_id=None,
            filename="other.pdf",
            document_type="PDF_CERTIFICADO",
            content_type=PDF_CONTENT_TYPE,
            content=_pdf_bytes() + b"\n% changed",
            idempotency_key="conflict-key",
        )


def test_storage_upload_failure_is_persisted_and_same_request_can_recover(
    vault_runtime,
):
    service = vault_runtime["service"]
    storage = vault_runtime["storage"]
    storage.fail_put = True

    kwargs = {
        "organization_id": vault_runtime["org_a_id"],
        "created_by_user_id": None,
        "filename": "certificate.pdf",
        "document_type": "PDF_CERTIFICADO",
        "content_type": PDF_CONTENT_TYPE,
        "content": _pdf_bytes(),
        "idempotency_key": "recoverable-upload",
    }

    with pytest.raises(VaultStorageOperationError):
        service.upload_document(**kwargs)

    with vault_runtime["SessionLocal"]() as session:
        failed = session.scalar(
            select(VaultDocument).where(
                VaultDocument.idempotency_key == "recoverable-upload"
            )
        )
        assert failed.status == "upload_failed"
        assert failed.last_error_code == "STORAGE_UPLOAD_FAILED"
        failed_public_id = failed.public_id

    storage.fail_put = False
    recovered = service.upload_document(**kwargs)

    assert recovered.public_id == failed_public_id
    assert recovered.status == "available"
    assert storage.put_count == 1


def test_finalize_database_failure_compensates_storage_and_marks_upload_failed(
    vault_runtime,
):
    normal_factory = vault_runtime["SessionLocal"]
    storage = vault_runtime["storage"]

    def failing_factory():
        session = normal_factory()
        original_commit = session.commit
        commit_count = {"value": 0}

        def commit_with_failure():
            commit_count["value"] += 1
            if commit_count["value"] == 2:
                raise RuntimeError("simulated finalize failure")
            return original_commit()

        session.commit = commit_with_failure
        return session

    service = VaultService(
        storage_settings=vault_runtime["settings"],
        storage=storage,
        session_factory=failing_factory,
    )

    with pytest.raises(VaultPersistenceError):
        service.upload_document(
            organization_id=vault_runtime["org_a_id"],
            created_by_user_id=None,
            filename="certificate.pdf",
            document_type="PDF_CERTIFICADO",
            content_type=PDF_CONTENT_TYPE,
            content=_pdf_bytes(),
            idempotency_key="db-finalize-failure",
        )

    assert storage.delete_count == 1
    assert storage.objects == {}

    with normal_factory() as session:
        failed = session.scalar(
            select(VaultDocument).where(
                VaultDocument.idempotency_key == "db-finalize-failure"
            )
        )
        assert failed is not None
        assert failed.status == "upload_failed"
        assert (
            failed.last_error_code
            == "UPLOAD_FINALIZE_DB_FAILED_COMPENSATED"
        )


def test_list_and_get_are_explicitly_tenant_scoped(vault_runtime):
    service = vault_runtime["service"]

    created = service.upload_document(
        organization_id=vault_runtime["org_a_id"],
        created_by_user_id=None,
        filename="tenant-a.pdf",
        document_type="PDF_CERTIFICADO",
        content_type=PDF_CONTENT_TYPE,
        content=_pdf_bytes(),
    )

    tenant_a = service.list_documents(
        organization_id=vault_runtime["org_a_id"],
    )
    tenant_b = service.list_documents(
        organization_id=vault_runtime["org_b_id"],
    )

    assert [item.public_id for item in tenant_a] == [created.public_id]
    assert tenant_b == []

    with pytest.raises(Exception) as exc_info:
        service.get_document(
            organization_id=vault_runtime["org_b_id"],
            document_id=created.public_id,
        )
    assert exc_info.value.__class__.__name__ == "VaultNotFoundError"


def test_verified_download_hashes_full_object_before_exposing_bytes(vault_runtime):
    service = vault_runtime["service"]

    created = service.upload_document(
        organization_id=vault_runtime["org_a_id"],
        created_by_user_id=None,
        filename="certificate.pdf",
        document_type="PDF_CERTIFICADO",
        content_type=PDF_CONTENT_TYPE,
        content=_pdf_bytes(),
    )

    with service.materialize_verified_download(
        organization_id=vault_runtime["org_a_id"],
        document_id=created.public_id,
    ) as download:
        assert b"".join(download.iter_chunks(chunk_size=5)) == _pdf_bytes()
        assert download.document.sha256 == created.sha256


def test_verified_download_detects_corrupted_object(vault_runtime):
    service = vault_runtime["service"]
    storage = vault_runtime["storage"]

    created = service.upload_document(
        organization_id=vault_runtime["org_a_id"],
        created_by_user_id=None,
        filename="certificate.pdf",
        document_type="PDF_CERTIFICADO",
        content_type=PDF_CONTENT_TYPE,
        content=_pdf_bytes(),
    )

    key = next(iter(storage.objects))
    storage.objects[key]["payload"] = b"X" * len(_pdf_bytes())

    with pytest.raises(VaultIntegrityError):
        service.materialize_verified_download(
            organization_id=vault_runtime["org_a_id"],
            document_id=created.public_id,
        )


def test_delete_is_soft_metadata_delete_and_idempotent(vault_runtime):
    service = vault_runtime["service"]
    storage = vault_runtime["storage"]

    created = service.upload_document(
        organization_id=vault_runtime["org_a_id"],
        created_by_user_id=None,
        filename="certificate.pdf",
        document_type="PDF_CERTIFICADO",
        content_type=PDF_CONTENT_TYPE,
        content=_pdf_bytes(),
    )

    deleted = service.delete_document(
        organization_id=vault_runtime["org_a_id"],
        document_id=created.public_id,
    )
    repeated = service.delete_document(
        organization_id=vault_runtime["org_a_id"],
        document_id=created.public_id,
    )

    assert deleted.status == "deleted"
    assert deleted.deleted_at is not None
    assert repeated.status == "deleted"
    assert storage.delete_count == 1
    assert service.list_documents(
        organization_id=vault_runtime["org_a_id"],
    ) == []

    with vault_runtime["SessionLocal"]() as session:
        row = session.scalar(
            select(VaultDocument).where(
                VaultDocument.public_id == created.public_id
            )
        )
        assert row is not None
        assert row.status == "deleted"


def test_delete_storage_failure_marks_delete_failed_and_can_retry(vault_runtime):
    service = vault_runtime["service"]
    storage = vault_runtime["storage"]

    created = service.upload_document(
        organization_id=vault_runtime["org_a_id"],
        created_by_user_id=None,
        filename="certificate.pdf",
        document_type="PDF_CERTIFICADO",
        content_type=PDF_CONTENT_TYPE,
        content=_pdf_bytes(),
    )

    storage.fail_delete = True
    with pytest.raises(VaultStorageOperationError):
        service.delete_document(
            organization_id=vault_runtime["org_a_id"],
            document_id=created.public_id,
        )

    with vault_runtime["SessionLocal"]() as session:
        row = session.scalar(
            select(VaultDocument).where(
                VaultDocument.public_id == created.public_id
            )
        )
        assert row.status == "delete_failed"
        assert row.last_error_code == "STORAGE_DELETE_FAILED"

    storage.fail_delete = False
    retried = service.delete_document(
        organization_id=vault_runtime["org_a_id"],
        document_id=created.public_id,
    )
    assert retried.status == "deleted"


def test_service_never_exposes_object_key_in_public_view(vault_runtime):
    created = vault_runtime["service"].upload_document(
        organization_id=vault_runtime["org_a_id"],
        created_by_user_id=None,
        filename="certificate.pdf",
        document_type="PDF_CERTIFICADO",
        content_type=PDF_CONTENT_TYPE,
        content=_pdf_bytes(),
    )

    assert not hasattr(created, "object_key")
    assert not hasattr(created, "storage_bucket")