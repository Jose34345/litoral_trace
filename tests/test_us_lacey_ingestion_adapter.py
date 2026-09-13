from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

from litoral_trace.us_lacey.ingestion import UsLaceyIngestionService


class _FakeSharedIngestion:
    def __init__(self) -> None:
        self.kwargs = None

    def ingest(
        self,
        *,
        organization_id,
        created_by_user_id,
        filename,
        content_type,
        content,
    ):
        self.kwargs = {
            "organization_id": organization_id,
            "created_by_user_id": created_by_user_id,
            "filename": filename,
            "content_type": content_type,
            "content": content,
        }
        return SimpleNamespace(
            assurance_document_id=44,
            assurance_public_id=uuid4(),
            vault_public_id=uuid4(),
            filename=filename,
            sha256="a" * 64,
            duplicate=False,
            processing_status="UPLOADED",
        )


class _FakeOperations:
    def __init__(self) -> None:
        self.kwargs = None

    def attach_document(self, **kwargs):
        self.kwargs = kwargs
        return 77


def _service() -> tuple[UsLaceyIngestionService, _FakeSharedIngestion, _FakeOperations]:
    service = UsLaceyIngestionService.__new__(UsLaceyIngestionService)
    shared = _FakeSharedIngestion()
    operations = _FakeOperations()
    service._ingestion = shared
    service._operations = operations
    return service, shared, operations


def test_adapter_passes_creator_to_existing_vault_first_ingestion_contract():
    service, shared, operations = _service()
    operation_public_id = uuid4()

    result = service.ingest_document(
        organization_id=9,
        user_id=21,
        operation_public_id=operation_public_id,
        filename="commercial_invoice.pdf",
        content_type="application/pdf",
        content=b"%PDF-1.7 sample %%EOF",
        document_role="commercial_invoice",
    )

    assert shared.kwargs == {
        "organization_id": 9,
        "created_by_user_id": 21,
        "filename": "commercial_invoice.pdf",
        "content_type": "application/pdf",
        "content": b"%PDF-1.7 sample %%EOF",
    }
    assert operations.kwargs == {
        "organization_id": 9,
        "operation_public_id": operation_public_id,
        "assurance_document_id": 44,
        "document_role": "COMMERCIAL_INVOICE",
    }
    assert result.operation_public_id == operation_public_id
    assert result.operation_document_link_id == 77
    assert result.assurance_document_id == 44


def test_unknown_customer_document_role_is_kept_but_does_not_invent_a_type():
    service, _shared, operations = _service()

    service.ingest_document(
        organization_id=9,
        user_id=21,
        operation_public_id=str(uuid4()),
        filename="mystery.pdf",
        content_type="application/pdf",
        content=b"%PDF-1.7 sample %%EOF",
        document_role="customer_special_form",
    )

    assert operations.kwargs["document_role"] == "OTHER"
