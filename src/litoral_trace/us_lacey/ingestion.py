"""U.S. Lacey operation ingestion built on the existing Vault-first pipeline."""
from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from litoral_trace.assurance.ingestion import AssuranceIngestionService
from litoral_trace.us_lacey.db import get_us_lacey_db_session
from litoral_trace.us_lacey.operations import UsLaceyOperationService
from litoral_trace.us_lacey.storage import (
    build_us_lacey_storage_settings,
    get_us_lacey_storage_client,
)


US_LACEY_DOCUMENT_ROLES = frozenset(
    {
        "COMMERCIAL_INVOICE",
        "PACKING_LIST",
        "BILL_OF_LADING",
        "SUPPLIER_SHEET",
        "SUPPLIER_DECLARATION",
        "CERTIFICATE",
        "OTHER",
        "UNKNOWN",
    }
)


@dataclass(frozen=True, slots=True)
class UsLaceyIngestionResult:
    operation_public_id: UUID
    operation_document_link_id: int
    assurance_document_id: int
    assurance_public_id: UUID
    vault_public_id: UUID
    filename: str
    sha256: str
    duplicate: bool
    processing_status: str


class UsLaceyIngestionService:
    """Persist original evidence in the isolated U.S. Vault and link it to an operation."""

    def __init__(self) -> None:
        storage_settings = build_us_lacey_storage_settings()
        self._ingestion = AssuranceIngestionService(
            storage_settings=storage_settings,
            storage=get_us_lacey_storage_client(),
            session_factory=get_us_lacey_db_session,
        )
        self._operations = UsLaceyOperationService(session_factory=get_us_lacey_db_session)

    @staticmethod
    def _role(value: str) -> str:
        role = str(value or "UNKNOWN").strip().upper() or "UNKNOWN"
        if role not in US_LACEY_DOCUMENT_ROLES:
            return "OTHER"
        return role

    @staticmethod
    def _operation_public_id(value: UUID | str) -> UUID:
        if isinstance(value, UUID):
            return value
        return UUID(str(value))

    def ingest_document(
        self,
        *,
        organization_id: int,
        user_id: int,
        operation_public_id: UUID | str,
        filename: str,
        content_type: str,
        content: bytes,
        document_role: str = "UNKNOWN",
    ) -> UsLaceyIngestionResult:
        # The shared ingestion layer validates extension/content, hashes the exact
        # original bytes, deduplicates within the tenant and persists them to Vault.
        result = self._ingestion.ingest(
            organization_id=organization_id,
            created_by_user_id=user_id,
            filename=filename,
            content_type=content_type,
            content=content,
        )
        link_id = self._operations.attach_document(
            organization_id=organization_id,
            operation_public_id=operation_public_id,
            assurance_document_id=result.assurance_document_id,
            document_role=self._role(document_role),
        )
        return UsLaceyIngestionResult(
            operation_public_id=self._operation_public_id(operation_public_id),
            operation_document_link_id=link_id,
            assurance_document_id=result.assurance_document_id,
            assurance_public_id=result.assurance_public_id,
            vault_public_id=result.vault_public_id,
            filename=result.filename,
            sha256=result.sha256,
            duplicate=result.duplicate,
            processing_status=result.processing_status,
        )
