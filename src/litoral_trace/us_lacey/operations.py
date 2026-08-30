"""Tenant-scoped operation service for the U.S. Lacey pilot."""
from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from litoral_trace.db.models import (
    AssuranceDocument,
    UsLaceyOperation,
    UsLaceyOperationDocument,
    UsLaceyOperationField,
    UsLaceyProcessingJob,
    UsLaceySubscription,
    VaultDocument,
)
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.us_lacey.db import get_us_lacey_db_session
from litoral_trace.us_lacey.domain import (
    US_LACEY_REVIEW_FIELDS,
    UsLaceyFieldStatus,
    UsLaceyOperationStatus,
)

SessionFactory = Callable[[], Session]


class UsLaceyOperationError(RuntimeError):
    pass


class UsLaceyOperationNotFound(UsLaceyOperationError):
    pass


class UsLaceyOperationConflict(UsLaceyOperationError):
    pass


@dataclass(frozen=True, slots=True)
class OperationSnapshot:
    public_id: UUID
    client_reference: str
    status: str
    document_count: int
    merchandise_line_count: int
    missing_field_count: int
    review_field_count: int


@dataclass(frozen=True, slots=True)
class OperationListItem:
    public_id: UUID
    client_reference: str
    status: str
    document_count: int
    merchandise_line_count: int
    created_at: datetime


@dataclass(frozen=True, slots=True)
class OperationDocumentView:
    assurance_public_id: UUID
    vault_public_id: UUID
    filename: str
    document_role: str
    version_number: int
    processing_status: str
    job_status: str | None
    last_error_code: str | None


@dataclass(frozen=True, slots=True)
class OperationFieldView:
    id: int
    line_reference: str
    field_name: str
    label: str
    proposed_value: str | None
    effective_value: str | None
    status: str
    confidence: float
    source_assurance_document_id: int | None
    source_page: int | None
    source_locator: str | None
    extractor: str | None
    extractor_version: str | None
    reviewed_by_user_id: int | None
    reviewed_at: datetime | None


@dataclass(frozen=True, slots=True)
class OperationDetail:
    public_id: UUID
    client_reference: str
    importer_name: str | None
    consignee_name: str | None
    broker_name: str | None
    supplier_name: str | None
    operation_date: date | None
    status: str
    document_count: int
    merchandise_line_count: int
    review_result: str | None
    created_at: datetime
    documents: tuple[OperationDocumentView, ...]
    fields: tuple[OperationFieldView, ...]


_FIELD_LABELS = dict(US_LACEY_REVIEW_FIELDS)


class UsLaceyOperationService:
    def __init__(self, *, session_factory: SessionFactory | None = None) -> None:
        self._session_factory = session_factory or get_us_lacey_db_session

    def _session(self, organization_id: int) -> Session:
        org_id = int(organization_id)
        if org_id <= 0:
            raise ValueError("organization_id must be positive")
        session = self._session_factory()
        set_tenant_db_context(session, org_id)
        return session

    @staticmethod
    def _public_id(value: UUID | str) -> UUID:
        if isinstance(value, UUID):
            return value
        try:
            return UUID(str(value))
        except (TypeError, ValueError, AttributeError) as exc:
            raise UsLaceyOperationNotFound("Operation not found.") from exc

    @staticmethod
    def _normalize_line_references(line_references: Sequence[str] | None) -> tuple[str, ...]:
        values = tuple(
            str(value).strip()
            for value in (line_references or ("1",))
            if str(value).strip()
        )
        if not values:
            return ("1",)
        if len(set(values)) != len(values):
            raise ValueError("Merchandise line references must be unique.")
        if len(values) > 500:
            raise ValueError("A single operation cannot contain more than 500 merchandise lines.")
        return values

    def create_operation(
        self,
        *,
        organization_id: int,
        created_by_user_id: int | None,
        client_reference: str,
        importer_name: str | None = None,
        consignee_name: str | None = None,
        broker_name: str | None = None,
        supplier_name: str | None = None,
        operation_date: date | None = None,
        line_references: Sequence[str] | None = None,
        consume_subscription_slot: bool = False,
    ) -> OperationSnapshot:
        reference = str(client_reference or "").strip()
        if not reference:
            raise ValueError("client_reference is required")
        if len(reference) > 255:
            raise ValueError("client_reference is too long")
        lines = self._normalize_line_references(line_references)
        org_id = int(organization_id)
        session = self._session(org_id)
        try:
            existing = session.scalar(
                select(UsLaceyOperation).where(
                    UsLaceyOperation.organization_id == org_id,
                    UsLaceyOperation.client_reference == reference,
                )
            )
            if existing is not None:
                raise UsLaceyOperationConflict(
                    "Client reference already exists for this company."
                )

            subscription: UsLaceySubscription | None = None
            if consume_subscription_slot:
                subscription = session.scalar(
                    select(UsLaceySubscription)
                    .where(UsLaceySubscription.organization_id == org_id)
                    .with_for_update()
                )
                if subscription is None:
                    raise UsLaceyOperationConflict("Subscription is not available.")
                if int(subscription.used_operations) >= int(subscription.monthly_operation_limit):
                    raise UsLaceyOperationConflict(
                        "This workspace has reached its current operation limit."
                    )

            operation = UsLaceyOperation(
                organization_id=org_id,
                created_by_user_id=created_by_user_id,
                client_reference=reference,
                importer_name=(str(importer_name).strip()[:255] if importer_name else None),
                consignee_name=(str(consignee_name).strip()[:255] if consignee_name else None),
                broker_name=(str(broker_name).strip()[:255] if broker_name else None),
                supplier_name=(str(supplier_name).strip()[:255] if supplier_name else None),
                operation_date=operation_date,
                status=UsLaceyOperationStatus.NEW.value,
                document_count=0,
                merchandise_line_count=len(lines),
            )
            session.add(operation)
            session.flush()

            # Missing stays missing until evidence or a human review supplies a value.
            for line_reference in lines:
                for field_name, _label in US_LACEY_REVIEW_FIELDS:
                    session.add(
                        UsLaceyOperationField(
                            organization_id=org_id,
                            operation_id=operation.id,
                            merchandise_line_reference=line_reference,
                            field_name=field_name,
                            field_status=UsLaceyFieldStatus.MISSING.value,
                            confidence=0.0,
                        )
                    )
            if subscription is not None:
                subscription.used_operations = int(subscription.used_operations) + 1
            session.commit()
            return self.get_operation(
                organization_id=org_id,
                operation_public_id=operation.public_id,
            )
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def _get_model(
        self,
        session: Session,
        *,
        organization_id: int,
        operation_public_id: UUID | str,
    ) -> UsLaceyOperation:
        public_id = self._public_id(operation_public_id)
        operation = session.scalar(
            select(UsLaceyOperation).where(
                UsLaceyOperation.organization_id == organization_id,
                UsLaceyOperation.public_id == public_id,
            )
        )
        if operation is None:
            raise UsLaceyOperationNotFound("Operation not found.")
        return operation

    def get_internal_id(
        self, *, organization_id: int, operation_public_id: UUID | str
    ) -> int:
        session = self._session(organization_id)
        try:
            return int(
                self._get_model(
                    session,
                    organization_id=int(organization_id),
                    operation_public_id=operation_public_id,
                ).id
            )
        finally:
            session.close()

    def list_operations(
        self, *, organization_id: int, limit: int = 100
    ) -> tuple[OperationListItem, ...]:
        org_id = int(organization_id)
        safe_limit = max(1, min(int(limit), 500))
        session = self._session(org_id)
        try:
            rows = session.scalars(
                select(UsLaceyOperation)
                .where(UsLaceyOperation.organization_id == org_id)
                .order_by(UsLaceyOperation.created_at.desc(), UsLaceyOperation.id.desc())
                .limit(safe_limit)
            ).all()
            return tuple(
                OperationListItem(
                    public_id=row.public_id,
                    client_reference=row.client_reference,
                    status=row.status,
                    document_count=int(row.document_count),
                    merchandise_line_count=int(row.merchandise_line_count),
                    created_at=row.created_at,
                )
                for row in rows
            )
        finally:
            session.close()

    def get_operation(
        self,
        *,
        organization_id: int,
        operation_public_id: UUID | str,
    ) -> OperationSnapshot:
        org_id = int(organization_id)
        session = self._session(org_id)
        try:
            operation = self._get_model(
                session,
                organization_id=org_id,
                operation_public_id=operation_public_id,
            )
            missing = session.scalar(
                select(func.count(UsLaceyOperationField.id)).where(
                    UsLaceyOperationField.organization_id == org_id,
                    UsLaceyOperationField.operation_id == operation.id,
                    UsLaceyOperationField.field_status == UsLaceyFieldStatus.MISSING.value,
                )
            ) or 0
            review = session.scalar(
                select(func.count(UsLaceyOperationField.id)).where(
                    UsLaceyOperationField.organization_id == org_id,
                    UsLaceyOperationField.operation_id == operation.id,
                    UsLaceyOperationField.field_status == UsLaceyFieldStatus.REVIEW.value,
                )
            ) or 0
            return OperationSnapshot(
                public_id=operation.public_id,
                client_reference=operation.client_reference,
                status=operation.status,
                document_count=operation.document_count,
                merchandise_line_count=operation.merchandise_line_count,
                missing_field_count=int(missing),
                review_field_count=int(review),
            )
        finally:
            session.close()

    def get_detail(
        self,
        *,
        organization_id: int,
        operation_public_id: UUID | str,
    ) -> OperationDetail:
        org_id = int(organization_id)
        session = self._session(org_id)
        try:
            operation = self._get_model(
                session,
                organization_id=org_id,
                operation_public_id=operation_public_id,
            )
            links = session.scalars(
                select(UsLaceyOperationDocument)
                .where(
                    UsLaceyOperationDocument.organization_id == org_id,
                    UsLaceyOperationDocument.operation_id == operation.id,
                    UsLaceyOperationDocument.is_current.is_(True),
                )
                .order_by(UsLaceyOperationDocument.id.asc())
            ).all()
            documents: list[OperationDocumentView] = []
            for link in links:
                assurance = session.scalar(
                    select(AssuranceDocument).where(
                        AssuranceDocument.organization_id == org_id,
                        AssuranceDocument.id == link.assurance_document_id,
                    )
                )
                if assurance is None:
                    continue
                vault = session.scalar(
                    select(VaultDocument).where(
                        VaultDocument.organization_id == org_id,
                        VaultDocument.id == assurance.vault_document_id,
                    )
                )
                if vault is None:
                    continue
                job = session.scalar(
                    select(UsLaceyProcessingJob)
                    .where(
                        UsLaceyProcessingJob.organization_id == org_id,
                        UsLaceyProcessingJob.operation_id == operation.id,
                        UsLaceyProcessingJob.assurance_document_id == assurance.id,
                    )
                    .order_by(UsLaceyProcessingJob.id.desc())
                )
                documents.append(
                    OperationDocumentView(
                        assurance_public_id=assurance.public_id,
                        vault_public_id=vault.public_id,
                        filename=vault.original_filename,
                        document_role=link.document_role,
                        version_number=int(link.version_number),
                        processing_status=assurance.processing_status,
                        job_status=None if job is None else job.status,
                        last_error_code=assurance.last_error_code,
                    )
                )

            field_rows = session.scalars(
                select(UsLaceyOperationField)
                .where(
                    UsLaceyOperationField.organization_id == org_id,
                    UsLaceyOperationField.operation_id == operation.id,
                )
                .order_by(
                    UsLaceyOperationField.merchandise_line_reference.asc(),
                    UsLaceyOperationField.id.asc(),
                )
            ).all()
            fields = tuple(
                OperationFieldView(
                    id=row.id,
                    line_reference=row.merchandise_line_reference,
                    field_name=row.field_name,
                    label=_FIELD_LABELS.get(row.field_name, row.field_name),
                    proposed_value=row.normalized_value or row.original_value,
                    effective_value=row.human_value or row.normalized_value or row.original_value,
                    status=row.field_status,
                    confidence=float(row.confidence),
                    source_assurance_document_id=row.source_assurance_document_id,
                    source_page=row.source_page,
                    source_locator=row.source_locator,
                    extractor=row.extractor,
                    extractor_version=row.extractor_version,
                    reviewed_by_user_id=row.reviewed_by_user_id,
                    reviewed_at=row.reviewed_at,
                )
                for row in field_rows
            )
            return OperationDetail(
                public_id=operation.public_id,
                client_reference=operation.client_reference,
                importer_name=operation.importer_name,
                consignee_name=operation.consignee_name,
                broker_name=operation.broker_name,
                supplier_name=operation.supplier_name,
                operation_date=operation.operation_date,
                status=operation.status,
                document_count=int(operation.document_count),
                merchandise_line_count=int(operation.merchandise_line_count),
                review_result=operation.review_result,
                created_at=operation.created_at,
                documents=tuple(documents),
                fields=fields,
            )
        finally:
            session.close()

    def attach_document(
        self,
        *,
        organization_id: int,
        operation_public_id: UUID | str,
        assurance_document_id: int,
        document_role: str,
    ) -> int:
        org_id = int(organization_id)
        role = str(document_role or "UNKNOWN").strip().upper() or "UNKNOWN"
        session = self._session(org_id)
        try:
            operation = self._get_model(
                session,
                organization_id=org_id,
                operation_public_id=operation_public_id,
            )
            assurance_document = session.scalar(
                select(AssuranceDocument).where(
                    AssuranceDocument.organization_id == org_id,
                    AssuranceDocument.id == int(assurance_document_id),
                )
            )
            if assurance_document is None:
                raise UsLaceyOperationNotFound("Document not found for this company.")

            current = session.scalar(
                select(UsLaceyOperationDocument)
                .where(
                    UsLaceyOperationDocument.organization_id == org_id,
                    UsLaceyOperationDocument.operation_id == operation.id,
                    UsLaceyOperationDocument.document_role == role,
                    UsLaceyOperationDocument.is_current.is_(True),
                )
                .order_by(UsLaceyOperationDocument.version_number.desc())
            )
            version = 1
            if current is not None:
                if current.assurance_document_id == assurance_document.id:
                    return current.id
                version = current.version_number + 1
                current.is_current = False

            link = UsLaceyOperationDocument(
                organization_id=org_id,
                operation_id=operation.id,
                assurance_document_id=assurance_document.id,
                document_role=role,
                version_number=version,
                is_current=True,
            )
            session.add(link)
            session.flush()
            operation.document_count = int(
                session.scalar(
                    select(func.count(UsLaceyOperationDocument.id)).where(
                        UsLaceyOperationDocument.organization_id == org_id,
                        UsLaceyOperationDocument.operation_id == operation.id,
                        UsLaceyOperationDocument.is_current.is_(True),
                    )
                )
                or 0
            )
            session.commit()
            return link.id
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
