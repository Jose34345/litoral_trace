"""Tenant-scoped operation service for the U.S. Lacey pilot."""
from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from litoral_trace.db.models import (
    AssuranceDocument,
    UsLaceyOperation,
    UsLaceyOperationDocument,
    UsLaceyOperationField,
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
        values = tuple(str(value).strip() for value in (line_references or ("1",)) if str(value).strip())
        if not values:
            return ("1",)
        if len(set(values)) != len(values):
            raise ValueError("Merchandise line references must be unique.")
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
    ) -> OperationSnapshot:
        reference = str(client_reference or "").strip()
        if not reference:
            raise ValueError("client_reference is required")
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
                raise UsLaceyOperationConflict("Client reference already exists for this company.")

            operation = UsLaceyOperation(
                organization_id=org_id,
                created_by_user_id=created_by_user_id,
                client_reference=reference,
                importer_name=importer_name,
                consignee_name=consignee_name,
                broker_name=broker_name,
                supplier_name=supplier_name,
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
