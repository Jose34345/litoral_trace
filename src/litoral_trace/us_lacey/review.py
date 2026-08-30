"""Human review and evidence-preserving exports for U.S. operations."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from io import BytesIO, StringIO
import csv
from uuid import UUID

from openpyxl import Workbook
from sqlalchemy import func, select

from litoral_trace.db.models import (
    AssuranceDocument,
    ReconciliationIssue,
    UsLaceyOperation,
    UsLaceyOperationField,
    UsLaceyProcessingJob,
    VaultDocument,
)
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.services.audit import (
    AuditAction,
    AuditActor,
    AuditOutcome,
    record_audit_event,
)
from litoral_trace.us_lacey.db import get_us_lacey_db_session
from litoral_trace.us_lacey.domain import US_LACEY_REVIEW_FIELDS
from litoral_trace.us_lacey.operations import UsLaceyOperationNotFound
from litoral_trace.us_lacey.projection import refresh_us_lacey_operation_status


class UsLaceyReviewError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class UsLaceyReviewResult:
    field_id: int
    field_status: str
    operation_status: str
    remaining_review_count: int
    remaining_missing_count: int
    open_conflict_count: int


@dataclass(frozen=True, slots=True)
class UsLaceyFinalizeResult:
    operation_status: str
    review_result: str


_LABELS = dict(US_LACEY_REVIEW_FIELDS)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _operation(session, *, organization_id: int, operation_public_id: UUID | str):
    try:
        public_id = operation_public_id if isinstance(operation_public_id, UUID) else UUID(str(operation_public_id))
    except (ValueError, TypeError, AttributeError) as exc:
        raise UsLaceyOperationNotFound("Operation not found.") from exc
    operation = session.scalar(
        select(UsLaceyOperation).where(
            UsLaceyOperation.organization_id == organization_id,
            UsLaceyOperation.public_id == public_id,
        )
    )
    if operation is None:
        raise UsLaceyOperationNotFound("Operation not found.")
    return operation


def _counts(session, *, organization_id: int, operation: UsLaceyOperation) -> tuple[int, int, int]:
    review = session.scalar(
        select(func.count(UsLaceyOperationField.id)).where(
            UsLaceyOperationField.organization_id == organization_id,
            UsLaceyOperationField.operation_id == operation.id,
            UsLaceyOperationField.field_status == "REVIEW",
        )
    ) or 0
    missing = session.scalar(
        select(func.count(UsLaceyOperationField.id)).where(
            UsLaceyOperationField.organization_id == organization_id,
            UsLaceyOperationField.operation_id == operation.id,
            UsLaceyOperationField.field_status == "MISSING",
        )
    ) or 0
    conflicts = session.scalar(
        select(func.count(ReconciliationIssue.id)).where(
            ReconciliationIssue.organization_id == organization_id,
            ReconciliationIssue.operation_reference == f"us_lacey:{operation.public_id}",
            ReconciliationIssue.status == "OPEN",
        )
    ) or 0
    return int(review), int(missing), int(conflicts)


def review_us_lacey_field(
    *,
    organization_id: int,
    operation_public_id: UUID | str,
    field_id: int,
    user_id: int,
    user_email: str,
    action: str,
    value: str | None = None,
) -> UsLaceyReviewResult:
    """Accept, edit or explicitly mark one preparation field as not required.

    Extracted originals/provenance are never overwritten. Human decisions live in
    ``human_value`` and reviewer metadata. Resolving a contradiction is explicit
    and audit logged.
    """
    org_id = int(organization_id)
    normalized_action = str(action or "").strip().lower()
    if normalized_action not in {"accept", "edit", "not_required"}:
        raise UsLaceyReviewError("Review action is invalid.")

    session = get_us_lacey_db_session()
    try:
        set_tenant_db_context(session, org_id)
        operation = _operation(
            session,
            organization_id=org_id,
            operation_public_id=operation_public_id,
        )
        field = session.scalar(
            select(UsLaceyOperationField).where(
                UsLaceyOperationField.organization_id == org_id,
                UsLaceyOperationField.operation_id == operation.id,
                UsLaceyOperationField.id == int(field_id),
            )
        )
        if field is None:
            raise UsLaceyReviewError("Review field was not found for this operation.")

        proposed = field.normalized_value or field.original_value
        before = {
            "field_id": field.id,
            "field_name": field.field_name,
            "field_status": field.field_status,
            "effective_value": field.human_value or proposed,
        }
        if normalized_action == "accept":
            if proposed is None or not str(proposed).strip():
                raise UsLaceyReviewError("There is no extracted value to accept.")
            field.human_value = str(proposed).strip()
            field.field_status = "MATCHED"
        elif normalized_action == "edit":
            human = str(value or "").strip()
            if not human:
                raise UsLaceyReviewError("Enter a value before saving this review.")
            if len(human) > 4000:
                raise UsLaceyReviewError("Reviewed value is too long.")
            field.human_value = human
            field.field_status = "MATCHED"
        else:
            field.human_value = None
            field.field_status = "NOT_REQUIRED"

        field.reviewed_by_user_id = int(user_id)
        field.reviewed_at = _utc_now()

        open_issues = session.scalars(
            select(ReconciliationIssue).where(
                ReconciliationIssue.organization_id == org_id,
                ReconciliationIssue.operation_reference == f"us_lacey:{operation.public_id}",
                ReconciliationIssue.field_name == field.field_name,
                ReconciliationIssue.status == "OPEN",
            )
        ).all()
        for issue in open_issues:
            issue.status = "RESOLVED"
            issue.resolution_justification = (
                f"Explicit human review ({normalized_action}) by U.S. portal user {user_id}."
            )
            issue.resolved_at = field.reviewed_at

        operation_status = refresh_us_lacey_operation_status(
            session,
            organization_id=org_id,
            operation=operation,
        )
        review_count, missing_count, conflict_count = _counts(
            session,
            organization_id=org_id,
            operation=operation,
        )
        actor = AuditActor(
            organization_id=org_id,
            user_id=int(user_id),
            username=str(user_email or "").strip() or None,
            role="us_lacey_customer",
        )
        record_audit_event(
            session,
            actor=actor,
            action=(
                AuditAction.ASSURANCE_REVIEW_APPROVE
                if normalized_action == "accept"
                else AuditAction.ASSURANCE_REVIEW_CORRECT
            ),
            entity_type="us_lacey_operation",
            entity_id=operation.id,
            outcome=AuditOutcome.SUCCESS,
            metadata={
                "operation_public_id": str(operation.public_id),
                "field_id": field.id,
                "field_name": field.field_name,
                "review_action": normalized_action,
                "resolved_conflict_count": len(open_issues),
            },
            before_data=before,
            after_data={
                "field_status": field.field_status,
                "effective_value": field.human_value or proposed,
                "operation_status": operation_status,
            },
            detail="U.S. document-preparation field reviewed by a customer user.",
        )
        session.commit()
        return UsLaceyReviewResult(
            field_id=field.id,
            field_status=field.field_status,
            operation_status=operation_status,
            remaining_review_count=review_count,
            remaining_missing_count=missing_count,
            open_conflict_count=conflict_count,
        )
    except (UsLaceyReviewError, UsLaceyOperationNotFound):
        session.rollback()
        raise
    except Exception as exc:
        session.rollback()
        raise UsLaceyReviewError("Unable to save this review decision.") from exc
    finally:
        session.close()


def finalize_us_lacey_review(
    *,
    organization_id: int,
    operation_public_id: UUID | str,
    user_id: int,
    user_email: str,
) -> UsLaceyFinalizeResult:
    """Close human review only when no unresolved preparation item remains."""
    org_id = int(organization_id)
    session = get_us_lacey_db_session()
    try:
        set_tenant_db_context(session, org_id)
        operation = _operation(
            session,
            organization_id=org_id,
            operation_public_id=operation_public_id,
        )
        active_jobs = session.scalar(
            select(func.count(UsLaceyProcessingJob.id)).where(
                UsLaceyProcessingJob.organization_id == org_id,
                UsLaceyProcessingJob.operation_id == operation.id,
                UsLaceyProcessingJob.status.in_(("QUEUED", "RUNNING", "RETRY")),
            )
        ) or 0
        failed_jobs = session.scalar(
            select(func.count(UsLaceyProcessingJob.id)).where(
                UsLaceyProcessingJob.organization_id == org_id,
                UsLaceyProcessingJob.operation_id == operation.id,
                UsLaceyProcessingJob.status == "FAILED",
            )
        ) or 0
        review_count, missing_count, conflict_count = _counts(
            session,
            organization_id=org_id,
            operation=operation,
        )
        if int(operation.document_count) <= 0:
            raise UsLaceyReviewError("Upload at least one document before completing review.")
        if int(active_jobs):
            raise UsLaceyReviewError("Document processing is still in progress.")
        if int(failed_jobs):
            raise UsLaceyReviewError("A document-processing failure must be resolved first.")
        if review_count or missing_count or conflict_count:
            raise UsLaceyReviewError(
                "Resolve every missing field, review item and contradiction before completing review."
            )

        operation.status = "COMPLETED"
        operation.review_result = "DOCUMENT_REVIEW_COMPLETE"
        actor = AuditActor(
            organization_id=org_id,
            user_id=int(user_id),
            username=str(user_email or "").strip() or None,
            role="us_lacey_customer",
        )
        record_audit_event(
            session,
            actor=actor,
            action=AuditAction.ASSURANCE_REVIEW_APPROVE,
            entity_type="us_lacey_operation",
            entity_id=operation.id,
            outcome=AuditOutcome.SUCCESS,
            metadata={
                "operation_public_id": str(operation.public_id),
                "review_result": operation.review_result,
            },
            after_data={
                "operation_status": operation.status,
                "review_result": operation.review_result,
            },
            detail=(
                "Human document review completed. This is not a legal compliance determination."
            ),
        )
        session.commit()
        return UsLaceyFinalizeResult(
            operation_status=operation.status,
            review_result=operation.review_result,
        )
    except (UsLaceyReviewError, UsLaceyOperationNotFound):
        session.rollback()
        raise
    except Exception as exc:
        session.rollback()
        raise UsLaceyReviewError("Unable to complete operation review.") from exc
    finally:
        session.close()


def _export_rows(*, organization_id: int, operation_public_id: UUID | str):
    org_id = int(organization_id)
    session = get_us_lacey_db_session()
    try:
        set_tenant_db_context(session, org_id)
        operation = _operation(
            session,
            organization_id=org_id,
            operation_public_id=operation_public_id,
        )
        fields = session.scalars(
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
        documents = session.scalars(
            select(AssuranceDocument).where(
                AssuranceDocument.organization_id == org_id,
                AssuranceDocument.id.in_(
                    tuple(
                        sorted(
                            {
                                field.source_assurance_document_id
                                for field in fields
                                if field.source_assurance_document_id is not None
                            }
                        )
                    )
                    or (-1,)
                ),
            )
        ).all()
        filenames: dict[int, str] = {}
        for document in documents:
            vault = session.scalar(
                select(VaultDocument).where(
                    VaultDocument.organization_id == org_id,
                    VaultDocument.id == document.vault_document_id,
                )
            )
            if vault is not None:
                filenames[document.id] = vault.original_filename
        issues = session.scalars(
            select(ReconciliationIssue)
            .where(
                ReconciliationIssue.organization_id == org_id,
                ReconciliationIssue.operation_reference == f"us_lacey:{operation.public_id}",
            )
            .order_by(ReconciliationIssue.created_at.asc(), ReconciliationIssue.id.asc())
        ).all()
        return operation, tuple(fields), filenames, tuple(issues)
    finally:
        session.close()


def export_us_lacey_csv(
    *, organization_id: int, operation_public_id: UUID | str
) -> bytes:
    operation, fields, _filenames, _issues = _export_rows(
        organization_id=organization_id,
        operation_public_id=operation_public_id,
    )
    line_refs = tuple(dict.fromkeys(field.merchandise_line_reference for field in fields))
    by_key = {(field.merchandise_line_reference, field.field_name): field for field in fields}
    output = StringIO(newline="")
    writer = csv.writer(output)
    writer.writerow(["Line Reference", *[label for _key, label in US_LACEY_REVIEW_FIELDS]])
    for line in line_refs:
        values = []
        for key, _label in US_LACEY_REVIEW_FIELDS:
            field = by_key.get((line, key))
            values.append("" if field is None else (field.human_value or field.normalized_value or field.original_value or ""))
        writer.writerow([line, *values])
    return output.getvalue().encode("utf-8-sig")


def export_us_lacey_xlsx(
    *, organization_id: int, operation_public_id: UUID | str
) -> bytes:
    operation, fields, filenames, issues = _export_rows(
        organization_id=organization_id,
        operation_public_id=operation_public_id,
    )
    line_refs = tuple(dict.fromkeys(field.merchandise_line_reference for field in fields))
    by_key = {(field.merchandise_line_reference, field.field_name): field for field in fields}

    workbook = Workbook()
    data_sheet = workbook.active
    data_sheet.title = "Preparation Data"
    data_sheet.append(["Line Reference", *[label for _key, label in US_LACEY_REVIEW_FIELDS]])
    for line in line_refs:
        row = [line]
        for key, _label in US_LACEY_REVIEW_FIELDS:
            field = by_key.get((line, key))
            row.append("" if field is None else (field.human_value or field.normalized_value or field.original_value or ""))
        data_sheet.append(row)
    data_sheet.freeze_panes = "A2"
    data_sheet.auto_filter.ref = data_sheet.dimensions

    evidence_sheet = workbook.create_sheet("Evidence")
    evidence_sheet.append(
        [
            "Line Reference",
            "Field",
            "Value",
            "Review Status",
            "Confidence",
            "Source Document",
            "Source Page",
            "Source Locator",
            "Extractor",
            "Extractor Version",
            "Reviewed By User ID",
            "Reviewed At",
        ]
    )
    for field in fields:
        evidence_sheet.append(
            [
                field.merchandise_line_reference,
                _LABELS.get(field.field_name, field.field_name),
                field.human_value or field.normalized_value or field.original_value or "",
                field.field_status,
                float(field.confidence),
                filenames.get(field.source_assurance_document_id or -1, ""),
                field.source_page,
                field.source_locator or "",
                field.extractor or "",
                field.extractor_version or "",
                field.reviewed_by_user_id,
                field.reviewed_at.isoformat() if field.reviewed_at else "",
            ]
        )
    evidence_sheet.freeze_panes = "A2"
    evidence_sheet.auto_filter.ref = evidence_sheet.dimensions

    exception_sheet = workbook.create_sheet("Exceptions")
    exception_sheet.append(
        [
            "Status",
            "Severity",
            "Field",
            "Left Value",
            "Right Value",
            "Explanation",
            "Resolution",
        ]
    )
    for issue in issues:
        exception_sheet.append(
            [
                issue.status,
                issue.severity,
                issue.field_name or "",
                issue.left_value or "",
                issue.right_value or "",
                issue.explanation,
                issue.resolution_justification or "",
            ]
        )

    readme = workbook.create_sheet("Read Me", 0)
    readme.append(["Litoral Trace — U.S. document preparation export"])
    readme.append(["Operation", operation.client_reference])
    readme.append(["Workspace status", operation.status])
    readme.append(
        [
            "Review statement",
            (
                "Document review complete; ready for export."
                if operation.status == "COMPLETED"
                else "Draft export; human review remains required."
            ),
        ]
    )
    readme.append(
        [
            "Important",
            "This workbook organizes source evidence for human review. It is not a legal compliance determination and is not an ACE/LAWGS filing.",
        ]
    )

    output = BytesIO()
    workbook.save(output)
    return output.getvalue()
