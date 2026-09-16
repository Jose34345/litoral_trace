"""Atomic customer confirmation for safe U.S. Lacey suggestions.

Bulk confirmation is an explicit human review action. It may promote only current
``FOUND`` proposals that are unambiguous and have no open reconciliation issue.
The whole click is committed as one transaction so a validation failure cannot
leave a partially-confirmed operation.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import select

from litoral_trace.db.models import (
    ReconciliationIssue,
    UsLaceyFieldCandidate,
    UsLaceyOperation,
    UsLaceyOperationField,
)
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.services.audit import (
    AuditAction,
    AuditActor,
    AuditOutcome,
    record_audit_event,
)
from litoral_trace.us_lacey.candidate_normalization import group_candidate_evidence
from litoral_trace.us_lacey.db import get_us_lacey_db_session
from litoral_trace.us_lacey.operations import UsLaceyOperationNotFound
from litoral_trace.us_lacey.ppq505 import validate_ppq_value
from litoral_trace.us_lacey.projection import refresh_us_lacey_operation_status
from litoral_trace.us_lacey.review import UsLaceyReviewError


@dataclass(frozen=True, slots=True)
class UsLaceyBulkAcceptResult:
    accepted_count: int
    operation_status: str


def _utc_now():
    return datetime.now(timezone.utc)


def accept_supported_us_lacey_fields(
    *,
    organization_id: int,
    operation_public_id: UUID | str,
    user_id: int,
    user_email: str,
) -> UsLaceyBulkAcceptResult:
    """Confirm every currently safe ``FOUND`` field in one locked transaction.

    Idempotency follows from the state transition itself: after a successful click
    each eligible field is ``MATCHED``. A repeated request therefore sees no
    eligible ``FOUND`` rows and writes no duplicate field-review audit events.
    ``REVIEW``/``MISSING`` fields and fields with genuinely different open conflicts
    are never touched. Multiple provenance rows supporting the same canonical value
    count as one safe candidate group rather than a conflict.
    """
    org_id = int(organization_id)
    try:
        public_id = (
            operation_public_id
            if isinstance(operation_public_id, UUID)
            else UUID(str(operation_public_id))
        )
    except (ValueError, TypeError, AttributeError) as exc:
        raise UsLaceyOperationNotFound("Operation not found.") from exc

    session = get_us_lacey_db_session()
    try:
        set_tenant_db_context(session, org_id)
        operation = session.scalar(
            select(UsLaceyOperation)
            .where(
                UsLaceyOperation.organization_id == org_id,
                UsLaceyOperation.public_id == public_id,
            )
            .with_for_update()
        )
        if operation is None:
            raise UsLaceyOperationNotFound("Operation not found.")

        found_fields = session.scalars(
            select(UsLaceyOperationField)
            .where(
                UsLaceyOperationField.organization_id == org_id,
                UsLaceyOperationField.operation_id == operation.id,
                UsLaceyOperationField.field_status == "FOUND",
            )
            .order_by(UsLaceyOperationField.id.asc())
            .with_for_update()
        ).all()

        if found_fields:
            field_ids = [field.id for field in found_fields]
            candidate_rows = session.scalars(
                select(UsLaceyFieldCandidate)
                .where(
                    UsLaceyFieldCandidate.organization_id == org_id,
                    UsLaceyFieldCandidate.operation_id == operation.id,
                    UsLaceyFieldCandidate.operation_field_id.in_(field_ids),
                )
                .order_by(UsLaceyFieldCandidate.id.asc())
            ).all()
            candidates_by_field: dict[int, list[UsLaceyFieldCandidate]] = {}
            for candidate in candidate_rows:
                candidates_by_field.setdefault(int(candidate.operation_field_id), []).append(candidate)
            conflicted_field_ids = set(
                session.scalars(
                    select(ReconciliationIssue.us_lacey_operation_field_id).where(
                        ReconciliationIssue.organization_id == org_id,
                        ReconciliationIssue.operation_reference
                        == f"us_lacey:{operation.public_id}",
                        ReconciliationIssue.status == "OPEN",
                        ReconciliationIssue.us_lacey_operation_field_id.in_(field_ids),
                    )
                ).all()
            )
        else:
            candidates_by_field = {}
            conflicted_field_ids = set()

        actor = AuditActor(
            organization_id=org_id,
            user_id=int(user_id),
            username=str(user_email or "").strip() or None,
            role="us_lacey_customer",
        )
        accepted = 0
        for field in found_fields:
            groups = group_candidate_evidence(
                field.field_name,
                candidates_by_field.get(int(field.id), ()),
            )
            if len(groups) > 1:
                continue
            if field.id in conflicted_field_ids:
                continue

            proposed = field.normalized_value or field.original_value
            if proposed is None or not str(proposed).strip():
                continue
            validation = validate_ppq_value(field.field_name, proposed)
            if validation.status.value in {"INVALID", "MISSING", "REVIEW_REQUIRED"}:
                # FOUND is an invariant asserting a safe, valid proposal. If that
                # invariant is broken, fail the entire click rather than partially
                # accepting neighboring fields.
                raise UsLaceyReviewError(
                    validation.error
                    or f"Supported field {field.field_name} is no longer safe to accept."
                )

            before = {
                "field_id": field.id,
                "field_name": field.field_name,
                "field_status": field.field_status,
                "effective_value": proposed,
            }
            reviewed_at = _utc_now()
            field.human_value = validation.normalized_value
            field.field_status = "MATCHED"
            field.validation_status = "VALID"
            field.validation_error = None
            field.not_required_reason_code = None
            field.reviewed_by_user_id = int(user_id)
            field.reviewed_at = reviewed_at

            record_audit_event(
                session,
                actor=actor,
                action=AuditAction.ASSURANCE_REVIEW_APPROVE,
                entity_type="us_lacey_operation",
                entity_id=operation.id,
                outcome=AuditOutcome.SUCCESS,
                metadata={
                    "operation_public_id": str(operation.public_id),
                    "field_id": field.id,
                    "field_name": field.field_name,
                    "review_action": "bulk_accept_supported",
                    "resolved_conflict_count": 0,
                },
                before_data=before,
                after_data={
                    "field_status": "MATCHED",
                    "effective_value": validation.normalized_value,
                },
                detail="U.S. supported preparation field explicitly confirmed by a customer bulk action.",
            )
            accepted += 1

        operation_status = refresh_us_lacey_operation_status(
            session,
            organization_id=org_id,
            operation=operation,
        )
        session.commit()
        return UsLaceyBulkAcceptResult(
            accepted_count=accepted,
            operation_status=operation_status,
        )
    except (UsLaceyReviewError, UsLaceyOperationNotFound):
        session.rollback()
        raise
    except Exception as exc:
        session.rollback()
        raise UsLaceyReviewError("Unable to accept supported suggestions.") from exc
    finally:
        session.close()
