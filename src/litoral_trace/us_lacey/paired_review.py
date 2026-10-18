"""One explicit, atomic reviewer decision for a net plant mass and its unit.

This is not auto-approval: two current, source-linked fields are reviewed by
one authenticated user action, using the existing per-field audit writer.
No change to documentary authority or Assurance V2 lab activation.
"""
from __future__ import annotations

import re
from uuid import UUID

from sqlalchemy import func, select

from litoral_trace.db.models import (
    ReconciliationIssue, UsLaceyFieldCandidate, UsLaceyOperation,
    UsLaceyOperationField, UsLaceyProcessingJob, UsLaceySourceSetRevision,
)
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.us_lacey.assurance_v2_review_adapter import review_journal_enabled
from litoral_trace.us_lacey.db import get_us_lacey_db_session
from litoral_trace.us_lacey.operations import UsLaceyOperationNotFound
from litoral_trace.us_lacey.ppq505 import validate_ppq_value
from litoral_trace.us_lacey.review import UsLaceyReviewError, review_us_lacey_field
from litoral_trace.us_lacey.review_source_authority import reviewed_source_class


def _validated_pair(fields: tuple, *, expected: tuple[str, str],
                    locators: tuple[str, str]) -> None:
    if len(fields) != 2 or {f.field_name for f in fields} != {"plant_quantity", "metric_unit"}:
        raise UsLaceyReviewError("Select a plant quantity and its unit from one line.")
    by_name = {f.field_name: f for f in fields}
    quantity, unit = by_name["plant_quantity"], by_name["metric_unit"]
    for field, displayed, locator in zip((quantity, unit), expected, locators):
        if (
            field.field_status != "REVIEW" or field.reviewed_at is not None
            or field.human_value is not None or field.field_scope != "PLANT_LINE"
            or field.extractor != "canonical-shipment-truth"
            or not field.source_locator or not field.source_locator.startswith("canonical:")
            or field.source_locator != locator
            or field.normalized_value != displayed
        ):
            raise UsLaceyReviewError("Review changed. Reload and inspect current evidence.")
        result = validate_ppq_value(field.field_name, field.normalized_value)
        if result.status.value != "VALID":
            raise UsLaceyReviewError("One of these values requires individual review.")
    if (
        quantity.operation_id != unit.operation_id
        or quantity.plant_line_id is None or quantity.plant_line_id != unit.plant_line_id
        or quantity.merchandise_line_reference != unit.merchandise_line_reference
        or quantity.source_assurance_document_id is None
        or quantity.source_assurance_document_id != unit.source_assurance_document_id
        or quantity.source_page is None or quantity.source_page != unit.source_page
    ):
        raise UsLaceyReviewError("Quantity and unit must belong to one line and original document.")


def confirm_plant_mass_pair(
    *, organization_id: int, operation_public_id: str, user_id: int,
    user_email: str, quantity_id: int, unit_id: int,
    expected_quantity: str, expected_unit: str,
    quantity_locator: str, unit_locator: str,
    session_factory=get_us_lacey_db_session,
) -> int:
    """Confirm both linked fields or neither, preserving separate field audit events."""
    if quantity_id == unit_id or min(quantity_id, unit_id, user_id, organization_id) <= 0:
        raise UsLaceyReviewError("Invalid reviewer or field selection.")
    try:
        operation_uuid = UUID(str(operation_public_id))
    except (ValueError, TypeError) as exc:
        raise UsLaceyOperationNotFound("Operation not found.") from exc

    db = session_factory()
    try:
        set_tenant_db_context(db, organization_id)
        operation = db.scalar(select(UsLaceyOperation).where(
            UsLaceyOperation.organization_id == organization_id,
            UsLaceyOperation.public_id == operation_uuid,
        ).with_for_update())
        if operation is None:
            raise UsLaceyOperationNotFound("Operation not found.")
        if review_journal_enabled(organization_id, operation.client_reference):
            raise UsLaceyReviewError("This review requires the source-fenced V2 workflow.")
        revision = db.scalar(select(UsLaceySourceSetRevision).where(
            UsLaceySourceSetRevision.organization_id == organization_id,
            UsLaceySourceSetRevision.operation_id == operation.id,
            UsLaceySourceSetRevision.is_current.is_(True),
            UsLaceySourceSetRevision.status == "FINALIZED",
        ).with_for_update())
        if revision is None:
            raise UsLaceyReviewError("Wait until the current document revision is finalized.")
        active = db.scalar(select(func.count(UsLaceyProcessingJob.id)).where(
            UsLaceyProcessingJob.organization_id == organization_id,
            UsLaceyProcessingJob.operation_id == operation.id,
            UsLaceyProcessingJob.status.in_(("QUEUED", "RUNNING", "RETRY", "FAILED")),
        )) or 0
        if active:
            raise UsLaceyReviewError("Document processing must finish before review.")
        fields = tuple(db.scalars(select(UsLaceyOperationField).where(
            UsLaceyOperationField.organization_id == organization_id,
            UsLaceyOperationField.operation_id == operation.id,
            UsLaceyOperationField.id.in_((quantity_id, unit_id)),
        ).order_by(UsLaceyOperationField.id).with_for_update()).all())
        _validated_pair(
            fields, expected=(expected_quantity, expected_unit),
            locators=(quantity_locator, unit_locator),
        )
        by_name = {field.field_name: field for field in fields}
        quantity, unit = by_name["plant_quantity"], by_name["metric_unit"]
        source = reviewed_source_class(
            db, organization_id=organization_id, operation_id=operation.id,
            assurance_document_id=quantity.source_assurance_document_id,
        )
        if source is None or source.code not in {"COMMERCIAL_SOURCE", "UNVERIFIED_COMMERCIAL"}:
            raise UsLaceyReviewError("A current commercial packing source is required.")
        # Do not elevate an UNKNOWN role to a verified commercial role. The
        # original PDF is still offered as evidence for explicit human review.
        # Check current Vault membership independently of any filename-derived
        # authority label. This query never upgrades a stored UNKNOWN role.
        from litoral_trace.db.models import AssuranceDocument, UsLaceyOperationDocument, VaultDocument
        filename = db.execute(select(VaultDocument.original_filename)
            .join(AssuranceDocument, AssuranceDocument.vault_document_id == VaultDocument.id)
            .join(UsLaceyOperationDocument,
                  UsLaceyOperationDocument.assurance_document_id == AssuranceDocument.id)
            .where(UsLaceyOperationDocument.organization_id == organization_id,
                   UsLaceyOperationDocument.operation_id == operation.id,
                   UsLaceyOperationDocument.is_current.is_(True),
                   UsLaceyOperationDocument.assurance_document_id == quantity.source_assurance_document_id,
                   VaultDocument.organization_id == organization_id,
                   VaultDocument.status == "available",
                   VaultDocument.deleted_at.is_(None))).scalar_one_or_none()
        filename = str(filename or "")
        if not re.search(r"\bpacking[\s_-]+list\b", filename.casefold()):
            raise UsLaceyReviewError("Inspect the original packing list and confirm separately.")
        for field in (quantity, unit):
            conflicts = db.scalar(select(func.count(ReconciliationIssue.id)).where(
                ReconciliationIssue.organization_id == organization_id,
                ReconciliationIssue.operation_reference == f"us_lacey:{operation.public_id}",
                ReconciliationIssue.us_lacey_operation_field_id == field.id,
                ReconciliationIssue.status == "OPEN",
            )) or 0
            if conflicts:
                raise UsLaceyReviewError("Conflicting evidence requires an individual decision.")
            candidates = db.scalars(select(UsLaceyFieldCandidate).where(
                UsLaceyFieldCandidate.organization_id == organization_id,
                UsLaceyFieldCandidate.operation_id == operation.id,
                UsLaceyFieldCandidate.operation_field_id == field.id,
            )).all()
            values = {
                validate_ppq_value(field.field_name, c.normalized_value or c.original_value).normalized_value
                for c in candidates
                if (c.normalized_value or c.original_value)
            }
            if len(values) > 1 or (values and field.normalized_value not in values):
                raise UsLaceyReviewError("Ambiguous quantity or unit needs individual review.")
        for field in (quantity, unit):
            review_us_lacey_field(
                organization_id=organization_id, operation_public_id=operation_uuid,
                field_id=field.id, user_id=user_id, user_email=user_email,
                action="accept", _session=db, _defer_commit=True,
            )
        db.commit()
        return 2
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
