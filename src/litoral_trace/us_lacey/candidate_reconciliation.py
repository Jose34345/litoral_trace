"""Resolve duplicate-value U.S. Lacey review evidence deterministically.

This pass is deliberately non-authoritative. It may turn an unreviewed REVIEW
field into FOUND only when an OPEN conflict is proven to be metadata-only, but it
never creates MATCHED. Human confirmation remains the only transition to a
confirmed declaration value.
"""
from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import select

from litoral_trace.db.models import (
    ReconciliationIssue,
    UsLaceyFieldCandidate,
    UsLaceyOperation,
    UsLaceyOperationField,
)
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.us_lacey.candidate_normalization import (
    TaxonomicComparisonContext,
    candidate_comparison_key,
    derive_taxonomic_comparison_context,
    group_candidate_evidence,
)
from litoral_trace.us_lacey.db import get_us_lacey_db_session
from litoral_trace.us_lacey.ppq505 import validate_ppq_value
from litoral_trace.us_lacey.projection import refresh_us_lacey_operation_status


@dataclass(frozen=True, slots=True)
class DuplicateCandidateReconciliationResult:
    promoted_count: int
    resolved_conflict_count: int


def _same_actual_value(
    field_name: str,
    left: object,
    right: object,
    *,
    comparison_context: TaxonomicComparisonContext | None = None,
) -> bool:
    left_key = candidate_comparison_key(
        field_name,
        left,
        comparison_context=comparison_context,
    )
    right_key = candidate_comparison_key(
        field_name,
        right,
        comparison_context=comparison_context,
    )
    return bool(left_key and right_key and left_key == right_key)


def _comparison_context_for_field(
    field: UsLaceyOperationField,
    *,
    fields_by_identity: dict[tuple[str, str], list[UsLaceyOperationField]],
    candidates_by_field: dict[int, list[UsLaceyFieldCandidate]],
) -> TaxonomicComparisonContext | None:
    if str(field.field_name or "").strip().casefold() != "species":
        return None

    line_reference = str(field.merchandise_line_reference or "").strip()
    genus_fields = fields_by_identity.get(("genus", line_reference), [])
    if len(genus_fields) != 1:
        return None

    genus_field = genus_fields[0]
    confirmed_genus = str(genus_field.human_value or "").strip() or None
    genus_candidates = candidates_by_field.get(int(genus_field.id), [])

    if confirmed_genus is not None:
        return derive_taxonomic_comparison_context(
            genus_candidates,
            confirmed_genus=confirmed_genus,
        )

    if genus_candidates:
        return derive_taxonomic_comparison_context(genus_candidates)

    # A persisted FOUND genus may predate candidate-row materialization. Reuse it
    # only when it independently meets the same high-confidence threshold.
    if str(genus_field.field_status or "").upper() == "FOUND":
        return derive_taxonomic_comparison_context((genus_field,))
    return None


def reconcile_duplicate_field_candidates(
    *,
    organization_id: int,
    operation_id: int,
) -> DuplicateCandidateReconciliationResult:
    """Merge corroborating candidate metadata and remove false OPEN conflicts.

    Candidate rows remain stored individually for provenance. Only their logical
    grouping changes: page/confidence differences cannot manufacture a contradiction.
    A field is promoted to FOUND only when it already has one or more OPEN
    reconciliation issues, every one of those issues compares canonically identical
    values, at least two candidate rows corroborate exactly one canonical value, the
    strongest evidence is >= 0.90, and PPQ validation is valid.

    Requiring an actual false-conflict issue is important: ordinary duplicated
    provenance can exist on fields that are intentionally still REVIEW for another
    reason. This pass must not silently broaden the machine-safe surface.
    """
    org_id = int(organization_id)
    session = get_us_lacey_db_session()
    try:
        set_tenant_db_context(session, org_id)
        operation = session.scalar(
            select(UsLaceyOperation).where(
                UsLaceyOperation.organization_id == org_id,
                UsLaceyOperation.id == int(operation_id),
            )
        )
        if operation is None:
            return DuplicateCandidateReconciliationResult(0, 0)

        fields = session.scalars(
            select(UsLaceyOperationField).where(
                UsLaceyOperationField.organization_id == org_id,
                UsLaceyOperationField.operation_id == operation.id,
            )
        ).all()
        candidates = session.scalars(
            select(UsLaceyFieldCandidate)
            .where(
                UsLaceyFieldCandidate.organization_id == org_id,
                UsLaceyFieldCandidate.operation_id == operation.id,
            )
            .order_by(UsLaceyFieldCandidate.id.asc())
        ).all()
        open_issues = session.scalars(
            select(ReconciliationIssue).where(
                ReconciliationIssue.organization_id == org_id,
                ReconciliationIssue.operation_reference == f"us_lacey:{operation.public_id}",
                ReconciliationIssue.status == "OPEN",
            )
        ).all()

        candidates_by_field: dict[int, list[UsLaceyFieldCandidate]] = {}
        for candidate in candidates:
            candidates_by_field.setdefault(int(candidate.operation_field_id), []).append(candidate)
        fields_by_identity: dict[tuple[str, str], list[UsLaceyOperationField]] = {}
        for candidate_field in fields:
            identity = (
                str(candidate_field.field_name or "").strip().casefold(),
                str(candidate_field.merchandise_line_reference or "").strip(),
            )
            fields_by_identity.setdefault(identity, []).append(candidate_field)
        issues_by_field: dict[int, list[ReconciliationIssue]] = {}
        for issue in open_issues:
            if issue.us_lacey_operation_field_id is not None:
                issues_by_field.setdefault(int(issue.us_lacey_operation_field_id), []).append(issue)

        promoted = 0
        resolved = 0
        for field in fields:
            if field.reviewed_at is not None or field.human_value:
                continue

            comparison_context = _comparison_context_for_field(
                field,
                fields_by_identity=fields_by_identity,
                candidates_by_field=candidates_by_field,
            )
            field_issues = issues_by_field.get(int(field.id), [])
            if not field_issues:
                # Duplicate provenance alone is not enough to change REVIEW -> FOUND.
                # We only repair a conflict that the reconciliation layer explicitly
                # created for values that are actually identical.
                continue
            if any(
                not _same_actual_value(
                    field.field_name,
                    issue.left_value,
                    issue.right_value,
                    comparison_context=comparison_context,
                )
                for issue in field_issues
            ):
                # At least one OPEN issue is a genuine value contradiction.
                continue

            rows = candidates_by_field.get(int(field.id), [])
            if len(rows) < 2:
                continue
            groups = group_candidate_evidence(
                field.field_name,
                rows,
                comparison_context=comparison_context,
            )
            if len(groups) != 1:
                # Multiple canonical candidate values are a real decision point.
                continue

            group = groups[0]
            representative = group.representative
            proposed = representative.normalized_value or representative.original_value
            validation = validate_ppq_value(field.field_name, proposed)
            if validation.status.value != "VALID" or not validation.normalized_value:
                continue
            if float(group.confidence) < 0.90:
                continue

            for issue in field_issues:
                issue.status = "RESOLVED"
                issue.resolution_justification = (
                    "Deterministic candidate-equivalence reconciliation: source values differ "
                    "in representation but resolve to the same comparison identity."
                )
                issue.evidence_json = {
                    **(issue.evidence_json or {}),
                    "duplicate_value_reconciled": True,
                    "canonical_value": group.canonical_value,
                    "comparison_mode": (
                        "TAXONOMIC_LINE_CONTEXT"
                        if comparison_context is not None and field.field_name == "species"
                        else "CANONICAL_PPQ_VALUE"
                    ),
                    "comparison_genus": (
                        comparison_context.genus if comparison_context is not None else None
                    ),
                }
                resolved += 1

            field.original_value = representative.original_value
            field.normalized_value = validation.normalized_value
            field.field_status = "FOUND"
            field.validation_status = "VALID"
            field.validation_error = None
            field.confidence = float(group.confidence)
            field.source_assurance_document_id = representative.source_assurance_document_id
            field.source_page = representative.source_page
            field.source_locator = representative.source_locator
            field.extractor = representative.extractor
            field.extractor_version = representative.extractor_version
            promoted += 1

        if promoted or resolved:
            refresh_us_lacey_operation_status(
                session,
                organization_id=org_id,
                operation=operation,
            )
        session.commit()
        return DuplicateCandidateReconciliationResult(promoted, resolved)
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
