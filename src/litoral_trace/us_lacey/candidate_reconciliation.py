"""Resolve duplicate-value U.S. Lacey review evidence deterministically.

This pass is deliberately non-authoritative. It may turn unreviewed REVIEW
into FOUND when an OPEN conflict is provably metadata-only, or when one B/L
reference is corroborated by two distinct, source-linked and hash-distinct
original shipping document types. It never creates MATCHED; human confirmation
remains the only transition to a confirmed declaration value.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

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
from litoral_trace.us_lacey.review_source_authority import (
    independent_review_source_identity, is_independent_review_source,
)
from litoral_trace.us_lacey.ppq505 import validate_ppq_value
from litoral_trace.us_lacey.projection import refresh_us_lacey_operation_status


@dataclass(frozen=True, slots=True)
class DuplicateCandidateReconciliationResult:
    promoted_count: int
    resolved_conflict_count: int


def _corroborated_shipping_reference(
    field: object,
    rows: list[UsLaceyFieldCandidate],
    *,
    current_carrier_source: Callable[[int], tuple[str, str] | None],
) -> bool:
    """Machine-support a B/L only with two independent anchored carrier sources.

    This is a FOUND proposal, never MATCHED/confirmed. All other REVIEW fields
    retain the existing human-review policy.
    """
    if (
        str(getattr(field, "field_name", "")) != "bill_of_lading"
        or str(getattr(field, "field_status", "")).upper() != "REVIEW"
        or getattr(field, "reviewed_at", None) is not None
        or getattr(field, "human_value", None)
    ):
        return False
    source_ids = {getattr(row, "source_assurance_document_id", None) for row in rows}
    if len(rows) < 2 or len(source_ids) < 2 or None in source_ids:
        return False
    corroborating_roles: set[str] = set()
    distinct_original_hashes: set[str] = set()
    for row in rows:
        if (
            str(getattr(row, "validation_status", "")).upper() != "VALID"
            or str(getattr(row, "decision", "")).upper() != "PENDING"
            or float(getattr(row, "confidence", 0) or 0) < 0.95
            or not getattr(row, "source_page", None)
            or not str(getattr(row, "source_locator", "") or "").strip()
        ):
            return False
        source_identity = current_carrier_source(int(row.source_assurance_document_id))
        if source_identity is None:
            return False
        role, sha256 = source_identity
        if role not in {"BILL_OF_LADING", "ARRIVAL_NOTICE"} or len(sha256) != 64:
            return False
        corroborating_roles.add(role)
        distinct_original_hashes.add(sha256)
    # Two uploads of the same bytes are never independent corroboration.
    return (
        corroborating_roles == {"BILL_OF_LADING", "ARRIVAL_NOTICE"}
        and len(distinct_original_hashes) >= 2
    )


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
            rows = candidates_by_field.get(int(field.id), [])
            if len(rows) < 2:
                continue
            if field_issues:
                if any(
                    not _same_actual_value(
                        field.field_name,
                        issue.left_value,
                        issue.right_value,
                        comparison_context=comparison_context,
                    )
                    for issue in field_issues
                ):
                    # A genuine value contradiction is never auto-resolved.
                    continue
            elif not _corroborated_shipping_reference(
                field,
                rows,
                current_carrier_source=lambda doc_id: independent_review_source_identity(
                    session, organization_id=org_id, operation_id=operation.id,
                    assurance_document_id=doc_id, field_name=field.field_name,
                ),
            ):
                # No open false-conflict issue: only an exact, high-confidence B/L
                # corroborated by two independently anchored carrier originals
                # can become an unconfirmed FOUND proposal.
                continue
            if field.field_name in {
                "bill_of_lading", "master_bill_of_lading", "house_bill_of_lading",
            } and not all(
                is_independent_review_source(
                    session, organization_id=org_id, operation_id=operation.id,
                    assurance_document_id=row.source_assurance_document_id,
                    field_name=field.field_name,
                )
                for row in rows
            ):
                # A botanical declaration may quote a B/L as context; duplicate
                # references to that declaration cannot automatically resolve a
                # carrier-issued field. Keep the original REVIEW/CONFLICT state.
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
