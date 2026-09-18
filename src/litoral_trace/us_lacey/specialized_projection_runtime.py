"""Transactional runtime bridge for specialized U.S. Lacey suggestions.

This module is deliberately fail-closed.  It can materialize deterministic plant-line
skeletons and expose evidence-backed specialized values only when the separate
projection mode is explicitly set to ``enforce``.  Projected values remain unconfirmed
``FOUND`` suggestions and their audit candidates remain ``PENDING``.
"""
from __future__ import annotations

import hashlib
import json
from typing import Mapping
from uuid import UUID

from sqlalchemy import select

from litoral_trace.db.models import (
    UsLaceyFieldCandidate,
    UsLaceyOperation,
    UsLaceyOperationField,
    UsLaceyPpqPlantLine,
)
from litoral_trace.lacey_engine.multi_agent.contracts import CandidateEnvelope
from litoral_trace.lacey_engine.multi_agent.field_judge import FieldJudgeEvaluation
from litoral_trace.lacey_engine.multi_agent.fusion import FusionConflict
from litoral_trace.us_lacey import specialized_projection


def _is_empty_target(target: UsLaceyOperationField) -> bool:
    return not str(target.normalized_value or target.original_value or "").strip()


def _candidate_fingerprint(target: UsLaceyOperationField) -> str:
    payload = {
        "projection_version": specialized_projection.SPECIALIZED_PROJECTION_VERSION,
        "operation_field_id": int(target.id),
        "normalized_value": str(target.normalized_value or ""),
        "original_value": str(target.original_value or ""),
        "source_assurance_document_id": int(target.source_assurance_document_id),
        "source_page": target.source_page,
        "source_locator": str(target.source_locator or ""),
        "extractor": str(target.extractor or ""),
        "extractor_version": str(target.extractor_version or ""),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _persist_pending_candidate(
    session,
    *,
    organization_id: int,
    operation_id: int,
    target: UsLaceyOperationField,
) -> bool:
    if target.source_assurance_document_id is None:
        raise RuntimeError("SPECIALIZED_PROJECTION_MISSING_SOURCE_ASSURANCE")
    if not target.original_value or not target.normalized_value:
        raise RuntimeError("SPECIALIZED_PROJECTION_MISSING_VALUE")
    if target.extractor != "specialized-field-judge":
        raise RuntimeError("SPECIALIZED_PROJECTION_UNEXPECTED_EXTRACTOR")

    fingerprint = _candidate_fingerprint(target)
    existing = session.scalar(
        select(UsLaceyFieldCandidate.id).where(
            UsLaceyFieldCandidate.organization_id == organization_id,
            UsLaceyFieldCandidate.operation_field_id == target.id,
            UsLaceyFieldCandidate.fingerprint == fingerprint,
        )
    )
    if existing is not None:
        return False

    session.add(
        UsLaceyFieldCandidate(
            organization_id=organization_id,
            operation_id=operation_id,
            operation_field_id=target.id,
            original_value=target.original_value,
            normalized_value=target.normalized_value,
            confidence=float(target.confidence or 0.0),
            source_assurance_document_id=target.source_assurance_document_id,
            source_page=target.source_page,
            source_locator=target.source_locator,
            extractor="specialized-field-judge",
            extractor_version=specialized_projection.SPECIALIZED_PROJECTION_VERSION,
            fingerprint=fingerprint,
            validation_status="VALID",
            validation_error=None,
            decision="PENDING",
        )
    )
    return True


def apply_specialized_projection_runtime(
    session,
    *,
    organization_id: int,
    operation_id: int,
    candidates: tuple[CandidateEnvelope, ...],
    fusion_conflicts: tuple[FusionConflict, ...],
    judge_evaluation: FieldJudgeEvaluation | None,
    source_assurance_by_document: Mapping[UUID, int],
) -> dict[str, object] | None:
    """Run projection gates and persistence in the caller's transaction.

    ``None`` means projection is disabled and preserves the pre-bridge behavior exactly.
    In ``shadow`` the full eligibility calculation runs but no operational row changes.
    In ``enforce`` only stable missing lines are materialized and newly projected fields
    receive one deterministic, idempotent PENDING candidate record.
    """
    mode = specialized_projection.specialized_projection_mode()
    if mode is specialized_projection.SpecializedProjectionMode.OFF:
        return None

    operation = session.scalar(
        select(UsLaceyOperation).where(
            UsLaceyOperation.organization_id == organization_id,
            UsLaceyOperation.id == operation_id,
        )
    )
    if operation is None:
        raise RuntimeError("SPECIALIZED_PROJECTION_OPERATION_NOT_FOUND")

    existing_line_references = tuple(
        session.scalars(
            select(UsLaceyPpqPlantLine.line_reference)
            .where(
                UsLaceyPpqPlantLine.organization_id == organization_id,
                UsLaceyPpqPlantLine.operation_id == operation_id,
            )
            .order_by(UsLaceyPpqPlantLine.ordinal.asc(), UsLaceyPpqPlantLine.id.asc())
        ).all()
    )
    plan = specialized_projection.plan_line_materialization(
        candidates,
        existing_line_references=existing_line_references,
    )

    materialized_line_references: tuple[str, ...] = ()
    if mode is specialized_projection.SpecializedProjectionMode.ENFORCE:
        materialized_line_references = specialized_projection.materialize_planned_plant_lines(
            session,
            organization_id=organization_id,
            operation=operation,
            plan=plan,
        )
        # Make newly materialized PPQ field slots visible to the projection query.
        session.flush()

    targets = tuple(
        session.scalars(
            select(UsLaceyOperationField)
            .where(
                UsLaceyOperationField.organization_id == organization_id,
                UsLaceyOperationField.operation_id == operation_id,
            )
            .order_by(UsLaceyOperationField.id.asc())
        ).all()
    )
    initially_empty = {
        int(target.id)
        for target in targets
        if _is_empty_target(target)
    }
    conflict_keys = frozenset(
        (
            conflict.key.field_key,
            conflict.key.line_item_key,
            conflict.key.unbound_identity,
        )
        for conflict in fusion_conflicts
    )

    result = specialized_projection.project_specialized_candidates(
        candidates=candidates,
        targets=targets,
        mode=mode,
        source_assurance_by_document=source_assurance_by_document,
        conflict_keys=conflict_keys,
        judge_evaluation=judge_evaluation,
    )

    if mode is specialized_projection.SpecializedProjectionMode.ENFORCE:
        newly_projected = tuple(
            target
            for target in targets
            if int(target.id) in initially_empty
            and target.field_status == "FOUND"
            and target.extractor == "specialized-field-judge"
            and target.extractor_version
            == specialized_projection.SPECIALIZED_PROJECTION_VERSION
            and bool(str(target.normalized_value or "").strip())
        )
        if len(newly_projected) != result.projected_count:
            raise RuntimeError("SPECIALIZED_PROJECTION_AUDIT_COUNT_MISMATCH")
        for target in newly_projected:
            _persist_pending_candidate(
                session,
                organization_id=organization_id,
                operation_id=operation_id,
                target=target,
            )

    return {
        "version": specialized_projection.SPECIALIZED_PROJECTION_VERSION,
        "mode": mode.value,
        "eligible_count": result.eligible_count,
        "projected_count": result.projected_count,
        "review_count": result.review_count,
        "skipped_count": result.skipped_count,
        "planned_line_count": len(plan.generated_lines),
        "materialized_line_count": len(materialized_line_references),
    }
