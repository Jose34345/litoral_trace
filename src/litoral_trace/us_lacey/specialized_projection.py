"""Fail-safe planning and projection gates for specialized U.S. Lacey output.

Specialized output is non-authoritative.  This module may expose an evidence-backed
value as an unconfirmed ``FOUND`` suggestion only when every deterministic safety gate
passes.  It never marks a value human-reviewed or accepted and defaults to ``off``.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import hashlib
import logging
import os
import re
from typing import Iterable, Mapping, Sequence
from uuid import UUID

from sqlalchemy import select

from litoral_trace.db.models import (
    UsLaceyOperationField,
    UsLaceyPlantDeclaration,
    UsLaceyPpqPlantLine,
)
from litoral_trace.lacey_engine.domain import EvidenceClass
from litoral_trace.lacey_engine.multi_agent.contracts import CandidateEnvelope
from litoral_trace.lacey_engine.multi_agent.field_judge import (
    FieldJudgeDecision,
    FieldJudgeEvaluation,
    FieldJudgeMode,
    candidate_identity as field_judge_candidate_identity,
)
from litoral_trace.lacey_engine.multi_agent.line_binding import LINE_SCOPED_FIELDS
from litoral_trace.us_lacey.ppq505 import (
    PPQ505_FIELDS_BY_KEY,
    PPQ505_PLANT_FIELDS,
    PPQ505_SHIPMENT_REFERENCE,
    PpqScope,
    PpqValidationStatus,
    validate_ppq_value,
)


LOGGER = logging.getLogger(__name__)
_SPECIALIZED_PROJECTION_MODE_ENV = "LT_AI_SPECIALIZED_PROJECTION_MODE"
SPECIALIZED_PROJECTION_VERSION = "lacey_specialized_projection_v1"

_ROW_KEY = re.compile(
    r"^ROW:(?P<document>[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}):P(?P<page>[1-9][0-9]*):"
    r"T(?P<table>[A-Z0-9._/-]+):R(?P<row>[0-9]+)$"
)
_LINE_KEY = re.compile(r"^LINE:[1-9][0-9]*$")
_SKU_KEY = re.compile(r"^SKU:[A-Z0-9][A-Z0-9._/-]*$", re.IGNORECASE)
_FINGERPRINT_KEY = re.compile(r"^FP:[A-F0-9]{24,64}$", re.IGNORECASE)
_SPECIALIZED_TO_PPQ_FIELD = {"description": "merchandise_description"}


class SpecializedProjectionMode(str, Enum):
    OFF = "off"
    SHADOW = "shadow"
    ENFORCE = "enforce"


@dataclass(frozen=True, slots=True)
class PlannedPlantLine:
    line_item_key: str
    line_reference: str


@dataclass(frozen=True, slots=True)
class LineMaterializationPlan:
    line_references: tuple[str, ...]
    generated_lines: tuple[PlannedPlantLine, ...]
    review_only_line_keys: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class SpecializedProjectionResult:
    mode: SpecializedProjectionMode
    eligible_count: int
    projected_count: int
    review_count: int
    skipped_count: int


def specialized_projection_mode(
    environ: Mapping[str, str] | None = None,
) -> SpecializedProjectionMode:
    """Read the closed projection mode and fail safely to ``off``."""
    source = os.environ if environ is None else environ
    raw = str(source.get(_SPECIALIZED_PROJECTION_MODE_ENV, "off") or "off").strip().lower()
    try:
        return SpecializedProjectionMode(raw)
    except ValueError:
        LOGGER.warning(
            "Invalid %s=%r; defaulting to off.",
            _SPECIALIZED_PROJECTION_MODE_ENV,
            raw,
        )
        return SpecializedProjectionMode.OFF


def _stable_line_key(value: str) -> bool:
    return bool(
        _SKU_KEY.fullmatch(value)
        or _LINE_KEY.fullmatch(value)
        or _ROW_KEY.fullmatch(value)
    )


def _derived_line_reference(line_item_key: str) -> str:
    digest = hashlib.sha256(line_item_key.encode("utf-8")).hexdigest()[:20].upper()
    return f"LT-{digest}"


def plan_line_materialization(
    candidates: Iterable[CandidateEnvelope],
    *,
    existing_line_references: tuple[str, ...],
) -> LineMaterializationPlan:
    """Plan deterministic missing plant lines without changing existing human order.

    Only line-scoped candidates participate. ``SKU``, positive ``LINE`` and complete
    ``ROW`` identities are stable enough for deterministic materialization. ``FP`` and
    any unknown/malformed non-empty identity remain review-only in V1.
    """
    materializable: set[str] = set()
    review_only: set[str] = set()

    for envelope in candidates:
        if envelope.candidate.field_key not in LINE_SCOPED_FIELDS:
            continue
        raw_key = str(envelope.line_item_key or "").strip()
        if not raw_key:
            continue
        if _stable_line_key(raw_key):
            materializable.add(raw_key)
        elif _FINGERPRINT_KEY.fullmatch(raw_key) or raw_key:
            review_only.add(raw_key)

    existing = tuple(existing_line_references)
    existing_set = set(existing)
    generated: list[PlannedPlantLine] = []
    for line_item_key in sorted(materializable):
        line_reference = _derived_line_reference(line_item_key)
        if line_reference in existing_set:
            continue
        generated.append(
            PlannedPlantLine(
                line_item_key=line_item_key,
                line_reference=line_reference,
            )
        )
        existing_set.add(line_reference)

    return LineMaterializationPlan(
        line_references=existing + tuple(item.line_reference for item in generated),
        generated_lines=tuple(generated),
        review_only_line_keys=tuple(sorted(review_only)),
    )


def materialize_planned_plant_lines(
    session,
    *,
    organization_id: int,
    operation: object,
    plan: LineMaterializationPlan,
) -> tuple[str, ...]:
    """Create only the missing deterministic plant-line skeletons from ``plan``.

    Materialization is intentionally structural: it creates an empty declaration and
    the normal PPQ operation-field slots, but it does not project any specialist value.
    Existing human-created lines are never reordered, renamed, or deleted. Re-running
    the same plan is idempotent because the current line references are re-read first.
    """
    org_id = int(organization_id)
    operation_id = int(getattr(operation, "id"))
    existing_lines = session.scalars(
        select(UsLaceyPpqPlantLine)
        .where(
            UsLaceyPpqPlantLine.organization_id == org_id,
            UsLaceyPpqPlantLine.operation_id == operation_id,
        )
        .order_by(UsLaceyPpqPlantLine.ordinal.asc(), UsLaceyPpqPlantLine.id.asc())
    ).all()

    existing_references = {str(line.line_reference) for line in existing_lines}
    next_ordinal = max((int(line.ordinal) for line in existing_lines), default=0) + 1
    created: list[str] = []

    for planned in plan.generated_lines:
        line_reference = str(planned.line_reference).strip()
        if not line_reference or line_reference in existing_references:
            continue

        plant_line = UsLaceyPpqPlantLine(
            organization_id=org_id,
            operation_id=operation_id,
            line_reference=line_reference,
            ordinal=next_ordinal,
        )
        session.add(plant_line)
        session.flush()
        session.add(
            UsLaceyPlantDeclaration(
                organization_id=org_id,
                plant_line_id=plant_line.id,
                ordinal=1,
            )
        )
        for field_contract in PPQ505_PLANT_FIELDS:
            session.add(
                UsLaceyOperationField(
                    organization_id=org_id,
                    operation_id=operation_id,
                    merchandise_line_reference=line_reference,
                    field_name=field_contract.key,
                    field_scope="PLANT_LINE",
                    plant_line_id=plant_line.id,
                    field_status="MISSING",
                    validation_status="MISSING",
                    confidence=0.0,
                )
            )

        existing_references.add(line_reference)
        created.append(line_reference)
        next_ordinal += 1

    setattr(operation, "merchandise_line_count", len(existing_references))
    return tuple(created)


def _ppq_field_key(candidate: CandidateEnvelope) -> str | None:
    source_key = candidate.candidate.field_key
    target_key = _SPECIALIZED_TO_PPQ_FIELD.get(source_key, source_key)
    return target_key if target_key in PPQ505_FIELDS_BY_KEY else None


def _target_reference(candidate: CandidateEnvelope, *, ppq_field_key: str) -> str | None:
    field = PPQ505_FIELDS_BY_KEY[ppq_field_key]
    if field.scope is PpqScope.SHIPMENT:
        # A line-bound candidate cannot silently collapse into a shipment field.
        if candidate.line_item_key is not None:
            return None
        return PPQ505_SHIPMENT_REFERENCE

    raw_key = str(candidate.line_item_key or "").strip()
    if not raw_key or not _stable_line_key(raw_key):
        return None
    return _derived_line_reference(raw_key)


def _target_is_unreviewed(target: object) -> bool:
    return getattr(target, "reviewed_at", None) is None and not str(
        getattr(target, "human_value", None) or ""
    ).strip()


def _target_is_empty(target: object) -> bool:
    return not str(
        getattr(target, "normalized_value", None)
        or getattr(target, "original_value", None)
        or ""
    ).strip()


def _judge_allows(
    candidate: CandidateEnvelope,
    evaluation: FieldJudgeEvaluation | None,
) -> bool | None:
    """Return True=ACCEPT, False=review, None=Judge does not gate this run."""
    if evaluation is None or evaluation.mode is not FieldJudgeMode.ENFORCE:
        return None
    candidate_id = field_judge_candidate_identity(candidate)
    matches = [
        item for item in evaluation.decisions if item.candidate_id == candidate_id
    ]
    if len(matches) != 1:
        return False
    record = matches[0]
    if record.field_key != candidate.candidate.field_key:
        return False
    if record.line_item_key != candidate.line_item_key:
        return False
    return record.decision is FieldJudgeDecision.ACCEPT


def project_specialized_candidates(
    *,
    candidates: Iterable[CandidateEnvelope],
    targets: Sequence[object],
    mode: SpecializedProjectionMode | str | None = None,
    source_assurance_by_document: Mapping[UUID, int] | None = None,
    conflict_keys: frozenset[tuple[str, str | None]] = frozenset(),
    judge_evaluation: FieldJudgeEvaluation | None = None,
) -> SpecializedProjectionResult:
    """Apply the pure safety gate and optionally expose unconfirmed ``FOUND`` values.

    ``shadow`` executes the complete eligibility logic but leaves targets untouched.
    ``enforce`` may fill only an empty, unreviewed target with a PPQ-valid value.  Human
    review metadata is never written here.
    """
    effective_mode = (
        specialized_projection_mode()
        if mode is None
        else SpecializedProjectionMode(mode)
    )
    if effective_mode is SpecializedProjectionMode.OFF:
        return SpecializedProjectionResult(effective_mode, 0, 0, 0, 0)

    target_index: dict[tuple[str, str], list[object]] = {}
    for target in targets:
        target_index.setdefault(
            (
                str(getattr(target, "field_name", "")),
                str(getattr(target, "merchandise_line_reference", "")),
            ),
            [],
        ).append(target)

    source_map = source_assurance_by_document or {}
    eligible = 0
    projected = 0
    review = 0
    skipped = 0

    for envelope in candidates:
        candidate = envelope.candidate
        ppq_field_key = _ppq_field_key(envelope)
        if ppq_field_key is None:
            skipped += 1
            continue
        if not candidate.evidence_verified or candidate.evidence_class is EvidenceClass.INFERRED:
            skipped += 1
            continue

        line_reference = _target_reference(envelope, ppq_field_key=ppq_field_key)
        if line_reference is None:
            skipped += 1
            continue

        field_definition = PPQ505_FIELDS_BY_KEY[ppq_field_key]
        expected_scope = field_definition.scope.value
        matches = target_index.get((ppq_field_key, line_reference), [])
        if len(matches) != 1:
            skipped += 1
            continue
        target = matches[0]
        if str(getattr(target, "field_scope", "")) != expected_scope:
            skipped += 1
            continue
        if not _target_is_unreviewed(target):
            skipped += 1
            continue

        validation = validate_ppq_value(ppq_field_key, candidate.value)
        if (
            validation.status is not PpqValidationStatus.VALID
            or not validation.normalized_value
        ):
            skipped += 1
            continue

        conflict_key = (candidate.field_key, envelope.line_item_key)
        if conflict_key in conflict_keys:
            review += 1
            continue

        judge_gate = _judge_allows(envelope, judge_evaluation)
        if judge_gate is False:
            review += 1
            continue

        existing_value = str(
            getattr(target, "normalized_value", None)
            or getattr(target, "original_value", None)
            or ""
        ).strip()
        if existing_value and existing_value != validation.normalized_value:
            review += 1
            continue

        eligible += 1
        if effective_mode is not SpecializedProjectionMode.ENFORCE:
            continue
        if not _target_is_empty(target):
            # Same-value reruns are idempotent and do not claim a new projection.
            continue

        target.original_value = candidate.value
        target.normalized_value = validation.normalized_value
        target.field_status = "FOUND"
        target.confidence = float(candidate.confidence)
        target.source_assurance_document_id = source_map.get(envelope.document_id)
        target.source_page = int(candidate.page)
        target.source_locator = f"specialized:{candidate.source_text[:1500]}"
        target.extractor = "specialized-field-judge"
        target.extractor_version = SPECIALIZED_PROJECTION_VERSION
        target.validation_status = "VALID"
        target.validation_error = None
        projected += 1

    return SpecializedProjectionResult(
        mode=effective_mode,
        eligible_count=eligible,
        projected_count=projected,
        review_count=review,
        skipped_count=skipped,
    )
