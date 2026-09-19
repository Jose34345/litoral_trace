"""Bounded asynchronous orchestration for Lacey extraction specialists.

The specialist implementations remain synchronous.  Phase 6 isolates those calls with
``asyncio.to_thread`` and bounds concurrent provider work with a semaphore so the event
loop stays responsive and external rate limits are not flooded.
"""
from __future__ import annotations

import asyncio
from dataclasses import replace
import os
from typing import Callable, Mapping, Protocol

from ..ai_shadow import AIShadowError
from .candidate_admission import CandidateAdmissionEvaluation
from .contracts import (
    CandidateEnvelope,
    MultiAgentExtractionResult,
    OperationStatus,
    SpecialistExecutionState,
    SpecialistExecutionStatus,
    SpecialistFailure,
    SpecialistResult,
    SpecialistRole,
)
from .field_judge import (
    FIELD_JUDGE_VERSION,
    FieldJudgeDecision,
    FieldJudgeEvaluation,
    FieldJudgeMode,
    candidate_identity,
    validate_field_judge_decisions,
)
from .fusion import fuse_candidates
from .line_binding import bind_line_items
from .router import RoutingPlan
from .specialist_runtime import SpecialistInputDocument


_DEFAULT_SPECIALIST_CONCURRENCY = 2
_MAX_SPECIALIST_CONCURRENCY = 3
_MIN_SPECIALIST_CONCURRENCY = 1
_ENV_SPECIALIST_CONCURRENCY = "LT_AI_SPECIALIST_CONCURRENCY"
CandidateVerifier = Callable[
    [tuple[CandidateEnvelope, ...]], tuple[CandidateEnvelope, ...]
]
CandidateJudge = Callable[[tuple[CandidateEnvelope, ...]], FieldJudgeEvaluation]
CandidateAdmitter = Callable[
    [tuple[CandidateEnvelope, ...]], CandidateAdmissionEvaluation
]


class SpecialistExtractor(Protocol):
    role: SpecialistRole

    def extract(self, documents: tuple[SpecialistInputDocument, ...]) -> SpecialistResult: ...


def specialist_concurrency_limit(raw: str | int | None = None) -> int:
    """Return a safe specialist concurrency limit in the inclusive range 1..3."""
    value: str | int
    if raw is None:
        value = os.getenv(_ENV_SPECIALIST_CONCURRENCY, str(_DEFAULT_SPECIALIST_CONCURRENCY))
    else:
        value = raw
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = _DEFAULT_SPECIALIST_CONCURRENCY
    return max(_MIN_SPECIALIST_CONCURRENCY, min(_MAX_SPECIALIST_CONCURRENCY, parsed))


async def run_specialist(
    extractor: SpecialistExtractor,
    documents: tuple[SpecialistInputDocument, ...],
    *,
    semaphore: asyncio.Semaphore,
) -> SpecialistResult | SpecialistFailure:
    """Run one synchronous specialist off-loop and contain safe provider failures."""
    async with semaphore:
        try:
            return await asyncio.to_thread(extractor.extract, documents)
        except AIShadowError as exc:
            return SpecialistFailure(role=extractor.role, error=str(exc))


async def orchestrate_specialists(
    *,
    routing_plan: RoutingPlan,
    documents: tuple[SpecialistInputDocument, ...],
    extractors: Mapping[SpecialistRole, SpecialistExtractor],
    concurrency: int | None = None,
    candidate_verifier: CandidateVerifier | None = None,
    candidate_admitter: CandidateAdmitter | None = None,
    field_judge_mode: FieldJudgeMode = FieldJudgeMode.OFF,
    candidate_judge: CandidateJudge | None = None,
) -> MultiAgentExtractionResult:
    """Execute specialists and apply the Judge after binding, before deterministic fusion."""
    limit = specialist_concurrency_limit(concurrency)
    semaphore = asyncio.Semaphore(limit)
    requested_roles = _requested_roles(routing_plan)
    sources_by_routed = {source.routed: source for source in documents}

    immediate_failures: dict[SpecialistRole, SpecialistFailure] = {}
    runnable: dict[
        SpecialistRole,
        tuple[SpecialistExtractor, tuple[SpecialistInputDocument, ...]],
    ] = {}

    for role in requested_roles:
        extractor = extractors.get(role)
        if extractor is None:
            immediate_failures[role] = SpecialistFailure(
                role=role,
                error="MISSING_SPECIALIST_EXTRACTOR",
            )
            continue

        role_sources: list[SpecialistInputDocument] = []
        missing_source = False
        for assignment in routing_plan.assignments:
            if role not in assignment.specialists:
                continue
            source = sources_by_routed.get(assignment.document)
            if source is None:
                missing_source = True
                break
            role_sources.append(source)

        if missing_source or not role_sources:
            immediate_failures[role] = SpecialistFailure(
                role=role,
                error="MISSING_SPECIALIST_INPUT",
            )
            continue
        runnable[role] = (extractor, tuple(role_sources))

    tasks: dict[SpecialistRole, asyncio.Task[SpecialistResult | SpecialistFailure]] = {}
    async with asyncio.TaskGroup() as task_group:
        for role in requested_roles:
            item = runnable.get(role)
            if item is None:
                continue
            extractor, role_documents = item
            tasks[role] = task_group.create_task(
                run_specialist(
                    extractor,
                    role_documents,
                    semaphore=semaphore,
                ),
                name=f"lacey-specialist-{role.value.lower()}",
            )

    successes: list[SpecialistResult] = []
    failures: list[SpecialistFailure] = []
    statuses: list[SpecialistExecutionStatus] = []

    for role in requested_roles:
        outcome: SpecialistResult | SpecialistFailure
        if role in immediate_failures:
            outcome = immediate_failures[role]
        else:
            outcome = tasks[role].result()

        if isinstance(outcome, SpecialistFailure):
            failures.append(outcome)
            statuses.append(
                SpecialistExecutionStatus(
                    role=role,
                    status=SpecialistExecutionState.FAILED,
                    error=outcome.error,
                )
            )
        else:
            successes.append(outcome)
            statuses.append(
                SpecialistExecutionStatus(
                    role=role,
                    status=SpecialistExecutionState.COMPLETED,
                )
            )

    candidates = tuple(
        candidate
        for specialist_result in successes
        for candidate in specialist_result.candidates
    )
    if candidate_verifier is not None:
        candidates = candidate_verifier(candidates)

    # Safety invariant: exact evidence verification -> line binding -> deterministic
    # admission -> semantic Judge -> deterministic fusion. Admission cannot establish
    # canonical truth; it only prevents unsafe candidates from entering later stages.
    bound_candidates = bind_line_items(candidates)
    candidate_admission = (
        candidate_admitter(bound_candidates)
        if candidate_admitter is not None
        else None
    )
    admitted_candidates = (
        candidate_admission.admitted_candidates
        if candidate_admission is not None
        else bound_candidates
    )
    effective_judge_mode = FieldJudgeMode(field_judge_mode)
    judge_evaluation: FieldJudgeEvaluation | None = None
    fusion_candidates = admitted_candidates

    if effective_judge_mode is not FieldJudgeMode.OFF:
        if candidate_judge is None:
            judge_evaluation = FieldJudgeEvaluation(
                version=FIELD_JUDGE_VERSION,
                mode=effective_judge_mode,
                provider="none",
                model="none",
                latency_ms=None,
                input_tokens=None,
                output_tokens=None,
                total_tokens=None,
                decisions=validate_field_judge_decisions(admitted_candidates, ()),
                safe_error="FIELD_JUDGE_UNAVAILABLE",
            )
        else:
            try:
                raw_evaluation = candidate_judge(admitted_candidates)
                judge_evaluation = replace(
                    raw_evaluation,
                    version=FIELD_JUDGE_VERSION,
                    mode=effective_judge_mode,
                    decisions=validate_field_judge_decisions(
                        admitted_candidates,
                        raw_evaluation.decisions,
                    ),
                )
            except Exception as exc:  # Judge is a bounded, non-fatal safety subsystem.
                judge_evaluation = FieldJudgeEvaluation(
                    version=FIELD_JUDGE_VERSION,
                    mode=effective_judge_mode,
                    provider="unknown",
                    model="unknown",
                    latency_ms=None,
                    input_tokens=None,
                    output_tokens=None,
                    total_tokens=None,
                    decisions=validate_field_judge_decisions(admitted_candidates, ()),
                    safe_error=str(exc),
                )

        if effective_judge_mode is FieldJudgeMode.ENFORCE:
            accepted_ids = {
                decision.candidate_id
                for decision in judge_evaluation.decisions
                if decision.decision is FieldJudgeDecision.ACCEPT
            }
            fusion_candidates = tuple(
                candidate
                for candidate in admitted_candidates
                if candidate_identity(candidate) in accepted_ids
            )

    fused = fuse_candidates(fusion_candidates)

    return MultiAgentExtractionResult(
        routing_plan=routing_plan,
        specialist_results=tuple(successes),
        fused_candidates=fused.fused_candidates,
        partial_failures=tuple(failures),
        operation=_operation_status(successes=successes, failures=failures),
        specialist_statuses=tuple(statuses),
        field_judge=judge_evaluation,
        fusion_conflicts=fused.conflicts,
        candidate_admission=candidate_admission,
    )


def _requested_roles(routing_plan: RoutingPlan) -> tuple[SpecialistRole, ...]:
    present = {
        role
        for assignment in routing_plan.assignments
        for role in assignment.specialists
    }
    return tuple(role for role in SpecialistRole if role in present)


def _operation_status(
    *,
    successes: list[SpecialistResult],
    failures: list[SpecialistFailure],
) -> OperationStatus:
    if failures and successes:
        return OperationStatus.PARTIAL
    if failures:
        return OperationStatus.FAILED
    return OperationStatus.COMPLETED
