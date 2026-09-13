"""Bounded asynchronous orchestration for Lacey extraction specialists.

The specialist implementations remain synchronous.  Phase 6 isolates those calls with
``asyncio.to_thread`` and bounds concurrent provider work with a semaphore so the event
loop stays responsive and external rate limits are not flooded.
"""
from __future__ import annotations

import asyncio
import os
from typing import Callable, Mapping, Protocol

from ..ai_shadow import AIShadowError
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
) -> MultiAgentExtractionResult:
    """Execute routed specialists concurrently while preserving partial successes."""
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
    bound_candidates = bind_line_items(candidates)
    fused = fuse_candidates(bound_candidates)

    return MultiAgentExtractionResult(
        routing_plan=routing_plan,
        specialist_results=tuple(successes),
        fused_candidates=fused.fused_candidates,
        partial_failures=tuple(failures),
        operation=_operation_status(successes=successes, failures=failures),
        specialist_statuses=tuple(statuses),
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
