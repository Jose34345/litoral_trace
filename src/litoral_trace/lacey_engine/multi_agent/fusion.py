"""Deterministic candidate fusion by evidence and documentary authority."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping

from .authority import (
    authority_tuple,
    candidate_identity,
    normalized_candidate_value,
)
from .contracts import CandidateEnvelope
from .line_binding import LINE_SCOPED_FIELDS
from .semantic_normalization import semantic_value_key


@dataclass(frozen=True, slots=True)
class FusionKey:
    field_key: str
    line_item_key: str | None
    # Line-scoped candidates that could not be bound must never collapse into one
    # synthetic line. Give each such item a deterministic private grouping identity.
    unbound_identity: str | None = None


@dataclass(frozen=True, slots=True)
class FusionConflict:
    key: FusionKey
    candidates: tuple[CandidateEnvelope, ...]
    normalized_values: tuple[str, ...]
    requires_ai_resolution: bool


@dataclass(frozen=True, slots=True)
class FusionResult:
    fused_candidates: tuple[CandidateEnvelope, ...]
    conflicts: tuple[FusionConflict, ...]


def fusion_key(candidate: CandidateEnvelope) -> FusionKey:
    field_key = candidate.candidate.field_key
    if field_key in LINE_SCOPED_FIELDS and candidate.line_item_key is None:
        return FusionKey(field_key, None, candidate_identity(candidate))
    return FusionKey(field_key, candidate.line_item_key, None)


def _genus_contexts(
    candidates: tuple[CandidateEnvelope, ...],
) -> dict[str, frozenset[str]]:
    by_line: dict[str, set[str]] = {}
    for candidate in candidates:
        if candidate.candidate.field_key != "genus" or candidate.line_item_key is None:
            continue
        genus = normalized_candidate_value(candidate)
        if genus:
            by_line.setdefault(candidate.line_item_key, set()).add(genus)
    return {line_key: frozenset(values) for line_key, values in by_line.items()}


def fuse_candidates(candidates: Iterable[CandidateEnvelope]) -> FusionResult:
    """Fuse candidates using semantic equivalence plus documentary authority."""
    candidate_tuple = tuple(candidates)
    genus_contexts = _genus_contexts(candidate_tuple)
    groups: dict[FusionKey, list[CandidateEnvelope]] = {}
    for candidate in candidate_tuple:
        groups.setdefault(fusion_key(candidate), []).append(candidate)

    fused: list[CandidateEnvelope] = []
    conflicts: list[FusionConflict] = []

    for key in sorted(groups, key=_fusion_key_sort):
        group = groups[key]
        genus_context = (
            genus_contexts.get(key.line_item_key, frozenset())
            if key.field_key == "species" and key.line_item_key is not None
            else frozenset()
        )
        documents_by_value: dict[str, set[object]] = {}
        for candidate in group:
            normalized = normalized_candidate_value(
                candidate,
                genus_context=genus_context,
            )
            documents_by_value.setdefault(normalized, set()).add(candidate.document_id)

        ranked = sorted(
            group,
            key=lambda candidate: (
                authority_tuple(
                    candidate,
                    corroborating_documents=len(
                        documents_by_value[
                            normalized_candidate_value(
                                candidate,
                                genus_context=genus_context,
                            )
                        ]
                    ),
                ),
                candidate_identity(candidate),
            ),
            reverse=True,
        )
        winner = ranked[0]
        fused.append(winner)

        values = tuple(sorted(documents_by_value))
        if len(values) > 1:
            conflicts.append(
                FusionConflict(
                    key=key,
                    candidates=tuple(ranked),
                    normalized_values=values,
                    requires_ai_resolution=_has_comparable_top_conflict(
                        ranked,
                        documents_by_value=documents_by_value,
                        genus_context=genus_context,
                    ),
                )
            )

    return FusionResult(tuple(fused), tuple(conflicts))


def cross_document_fusion_accuracy(
    expected: Mapping[FusionKey, str],
    result: FusionResult,
) -> float:
    """Exact normalized-value accuracy over a labeled fusion set."""
    if not expected:
        return 1.0
    candidate_tuple = tuple(result.fused_candidates)
    genus_contexts = _genus_contexts(candidate_tuple)
    actual = {
        fusion_key(candidate): normalized_candidate_value(
            candidate,
            genus_context=(
                genus_contexts.get(candidate.line_item_key, frozenset())
                if candidate.candidate.field_key == "species"
                and candidate.line_item_key is not None
                else frozenset()
            ),
        )
        for candidate in candidate_tuple
    }
    normalized_expected = {
        key: semantic_value_key(
            key.field_key,
            value,
            genus_context=(
                genus_contexts.get(key.line_item_key, frozenset())
                if key.field_key == "species" and key.line_item_key is not None
                else frozenset()
            ),
        )
        for key, value in expected.items()
    }
    correct = sum(
        1 for key, value in normalized_expected.items() if actual.get(key) == value
    )
    return correct / len(expected)


def _has_comparable_top_conflict(
    ranked: list[CandidateEnvelope],
    *,
    documents_by_value: dict[str, set[object]],
    genus_context: frozenset[str],
) -> bool:
    if len(ranked) < 2:
        return False
    top = ranked[0]
    top_value = normalized_candidate_value(top, genus_context=genus_context)
    top_score = authority_tuple(
        top,
        corroborating_documents=len(documents_by_value[top_value]),
    )
    # Confidence is intentionally excluded from comparability: if two conflicting
    # candidates tie through corroboration, model confidence must not silently settle
    # a regulatory conflict. Phase 5 may select an existing candidate or NEEDS_REVIEW.
    top_structural = top_score[:-1]
    return any(
        normalized_candidate_value(other, genus_context=genus_context) != top_value
        and authority_tuple(
            other,
            corroborating_documents=len(
                documents_by_value[
                    normalized_candidate_value(
                        other,
                        genus_context=genus_context,
                    )
                ]
            ),
        )[:-1]
        == top_structural
        for other in ranked[1:]
    )


def _fusion_key_sort(key: FusionKey) -> tuple[str, str, str]:
    return (key.field_key, key.line_item_key or "", key.unbound_identity or "")
