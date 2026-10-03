"""Deterministic orchestration for rule-scoped U.S. Lacey assessments.

The engine owns iteration over primary regulatory subjects (botanical plant lines).
Rules receive one subject at a time and may use optional enrichment, but Product
Intelligence/BOM data is never required for a subject to enter the evaluation flow.

This module is intentionally persistence-free. Database/source-set fencing remains
in the regulatory assessment snapshot boundary.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Mapping, Protocol, Sequence

from litoral_trace.us_lacey.regulatory.rules.domain import EvidenceRef, RuleAssessment


@dataclass(frozen=True, slots=True)
class RegulatorySubject:
    """One primary botanical line presented to every registered rule.

    rule_inputs contains already-adapted, evidence-safe facts for specific rules.
    enrichment may contain optional BOM/Product Intelligence context. Neither is
    required for the botanical line itself to be evaluated.
    """

    subject_ref: str
    line_reference: str
    hts10: str | None = None
    article_component: str | None = None
    genus: str | None = None
    species: str | None = None
    country_of_harvest: str | None = None
    quantity: str | None = None
    unit: str | None = None
    evidence_refs: tuple[EvidenceRef, ...] = ()
    rule_inputs: Mapping[str, object] = field(default_factory=dict)
    enrichment: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not str(self.subject_ref).strip():
            raise ValueError("RegulatorySubject.subject_ref must be non-empty.")
        if not str(self.line_reference).strip():
            raise ValueError("RegulatorySubject.line_reference must be non-empty.")


@dataclass(frozen=True, slots=True)
class RegulatoryContext:
    """Immutable orchestration context shared by all rule evaluations."""

    subjects: tuple[RegulatorySubject, ...]
    evaluation_date: date | None = None
    shipment_facts: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        subject_refs = tuple(subject.subject_ref for subject in self.subjects)
        if len(subject_refs) != len(set(subject_refs)):
            raise ValueError("RegulatoryContext subjects must have unique subject_ref values.")


@dataclass(frozen=True, slots=True)
class EvaluatedRule:
    """One rule result bound explicitly to its primary botanical subject."""

    subject_ref: str
    assessment: RuleAssessment

    def __post_init__(self) -> None:
        if not str(self.subject_ref).strip():
            raise ValueError("EvaluatedRule.subject_ref must be non-empty.")


class RegulatoryRule(Protocol):
    """Protocol implemented by deterministic subject-scoped rules."""

    rule_id: str

    def evaluate(
        self,
        *,
        subject: RegulatorySubject,
        context: RegulatoryContext,
    ) -> RuleAssessment | None:
        """Return one rule-scoped result, or None when explicitly not applicable."""


def evaluate_regulatory_rules(
    context: RegulatoryContext,
    *,
    rules: Sequence[RegulatoryRule],
) -> tuple[EvaluatedRule, ...]:
    """Evaluate every primary subject against every rule in deterministic order.

    Ordering is subject-major, then rule order. This is deliberate: the primary
    evaluation inventory comes from botanical lines, never from optional BOM data.

    Rule exceptions are intentionally not swallowed. A broken rule must fail the
    non-canonical regulatory snapshot stage rather than silently omit an assessment
    or manufacture a PASS/INDETERMINATE result without rule-specific evidence.
    """

    rule_ids = tuple(str(rule.rule_id).strip() for rule in rules)
    if any(not rule_id for rule_id in rule_ids):
        raise ValueError("Every RegulatoryRule must expose a non-empty rule_id.")
    if len(rule_ids) != len(set(rule_ids)):
        raise ValueError("RegulatoryRule rule_id values must be unique per evaluation run.")

    evaluated: list[EvaluatedRule] = []
    for subject in context.subjects:
        for rule in rules:
            assessment = rule.evaluate(subject=subject, context=context)
            if assessment is None:
                continue
            if assessment.rule_id != rule.rule_id:
                raise ValueError(
                    "RegulatoryRule returned an assessment with a mismatched rule_id: "
                    f"expected {rule.rule_id!r}, got {assessment.rule_id!r}."
                )
            evaluated.append(
                EvaluatedRule(
                    subject_ref=subject.subject_ref,
                    assessment=assessment,
                )
            )

    return tuple(evaluated)


__all__ = [
    "EvaluatedRule",
    "RegulatoryContext",
    "RegulatoryRule",
    "RegulatorySubject",
    "evaluate_regulatory_rules",
]
