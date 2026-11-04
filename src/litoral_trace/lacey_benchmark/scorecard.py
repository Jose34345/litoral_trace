"""Deterministic scorecards for the U.S. Lacey Golden Benchmark."""
from __future__ import annotations

from dataclasses import dataclass
import math


@dataclass(frozen=True, slots=True)
class DocumentClassificationScore:
    total: int
    correct: int

    @property
    def accuracy(self) -> float:
        return 1.0 if self.total == 0 else self.correct / self.total


@dataclass(frozen=True, slots=True)
class GoldenBenchmarkScore:
    expected_field_count: int
    observed_field_count: int
    correct_field_count: int
    correct_line_binding_count: int
    line_binding_denominator: int
    correct_evidence_count: int
    evidence_denominator: int
    false_safe_count: int
    human_review_count: int
    latency_samples_ms: tuple[int, ...] = ()
    cost_samples_usd: tuple[float, ...] = ()

    @property
    def field_precision(self) -> float:
        if self.observed_field_count == 0:
            return 1.0 if self.expected_field_count == 0 else 0.0
        return self.correct_field_count / self.observed_field_count

    @property
    def field_recall(self) -> float:
        if self.expected_field_count == 0:
            return 1.0
        return self.correct_field_count / self.expected_field_count

    @property
    def line_binding_accuracy(self) -> float:
        if self.line_binding_denominator == 0:
            return 1.0
        return self.correct_line_binding_count / self.line_binding_denominator

    @property
    def evidence_verification_accuracy(self) -> float:
        if self.evidence_denominator == 0:
            return 1.0
        return self.correct_evidence_count / self.evidence_denominator

    @property
    def false_safe_rate(self) -> float:
        if self.observed_field_count == 0:
            return 0.0
        return self.false_safe_count / self.observed_field_count

    @property
    def human_review_rate(self) -> float:
        if self.observed_field_count == 0:
            return 0.0
        return self.human_review_count / self.observed_field_count

    @property
    def safe_gate_passed(self) -> bool:
        return self.false_safe_count == 0

    @staticmethod
    def _nearest_rank(samples: tuple[int, ...], percentile: float) -> int | None:
        if not samples:
            return None
        ordered = sorted(samples)
        rank = max(1, math.ceil(percentile * len(ordered)))
        return ordered[rank - 1]

    @property
    def latency_p50_ms(self) -> int | None:
        return self._nearest_rank(self.latency_samples_ms, 0.50)

    @property
    def latency_p95_ms(self) -> int | None:
        return self._nearest_rank(self.latency_samples_ms, 0.95)

    @property
    def total_cost_usd(self) -> float:
        return round(sum(self.cost_samples_usd), 6)
