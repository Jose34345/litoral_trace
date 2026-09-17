from __future__ import annotations

from collections import Counter
from decimal import Decimal
from typing import Iterable

from pydantic import BaseModel, ConfigDict, Field

from .contracts import EvalClassification, FieldStatus


ScalarValue = str | Decimal


class FieldEvaluation(BaseModel):
    """One deterministic field-level benchmark decision."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    case_id: str | None = None
    line_id: str
    field_name: str
    expected_value: ScalarValue
    actual_value: ScalarValue | None = None
    actual_status: FieldStatus | None = None
    classification: EvalClassification


class Scorecard(BaseModel):
    """Aggregate semantic evaluation results and executive metrics."""

    model_config = ConfigDict(extra="forbid")

    results: list[FieldEvaluation] = Field(default_factory=list)

    @classmethod
    def from_results(cls, results: Iterable[FieldEvaluation]) -> "Scorecard":
        return cls(results=list(results))

    @property
    def total_fields(self) -> int:
        return len(self.results)

    @property
    def counts(self) -> Counter[EvalClassification]:
        """Return classification counts with zero-default lookup semantics."""
        return Counter(result.classification for result in self.results)

    def count(self, classification: EvalClassification) -> int:
        return self.counts[classification]

    def rate(self, classification: EvalClassification) -> float:
        if self.total_fields == 0:
            return 0.0
        return self.count(classification) / self.total_fields

    @property
    def automation_rate(self) -> float:
        """Share of evaluated fields safely automated, excluding false-safe output."""
        return self.rate(EvalClassification.TRUE_SAFE)

    @property
    def false_safe_rate(self) -> float:
        return self.rate(EvalClassification.FALSE_SAFE)

    @property
    def unnecessary_review_rate(self) -> float:
        return self.rate(EvalClassification.UNNECESSARY_REVIEW)

    @property
    def false_missing_rate(self) -> float:
        return self.rate(EvalClassification.FALSE_MISSING)

    @property
    def wrong_entity_association_rate(self) -> float:
        return self.rate(EvalClassification.WRONG_ENTITY_ASSOCIATION)

    @property
    def anomalies(self) -> list[FieldEvaluation]:
        return [
            result
            for result in self.results
            if result.classification is not EvalClassification.TRUE_SAFE
        ]

    def render_console(self) -> str:
        """Return a dependency-free deterministic executive scorecard."""
        counts = self.counts
        classification_rows = [
            (classification.value, counts[classification], self.rate(classification))
            for classification in EvalClassification
        ]
        metric_rows = [
            ("Automation Rate", self.automation_rate),
            ("False Safe Rate", self.false_safe_rate),
            ("Unnecessary Review Rate", self.unnecessary_review_rate),
            ("False Missing Rate", self.false_missing_rate),
            ("Wrong Entity Assoc Rate", self.wrong_entity_association_rate),
        ]

        lines = [
            "GOLDEN BENCHMARK SCORECARD",
            "=" * 66,
            f"Total evaluated fields: {self.total_fields}",
            "",
            f"{'Classification':<30} {'Count':>8} {'Rate':>12}",
            "-" * 52,
        ]
        for label, count, rate in classification_rows:
            lines.append(f"{label:<30} {count:>8} {rate:>11.2%}")

        lines.extend(
            [
                "",
                f"{'Executive metric':<30} {'Rate':>12}",
                "-" * 43,
            ]
        )
        for label, rate in metric_rows:
            lines.append(f"{label:<30} {rate:>11.2%}")

        if self.anomalies:
            lines.extend(
                [
                    "",
                    "ANOMALIES",
                    "-" * 66,
                    f"{'Case':<20} {'Line':<12} {'Field':<10} {'Classification':<24}",
                ]
            )
            for result in self.anomalies:
                lines.append(
                    f"{(result.case_id or '-'):<20.20} "
                    f"{result.line_id:<12.12} "
                    f"{result.field_name:<10.10} "
                    f"{result.classification.value:<24.24}"
                )
                lines.append(
                    f"  expected={result.expected_value!s} actual={result.actual_value!s}"
                )
        return "\n".join(lines)