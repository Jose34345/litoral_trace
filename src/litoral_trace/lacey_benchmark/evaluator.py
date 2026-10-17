"""Deterministic evaluators for the U.S. Lacey Golden Benchmark."""
from __future__ import annotations

from collections.abc import Iterable

from litoral_trace.lacey_benchmark.contracts import (
    BenchmarkTelemetry,
    DocumentPrediction,
    FieldDecisionStatus,
    FieldObservation,
    FieldTruth,
    RouterCorpusDocument,
)
from litoral_trace.lacey_benchmark.scorecard import (
    DocumentClassificationScore,
    GoldenBenchmarkScore,
)


def _norm(value: str | None) -> str:
    return " ".join(str(value or "").split())


def _same_source(expected: FieldTruth, observed: FieldObservation) -> bool:
    return (
        observed.document_id == expected.document_id
        and observed.page == expected.page
    )


def _same_line(expected: FieldTruth, observed: FieldObservation) -> bool:
    if expected.line_item_key is None:
        return True
    return observed.line_item_key == expected.line_item_key


def _same_evidence(expected: FieldTruth, observed: FieldObservation) -> bool:
    return (
        observed.evidence_verified is True
        and _same_source(expected, observed)
        and _norm(observed.evidence_text) == _norm(expected.evidence_text)
    )


class GoldenBenchmarkEvaluator:
    @staticmethod
    def evaluate_document_classification(
        expected: Iterable[RouterCorpusDocument],
        observed: Iterable[DocumentPrediction],
    ) -> DocumentClassificationScore:
        expected_items = tuple(expected)
        actual_by_id = {item.document_id: item.predicted_type for item in observed}
        correct = sum(
            1
            for item in expected_items
            if actual_by_id.get(item.document_id) is item.expected_type
        )
        return DocumentClassificationScore(total=len(expected_items), correct=correct)

    @staticmethod
    def _pair_observations(
        expected: tuple[FieldTruth, ...],
        observed: tuple[FieldObservation, ...],
    ) -> tuple[tuple[FieldTruth | None, FieldObservation], ...]:
        unused = set(range(len(expected)))
        pairs: list[tuple[FieldTruth | None, FieldObservation]] = []

        for observation in observed:
            exact = [
                index
                for index in unused
                if expected[index].field_key == observation.field_key
                and expected[index].document_id == observation.document_id
                and expected[index].page == observation.page
                and expected[index].line_item_key == observation.line_item_key
            ]
            if len(exact) == 1:
                index = exact[0]
                unused.remove(index)
                pairs.append((expected[index], observation))
                continue

            same_value = [
                index
                for index in unused
                if expected[index].field_key == observation.field_key
                and expected[index].document_id == observation.document_id
                and expected[index].page == observation.page
                and _norm(expected[index].value) == _norm(observation.value)
            ]
            if len(same_value) == 1:
                index = same_value[0]
                unused.remove(index)
                pairs.append((expected[index], observation))
                continue

            same_field_source = [
                index
                for index in unused
                if expected[index].field_key == observation.field_key
                and expected[index].document_id == observation.document_id
                and expected[index].page == observation.page
            ]
            if len(same_field_source) == 1:
                index = same_field_source[0]
                unused.remove(index)
                pairs.append((expected[index], observation))
                continue

            pairs.append((None, observation))

        return tuple(pairs)

    @staticmethod
    def evaluate_fields(
        *,
        expected: Iterable[FieldTruth],
        observed: Iterable[FieldObservation],
        telemetry: Iterable[BenchmarkTelemetry] = (),
    ) -> GoldenBenchmarkScore:
        expected_items = tuple(expected)
        observed_items = tuple(observed)
        pairs = GoldenBenchmarkEvaluator._pair_observations(
            expected_items,
            observed_items,
        )

        correct_field_count = 0
        line_binding_denominator = 0
        correct_line_binding_count = 0
        evidence_denominator = 0
        correct_evidence_count = 0
        false_safe_count = 0
        human_review_count = 0

        for expected_field, observation in pairs:
            if observation.status is FieldDecisionStatus.REVIEW_REQUIRED:
                human_review_count += 1

            value_correct = (
                expected_field is not None
                and _norm(observation.value) == _norm(expected_field.value)
            )
            if value_correct:
                correct_field_count += 1

            line_correct = True
            evidence_correct = False
            if expected_field is not None:
                if expected_field.line_item_key is not None:
                    line_binding_denominator += 1
                    line_correct = _same_line(expected_field, observation)
                    if line_correct:
                        correct_line_binding_count += 1

                evidence_denominator += 1
                evidence_correct = _same_evidence(expected_field, observation)
                if evidence_correct:
                    correct_evidence_count += 1
            else:
                line_correct = False

            if observation.status is FieldDecisionStatus.SAFE_SUGGESTION and (
                not value_correct
                or not line_correct
                or not evidence_correct
            ):
                false_safe_count += 1

        telemetry_items = tuple(telemetry)
        return GoldenBenchmarkScore(
            expected_field_count=len(expected_items),
            observed_field_count=len(observed_items),
            correct_field_count=correct_field_count,
            correct_line_binding_count=correct_line_binding_count,
            line_binding_denominator=line_binding_denominator,
            correct_evidence_count=correct_evidence_count,
            evidence_denominator=evidence_denominator,
            false_safe_count=false_safe_count,
            human_review_count=human_review_count,
            latency_samples_ms=tuple(
                max(0, int(item.latency_ms))
                for item in telemetry_items
            ),
            cost_samples_usd=tuple(
                max(0.0, float(item.cost_usd))
                for item in telemetry_items
            ),
        )
