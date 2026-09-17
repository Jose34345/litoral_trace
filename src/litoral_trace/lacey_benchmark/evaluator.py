from __future__ import annotations

from collections import defaultdict
from decimal import Decimal
from typing import TypeAlias

from .contracts import EvalClassification, FieldStatus, PlantLine, ShipmentTruth
from .scorecard import FieldEvaluation, Scorecard


FieldValue: TypeAlias = str | Decimal
_EVALUATED_FIELDS: tuple[str, ...] = ("hts", "genus", "species", "quantity")


class SemanticEvaluator:
    """Pure line-id-based semantic evaluator for Golden Corpus outputs.

    This class deliberately knows nothing about Engine 2, persistence, document
    provenance, or HTTP. Expected and actual JSONs meet only at the benchmark
    contract boundary.
    """

    @staticmethod
    def _actual_value_index(
        actual: ShipmentTruth,
    ) -> dict[str, dict[FieldValue, set[str]]]:
        """Precompute field values across actual lines for contamination detection."""
        index: dict[str, dict[FieldValue, set[str]]] = {
            field_name: defaultdict(set) for field_name in _EVALUATED_FIELDS
        }
        for line in actual.plant_lines:
            for field_name in _EVALUATED_FIELDS:
                field = getattr(line, field_name)
                value = field.value
                if value is not None:
                    index[field_name][value].add(line.line_id)
        return index

    @staticmethod
    def _false_missing(
        *,
        case_id: str | None,
        line_id: str,
        field_name: str,
        expected_value: FieldValue,
        actual_value: FieldValue | None = None,
        actual_status: FieldStatus | None = None,
    ) -> FieldEvaluation:
        return FieldEvaluation(
            case_id=case_id,
            line_id=line_id,
            field_name=field_name,
            expected_value=expected_value,
            actual_value=actual_value,
            actual_status=actual_status,
            classification=EvalClassification.FALSE_MISSING,
        )

    @staticmethod
    def evaluate(
        expected: ShipmentTruth,
        actual: ShipmentTruth,
        *,
        case_id: str | None = None,
    ) -> Scorecard:
        """Compare one expected shipment with one actual shipment deterministically."""
        actual_by_line: dict[str, PlantLine] = {
            line.line_id: line for line in actual.plant_lines
        }
        actual_value_index = SemanticEvaluator._actual_value_index(actual)
        results: list[FieldEvaluation] = []

        for expected_line in expected.plant_lines:
            actual_line = actual_by_line.get(expected_line.line_id)

            # A missing entity is not a field-level association mistake. Once the
            # stable identity is absent, every golden field on that entity is missing.
            if actual_line is None:
                for field_name in _EVALUATED_FIELDS:
                    expected_field = getattr(expected_line, field_name)
                    if expected_field.value is None:
                        continue
                    results.append(
                        SemanticEvaluator._false_missing(
                            case_id=case_id,
                            line_id=expected_line.line_id,
                            field_name=field_name,
                            expected_value=expected_field.value,
                        )
                    )
                continue

            for field_name in _EVALUATED_FIELDS:
                expected_field = getattr(expected_line, field_name)
                expected_value = expected_field.value
                if expected_value is None:
                    # Null in the Golden Corpus means explicitly out of evaluation
                    # scope, not an assertion that the actual output must be null.
                    continue

                actual_field = getattr(actual_line, field_name)
                actual_value = actual_field.value
                actual_status = actual_field.status
                values_match = actual_value == expected_value

                if not values_match:
                    other_lines = actual_value_index[field_name].get(expected_value, set()) - {
                        expected_line.line_id
                    }
                    if other_lines:
                        classification = EvalClassification.WRONG_ENTITY_ASSOCIATION
                    elif actual_value is None or actual_status is FieldStatus.MISSING:
                        classification = EvalClassification.FALSE_MISSING
                    elif actual_status is FieldStatus.AUTO_SUPPORTED:
                        classification = EvalClassification.FALSE_SAFE
                    else:
                        classification = EvalClassification.OTHER_REVIEW
                elif actual_status is FieldStatus.AUTO_SUPPORTED:
                    classification = EvalClassification.TRUE_SAFE
                elif actual_status is FieldStatus.REVIEW_REQUIRED:
                    classification = EvalClassification.UNNECESSARY_REVIEW
                else:
                    # A non-null matching payload marked MISSING is internally
                    # inconsistent from a user's perspective; score it as missing.
                    classification = EvalClassification.FALSE_MISSING

                results.append(
                    FieldEvaluation(
                        case_id=case_id,
                        line_id=expected_line.line_id,
                        field_name=field_name,
                        expected_value=expected_value,
                        actual_value=actual_value,
                        actual_status=actual_status,
                        classification=classification,
                    )
                )

        return Scorecard.from_results(results)
