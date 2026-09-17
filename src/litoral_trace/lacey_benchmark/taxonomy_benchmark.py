from __future__ import annotations

from .contracts import PlantLine, ShipmentTruth
from .evaluator import SemanticEvaluator
from .scorecard import Scorecard
from .taxonomy_resolver import TaxonomyResolver


class TaxonomyAwareSemanticEvaluator:
    """Normalize only authoritative taxonomy before semantic evaluation.

    Alias and fuzzy resolver outcomes remain untouched because they require
    human review and therefore must not be promoted into benchmark truth.
    """

    @staticmethod
    def _normalize_line(line: PlantLine, *, resolver: TaxonomyResolver) -> PlantLine:
        genus = line.genus.value
        species = line.species.value
        if not genus or not species:
            return line

        resolution = resolver.resolve(genus=genus, species=species)
        if (
            resolution.review_required
            or not resolution.genus
            or not resolution.species
        ):
            return line

        return line.model_copy(
            update={
                "genus": line.genus.model_copy(update={"value": resolution.genus}),
                "species": line.species.model_copy(update={"value": resolution.species}),
            }
        )

    @staticmethod
    def evaluate(
        expected: ShipmentTruth,
        actual: ShipmentTruth,
        *,
        resolver: TaxonomyResolver,
        case_id: str | None = None,
    ) -> Scorecard:
        normalized_actual = actual.model_copy(
            update={
                "plant_lines": [
                    TaxonomyAwareSemanticEvaluator._normalize_line(line, resolver=resolver)
                    for line in actual.plant_lines
                ]
            }
        )
        return SemanticEvaluator.evaluate(
            expected,
            normalized_actual,
            case_id=case_id,
        )
