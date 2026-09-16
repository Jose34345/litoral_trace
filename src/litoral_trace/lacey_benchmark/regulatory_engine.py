from __future__ import annotations

from decimal import Decimal, InvalidOperation
import hashlib
import json

from litoral_trace.lacey_benchmark.regulatory_contracts import (
    AssessmentResult,
    CompletenessInput,
    CompositeInput,
    DecisionStatus,
    DeMinimisInput,
    Measurement,
    RegulatoryEvaluationInput,
    RegulatoryEvaluationResult,
    RuleTrace,
    SudInput,
)
from litoral_trace.lacey_benchmark.regulatory_ruleset import RegulatoryRuleSet
from litoral_trace.lacey_benchmark.special_use import SpecialUseRegistry


def _normalized_label(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = " ".join(value.strip().upper().replace("_", " ").split())
    return normalized or None


def _overall_status(statuses: list[DecisionStatus]) -> DecisionStatus:
    if any(status is DecisionStatus.FAIL for status in statuses):
        return DecisionStatus.FAIL
    if any(status is DecisionStatus.INDETERMINATE for status in statuses):
        return DecisionStatus.INDETERMINATE
    return DecisionStatus.PASS


class RegulatoryEngine:
    """Pure deterministic evaluator over a versioned, offline Lacey RuleSet."""

    def __init__(self, ruleset: RegulatoryRuleSet, special_use: SpecialUseRegistry) -> None:
        self.ruleset = ruleset
        self.special_use = special_use
        missing_refs = {
            ruleset.de_minimis.source_ref,
            ruleset.composite.source_ref,
            ruleset.completeness.source_ref,
        } - ruleset.source_refs
        if missing_refs:
            raise ValueError(f"ruleset contains unresolved source_ref values: {sorted(missing_refs)}")

    def convert_measurement(self, measurement: Measurement, target_unit: str) -> Measurement:
        source_unit = measurement.unit.upper()
        target = target_unit.strip().upper()
        source_rule = self.ruleset.valid_units.get(source_unit)
        target_rule = self.ruleset.valid_units.get(target)
        if source_rule is None or target_rule is None:
            raise ValueError(f"unsupported metric conversion: {source_unit} -> {target}")
        if source_rule.dimension != target_rule.dimension:
            raise ValueError(
                f"incompatible measurement dimensions: {source_rule.dimension} -> {target_rule.dimension}"
            )
        base_value = measurement.value * source_rule.to_base
        return Measurement(value=base_value / target_rule.to_base, unit=target)

    def assess_de_minimis(self, data: DeMinimisInput) -> AssessmentResult:
        source_ref = self.ruleset.de_minimis.source_ref
        measurements = (
            data.total_product_weight_per_unit,
            data.plant_material_weight_per_unit,
            data.entry_plant_material_weight_same_hts,
        )
        if any(
            self.ruleset.valid_units.get(item.unit) is None
            or self.ruleset.valid_units[item.unit].dimension != "MASS"
            for item in measurements
        ):
            return AssessmentResult(
                status=DecisionStatus.INDETERMINATE,
                trace=(
                    RuleTrace(
                        rule_id="DE_MINIMIS.MASS_INPUTS",
                        status=DecisionStatus.INDETERMINATE,
                        source_ref=source_ref,
                        message="De minimis requires comparable weight inputs; at least one input is not a supported mass unit.",
                        facts={"units": ",".join(item.unit for item in measurements)},
                    ),
                ),
            )

        total_kg = self.convert_measurement(measurements[0], "KG").value
        plant_kg = self.convert_measurement(measurements[1], "KG").value
        entry_kg = self.convert_measurement(measurements[2], "KG").value
        if total_kg <= 0:
            return AssessmentResult(
                status=DecisionStatus.INDETERMINATE,
                trace=(
                    RuleTrace(
                        rule_id="DE_MINIMIS.UNIT_PERCENT",
                        status=DecisionStatus.INDETERMINATE,
                        source_ref=source_ref,
                        message="Total product weight per unit must be greater than zero.",
                        facts={"total_product_weight_kg": str(total_kg)},
                    ),
                ),
            )

        percent = (plant_kg / total_kg) * Decimal("100")
        percent_status = (
            DecisionStatus.PASS
            if percent <= self.ruleset.de_minimis.max_unit_percent
            else DecisionStatus.FAIL
        )
        entry_status = (
            DecisionStatus.PASS
            if entry_kg <= self.ruleset.de_minimis.max_entry_kg_same_hts
            else DecisionStatus.FAIL
        )
        if data.protected_species_present is None:
            protected_status = DecisionStatus.INDETERMINATE
            protected_message = "Protected-species applicability is unknown."
        elif data.protected_species_present:
            protected_status = DecisionStatus.FAIL
            protected_message = "Protected plant material is not eligible for the de minimis exception."
        else:
            protected_status = DecisionStatus.PASS
            protected_message = "No protected plant material is indicated by the provided facts."

        trace = (
            RuleTrace(
                rule_id="DE_MINIMIS.UNIT_PERCENT",
                status=percent_status,
                source_ref=source_ref,
                message="Plant-material weight per product unit evaluated against the RuleSet threshold.",
                calculation=f"({plant_kg} kg / {total_kg} kg) * 100 = {percent}%",
                facts={"max_percent": str(self.ruleset.de_minimis.max_unit_percent)},
            ),
            RuleTrace(
                rule_id="DE_MINIMIS.ENTRY_WEIGHT",
                status=entry_status,
                source_ref=source_ref,
                message="Entry plant-material weight for products in the same 10-digit HTS evaluated against the RuleSet threshold.",
                calculation=f"{entry_kg} kg <= {self.ruleset.de_minimis.max_entry_kg_same_hts} kg",
                facts={"entry_weight_kg": str(entry_kg)},
            ),
            RuleTrace(
                rule_id="DE_MINIMIS.PROTECTED_MATERIAL",
                status=protected_status,
                source_ref=source_ref,
                message=protected_message,
                facts={"protected_species_present": str(data.protected_species_present)},
            ),
        )
        return AssessmentResult(
            status=_overall_status([item.status for item in trace]),
            trace=trace,
        )

    def classify_composite(self, data: CompositeInput) -> AssessmentResult:
        source_ref = self.ruleset.composite.source_ref
        label = _normalized_label(data.material_type)
        positives = {_normalized_label(value) for value in self.ruleset.composite.known_composite}
        negatives = {_normalized_label(value) for value in self.ruleset.composite.known_non_composite}

        if label in positives:
            status = DecisionStatus.PASS
            message = f"{data.material_type!r} is an explicit composite material in this RuleSet."
        elif label in negatives:
            status = DecisionStatus.FAIL
            message = f"{data.material_type!r} is explicitly modeled as non-composite in this RuleSet."
        else:
            structural = (
                data.small_fibers,
                data.multiple_plant_kinds,
                data.chemically_bonded,
            )
            if all(value is True for value in structural):
                status = DecisionStatus.PASS
                message = "Provided structural facts satisfy the composite definition."
            elif any(value is False for value in structural):
                status = DecisionStatus.FAIL
                message = "At least one required composite structural fact is explicitly false."
            else:
                status = DecisionStatus.INDETERMINATE
                message = "Material type and structural facts are insufficient to classify the material."

        return AssessmentResult(
            status=status,
            trace=(
                RuleTrace(
                    rule_id="COMPOSITE.CLASSIFICATION",
                    status=status,
                    source_ref=source_ref,
                    message=message,
                    facts={
                        "material_type": str(data.material_type),
                        "small_fibers": str(data.small_fibers),
                        "multiple_plant_kinds": str(data.multiple_plant_kinds),
                        "chemically_bonded": str(data.chemically_bonded),
                    },
                ),
            ),
        )

    def evaluate_sud(self, data: SudInput) -> AssessmentResult:
        genus = data.genus.strip().upper()
        species = data.species.strip().upper()
        designation = self.special_use.resolve(genus, species)

        if designation is None and species == "HYBRID":
            source_ref = "APHIS_SUD_2025-07-30"
            if data.cultivated_hybrid is True:
                status = DecisionStatus.PASS
                message = "Cultivated hybrid fact supports use of the HYBRID species designation with the supplied genus."
            elif data.cultivated_hybrid is False:
                status = DecisionStatus.FAIL
                message = "HYBRID designation is not supported because cultivated_hybrid is false."
            else:
                status = DecisionStatus.INDETERMINATE
                message = "Cultivated-hybrid status is unknown."
            return AssessmentResult(
                status=status,
                trace=(RuleTrace(rule_id="SUD.HYBRID", status=status, source_ref=source_ref, message=message),),
            )

        if designation is None:
            return AssessmentResult(
                status=DecisionStatus.FAIL,
                trace=(
                    RuleTrace(
                        rule_id="SUD.KNOWN_DESIGNATION",
                        status=DecisionStatus.FAIL,
                        source_ref="APHIS_SUD_2025-07-30",
                        message=f"{genus}/{species} is not a current designation in the loaded APHIS snapshot.",
                    ),
                ),
            )

        source_ref = designation.source_ref
        category = designation.category.upper()
        if category == "SPECIES_GROUP":
            if not data.possible_species:
                status = DecisionStatus.INDETERMINATE
                message = "Possible species are required to validate the species-group designation."
            else:
                allowed = {value.casefold() for value in designation.members}
                provided = {value.strip().casefold() for value in data.possible_species if value.strip()}
                if provided and provided.issubset(allowed):
                    status = DecisionStatus.PASS
                    message = "All provided possible species belong to the loaded APHIS species group."
                else:
                    status = DecisionStatus.FAIL
                    message = "At least one provided possible species falls outside the loaded APHIS species group."
            return AssessmentResult(
                status=status,
                trace=(
                    RuleTrace(
                        rule_id=f"SUD.GROUP.{species}",
                        status=status,
                        source_ref=source_ref,
                        message=message,
                        facts={
                            "allowed_members": "|".join(designation.members),
                            "possible_species": "|".join(data.possible_species),
                        },
                    ),
                ),
            )

        if category == "COMPOSITE":
            if data.due_care_cannot_determine_species is False:
                return self._sud_single(
                    "SUD.COMPOSITE.DUE_CARE",
                    DecisionStatus.FAIL,
                    source_ref,
                    "SPECIAL/COMPOSITE is not justified when due care can determine the species.",
                )
            if data.due_care_cannot_determine_species is None or data.composite is None:
                return self._sud_single(
                    "SUD.COMPOSITE.DUE_CARE",
                    DecisionStatus.INDETERMINATE,
                    source_ref,
                    "Due-care or composite-material facts are incomplete.",
                )
            composite = self.classify_composite(data.composite)
            status = composite.status
            decision = RuleTrace(
                rule_id="SUD.COMPOSITE.APPLICABILITY",
                status=status,
                source_ref=source_ref,
                message=(
                    "SPECIAL/COMPOSITE applicability is supported."
                    if status is DecisionStatus.PASS
                    else "Composite classification does not establish SPECIAL/COMPOSITE applicability."
                ),
            )
            return AssessmentResult(status=status, trace=(*composite.trace, decision))

        if category in {"RECYCLED", "RECLAIMED", "PREAMENDMENT"}:
            fact = {
                "RECYCLED": data.is_recycled,
                "RECLAIMED": data.is_reclaimed,
                "PREAMENDMENT": data.qualifies_preamendment,
            }[category]
            if fact is False:
                status = DecisionStatus.FAIL
            elif fact is None or (designation.requires_due_care and data.due_care_cannot_determine_species is None):
                status = DecisionStatus.INDETERMINATE
            elif designation.requires_due_care and data.due_care_cannot_determine_species is False:
                status = DecisionStatus.FAIL
            else:
                status = DecisionStatus.PASS
            return self._sud_single(
                f"SUD.{category}.APPLICABILITY",
                status,
                source_ref,
                f"{genus}/{species} evaluated from explicit applicability and due-care facts.",
            )

        return self._sud_single(
            "SUD.UNSUPPORTED_CATEGORY",
            DecisionStatus.INDETERMINATE,
            source_ref,
            f"Loaded designation category {designation.category!r} has no V1 evaluator.",
        )

    def assess_completeness(self, data: CompletenessInput) -> AssessmentResult:
        source_ref = self.ruleset.completeness.source_ref
        countries = tuple(value.strip() for value in data.countries_of_harvest if value.strip())
        country_status = DecisionStatus.PASS if countries else DecisionStatus.FAIL
        quantity = data.quantity
        quantity_status = (
            DecisionStatus.PASS
            if quantity is not None
            and quantity.unit in self.ruleset.valid_units
            and quantity.value > 0
            else DecisionStatus.FAIL
        )
        trace = (
            RuleTrace(
                rule_id="COMPLETENESS.COUNTRY_OF_HARVEST",
                status=country_status,
                source_ref=source_ref,
                message=(
                    "At least one country of harvest is present."
                    if country_status is DecisionStatus.PASS
                    else "Country of harvest is missing."
                ),
                facts={"countries": "|".join(countries)},
            ),
            RuleTrace(
                rule_id="COMPLETENESS.QUANTITY_METRIC_UNIT",
                status=quantity_status,
                source_ref=source_ref,
                message=(
                    "Quantity uses a valid RuleSet metric unit and is greater than zero."
                    if quantity_status is DecisionStatus.PASS
                    else "Quantity is missing, zero, or uses a non-accepted unit."
                ),
                facts={
                    "quantity": str(quantity.value) if quantity is not None else "None",
                    "unit": quantity.unit if quantity is not None else "None",
                },
            ),
        )
        return AssessmentResult(
            status=_overall_status([item.status for item in trace]),
            trace=trace,
        )

    def evaluate(self, data: RegulatoryEvaluationInput) -> RegulatoryEvaluationResult:
        assessments = [self.assess_completeness(data.completeness)]
        if data.de_minimis is not None:
            assessments.append(self.assess_de_minimis(data.de_minimis))
        if data.composite is not None:
            assessments.append(self.classify_composite(data.composite))
        if data.sud is not None:
            assessments.append(self.evaluate_sud(data.sud))

        statuses = [assessment.status for assessment in assessments]
        trace = tuple(item for assessment in assessments for item in assessment.trace)
        canonical_input = json.dumps(
            data.model_dump(mode="json"),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
        return RegulatoryEvaluationResult(
            ruleset_id=self.ruleset.ruleset_id,
            ruleset_version=self.ruleset.version,
            ruleset_fingerprint=self.ruleset.fingerprint,
            input_fingerprint=hashlib.sha256(canonical_input).hexdigest(),
            status=_overall_status(statuses),
            trace=trace,
        )

    @staticmethod
    def _sud_single(
        rule_id: str,
        status: DecisionStatus,
        source_ref: str,
        message: str,
    ) -> AssessmentResult:
        return AssessmentResult(
            status=status,
            trace=(RuleTrace(rule_id=rule_id, status=status, source_ref=source_ref, message=message),),
        )
