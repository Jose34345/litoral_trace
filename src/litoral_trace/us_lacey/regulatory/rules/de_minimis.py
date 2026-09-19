"""Deterministic APHIS Lacey de minimis assessment."""
from __future__ import annotations

from decimal import Decimal, InvalidOperation
import re
from collections.abc import Mapping
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from litoral_trace.us_lacey.regulatory.engine import RegulatoryContext, RegulatorySubject

from .domain import (
    RULESET_VERSION,
    DeMinimisInput,
    ProtectedPlantStatus,
    RuleAssessment,
    RuleStatus,
)


_MAX_UNIT_PLANT_PERCENT = Decimal("5.00")
_MAX_ENTRY_SAME_HTS_PLANT_KG = Decimal("2.900")
_HTS10 = re.compile(r"^\d{10}$")


def _valid_nonnegative(value: Decimal | None) -> bool:
    return value is not None and value.is_finite() and value >= 0


def _assessment(
    inputs: DeMinimisInput,
    *,
    status: RuleStatus,
    reasons: tuple[str, ...],
    explanation: str,
    trace: dict[str, str | None] | None = None,
) -> RuleAssessment:
    return RuleAssessment(
        rule_id="DE_MINIMIS",
        ruleset_version=RULESET_VERSION,
        status=status,
        reason_codes=reasons,
        explanation=explanation,
        calculation_trace=trace or {},
        evidence_refs=inputs.evidence_refs,
        review_required=status is RuleStatus.INDETERMINATE,
    )


def evaluate_de_minimis(inputs: DeMinimisInput) -> RuleAssessment:
    """Evaluate only supported facts; missing or invalid facts never become PASS."""
    reasons: list[str] = []
    required = (
        inputs.hts10,
        inputs.plant_mass_per_unit_kg,
        inputs.total_unit_mass_kg,
        inputs.entry_same_hts_plant_mass_kg,
    )
    if any(value is None for value in required):
        reasons.append("MISSING_REQUIRED_INPUTS")

    if inputs.hts10 is not None and not _HTS10.fullmatch(str(inputs.hts10)):
        reasons.append("INVALID_HTS10")
    if inputs.plant_mass_per_unit_kg is not None and not _valid_nonnegative(inputs.plant_mass_per_unit_kg):
        reasons.append("INVALID_PLANT_UNIT_MASS")
    if inputs.total_unit_mass_kg is not None and (
        not inputs.total_unit_mass_kg.is_finite() or inputs.total_unit_mass_kg <= 0
    ):
        reasons.append("INVALID_TOTAL_UNIT_MASS")
    if inputs.entry_same_hts_plant_mass_kg is not None and not _valid_nonnegative(
        inputs.entry_same_hts_plant_mass_kg
    ):
        reasons.append("INVALID_ENTRY_PLANT_MASS")

    if reasons:
        return _assessment(
            inputs,
            status=RuleStatus.INDETERMINATE,
            reasons=tuple(reasons),
            explanation="Required de minimis inputs are missing or invalid; review is required.",
        )

    if inputs.protected_status is ProtectedPlantStatus.UNKNOWN:
        return _assessment(
            inputs,
            status=RuleStatus.INDETERMINATE,
            reasons=("PROTECTED_STATUS_UNKNOWN",),
            explanation="Protected-plant status is unknown; the de minimis exception cannot be supported.",
        )
    if inputs.protected_status is ProtectedPlantStatus.PRESENT:
        return _assessment(
            inputs,
            status=RuleStatus.FAIL,
            reasons=("PROTECTED_PLANT_PRESENT",),
            explanation="Protected plant material is present, so the de minimis exception is not supported.",
        )

    # Values are proven non-null and valid above.
    assert inputs.plant_mass_per_unit_kg is not None
    assert inputs.total_unit_mass_kg is not None
    assert inputs.entry_same_hts_plant_mass_kg is not None
    plant_percent = (inputs.plant_mass_per_unit_kg * Decimal("100")) / inputs.total_unit_mass_kg
    trace = {
        "subject_ref": inputs.subject_ref,
        "hts10": inputs.hts10,
        "plant_mass_per_unit_kg": str(inputs.plant_mass_per_unit_kg),
        "total_unit_mass_kg": str(inputs.total_unit_mass_kg),
        "plant_percent": format(plant_percent, ".2f"),
        "unit_percent_limit": format(_MAX_UNIT_PLANT_PERCENT, ".2f"),
        "entry_same_hts_plant_mass_kg": str(inputs.entry_same_hts_plant_mass_kg),
        "entry_same_hts_limit_kg": str(_MAX_ENTRY_SAME_HTS_PLANT_KG),
        "protected_status": inputs.protected_status.value,
    }

    failures: list[str] = []
    if plant_percent > _MAX_UNIT_PLANT_PERCENT:
        failures.append("UNIT_PLANT_PERCENT_EXCEEDED")
    if inputs.entry_same_hts_plant_mass_kg > _MAX_ENTRY_SAME_HTS_PLANT_KG:
        failures.append("ENTRY_PLANT_MASS_EXCEEDED")
    if failures:
        return _assessment(
            inputs,
            status=RuleStatus.FAIL,
            reasons=tuple(failures),
            explanation="One or more de minimis weight thresholds are exceeded.",
            trace=trace,
        )

    return _assessment(
        inputs,
        status=RuleStatus.PASS,
        reasons=("QUALIFIES_DE_MINIMIS",),
        explanation="The supplied evidence satisfies the rule-scoped de minimis weight criteria.",
        trace=trace,
    )


def _decimal_from_rule_input(value: object) -> Decimal | None:
    if value is None or value == "":
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return Decimal("NaN")


def _protected_status_from_rule_input(value: object) -> ProtectedPlantStatus:
    if isinstance(value, ProtectedPlantStatus):
        return value
    try:
        return ProtectedPlantStatus(str(value).strip().upper())
    except (ValueError, AttributeError):
        return ProtectedPlantStatus.UNKNOWN


class DeMinimisRule:
    """Protocol adapter around the existing pure de minimis evaluator."""

    rule_id = "DE_MINIMIS"

    def evaluate(
        self,
        *,
        subject: "RegulatorySubject",
        context: "RegulatoryContext",
    ) -> RuleAssessment:
        del context
        configured = subject.rule_inputs.get(self.rule_id)

        if isinstance(configured, DeMinimisInput):
            inputs = DeMinimisInput(
                subject_ref=subject.subject_ref,
                hts10=configured.hts10,
                plant_mass_per_unit_kg=configured.plant_mass_per_unit_kg,
                total_unit_mass_kg=configured.total_unit_mass_kg,
                entry_same_hts_plant_mass_kg=configured.entry_same_hts_plant_mass_kg,
                protected_status=configured.protected_status,
                evidence_refs=configured.evidence_refs or subject.evidence_refs,
            )
            return evaluate_de_minimis(inputs)

        values: Mapping[str, object]
        if isinstance(configured, Mapping):
            values = configured
        else:
            values = {}

        inputs = DeMinimisInput(
            subject_ref=subject.subject_ref,
            hts10=str(values.get("hts10") or subject.hts10).strip()
            if (values.get("hts10") or subject.hts10)
            else None,
            plant_mass_per_unit_kg=_decimal_from_rule_input(
                values.get("plant_mass_per_unit_kg")
            ),
            total_unit_mass_kg=_decimal_from_rule_input(
                values.get("total_unit_mass_kg")
            ),
            entry_same_hts_plant_mass_kg=_decimal_from_rule_input(
                values.get("entry_same_hts_plant_mass_kg")
            ),
            protected_status=_protected_status_from_rule_input(
                values.get("protected_status")
            ),
            evidence_refs=subject.evidence_refs,
        )
        return evaluate_de_minimis(inputs)


__all__ = ["DeMinimisRule", "evaluate_de_minimis"]
