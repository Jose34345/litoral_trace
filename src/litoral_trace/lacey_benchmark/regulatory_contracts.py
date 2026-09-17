from __future__ import annotations

from decimal import Decimal
from enum import Enum
from typing import Any, Tuple

from pydantic import BaseModel, ConfigDict, Field, field_validator


class DecisionStatus(str, Enum):
    PASS = "PASS"
    FAIL = "FAIL"
    INDETERMINATE = "INDETERMINATE"


class Measurement(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    value: Decimal = Field(ge=0)
    unit: str = Field(min_length=1)

    @field_validator("unit")
    @classmethod
    def normalize_unit(cls, value: str) -> str:
        normalized = value.strip().upper()
        if not normalized:
            raise ValueError("measurement unit cannot be blank")
        return normalized


class RuleTrace(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    rule_id: str = Field(min_length=1)
    status: DecisionStatus
    source_ref: str = Field(min_length=1)
    message: str = Field(min_length=1)
    calculation: str | None = None
    facts: dict[str, str] = Field(default_factory=dict)


class AssessmentResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    status: DecisionStatus
    trace: Tuple[RuleTrace, ...]


class DeMinimisInput(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    total_product_weight_per_unit: Measurement
    plant_material_weight_per_unit: Measurement
    entry_plant_material_weight_same_hts: Measurement
    protected_species_present: bool | None


class CompositeInput(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    material_type: str | None = None
    small_fibers: bool | None = None
    multiple_plant_kinds: bool | None = None
    chemically_bonded: bool | None = None


class SudInput(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    genus: str = Field(min_length=1)
    species: str = Field(min_length=1)
    due_care_cannot_determine_species: bool | None = None
    possible_species: Tuple[str, ...] = ()
    composite: CompositeInput | None = None
    is_recycled: bool | None = None
    is_reclaimed: bool | None = None
    qualifies_preamendment: bool | None = None
    cultivated_hybrid: bool | None = None


class CompletenessInput(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    countries_of_harvest: Tuple[str, ...]
    quantity: Measurement | None


class RegulatoryEvaluationInput(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    completeness: CompletenessInput
    de_minimis: DeMinimisInput | None = None
    composite: CompositeInput | None = None
    sud: SudInput | None = None


class RegulatoryEvaluationResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    ruleset_id: str
    ruleset_version: str
    ruleset_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    input_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    status: DecisionStatus
    trace: Tuple[RuleTrace, ...]
