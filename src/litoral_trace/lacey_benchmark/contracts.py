from __future__ import annotations

from decimal import Decimal
from enum import Enum
from typing import Generic, TypeVar

from pydantic import BaseModel, ConfigDict, Field, model_validator


class FieldStatus(str, Enum):
    AUTO_SUPPORTED = "AUTO_SUPPORTED"
    REVIEW_REQUIRED = "REVIEW_REQUIRED"
    MISSING = "MISSING"


class EvalClassification(str, Enum):
    TRUE_SAFE = "TRUE_SAFE"
    FALSE_SAFE = "FALSE_SAFE"
    UNNECESSARY_REVIEW = "UNNECESSARY_REVIEW"
    FALSE_MISSING = "FALSE_MISSING"
    WRONG_ENTITY_ASSOCIATION = "WRONG_ENTITY_ASSOCIATION"
    OTHER_REVIEW = "OTHER_REVIEW"


T = TypeVar("T")


class CanonicalField(BaseModel, Generic[T]):
    """One benchmark-visible field decision.

    The benchmark intentionally carries status and confidence with the value so it
    can score operational friction and unsafe automation separately from extraction
    accuracy. It does not import or mirror Engine 2 runtime models.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    value: T | None = None
    status: FieldStatus
    confidence: float = Field(ge=0.0, le=1.0)


class PlantLine(BaseModel):
    """Minimal stable line identity plus fields evaluated by the first corpus."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    line_id: str = Field(min_length=1)
    sku: str | None = None
    hts: CanonicalField[str]
    genus: CanonicalField[str]
    species: CanonicalField[str]
    quantity: CanonicalField[Decimal]


class ShipmentTruth(BaseModel):
    """Evaluation contract independent from Engine 2 and persistence models."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    plant_lines: list[PlantLine] = Field(default_factory=list)

    @model_validator(mode="after")
    def require_unique_line_ids(self) -> "ShipmentTruth":
        line_ids = [line.line_id for line in self.plant_lines]
        if len(line_ids) != len(set(line_ids)):
            raise ValueError("ShipmentTruth line_id values must be unique")
        return self
