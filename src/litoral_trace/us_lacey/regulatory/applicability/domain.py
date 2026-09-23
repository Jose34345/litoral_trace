"""Domain contracts for future U.S. Lacey declaration applicability evaluation.

This module defines deterministic/review-required regulatory contracts only.
It performs no extraction, persistence, canonical publication, or projection.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class DeclarationScope(StrEnum):
    NOT_REQUIRED = "NOT_REQUIRED"
    POTENTIALLY_REQUIRED = "POTENTIALLY_REQUIRED"
    REVIEW_REQUIRED = "REVIEW_REQUIRED"


class PlantMaterialEvidence(StrEnum):
    PRESENT = "PRESENT"
    ABSENT = "ABSENT"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True, slots=True)
class MerchandiseLineFacts:
    line_key: str
    hts10: str | None
    description: str | None
    entered_value: str | None
    plant_material: PlantMaterialEvidence
    evidence_refs: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not str(self.line_key or "").strip():
            raise ValueError(
                "MerchandiseLineFacts.line_key must be non-empty."
            )
        if self.hts10 is not None and not str(self.hts10).strip():
            raise ValueError(
                "MerchandiseLineFacts.hts10 must be None or non-empty."
            )
        if (
            self.description is not None
            and not str(self.description).strip()
        ):
            raise ValueError(
                "MerchandiseLineFacts.description must be None or non-empty."
            )
        if (
            self.entered_value is not None
            and not str(self.entered_value).strip()
        ):
            raise ValueError(
                "MerchandiseLineFacts.entered_value must be None or non-empty."
            )
        if any(
            not str(reference or "").strip()
            for reference in self.evidence_refs
        ):
            raise ValueError(
                "MerchandiseLineFacts.evidence_refs cannot contain empty values."
            )


@dataclass(frozen=True, slots=True)
class ApplicabilityDecision:
    line_key: str
    scope: DeclarationScope
    reason_codes: tuple[str, ...]
    catalog_version: str
    requires_botanical_fields: bool

    def __post_init__(self) -> None:
        if not str(self.line_key or "").strip():
            raise ValueError(
                "ApplicabilityDecision.line_key must be non-empty."
            )
        if not str(self.catalog_version or "").strip():
            raise ValueError(
                "ApplicabilityDecision.catalog_version must be non-empty."
            )
        if not self.reason_codes:
            raise ValueError(
                "ApplicabilityDecision requires at least one reason code."
            )
        if any(
            not str(reason or "").strip()
            for reason in self.reason_codes
        ):
            raise ValueError(
                "ApplicabilityDecision reason codes cannot be empty."
            )
        if len(self.reason_codes) != len(set(self.reason_codes)):
            raise ValueError(
                "ApplicabilityDecision reason codes must be unique."
            )
        if (
            self.scope is DeclarationScope.NOT_REQUIRED
            and self.requires_botanical_fields
        ):
            raise ValueError(
                "NOT_REQUIRED cannot require botanical fields."
            )
        if (
            self.scope is DeclarationScope.REVIEW_REQUIRED
            and self.requires_botanical_fields
        ):
            raise ValueError(
                "REVIEW_REQUIRED cannot materialize botanical fields."
            )
        if (
            self.scope is DeclarationScope.POTENTIALLY_REQUIRED
            and not self.requires_botanical_fields
        ):
            raise ValueError(
                "POTENTIALLY_REQUIRED must require botanical fields."
            )
