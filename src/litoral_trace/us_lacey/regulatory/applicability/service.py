"""Deterministic declaration-applicability gate for merchandise lines."""
from __future__ import annotations

import re

from litoral_trace.us_lacey.regulatory.applicability.domain import (
    ApplicabilityDecision,
    DeclarationScope,
    MerchandiseLineFacts,
    PlantMaterialEvidence,
)
from litoral_trace.us_lacey.regulatory.catalogs.hts_schedule import (
    APHIS_HTS_SCHEDULE,
    HtsScheduleCatalog,
)


_HTS10 = re.compile(r"^\d{10}$")


class DeclarationApplicabilityService:
    """Decide whether a commercial line may materialize PPQ botanical fields.

    This service deliberately answers only the HTS + plant-material gate. Entry
    type, de minimis, composite/recycled and other downstream rules remain owned
    by the regulatory engine.
    """

    def __init__(self, catalog: HtsScheduleCatalog = APHIS_HTS_SCHEDULE) -> None:
        self._catalog = catalog

    @property
    def catalog(self) -> HtsScheduleCatalog:
        return self._catalog

    def evaluate(self, facts: MerchandiseLineFacts) -> ApplicabilityDecision:
        raw_hts = str(facts.hts10 or "").strip()

        if not _HTS10.fullmatch(raw_hts):
            return ApplicabilityDecision(
                line_key=facts.line_key,
                scope=DeclarationScope.REVIEW_REQUIRED,
                reason_codes=("VALID_HTS10_REQUIRED",),
                catalog_version=self._catalog.version,
                requires_botanical_fields=False,
            )

        if not self._catalog.contains(raw_hts):
            return ApplicabilityDecision(
                line_key=facts.line_key,
                scope=DeclarationScope.NOT_REQUIRED,
                reason_codes=("HTS_NOT_ON_APHIS_SCHEDULE",),
                catalog_version=self._catalog.version,
                requires_botanical_fields=False,
            )

        if facts.plant_material is PlantMaterialEvidence.ABSENT:
            return ApplicabilityDecision(
                line_key=facts.line_key,
                scope=DeclarationScope.NOT_REQUIRED,
                reason_codes=("NO_PLANT_MATERIAL",),
                catalog_version=self._catalog.version,
                requires_botanical_fields=False,
            )

        if facts.plant_material is PlantMaterialEvidence.UNKNOWN:
            return ApplicabilityDecision(
                line_key=facts.line_key,
                scope=DeclarationScope.REVIEW_REQUIRED,
                reason_codes=("PLANT_MATERIAL_NOT_ESTABLISHED",),
                catalog_version=self._catalog.version,
                requires_botanical_fields=False,
            )

        return ApplicabilityDecision(
            line_key=facts.line_key,
            scope=DeclarationScope.POTENTIALLY_REQUIRED,
            reason_codes=(
                "HTS_ON_APHIS_SCHEDULE",
                "PLANT_MATERIAL_PRESENT",
            ),
            catalog_version=self._catalog.version,
            requires_botanical_fields=True,
        )


__all__ = ["DeclarationApplicabilityService"]
