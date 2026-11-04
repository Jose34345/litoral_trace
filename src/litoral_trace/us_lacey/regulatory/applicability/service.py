"""Deterministic declaration-applicability gate for U.S. Lacey merchandise lines."""
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
    """Decide whether botanical declaration fields may be materialized.

    This service is intentionally narrower than the full regulatory engine. It is
    a pre-materialization jurisdiction gate: a merchandise row must first have a
    valid HTS10 on the APHIS schedule and explicit evidence of plant material before
    PPQ botanical fields are allowed to exist.
    """

    def __init__(self, catalog: HtsScheduleCatalog = APHIS_HTS_SCHEDULE) -> None:
        if not catalog.is_complete:
            raise ValueError(
                "DeclarationApplicabilityService requires a complete APHIS schedule catalog."
            )
        self._catalog = catalog

    @property
    def catalog(self) -> HtsScheduleCatalog:
        return self._catalog

    def evaluate(self, facts: MerchandiseLineFacts) -> ApplicabilityDecision:
        raw_hts = None if facts.hts10 is None else str(facts.hts10).strip()

        if raw_hts is None or not _HTS10.fullmatch(raw_hts):
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
