from __future__ import annotations

import pytest

from litoral_trace.us_lacey.regulatory.applicability.domain import (
    DeclarationScope,
    MerchandiseLineFacts,
    PlantMaterialEvidence,
)
from litoral_trace.us_lacey.regulatory.applicability.service import (
    DeclarationApplicabilityService,
)
from litoral_trace.us_lacey.regulatory.catalogs.hts_schedule import (
    APHIS_HTS_SCHEDULE_VERSION,
)


def _facts(hts10: str | None, plant: PlantMaterialEvidence) -> MerchandiseLineFacts:
    return MerchandiseLineFacts(
        line_key="line-1",
        hts10=hts10,
        description="Fixture merchandise",
        entered_value="100.00",
        plant_material=plant,
        evidence_refs=("fixture:1",),
    )


@pytest.mark.parametrize("hts10", (None, "", "4407", "44.07.99.0190", "abcdefghij"))
def test_invalid_hts_requires_review_without_botanical_fields(hts10):
    decision = DeclarationApplicabilityService().evaluate(
        _facts(hts10, PlantMaterialEvidence.PRESENT)
    )
    assert decision.scope is DeclarationScope.REVIEW_REQUIRED
    assert decision.reason_codes == ("VALID_HTS10_REQUIRED",)
    assert decision.requires_botanical_fields is False


def test_valid_hts_not_on_schedule_is_not_required_before_material_question():
    decision = DeclarationApplicabilityService().evaluate(
        _facts("8716805070", PlantMaterialEvidence.UNKNOWN)
    )
    assert decision.scope is DeclarationScope.NOT_REQUIRED
    assert decision.reason_codes == ("HTS_NOT_ON_APHIS_SCHEDULE",)
    assert decision.requires_botanical_fields is False


def test_scheduled_hts_with_explicit_absence_is_not_required():
    decision = DeclarationApplicabilityService().evaluate(
        _facts("4407990190", PlantMaterialEvidence.ABSENT)
    )
    assert decision.scope is DeclarationScope.NOT_REQUIRED
    assert decision.reason_codes == ("NO_PLANT_MATERIAL",)
    assert decision.requires_botanical_fields is False


def test_scheduled_hts_with_unknown_material_requires_material_review_only():
    decision = DeclarationApplicabilityService().evaluate(
        _facts("4407990190", PlantMaterialEvidence.UNKNOWN)
    )
    assert decision.scope is DeclarationScope.REVIEW_REQUIRED
    assert decision.reason_codes == ("PLANT_MATERIAL_NOT_ESTABLISHED",)
    assert decision.requires_botanical_fields is False


def test_scheduled_hts_with_present_material_can_materialize_botanical_fields():
    decision = DeclarationApplicabilityService().evaluate(
        _facts("4407990190", PlantMaterialEvidence.PRESENT)
    )
    assert decision.scope is DeclarationScope.POTENTIALLY_REQUIRED
    assert decision.reason_codes == (
        "HTS_ON_APHIS_SCHEDULE",
        "PLANT_MATERIAL_PRESENT",
    )
    assert decision.catalog_version == APHIS_HTS_SCHEDULE_VERSION
    assert decision.requires_botanical_fields is True
