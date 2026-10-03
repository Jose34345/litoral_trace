from __future__ import annotations

from litoral_trace.us_lacey.regulatory.applicability import (
    DeclarationApplicabilityService,
    DeclarationScope,
    MerchandiseLineFacts,
    PlantMaterialEvidence,
)


def _facts(
    *,
    hts10: str | None,
    plant_material: PlantMaterialEvidence,
) -> MerchandiseLineFacts:
    return MerchandiseLineFacts(
        line_key="1",
        hts10=hts10,
        description="Test product",
        entered_value="100.00",
        plant_material=plant_material,
    )


def test_invalid_hts10_requires_review_without_botanical_fields():
    result = DeclarationApplicabilityService().evaluate(
        _facts(
            hts10="4407",
            plant_material=PlantMaterialEvidence.PRESENT,
        )
    )

    assert result.scope is DeclarationScope.REVIEW_REQUIRED
    assert result.reason_codes == ("VALID_HTS10_REQUIRED",)
    assert result.requires_botanical_fields is False


def test_valid_hts10_outside_schedule_is_not_required_even_when_material_unknown():
    result = DeclarationApplicabilityService().evaluate(
        _facts(
            hts10="7613000000",
            plant_material=PlantMaterialEvidence.UNKNOWN,
        )
    )

    assert result.scope is DeclarationScope.NOT_REQUIRED
    assert result.reason_codes == ("HTS_NOT_ON_APHIS_SCHEDULE",)
    assert result.requires_botanical_fields is False


def test_scheduled_hts_with_explicit_absent_plant_material_is_not_required():
    result = DeclarationApplicabilityService().evaluate(
        _facts(
            hts10="4407990190",
            plant_material=PlantMaterialEvidence.ABSENT,
        )
    )

    assert result.scope is DeclarationScope.NOT_REQUIRED
    assert result.reason_codes == ("NO_PLANT_MATERIAL",)
    assert result.requires_botanical_fields is False


def test_scheduled_hts_with_unknown_plant_material_requires_review_only():
    result = DeclarationApplicabilityService().evaluate(
        _facts(
            hts10="4407990190",
            plant_material=PlantMaterialEvidence.UNKNOWN,
        )
    )

    assert result.scope is DeclarationScope.REVIEW_REQUIRED
    assert result.reason_codes == ("PLANT_MATERIAL_NOT_ESTABLISHED",)
    assert result.requires_botanical_fields is False


def test_scheduled_hts_with_present_plant_material_can_materialize_botanical_fields():
    result = DeclarationApplicabilityService().evaluate(
        _facts(
            hts10="4407990190",
            plant_material=PlantMaterialEvidence.PRESENT,
        )
    )

    assert result.scope is DeclarationScope.POTENTIALLY_REQUIRED
    assert result.reason_codes == (
        "HTS_ON_APHIS_SCHEDULE",
        "PLANT_MATERIAL_PRESENT",
    )
    assert result.requires_botanical_fields is True
