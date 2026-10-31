from __future__ import annotations

import pytest

from litoral_trace.us_lacey.regulatory.applicability.domain import (
    ApplicabilityDecision,
    DeclarationScope,
    MerchandiseLineFacts,
    PlantMaterialEvidence,
)


def test_merchandise_line_requires_identity():
    with pytest.raises(ValueError, match="line_key"):
        MerchandiseLineFacts(
            line_key="",
            hts10=None,
            description=None,
            entered_value=None,
            plant_material=PlantMaterialEvidence.UNKNOWN,
        )


@pytest.mark.parametrize(
    ("scope", "requires_botanical_fields"),
    (
        (DeclarationScope.NOT_REQUIRED, True),
        (DeclarationScope.REVIEW_REQUIRED, True),
        (DeclarationScope.POTENTIALLY_REQUIRED, False),
    ),
)
def test_applicability_decision_rejects_impossible_scope_states(
    scope,
    requires_botanical_fields,
):
    with pytest.raises(ValueError):
        ApplicabilityDecision(
            line_key="line-1",
            scope=scope,
            reason_codes=("TEST_REASON",),
            catalog_version="test-catalog",
            requires_botanical_fields=requires_botanical_fields,
        )


@pytest.mark.parametrize(
    ("scope", "requires_botanical_fields"),
    (
        (DeclarationScope.NOT_REQUIRED, False),
        (DeclarationScope.REVIEW_REQUIRED, False),
        (DeclarationScope.POTENTIALLY_REQUIRED, True),
    ),
)
def test_applicability_decision_accepts_valid_scope_states(
    scope,
    requires_botanical_fields,
):
    decision = ApplicabilityDecision(
        line_key="line-1",
        scope=scope,
        reason_codes=("TEST_REASON",),
        catalog_version="test-catalog",
        requires_botanical_fields=requires_botanical_fields,
    )

    assert decision.scope is scope


def test_applicability_decision_requires_catalog_and_reason_codes():
    with pytest.raises(ValueError, match="catalog_version"):
        ApplicabilityDecision(
            line_key="line-1",
            scope=DeclarationScope.REVIEW_REQUIRED,
            reason_codes=("VALID_HTS10_REQUIRED",),
            catalog_version="",
            requires_botanical_fields=False,
        )

    with pytest.raises(ValueError, match="at least one reason"):
        ApplicabilityDecision(
            line_key="line-1",
            scope=DeclarationScope.REVIEW_REQUIRED,
            reason_codes=(),
            catalog_version="catalog-v1",
            requires_botanical_fields=False,
        )
