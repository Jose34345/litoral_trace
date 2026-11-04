from __future__ import annotations

import pytest

from litoral_trace.assurance.extraction import _candidate_from_table
from litoral_trace.us_lacey.projection import _is_candidate_admissible


@pytest.mark.parametrize(
    "target,value",
    [
        ("species", "WOODEN, CUTTING, BOARDS"),
        ("species", "WOODEN-CUTTING-BOARDS"),
        ("species", "wood"),
        ("species", "Cutting board"),
        ("species", "hardwood lumber"),
        ("genus", "PALLET"),
        ("genus", "wooden pallets"),
    ],
)
def test_taxonomy_commercial_stop_words_are_rejected(target: str, value: str) -> None:
    assert _is_candidate_admissible(target, value) is False


def test_assurance_species_extraction_returns_none_for_commercial_taxonomy() -> None:
    candidate = _candidate_from_table(
        "species",
        "WOODEN, CUTTING, BOARDS",
        locator="test:species",
        page=1,
        confidence=0.99,
    )
    assert candidate is None


def test_legitimate_botanical_values_remain_admissible() -> None:
    assert _is_candidate_admissible("genus", "Quercus") is True
    assert _is_candidate_admissible("species", "rubra") is True
    assert _is_candidate_admissible("species", "Pinus taeda") is True


def test_stop_words_do_not_block_non_taxonomy_fields() -> None:
    assert _is_candidate_admissible("merchandise_description", "WOODEN CUTTING BOARDS") is True
