from types import SimpleNamespace

import pytest

from litoral_trace.us_lacey.projection import _is_structural_artifact, _target_field


@pytest.mark.parametrize(
    "value",
    [
        "Seal Number 1",
        "Equipment Description Code",
        "Equipment Description",
        "Container Length",
        "Container Height",
        "Container Width",
        "Container Type",
        "Load Status",
        "URL",
    ],
)
def test_container_structural_labels_are_rejected_before_candidate_admission(value: str):
    row = SimpleNamespace(
        field_name="raw.table.1.Container Number",
        original_value=value,
        normalized_value=None,
    )
    assert _is_structural_artifact("container_number", value) is True
    assert _target_field(row) == (None, 0)


@pytest.mark.parametrize(
    "value",
    [
        "Address Line 1",
        "City",
        "State Province",
        "Zip Code",
        "Country Code",
        "COMM Number",
        "COMM Number 1",
    ],
)
def test_consignee_structural_labels_are_rejected_before_candidate_admission(value: str):
    row = SimpleNamespace(
        field_name="raw.table.1.Consignee Name",
        original_value=value,
        normalized_value=None,
    )
    assert _is_structural_artifact("consignee_name", value) is True
    assert _target_field(row) == (None, 0)


def test_valid_container_and_consignee_controls_remain_admissible():
    container = SimpleNamespace(
        field_name="raw.table.1.Container Number",
        original_value="MSKU9228574",
        normalized_value=None,
    )
    consignee = SimpleNamespace(
        field_name="raw.table.1.Consignee Name",
        original_value="WOOD BROKERAGE INTERNATIONAL",
        normalized_value=None,
    )

    assert _is_structural_artifact("container_number", container.original_value) is False
    assert _target_field(container) == ("container_number", 3)
    assert _is_structural_artifact("consignee_name", consignee.original_value) is False
    assert _target_field(consignee) == ("consignee_name", 3)
