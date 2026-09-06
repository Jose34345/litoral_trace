from types import SimpleNamespace

import pytest

from litoral_trace.us_lacey.projection import (
    _fold,
    _is_candidate_admissible,
    _is_structural_artifact,
    _target_field,
)


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
        "COMM Number Qualifier",
        "Consignee Name",
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


def test_real_importinfo_url_cannot_become_container_number():
    value = "www.importinfo.com/wood-brokerage-international?utm_source=chatgpt.com"
    row = SimpleNamespace(
        field_name="raw.table.1.Container Number",
        original_value=value,
        normalized_value=None,
    )
    assert _is_candidate_admissible("container_number", value) is False
    assert _target_field(row) == (None, 0)


@pytest.mark.parametrize(
    "value",
    ["MSKU9228574", "MSKU 9228574", "MSKU-922857-4", "CSQU 305438 3"],
)
def test_valid_container_identifiers_allow_common_source_separators(value: str):
    row = SimpleNamespace(
        field_name="raw.table.1.Container Number",
        original_value=value,
        normalized_value=None,
    )
    assert _is_candidate_admissible("container_number", value) is True
    assert _target_field(row) == ("container_number", 3)


@pytest.mark.parametrize(
    "value",
    ["EEUU", "US", "USA", "UNITED STATES", "UNITED STATES OF AMERICA"],
)
def test_country_only_values_cannot_become_importer_names(value: str):
    row = SimpleNamespace(
        field_name="raw.table.1.Importer Name",
        original_value=value,
        normalized_value=None,
    )
    assert _is_candidate_admissible("importer_name", value) is False
    assert _target_field(row) == (None, 0)


def test_any_known_table_header_is_rejected_when_parser_shifts_it_into_a_value():
    table_headers = frozenset(
        {
            _fold("Consignee Name"),
            _fold("COMM Number Qualifier"),
            _fold("Marks and Numbers 1"),
        }
    )
    consignee = SimpleNamespace(
        field_name="raw.table.1.Consignee Name",
        original_value="COMM Number Qualifier",
        normalized_value=None,
    )
    description = SimpleNamespace(
        field_name="raw.table.2.Cargo Description 1",
        original_value="Marks and Numbers 1",
        normalized_value=None,
    )

    assert _target_field(consignee, table_headers=table_headers) == (None, 0)
    assert _target_field(description, table_headers=table_headers) == (None, 0)


def test_explicit_lading_and_description_headers_map_to_safe_targets():
    bol = SimpleNamespace(
        field_name="raw.table.1.Master BOL #",
        original_value="MAEU274342495",
        normalized_value=None,
    )
    cargo = SimpleNamespace(
        field_name="raw.table.2.Cargo Description 1",
        original_value="SINGLE PACKS OF PINUS RADIATA TIMBER PINUS RADIATA",
        normalized_value=None,
    )
    commodity = SimpleNamespace(
        field_name="raw.table.2.Commodity Description",
        original_value="SINGLE PACKS OF PINUS RADIATA TIMBER",
        normalized_value=None,
    )

    assert _target_field(bol) == ("bill_of_lading", 3)
    assert _target_field(cargo) == ("merchandise_description", 3)
    assert _target_field(commodity) == ("merchandise_description", 3)


def test_valid_container_consignee_importer_and_description_controls_remain_admissible():
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
    importer = SimpleNamespace(
        field_name="raw.table.1.Importer Name",
        original_value="WOOD BROKERAGE INTERNATIONAL LLC",
        normalized_value=None,
    )
    description = SimpleNamespace(
        field_name="raw.table.2.Cargo Description 1",
        original_value="SINGLE PACKS OF PINUS RADIATA TIMBER",
        normalized_value=None,
    )

    assert _target_field(container) == ("container_number", 3)
    assert _target_field(consignee) == ("consignee_name", 3)
    assert _target_field(importer) == ("importer_name", 3)
    assert _target_field(description) == ("merchandise_description", 3)
