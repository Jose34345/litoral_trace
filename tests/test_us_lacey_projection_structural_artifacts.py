from types import SimpleNamespace

import pytest

from litoral_trace.us_lacey.projection import (
    _fold,
    _is_candidate_admissible,
    _is_structural_artifact,
    _is_supported_explicit_suggestion,
    _line_reference,
    _target_field,
)


def _row(*, field_name: str, original_value: str):
    """Match the ExtractedDocumentField attributes consumed by projection."""
    return SimpleNamespace(
        field_name=field_name,
        original_value=original_value,
        normalized_value=None,
        source_locator=None,
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
    row = _row(field_name="raw.table.1.Container Number", original_value=value)
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
    row = _row(field_name="raw.table.1.Consignee Name", original_value=value)
    assert _is_structural_artifact("consignee_name", value) is True
    assert _target_field(row) == (None, 0)


def test_real_importinfo_url_cannot_become_container_number():
    value = "www.importinfo.com/wood-brokerage-international?utm_source=chatgpt.com"
    row = _row(field_name="raw.table.1.Container Number", original_value=value)
    assert _is_candidate_admissible("container_number", value) is False
    assert _target_field(row) == (None, 0)


@pytest.mark.parametrize(
    "value",
    ["MSKU9228574", "MSKU 9228574", "MSKU-922857-4", "CSQU 305438 3"],
)
def test_valid_container_identifiers_allow_common_source_separators(value: str):
    row = _row(field_name="raw.table.1.Container Number", original_value=value)
    assert _is_candidate_admissible("container_number", value) is True
    assert _target_field(row) == ("container_number", 3)


@pytest.mark.parametrize(
    "value",
    ["EEUU", "US", "USA", "UNITED STATES", "UNITED STATES OF AMERICA"],
)
def test_country_only_values_cannot_become_importer_names(value: str):
    row = _row(field_name="raw.table.1.Importer Name", original_value=value)
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
    consignee = _row(
        field_name="raw.table.1.Consignee Name",
        original_value="COMM Number Qualifier",
    )
    description = _row(
        field_name="raw.table.2.Cargo Description 1",
        original_value="Marks and Numbers 1",
    )

    assert _target_field(consignee, table_headers=table_headers) == (None, 0)
    assert _target_field(description, table_headers=table_headers) == (None, 0)


def test_explicit_lading_and_description_headers_map_to_safe_targets():
    bol = _row(
        field_name="raw.table.1.Master BOL #",
        original_value="MAEU274342495",
    )
    cargo = _row(
        field_name="raw.table.2.Cargo Description 1",
        original_value="SINGLE PACKS OF PINUS RADIATA TIMBER PINUS RADIATA",
    )
    commodity = _row(
        field_name="raw.table.2.Commodity Description",
        original_value="SINGLE PACKS OF PINUS RADIATA TIMBER",
    )

    assert _target_field(bol) == ("bill_of_lading", 3)
    assert _target_field(cargo) == ("merchandise_description", 3)
    assert _target_field(commodity) == ("merchandise_description", 3)


def test_valid_container_consignee_importer_and_description_controls_remain_admissible():
    container = _row(
        field_name="raw.table.1.Container Number",
        original_value="MSKU9228574",
    )
    consignee = _row(
        field_name="raw.table.1.Consignee Name",
        original_value="WOOD BROKERAGE INTERNATIONAL",
    )
    importer = _row(
        field_name="raw.table.1.Importer Name",
        original_value="WOOD BROKERAGE INTERNATIONAL LLC",
    )
    description = _row(
        field_name="raw.table.2.Cargo Description 1",
        original_value="SINGLE PACKS OF PINUS RADIATA TIMBER",
    )

    assert _target_field(container) == ("container_number", 3)
    assert _target_field(consignee) == ("consignee_name", 3)
    assert _target_field(importer) == ("importer_name", 3)
    assert _target_field(description) == ("merchandise_description", 3)



def test_evidence_only_customs_broker_alias_is_not_routed_into_ppq_contract():
    row = _row(
        field_name="raw.table.1.Customs Broker",
        original_value="Harbor Customs Brokerage LLC",
    )

    assert _target_field(row) == (None, 0)


def test_unknown_non_ppq_target_fails_closed_in_line_reference():
    assert _line_reference(
        target="filer_name",
        source_locator="table:1;data_row:1;column:1",
        line_references=("1",),
    ) == ""


def test_bom_component_header_is_not_a_ppq_article_component():
    row = _row(
        field_name="raw.table.1.Component",
        original_value="Chair leg",
    )
    bom_headers = frozenset(
        {"sku", "product", "component", "material", "qty", "weight", "uom"}
    )

    assert _target_field(row, table_headers=bom_headers) == (None, 0)


def test_generic_pdf_line_item_description_is_not_shipment_description_when_table_is_allocated():
    row = SimpleNamespace(
        field_name="product",
        original_value="Sawn eucalyptus boards, kiln-dried",
        normalized_value=None,
        source_locator="pdf:page:1;table:2;header_row:1;data_row:1;column:3;header:Description of Merchandise",
    )
    headers = {
        2: frozenset({"line", "hts number", "description", "entered value"}),
    }

    assert _target_field(row, table_headers=headers) == (None, 0)


def test_generic_bom_material_cannot_become_shipment_merchandise_description():
    row = SimpleNamespace(
        field_name="product",
        original_value="Eucalyptus grandis",
        normalized_value=None,
        source_locator="sheet:BOM;header_row:1;data_row:1;column:4;header:Material",
    )

    assert _target_field(row, table_headers={}) == (None, 0)


def test_exact_high_confidence_ppq_raw_cell_is_supported_even_with_parser_review_flag():
    source = SimpleNamespace(
        field_name="raw.table.2.HTS Number",
        confidence=0.98,
        needs_review=True,
    )

    assert _is_supported_explicit_suggestion(
        source=source,
        source_priority=3,
        validation_status="VALID",
    ) is True


def test_generic_or_low_confidence_review_evidence_stays_out_of_supported_bucket():
    generic = SimpleNamespace(field_name="product", confidence=0.98, needs_review=True)
    low = SimpleNamespace(
        field_name="raw.table.1.HTS Number", confidence=0.89, needs_review=True
    )

    assert _is_supported_explicit_suggestion(
        source=generic, source_priority=2, validation_status="VALID"
    ) is False
    assert _is_supported_explicit_suggestion(
        source=low, source_priority=3, validation_status="VALID"
    ) is False