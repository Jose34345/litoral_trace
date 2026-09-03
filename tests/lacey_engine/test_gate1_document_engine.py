from __future__ import annotations

from pathlib import Path

import pytest

from litoral_trace.lacey_engine.admission import admit
from litoral_trace.lacey_engine.domain import (
    EvidenceClass, FieldStatus, LayoutBlock, ParsedLayout, RawCandidate, ResolvedField,
)
from litoral_trace.lacey_engine.layout_parser import layout_from_key_value_rows
from litoral_trace.lacey_engine.pipeline import _extract, process_document


def _raw(field: str, value: str, label: str) -> RawCandidate:
    block = LayoutBlock("test-1", 1, None, f"{label}: {value}", "TABLE_ROW", key_text=label, value_text=value)
    return RawCandidate(field, value, value, block, EvidenceClass.EXPLICIT, "test", "1", label=label)


def test_vertical_key_value_table_keeps_each_row_value_with_its_own_label():
    layout = layout_from_key_value_rows([
        ("Container Number", "MSKU9228574"),
        ("Seal Number 1", "NZ681338"),
        ("Container Type", "45G1"),
    ])
    assert [(block.key_text, block.value_text) for block in layout.blocks] == [
        ("Container Number", "MSKU9228574"),
        ("Seal Number 1", "NZ681338"),
        ("Container Type", "45G1"),
    ]
    extracted = _extract(layout)
    assert [candidate.normalized_value for candidate in extracted["container_number"]] == ["MSKU9228574"]


def test_container_admission_rejects_labels_and_accepts_explicit_iso_shape():
    assert admit(_raw("container_number", "MSKU9228574", "Container Number"))
    assert not admit(_raw("container_number", "Seal Number 1", "Container Number"))
    assert not admit(_raw("container_number", "Container Height", "Container Number"))


def test_scientific_taxon_yields_explicit_species_and_derived_genus():
    layout = ParsedLayout((LayoutBlock("p1-l1", 1, None, "SINGLE PACKS OF PINUS RADIATA TIMBER", "TEXT_LINE"),), 1)
    extracted = _extract(layout)
    assert extracted["species"][0].normalized_value == "radiata"
    assert extracted["species"][0].evidence_class is EvidenceClass.EXPLICIT
    assert extracted["genus"][0].normalized_value == "Pinus"
    assert extracted["genus"][0].evidence_class is EvidenceClass.DERIVED
    assert extracted["genus"][0].derived_from_field_key == "species"


def test_country_of_harvest_is_not_inferred_from_new_zealand_context():
    layout = layout_from_key_value_rows([
        ("Supplier", "Southwood NZ Limited"),
        ("Port", "Port Chalmers, New Zealand"),
        ("Country Code", "NZ"),
    ])
    assert _extract(layout)["country_of_harvest"] == []


def test_field_status_invariants_reject_impossible_states():
    with pytest.raises(ValueError, match="MATCHED"):
        ResolvedField("container_number", FieldStatus.MATCHED, None, None)
    with pytest.raises(ValueError, match="MISSING"):
        ResolvedField("container_number", FieldStatus.MISSING, None, object())
    with pytest.raises(ValueError, match="CONFLICT"):
        ResolvedField("container_number", FieldStatus.CONFLICT, None, None, ())


def test_real_import_info_gate1_regression_fixture():
    fixture = Path(__file__).parents[1] / "fixtures" / "import_info_wood_brokerage_real.pdf"
    if not fixture.exists():
        pytest.skip(f"REAL_FIXTURE_NOT_AVAILABLE: {fixture}")
    resolution = process_document(filename=fixture.name, content=fixture.read_bytes(), role_hint="SUPPLIER_SHEET")
    expected = {
        "estimated_arrival_date": "2026-09-01",
        "bill_of_lading": "MAEU274342495",
        "container_number": "MSKU9228574",
        "consignee_name": "WOOD BROKERAGE INTERNATIONAL",
        "species": "radiata",
        "genus": "Pinus",
    }
    for key, value in expected.items():
        assert resolution.field(key).status is FieldStatus.MATCHED
        assert resolution.field(key).effective_value == value
        winner = resolution.field(key).winning_candidate
        assert winner is not None
        assert winner.provenance.page >= 1
        assert winner.provenance.bbox is not None
        assert winner.provenance.block_id
        assert winner.provenance.source_text
        assert winner.provenance.extractor_name
        assert winner.provenance.extractor_version
    assert resolution.field("consignee_address").status is FieldStatus.MATCHED
    assert resolution.field("consignee_address").effective_value == "SUITE 130 5285 MEADOWS RD LAKE OSWE; LAKE OSWEGO, OR 97035"
    for key in ("filing_entry_reference", "manufacturer_id", "hts_code", "country_of_harvest"):
        assert resolution.field(key).status is FieldStatus.MISSING
