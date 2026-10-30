"""Public packet: raw persisted observations reach the legacy review seam."""
from pathlib import Path
from types import SimpleNamespace

import pytest

from litoral_trace.assurance.parsers import parse_document
from litoral_trace.assurance.processing import _persist_raw_parsed_fields
from litoral_trace.lacey_engine.documentary_review_rows import documentary_review_rows
from litoral_trace.lacey_engine.pipeline import process_bundle
from litoral_trace.lacey_engine.errors import LaceyEngineError
from litoral_trace.us_lacey.projection import _target_field
from litoral_trace.us_lacey.document_evidence_class import classify_document
from litoral_trace.us_lacey.quick_review_eligibility import quick_accept_eligible

ROOT = Path(__file__).parent / 'fixtures' / 'us_lacey_quality_output'
NAME = 'Atlantic Home Goods Distribution Inc.'
ADDRESS = '825 Portside Logistics Pkwy., Pooler, GA 31322, United States'
DESCRIPTION = 'Sawn wood boards, wooden shipping crates, and solid-wood dining chairs.'


class RawRows:
    def __init__(self):
        self.rows = []

    def add(self, row):
        row.id = len(self.rows) + 1
        self.rows.append(row)


@pytest.fixture(scope='module')
def packet_rows():
    documents = []
    for path in sorted(ROOT.iterdir()):
        if path.suffix not in {'.pdf', '.xlsx'}:
            continue
        parsed = parse_document(path.name, path.read_bytes())
        raw = RawRows()
        _persist_raw_parsed_fields(raw, organization_id=1,
                                  assurance_document=SimpleNamespace(id=1),
                                  extraction_run=SimpleNamespace(id=1), parsed=parsed)
        documents.append((path.name, raw.rows))
    return documents


def _projected(rows):
    headers = {}
    for row in rows:
        parts = row.field_name.split('.', 3)
        if len(parts) == 4 and parts[:2] == ['raw', 'table']:
            headers.setdefault(int(parts[2]), set()).add(parts[3])
    # The public projector folds headers; use its existing helper.
    from litoral_trace.us_lacey.projection import _fold
    headers = {k: frozenset(_fold(v) for v in values) for k, values in headers.items()}
    return [(target, row) for row in rows
            if (target := _target_field(row, table_headers=headers)[0]) in
            {'consignee_name', 'consignee_address', 'merchandise_description'}]


def test_all_three_original_sources_feed_separate_consignee_fields(packet_rows):
    invoice_name, original = packet_rows[0]
    assert invoice_name.startswith('01_')
    before = [(r.field_name, r.original_value, r.source_locator) for r in original]
    rows = documentary_review_rows(original)
    projected = _projected(rows)
    names = [r for target, r in projected if target == 'consignee_name']
    addresses = [r for target, r in projected if target == 'consignee_address']
    assert names and {r.original_value for r in names} == {NAME}
    assert addresses and {r.original_value for r in addresses} == {ADDRESS}
    assert any('pdf:page:1;table:1;' in r.source_locator for r in addresses)
    assert any(r.source_page == 1 and r.id in {o.id for o in original} for r in addresses)
    # Original evidence rows, IDs, locators and customer bytes remain unchanged.
    assert before == [(r.field_name, r.original_value, r.source_locator) for r in original]


def test_explicit_shipment_description_is_recovered_without_mapping_commercial_rows(packet_rows):
    normalized = [(filename, _projected(documentary_review_rows(rows))) for filename, rows in packet_rows]
    invoice = normalized[0][1]
    descriptions = [r for target, r in invoice if target == 'merchandise_description']
    assert descriptions and {r.original_value for r in descriptions} == {DESCRIPTION}
    assert all(r.source_page == 2 for r in descriptions)
    assert all('table:1;' in r.source_locator for r in descriptions)
    # Three different commercial descriptions remain line-level observations,
    # rather than conflicting scalar values or an invented BOM composition.
    assert not any('pdf:page:1;table:2;' in r.source_locator for r in descriptions)
    workbook = normalized[2][1]
    assert [(target, r.original_value) for target, r in workbook] == [
        ('consignee_name', NAME), ('consignee_address', ADDRESS),
        ('merchandise_description', DESCRIPTION),
    ]
    assert all('sheet:Shipment Data;' in r.source_locator for _, r in workbook)
    assert not normalized[1][1]


def test_packet_worksheet_is_never_eligible_for_quick_confirmation(packet_rows):
    filename, rows = packet_rows[0]
    assert classify_document(filename, 'COMMERCIAL_INVOICE').code == 'DERIVED_PREPARATION'
    for target, row in _projected(documentary_review_rows(rows)):
        field = SimpleNamespace(field_name=target, status='REVIEW',
                                source_assurance_document_id=1, source_filename=filename)
        candidate = SimpleNamespace(id=1, source_document_id=1, decision='PENDING',
                                    original_value=row.original_value,
                                    normalized_value=row.normalized_value, validation_status='VALID')
        assert not quick_accept_eligible(field, candidate)


def test_existing_xlsx_engine2_partial_boundary_is_not_bypassed():
    path = ROOT / '03_BOM_Packing_List.xlsx'
    with pytest.raises(LaceyEngineError):
        process_bundle(filename=path.name, content=path.read_bytes())


def _raw_row(header, value, *, table=1, row=1, column=1):
    return SimpleNamespace(id=1, field_name=f'raw.table.{table}.{header}',
                           original_value=value, normalized_value=value, confidence=0.98,
                           source_locator=f'table:{table};data_row:{row};column:{column}', source_page=1)


@pytest.mark.parametrize('value', [
    'Ambiguous LLC | Care of Another LLC',
    'Atlantic LLC | 42 First Street | 10 Other Street',
    'Atlantic LLC 42 First Street',
    'Only Company LLC',
])
def test_uncertain_party_blocks_are_not_split_or_given_an_invented_address(value):
    source = _raw_row('Consignee', value)
    result = documentary_review_rows([source])
    assert len(result) == 1
    assert result[0].original_value == value
    assert _target_field(result[0])[0] == 'consignee_name'
    assert source.original_value == value


def test_distinct_literal_consignee_blocks_retain_conflicting_addresses():
    rows = [_raw_row('Consignee', 'ACME LLC | 42 Main Street', row=1),
            _raw_row('Consignee', 'ACME LLC | 900 Other Street', row=2)]
    projected = _projected(documentary_review_rows(rows))
    assert {r.original_value for target, r in projected if target == 'consignee_address'} == {
        '42 Main Street', '900 Other Street',
    }


def test_normalization_leaves_unrelated_regulatory_control_observations_unchanged():
    rows = [_raw_row(header, value, row=index) for index, (header, value) in enumerate([
        ('Importer Name', 'Northstar LLC'), ('Importer Address', '104 Harbor Commerce Blvd.'),
        ('Manufacturer Identification Code (MID)', 'VNMFACT123HCM'),
        ('Bill of Lading', 'MAEU-TEST-260921-01'), ('HTS Number', '4407.99.0190'),
        ('Species', 'grandis'), ('Country of Harvest', 'Brazil'), ('Entry Number', '123-4567890-1'),
    ], 1)]
    normalized = documentary_review_rows(rows)
    assert len(normalized) == len(rows)
    assert all(actual is original for actual, original in zip(normalized, rows))
