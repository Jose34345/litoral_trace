"""Issue 473: literal public fixtures through extraction, binding and publication."""
from pathlib import Path

import pytest

from litoral_trace.lacey_engine.domain import (
    BoundingBox, FieldStatus, LayoutBlock, LayoutStructureType, ParsedLayout,
)
from litoral_trace.lacey_engine.pipeline import _extract, process_bundle, process_document
from litoral_trace.lacey_engine.serialization import serialize_shipment_resolution
from litoral_trace.lacey_engine.shipment import ShipmentDocumentInput, process_shipment
from litoral_trace.us_lacey.canonical_shipment_truth import build_canonical_shipment_truth
from litoral_trace.us_lacey.document_evidence_class import classify_document

FIXTURES = Path(__file__).parents[1] / 'fixtures' / 'us_lacey_quality_output'
INVOICE = FIXTURES / '01_Commercial_Invoice_Entry_Worksheet.pdf'
NAME = 'Atlantic Home Goods Distribution Inc.'
ADDRESS = '825 Portside Logistics Pkwy., Pooler, GA 31322, United States'
DESCRIPTION = 'Sawn wood boards, wooden shipping crates, and solid-wood dining chairs.'
LINE_DESCRIPTIONS = (
    'Sawn eucalyptus boards, kiln-dried',
    'Wooden shipping crates, assembled',
    'Solid-wood dining chairs',
)


@pytest.fixture(scope='module')
def invoice_bundle():
    return process_bundle(filename=INVOICE.name, content=INVOICE.read_bytes())


def test_fixture_consignee_name_already_preserves_di03_separation(invoice_bundle):
    field = invoice_bundle.documents[0].resolution.field('consignee_name')
    assert field.status is FieldStatus.MATCHED
    assert field.effective_value == NAME
    assert all(c.raw.normalized_value == NAME for c in field.candidates)
    assert any(c.provenance.block_id == 'p1-t1-r2' for c in field.candidates)


def test_fixture_wrapped_consignee_address_uses_its_complete_physical_cell(invoice_bundle):
    resolution = invoice_bundle.documents[0].resolution
    field = resolution.field('consignee_address')
    assert field.status is FieldStatus.MATCHED
    assert field.effective_value == ADDRESS
    assert {c.raw.normalized_value for c in field.candidates} == {ADDRESS}
    winner = field.winning_candidate
    assert winner.provenance.page == 1
    assert winner.provenance.block_id == 'p1-t1-r2'
    assert winner.provenance.source_text == f'Consignee: {NAME} | {ADDRESS}'
    assert winner.provenance.bbox is not None
    # Original visual lines remain inspectable; no source text is overwritten.
    assert any(b.block_id == 'p1-l9' and b.text.endswith('31322,') for b in resolution.layout.blocks)
    assert any(b.block_id == 'p1-l10' and b.text == 'United States' for b in resolution.layout.blocks)


def test_fixture_explicit_merchandise_descriptions_are_not_lost(invoice_bundle):
    field = invoice_bundle.documents[0].resolution.field('description')
    by_block = {c.provenance.block_id: c for c in field.candidates}
    assert by_block['p1-l33'].raw.normalized_value == DESCRIPTION
    for row, expected in enumerate(LINE_DESCRIPTIONS, 1):
        candidate = by_block[f'p1-t2-r{row}-c2']
        assert candidate.raw.normalized_value == expected
        assert candidate.provenance.source_text == f'Description of Merchandise: {expected}'
        assert candidate.provenance.page == 1
        assert candidate.raw.source_block.row_index == row
    assert not any('HTS Number Qty' in c.raw.normalized_value for c in field.candidates)


@pytest.mark.parametrize('kind', ['TABLE_ROW', 'TEXT_LINE'])
def test_explicit_description_label_is_supported_without_inference(monkeypatch, kind):
    block = LayoutBlock('p1-desc', 1, None, f'Description of Merchandise: {DESCRIPTION}', kind,
                        key_text='Description of Merchandise' if kind == 'TABLE_ROW' else None,
                        value_text=DESCRIPTION if kind == 'TABLE_ROW' else None)
    monkeypatch.setattr('litoral_trace.lacey_engine.pipeline.parse_layout', lambda *_: ParsedLayout((block,), 1))
    field = process_document(filename='commercial.pdf', content=b'unused').field('description')
    assert field.status is FieldStatus.MATCHED
    assert field.effective_value == DESCRIPTION
    assert field.winning_candidate.provenance.source_text == block.text


def _party_layout(*, row_bbox=BoundingBox(0, 10, 500, 50), page=1, full_address=ADDRESS):
    line = LayoutBlock('p1-line', 1, BoundingBox(0, 15, 490, 25),
                       f'Consignee {NAME} | 825 Portside Logistics Pkwy., Pooler, GA 31322,', 'TEXT_LINE')
    row = LayoutBlock('table-row', page, row_bbox, f'Consignee: {NAME} | {full_address}', 'TABLE_ROW',
                      LayoutStructureType.KEY_VALUE_TABLE, 'table', 0, None,
                      'Consignee', 'Consignee', f'{NAME} | {full_address}')
    return ParsedLayout((line, row), max(1, page))


def test_same_physical_party_row_does_not_create_a_truncated_address_candidate():
    candidates = _extract(_party_layout())['consignee_address']
    assert {c.normalized_value for c in candidates} == {ADDRESS}
    assert {c.source_block.block_id for c in candidates} == {'table-row'}


@pytest.mark.parametrize('change', ['no_coordinates', 'other_row', 'other_page', 'different_address'])
def test_prefix_alone_never_erases_independent_address_evidence(change):
    kwargs = {
        'no_coordinates': {'row_bbox': None},
        'other_row': {'row_bbox': BoundingBox(0, 80, 500, 120)},
        'other_page': {'page': 2},
        'different_address': {'full_address': '900 Other Street, Pooler, GA 31322, United States'},
    }[change]
    candidates = _extract(_party_layout(**kwargs))['consignee_address']
    assert len({c.normalized_value for c in candidates}) == 2
    assert {c.source_block.block_id for c in candidates} == {'p1-line', 'table-row'}


def test_fixture_values_reach_canonical_review_with_source_row_identity_and_provenance(invoice_bundle):
    # Commercial page only: do not use worksheet rows to manufacture authority.
    source = invoice_bundle.documents[0]
    payload = serialize_shipment_resolution(process_shipment(documents=[
        ShipmentDocumentInput('17:logical-001', INVOICE.name, resolution=source.resolution),
    ]))
    truth = build_canonical_shipment_truth(payload)
    assert truth.shipment_fields['consignee_name'].values == (NAME,)
    assert truth.shipment_fields['consignee_address'].values == (ADDRESS,)
    by_row = {line.entity_key: line for line in truth.plant_lines}
    assert len(by_row) == 3
    for row, expected in enumerate(LINE_DESCRIPTIONS, 1):
        line_key = f'17:logical-001:p1-t2:row:{row}'
        field = by_row[line_key].fields['merchandise_description']
        assert {e.line_key for e in field.evidence} == {line_key}
        sku_cell = next(b for b in source.resolution.layout.blocks if b.block_id == f'p1-t2-r{row}-c1')
        assert sku_cell.value_text == ('EUC-BRD-01', 'PINE-CRT-02', 'TEAK-CHR-03')[row - 1]
        assert field.values == (expected,)
        assert all(e.document_id == '17:logical-001' and e.source_page == 1 for e in field.evidence)
        assert all(e.source_text == f'Description of Merchandise: {expected}' for e in field.evidence)
    description = truth.shipment_fields['merchandise_description']
    assert all(value in ' '.join(description.values) for value in LINE_DESCRIPTIONS)
    assert description.evidence
    # Extraction/ranking is not the human review authority gate.
    assert classify_document(INVOICE.name, 'COMMERCIAL_INVOICE').code == 'DERIVED_PREPARATION'


def test_quality_version_changes_document_identity_and_source_set_fingerprint():
    from types import SimpleNamespace
    from litoral_trace.lacey_engine.pipeline import ENGINE_VERSION
    from litoral_trace.us_lacey.lacey_engine_service import UsLaceyEngine2Service, source_set_fingerprint
    assert ENGINE_VERSION != 'lacey-engine-2.6.3'
    old = UsLaceyEngine2Service(vault_service=None, engine_version='lacey-engine-2.6.3')
    current = UsLaceyEngine2Service(vault_service=None)
    identity = dict(organization_id=1, assurance_document_id=17,
                    source_sha256='fixture-hash', role_hint='UNKNOWN', status='SUCCEEDED')
    assert old._engine2_document_run_identity(**identity) != current._engine2_document_run_identity(**identity)
    kwargs = dict(organization_id=1, operation_id=2, documents=[
        (SimpleNamespace(id=17, assurance_document_id=17, version_number=1),
         SimpleNamespace(sha256='fixture-hash')),
    ])
    assert source_set_fingerprint(**kwargs, engine_version='lacey-engine-2.6.3') != source_set_fingerprint(**kwargs)
