"""P0 DI-02: complete source SKU identity, never substring authorization."""
from dataclasses import replace
from uuid import UUID

import pytest

from litoral_trace.lacey_engine.ai_shadow import AICandidate
from litoral_trace.lacey_engine.domain import (
    DocumentResolution, DocumentType as EngineDocumentType, EvidenceClass,
    LayoutBlock, ParsedLayout,
)
from litoral_trace.lacey_engine.multi_agent.candidate_admission import evaluate_verified_candidate_admission
from litoral_trace.lacey_engine.multi_agent.contracts import CandidateEnvelope, DocumentType, SpecialistRole
from litoral_trace.lacey_engine.multi_agent.fusion import fuse_candidates, fusion_key
from litoral_trace.lacey_engine.multi_agent.line_binding import bind_line_items
from litoral_trace.us_lacey.specialized_shadow import _verify_candidates


def _verified(text, *, supplied_sku='ACT-TRAY-18', table=False, document=1):
    candidate = AICandidate(
        field_key='species', value='taeda', normalized_value='taeda',
        evidence_class=EvidenceClass.EXPLICIT, page=1, source_text=text,
        confidence=.99, provider='offline', model='fixture',
    )
    envelope = CandidateEnvelope(
        candidate, UUID(int=document), DocumentType.BOTANICAL_DECLARATION,
        SpecialistRole.BOTANICAL, UUID(int=3), None, UUID(int=4),
        source_line_key=supplied_sku,
        source_table_id='botanical' if table else None,
        source_row_index=2 if table else None,
    )
    resolution = DocumentResolution(
        'synthetic.pdf', 'fixture', EngineDocumentType.SPECIES_DECLARATION, 1.0,
        ParsedLayout((LayoutBlock('b1', 1, None, text, 'TEXT_LINE',
                                  table_id='botanical' if table else None,
                                  row_index=2 if table else None),), 1), {},
    )
    verified = _verify_candidates((envelope,), resolutions={envelope.document_id: resolution})[0]
    return envelope, verified


def test_sku_suffix_is_not_exact_source_identity():
    # Original audit reproducer, with only synthetic source text.
    _, verified = _verified('SKU OTHER-ACT-TRAY-18 Species Pinus taeda')
    assert bind_line_items((verified,))[0].line_item_key == 'SKU:OTHER-ACT-TRAY-18'


@pytest.mark.parametrize('sku', [
    'OTHER-ACT-TRAY-18', 'ACT-TRAY-18-OTHER', 'XACT-TRAY-18',
    'ACT-TRAY-180', 'ACT-TRAY-18_REV', 'ACT-TRAY-18/REV',
    'ACT-TRAY-18.REV', 'ACT-TRAY-18+REV',
])
@pytest.mark.parametrize('table', [False, True])
def test_partial_sidecar_cannot_override_complete_source_sku(sku, table):
    original, verified = _verified(f'SKU {sku} Species Pinus taeda', table=table)
    assert verified.source_line_key is None
    bound = bind_line_items((verified,))[0]
    assert bound.line_item_key == f'SKU:{sku}'
    assert bound.candidate.evidence_verified
    assert bound.candidate.source_text == original.candidate.source_text
    assert bound.candidate.value == original.candidate.value
    assert bound.document_id == original.document_id
    assert bound.candidate.page == 1
    assert bound.source_span_id == original.source_span_id
    if table:
        assert bound.source_table_id == 'botanical'
        assert bound.source_row_index == 2


@pytest.mark.parametrize('sku', ['ACT-TRAY-18', 'act-tray-18', 'ACT-TRAY-18-',
                                'ACT_TRAY_18', 'ACT/TRAY/18', 'ACT.TRAY-18',
                                'ACT-TRAY-18+REV'])
def test_exact_sku_keeps_significant_characters_and_is_admitted(sku):
    _, verified = _verified(f'SKU {sku} Species Pinus taeda', supplied_sku=f'  {sku}  ')
    bound = bind_line_items((verified,))[0]
    assert bound.line_item_key == 'SKU:' + sku.upper()
    admission = evaluate_verified_candidate_admission((bound,))
    assert admission.admitted_candidates == (bound,)


def test_two_supplier_identifiers_never_fuse_by_shared_description_or_hts():
    a = _verified('SKU VENDOR-A-ACT-18 Species Pinus taeda', document=1)[1]
    b = _verified('SKU VENDOR-B-ACT-18 Species Pinus taeda', document=2)[1]
    bound = bind_line_items((a, b))
    assert [c.line_item_key for c in bound] == ['SKU:VENDOR-A-ACT-18', 'SKU:VENDOR-B-ACT-18']
    for field, value in [('description', 'Solid wood tray'), ('hts_code', '4419199010')]:
        candidates = tuple(replace(c, candidate=replace(c.candidate, field_key=field,
                                                     value=value, normalized_value=value)) for c in bound)
        fused = fuse_candidates(candidates)
        assert len(fused.fused_candidates) == 2
        assert len({fusion_key(c) for c in fused.fused_candidates}) == 2


@pytest.mark.parametrize('reverse', [False, True])
def test_exact_identity_across_documents_is_order_independent(reverse):
    a = _verified('SKU ACT-TRAY-18 Species Pinus taeda', document=1)[1]
    b = _verified('SKU ACT-TRAY-18 Species Pinus taeda', document=2)[1]
    bound = bind_line_items((b, a) if reverse else (a, b))
    assert {c.line_item_key for c in bound} == {'SKU:ACT-TRAY-18'}
    assert len(fuse_candidates(bound).fused_candidates) == 1
    assert {c.document_id for c in bound} == {UUID(int=1), UUID(int=2)}


def test_sparse_evidence_without_sku_keeps_source_row_identity():
    _, verified = _verified('Species Pinus taeda', table=True)
    assert verified.source_line_key is None
    bound = bind_line_items((verified,))[0]
    assert bound.line_item_key == 'ROW:00000000-0000-0000-0000-000000000001:P1:TBOTANICAL:R2'

@pytest.mark.parametrize('table', [False, True])
def test_contradictory_label_beats_sidecar_even_when_alias_contains_exact_key(table):
    _, verified = _verified('SKU OTHER-ACT-TRAY-18 Legacy ACT-TRAY-18 Species Pinus taeda', table=table)
    assert verified.source_line_key is None
    assert bind_line_items((verified,))[0].line_item_key == 'SKU:OTHER-ACT-TRAY-18'


def test_two_explicit_source_skus_cannot_choose_first_identity():
    _, verified = _verified('SKU ACT-TRAY-18 SKU OTHER-ACT-TRAY-18 Species Pinus taeda', table=True)
    assert verified.source_line_key is None
    bound = bind_line_items((verified,))[0]
    assert bound.line_item_key == 'ROW:00000000-0000-0000-0000-000000000001:P1:TBOTANICAL:R2'


def test_unverified_sidecar_cannot_override_contradictory_source_label():
    original, _ = _verified('SKU OTHER-ACT-TRAY-18 Species Pinus taeda')
    assert bind_line_items((original,))[0].line_item_key == 'SKU:OTHER-ACT-TRAY-18'

@pytest.mark.parametrize('sku', ['act-tray-18', 'ACT-TRAY-18+REV'])
def test_verified_sparse_sidecar_materializes_the_same_complete_identity(sku):
    from litoral_trace.us_lacey.specialized_projection import plan_line_materialization
    original, _ = _verified('Species Pinus taeda', supplied_sku=sku)
    # A real row can verify the SKU beside a sparse evidence value. This direct
    # binding contract receives that already-verified sidecar, as Pack 3 does.
    verified = replace(original, candidate=replace(original.candidate, evidence_verified=True))
    bound = bind_line_items((verified,))
    assert bound[0].line_item_key == 'SKU:' + sku.upper()
    plan = plan_line_materialization(bound, existing_line_references=())
    assert [line.line_item_key for line in plan.generated_lines] == ['SKU:' + sku.upper()]
    assert plan.review_only_line_keys == ()


def test_complete_plus_separator_identity_reaches_canonical_reconciliation_without_taxon_inference():
    from test_quantitative_line_binding import _evidence, _field, _payload
    from litoral_trace.us_lacey.cross_document_line_identity import reconcile_cross_document_line_identity
    sku = 'SKU:ACT-TRAY-18+REV'
    fields = {}
    for field, value in [('description', 'Solid wood trays'), ('hts_code', '4419199010')]:
        row = _evidence(field=field, value=value, document_id=10,
                        text=f'SKU ACT-TRAY-18+REV {value}', line_key=sku)
        fields[field] = _field(field, 'SUPPORTED', [row])
    row = _evidence(field='plant_quantity', value='30', document_id=20,
                    text='SKU ACT-TRAY-18+REV Plant quantity 30', component_key=sku)
    fields['plant_quantity'] = _field('plant_quantity', 'SUPPORTED', [row])
    payload = _payload(fields)
    reconciled = reconcile_cross_document_line_identity(payload)
    linked = reconciled['canonical_fields']['plant_quantity']['supporting_evidence'][0]
    assert linked['line_key'] == sku
    assert linked['component_key'] == sku
    assert linked['normalized_value'] == '30'
    assert payload['canonical_fields']['plant_quantity']['supporting_evidence'][0]['line_key'] is None
    assert linked['candidate']['provenance']['source_text'] == 'SKU ACT-TRAY-18+REV Plant quantity 30'

@pytest.mark.parametrize('reverse', [False, True])
def test_canonical_keeps_two_verified_skus_with_same_hts_and_description(reverse):
    from test_canonical_shipment_truth import _evidence, _field
    from litoral_trace.us_lacey.canonical_shipment_truth import build_canonical_shipment_truth
    a = _verified('SKU ACT-TRAY-18 Species Pinus taeda', document=1)[1]
    b = _verified('SKU OTHER-ACT-TRAY-18 Species Pinus taeda', document=2)[1]
    bound = bind_line_items((b, a) if reverse else (a, b))
    fields = {}
    for field, value in [('description', 'Solid wood trays'), ('hts_code', '4419199010')]:
        rows = [_evidence(field=field, value=value, document_id=c.document_id.int,
                           text=c.candidate.source_text, line_key=c.line_item_key) for c in bound]
        fields[field] = _field(field, 'SUPPORTED_MULTIPLE', rows)
    truth = build_canonical_shipment_truth({'canonical_fields': fields, 'issues': []})
    assert {line.entity_key for line in truth.plant_lines} == {'SKU:ACT-TRAY-18', 'SKU:OTHER-ACT-TRAY-18'}
    for line in truth.plant_lines:
        assert line.fields['hts_code'].values == ('4419199010',)
        assert line.fields['merchandise_description'].values == ('Solid wood trays',)

@pytest.mark.parametrize('prefix', ['OTHER-ACT-TRAY-18', '1 OTHER-ACT-TRAY-18'])
def test_contradictory_leading_source_identifier_beats_legacy_sidecar(prefix):
    _, verified = _verified(f'{prefix} Legacy ACT-TRAY-18 Species Pinus taeda')
    assert verified.source_line_key is None
    assert bind_line_items((verified,))[0].line_item_key == 'SKU:OTHER-ACT-TRAY-18'


def test_contradictory_leading_and_labelled_skus_keep_verified_local_row():
    _, verified = _verified('OTHER-ACT-TRAY-18 SKU ACT-TRAY-18 Species Pinus taeda', table=True)
    assert verified.source_line_key is None
    bound = bind_line_items((verified,))[0]
    assert bound.line_item_key == 'ROW:00000000-0000-0000-0000-000000000001:P1:TBOTANICAL:R2'


def test_unsupported_unicode_sku_never_manufactures_ascii_equivalence():
    from litoral_trace.lacey_engine.sku_identity import has_exact_source_identity, normalize_sku
    unsupported = "ACT-" + chr(223) + "18"
    assert not has_exact_source_identity(unsupported, "SKU " + unsupported)
    assert normalize_sku(unsupported) == unsupported


@pytest.mark.parametrize("table", [False, True])
@pytest.mark.parametrize("source_character,ascii_alias", [(chr(223), "SS"), (chr(383), "S"), (chr(8490), "K")])
def test_unicode_source_never_verifies_an_ascii_sidecar_through_casefold(table, source_character, ascii_alias):
    source_sku = "ACT-" + source_character + "-18"
    alias = "ACT-" + ascii_alias + "-18"
    text = "SKU " + source_sku + " Species Pinus taeda"
    original, verified = _verified(text, supplied_sku=alias, table=table)
    assert verified.source_line_key is None
    bound = bind_line_items((verified,))[0]
    assert bound.line_item_key != "SKU:" + alias
    assert bound.line_item_key.startswith("ROW:" if table else "FP:")
    assert bound.candidate.evidence_verified
    assert bound.candidate.source_text == original.candidate.source_text == text
    assert bound.document_id == original.document_id


@pytest.mark.parametrize("label", ["SKU No:", "SKU No.:", "SKU Number:", "SKU ID:", "SKU #:"])
def test_complete_sku_label_modifier_never_becomes_product_identity(label):
    from litoral_trace.lacey_engine.sku_identity import observed_source_skus
    bound = []
    for document, sku in [(1, "ACT-TRAY-18"), (2, "OTHER-ACT-TRAY-18")]:
        text = label + " " + sku + " Species Pinus taeda"
        original, verified = _verified(text, supplied_sku=sku, document=document)
        assert observed_source_skus(text) == {sku}
        assert verified.source_line_key == sku
        item = bind_line_items((verified,))[0]
        assert item.line_item_key == "SKU:" + sku
        assert item.candidate.source_text == original.candidate.source_text
        bound.append(item)
    assert len(fuse_candidates(tuple(bound)).fused_candidates) == 2


@pytest.mark.parametrize("table", [False, True])
@pytest.mark.parametrize("quote,raw", [
    ("ACT-TRAY-18 Species Pinus taeda", "OTHER-ACT-TRAY-18 Species Pinus taeda"),
    ("ACT-TRAY-18", "OTHER-ACT-TRAY-18-SUFFIX Species Pinus taeda"),
])
def test_cropped_ai_quote_cannot_verify_a_partial_sku_against_real_source(table, quote, raw):
    from litoral_trace.lacey_engine.domain import DocumentResolution, DocumentType as EngineDocumentType, ParsedLayout, LayoutBlock
    original, _ = _verified(quote, table=table)
    block = LayoutBlock("b1", 1, None, raw, "TEXT_LINE",
        table_id="botanical" if table else None, row_index=2 if table else None)
    resolution = DocumentResolution("synthetic.pdf", "fixture", EngineDocumentType.SPECIES_DECLARATION,
        1.0, ParsedLayout((block,), 1), {})
    verified = _verify_candidates((original,), resolutions={original.document_id: resolution})[0]
    bound = bind_line_items((verified,))[0]
    assert not verified.candidate.evidence_verified
    assert evaluate_verified_candidate_admission((bound,)).admitted_candidates == ()
    assert verified.source_line_key is None
    assert verified.candidate.source_text == quote
    assert resolution.layout.blocks[0].text == raw


@pytest.mark.parametrize("table", [False, True])
def test_legitimate_partial_quote_without_sku_keeps_verified_coherent_source_identity(table):
    from litoral_trace.lacey_engine.domain import DocumentResolution, DocumentType as EngineDocumentType, ParsedLayout, LayoutBlock
    quote = "Species Pinus taeda"
    raw = "SKU ACT-TRAY-18 " + quote
    original, _ = _verified(quote, table=table)
    block = LayoutBlock("b1", 1, None, raw, "TEXT_LINE",
        table_id="botanical" if table else None, row_index=2 if table else None)
    resolution = DocumentResolution("synthetic.pdf", "fixture", EngineDocumentType.SPECIES_DECLARATION,
        1.0, ParsedLayout((block,), 1), {})
    verified = _verify_candidates((original,), resolutions={original.document_id: resolution})[0]
    bound = bind_line_items((verified,))[0]
    assert verified.candidate.evidence_verified
    assert verified.source_line_key == "ACT-TRAY-18"
    assert bound.line_item_key == "SKU:ACT-TRAY-18"
    assert evaluate_verified_candidate_admission((bound,)).admitted_candidates == (bound,)
    assert verified.candidate.source_text == quote


@pytest.mark.parametrize("table", [False, True])
@pytest.mark.parametrize("unsupported,alias", [("ACT-" + chr(223) + "-18", "ACT-SS-18"), ("ACT#18", "ACT-18")])
def test_unsupported_explicit_sku_never_falls_back_to_a_legacy_alias(table, unsupported, alias):
    from litoral_trace.lacey_engine.sku_identity import has_exact_source_identity
    text = "SKU No: " + unsupported + " Legacy " + alias + " Species Pinus taeda"
    original, verified = _verified(text, supplied_sku=alias, table=table)
    assert not has_exact_source_identity(alias, text)
    from test_source_linked_claims import _claim, AssociationStatus
    claim, _ = _claim(text, "taeda", sku=alias)
    assert claim.association_status is AssociationStatus.UNBOUND
    assert claim.sku is None and claim.source_excerpt == text
    assert verified.source_line_key is None
    for item in (original, verified):
        bound = bind_line_items((item,))[0]
        assert not bound.line_item_key.startswith("SKU:")
        assert bound.line_item_key.startswith("ROW:" if table else "FP:")
        assert bound.candidate.source_text == text
        assert bound.document_id == original.document_id
    assert verified.candidate.evidence_verified


@pytest.mark.parametrize("table", [False, True])
@pytest.mark.parametrize("sidecar", ["ACT-TRAY-18", "OTHER-ACT-TRAY-18"])
def test_supported_explicit_sku_remains_authoritative_over_a_literal_legacy_alias(table, sidecar):
    text = "SKU No: ACT-TRAY-18 Legacy OTHER-ACT-TRAY-18 Species Pinus taeda"
    original, verified = _verified(text, supplied_sku=sidecar, table=table)
    bound = bind_line_items((verified,))[0]
    assert bound.line_item_key == "SKU:ACT-TRAY-18"
    assert bound.candidate.evidence_verified
    assert bound.candidate.source_text == original.candidate.source_text
    assert evaluate_verified_candidate_admission((bound,)).admitted_candidates == (bound,)



def test_unsupported_distinct_source_skus_keep_distinct_local_fingerprints_and_fusion():
    texts = ("SKU No: ACT#18 Species Pinus taeda", "SKU No: ACT@18 Species Pinus taeda")
    verified = tuple(_verified(text, document=1)[1] for text in texts)
    bound = bind_line_items(verified)
    assert all(item.candidate.evidence_verified for item in bound)
    assert all(item.line_item_key.startswith("FP:") for item in bound)
    assert len({item.line_item_key for item in bound}) == 2
    assert len(fuse_candidates(bound).fused_candidates) == 2
    assert tuple(item.candidate.source_text for item in bound) == texts


@pytest.mark.parametrize("sku", ["ACT.TRAY.18", "ABC123", "ACT-TRAY-18", "ACT_TRAY_18", "ACT+TRAY+18"])
def test_verified_sparse_quote_keeps_explicit_sku_without_requiring_a_separator(sku):
    from litoral_trace.lacey_engine.domain import DocumentResolution, DocumentType as EngineDocumentType, ParsedLayout, LayoutBlock
    quote = "Species Pinus taeda"
    original, _ = _verified(quote, supplied_sku=sku, table=True)
    raw = "SKU " + sku + " " + quote
    block = LayoutBlock("b1", 1, None, raw, "TEXT_LINE", table_id="botanical", row_index=2)
    resolution = DocumentResolution("synthetic.pdf", "fixture", EngineDocumentType.SPECIES_DECLARATION,
        1.0, ParsedLayout((block,), 1), {})
    verified = _verify_candidates((original,), resolutions={original.document_id: resolution})[0]
    assert verified.source_line_key == sku and verified.candidate.evidence_verified
    bound = bind_line_items((verified,))[0]
    assert bound.line_item_key == "SKU:" + sku
    assert bound.candidate.source_text == quote
    assert evaluate_verified_candidate_admission((bound,)).admitted_candidates == (bound,)



@pytest.mark.parametrize("table", [False, True])
@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("partial_quote", [False, True])
def test_unsupported_sku_abstention_survives_description_reconciliation_and_admission(table, reverse, partial_quote):
    value = "Solid wood tray"
    candidates = []
    for document, sku in [(1, "ACT#18"), (2, "OTHER-ACT-18")]:
        raw = "SKU " + sku + " Description " + value
        original, _ = _verified(raw, supplied_sku=sku, table=table, document=document)
        original = replace(original, document_type=DocumentType.COMMERCIAL_INVOICE,
            specialist=SpecialistRole.COMMERCIAL_LINES,
            candidate=replace(original.candidate, field_key="description", value=value, normalized_value=value,
                source_text="Description " + value if partial_quote else raw))
        block = LayoutBlock("b1", 1, None, raw, "TEXT_LINE",
            table_id="botanical" if table else None, row_index=2 if table else None)
        resolution = DocumentResolution("synthetic.pdf", "fixture", EngineDocumentType.COMMERCIAL_INVOICE,
            1.0, ParsedLayout((block,), 1), {})
        verified = _verify_candidates((original,), resolutions={original.document_id: resolution})[0]
        assert verified.candidate.evidence_verified
        candidates.append(verified)
    if reverse:
        candidates.reverse()
    bound = bind_line_items(tuple(candidates))
    local = next(item for item in bound if item.document_id == UUID(int=1))
    known = next(item for item in bound if item.document_id == UUID(int=2))
    assert local.line_item_key.startswith("ROW:" if table else "FP:")
    assert known.line_item_key == "SKU:OTHER-ACT-18"
    assert local.candidate.source_text == ("Description " + value if partial_quote else "SKU ACT#18 Description " + value)
    assert known.candidate.source_text == ("Description " + value if partial_quote else "SKU OTHER-ACT-18 Description " + value)
    admission = evaluate_verified_candidate_admission(tuple(bound))
    assert known in admission.admitted_candidates
    assert (local in admission.admitted_candidates) is table
    if not table:
        record = next(record for record in admission.records if record.document_id == local.document_id)
        assert record.reason.value == "LINE_BINDING_AMBIGUOUS"
    fused = fuse_candidates(admission.admitted_candidates)
    assert len(fused.fused_candidates) == (2 if table else 1)
    assert {item.line_item_key for item in fused.fused_candidates} == (
        {local.line_item_key, known.line_item_key} if table else {known.line_item_key})


@pytest.mark.parametrize("table", [False, True])
@pytest.mark.parametrize("position", ["before", "after"])
@pytest.mark.parametrize("space", [chr(160), chr(8239)])
def test_unicode_whitespace_boundaries_keep_ascii_quote_verified_against_original_anchor(table, position, space):
    quote = "SKU ACT-TRAY-18 Species Pinus taeda"
    raw = ("SKU" + space + "ACT-TRAY-18 Species Pinus taeda" if position == "before"
        else "SKU ACT-TRAY-18" + space + "Species Pinus taeda")
    original, _ = _verified(quote, table=table)
    block = LayoutBlock("b1", 1, None, raw, "TEXT_LINE",
        table_id="botanical" if table else None, row_index=2 if table else None)
    resolution = DocumentResolution("synthetic.pdf", "fixture", EngineDocumentType.SPECIES_DECLARATION,
        1.0, ParsedLayout((block,), 1), {})
    verified = _verify_candidates((original,), resolutions={original.document_id: resolution})[0]
    assert verified.candidate.evidence_verified
    assert verified.source_line_key == "ACT-TRAY-18"
    bound = bind_line_items((verified,))[0]
    assert bound.line_item_key == "SKU:ACT-TRAY-18"
    admission = evaluate_verified_candidate_admission((bound,))
    assert admission.admitted_candidates == (bound,)
    assert len(fuse_candidates(admission.admitted_candidates).fused_candidates) == 1
    assert verified.candidate.source_text == quote
    assert verified.source_span_id == original.source_span_id
    assert resolution.layout.blocks[0].text == raw


def test_unicode_whitespace_never_joins_two_parts_inside_a_sku():
    from litoral_trace.lacey_engine.sku_identity import has_exact_source_identity
    for space in (chr(160), chr(8239)):
        assert not has_exact_source_identity("ACT-TRAY-18", "SKU ACT-" + space + "TRAY-18")



def test_negative_identity_metadata_defaults_compatible_and_fences_prior_binding_cache(monkeypatch):
    from litoral_trace.us_lacey import specialized_shadow as shadow
    original, _ = _verified("SKU ACT-TRAY-18 Species Pinus taeda")
    assert "source_sku_identity_blocked" in CandidateEnvelope.__dataclass_fields__
    assert original.source_sku_identity_blocked is False
    current = shadow.specialized_engine_version(provider="offline", model="fixture")
    monkeypatch.setattr(shadow, "LINE_BINDING_VERSION", "lacey_document_local_line_binding_v2_exact_sku")
    previous = shadow.specialized_engine_version(provider="offline", model="fixture")
    assert current != previous


def test_verified_negative_identity_metadata_survives_payload_and_cache_roundtrip():
    from test_specialized_shadow import _document
    from litoral_trace.lacey_engine.multi_agent.specialist_runtime import ScopedAIExtractionResult, SourceRowIdentity
    from litoral_trace.us_lacey.specialized_shadow import (
        run_specialized_shadow_operation, serialize_specialized_document_run, _rehydrate_cached_run)

    quote = "Description Solid wood tray"
    raw = "COMMERCIAL INVOICE\nSKU ACT#18 " + quote
    block = LayoutBlock("b1", 1, None, raw, "TEXT_LINE", table_id="commercial", row_index=1)
    resolution = DocumentResolution("synthetic.pdf", "fixture", EngineDocumentType.COMMERCIAL_INVOICE,
        1.0, ParsedLayout((block,), 1), {})
    document = replace(_document(), engine2_resolution=resolution)

    class Provider:
        name = "offline"
        model = "fixture"
        def extract_scoped(self, *, allowed_fields, **_kwargs):
            candidates = (AICandidate(field_key="description", value="Solid wood tray",
                normalized_value="Solid wood tray", evidence_class=EvidenceClass.EXPLICIT,
                page=1, source_text=quote, confidence=.9, provider=self.name, model=self.model),) if "description" in allowed_fields else ()
            return ScopedAIExtractionResult(provider=self.name, model=self.model, schema_version="fixture",
                candidates=candidates, page_count=1, row_identities=(SourceRowIdentity("OTHER-ACT-18", "commercial", 1),) if candidates else ())

    run = run_specialized_shadow_operation(documents=(document,), provider=Provider(), concurrency=1)
    assert len(run.result.fused_candidates) == 1
    original = run.result.fused_candidates[0]
    assert getattr(original, "source_sku_identity_blocked", False) is True
    assert original.candidate.evidence_verified and original.line_item_key.startswith("ROW:")
    payload = serialize_specialized_document_run(run=run, document_id=document.document_id, source_set_fingerprint="synthetic")
    assert payload["candidates"][0]["source_sku_identity_blocked"] is True
    cached = _rehydrate_cached_run((payload,), documents=(document,), computation_fingerprint="synthetic")
    assert cached is not None
    rebound = cached.result.fused_candidates[0]
    assert rebound.source_sku_identity_blocked is True
    assert rebound.candidate.source_text == quote
    assert rebound.line_item_key == original.line_item_key
    # Rebinding cannot grant a global SKU even if another caller reuses the envelope.
    assert bind_line_items((rebound,))[0].line_item_key.startswith("FP:")
    payload["candidates"][0]["source_sku_identity_blocked"] = "false"
    assert _rehydrate_cached_run((payload,), documents=(document,), computation_fingerprint="synthetic") is None


@pytest.mark.parametrize("table", [False, True])
@pytest.mark.parametrize("reverse", [False, True])
def test_two_explicit_contradictory_source_skus_cannot_reconcile_to_other_product(table, reverse):
    """A verified contradictory row must not become a global SKU by matching descriptions."""
    value = "Solid wood tray"
    candidates = []
    for document, raw in [
        (1, "SKU ACT-18 SKU OTHER-ACT-18 Description Solid wood tray"),
        (2, "SKU OTHER-ACT-18 Description Solid wood tray"),
    ]:
        original, _ = _verified(raw, supplied_sku="OTHER-ACT-18", table=table, document=document)
        original = replace(
            original, document_type=DocumentType.COMMERCIAL_INVOICE,
            specialist=SpecialistRole.COMMERCIAL_LINES,
            candidate=replace(original.candidate, field_key="description", value=value, normalized_value=value),
        )
        block = LayoutBlock(
            "b1", 1, None, raw, "TEXT_LINE",
            table_id="botanical" if table else None,
            row_index=2 if table else None,
        )
        resolution = DocumentResolution(
            "synthetic.pdf", "fixture", EngineDocumentType.COMMERCIAL_INVOICE,
            1.0, ParsedLayout((block,), 1), {},
        )
        verified = _verify_candidates((original,), resolutions={original.document_id: resolution})[0]
        assert verified.candidate.evidence_verified
        candidates.append(verified)
    if reverse:
        candidates.reverse()
    bound = bind_line_items(tuple(candidates))
    conflicting = next(item for item in bound if item.document_id == UUID(int=1))
    other = next(item for item in bound if item.document_id == UUID(int=2))
    assert conflicting.source_sku_identity_blocked
    assert conflicting.source_line_key is None
    assert conflicting.line_item_key.startswith("ROW:" if table else "FP:")
    assert other.line_item_key == "SKU:OTHER-ACT-18"
    admission = evaluate_verified_candidate_admission(bound)
    assert other in admission.admitted_candidates
    assert (conflicting in admission.admitted_candidates) is table
    if table:
        assert len(fuse_candidates(admission.admitted_candidates).fused_candidates) == 2


@pytest.mark.parametrize("reverse", [False, True])
def test_taxonomy_match_alone_never_supplies_an_unbound_commercial_sku(reverse):
    """Chair and tray may share Pinus taeda, but not a product identity."""
    chair, _ = _verified("Description Solid wood chair with Pinus taeda", document=1, table=True)
    chair = replace(
        chair, source_line_key=None, document_type=DocumentType.COMMERCIAL_INVOICE,
        specialist=SpecialistRole.COMMERCIAL_LINES,
        candidate=replace(
            chair.candidate, field_key="description",
            value="Solid wood chair with Pinus taeda",
            normalized_value="Solid wood chair with Pinus taeda",
        ),
    )
    genus = _verified("SKU ACT-TRAY-18 Genus Pinus", document=2)[1]
    genus = replace(genus, candidate=replace(genus.candidate, field_key="genus", value="Pinus", normalized_value="Pinus"))
    species = _verified("SKU ACT-TRAY-18 Species taeda", document=2)[1]
    items = (chair, genus, species)
    bound = bind_line_items(tuple(reversed(items)) if reverse else items)
    chair_bound = next(item for item in bound if item.document_id == UUID(int=1))
    assert chair_bound.line_item_key.startswith("ROW:")
    assert {item.line_item_key for item in bound if item.document_id == UUID(int=2)} == {"SKU:ACT-TRAY-18"}


def test_multi_sku_anchor_keeps_literal_quoted_evidence_but_abstains_from_sku():
    """Preserve the source claim; reject only its ambiguous product identity."""
    from litoral_trace.lacey_engine.sku_identity import has_complete_source_sku_token

    full = "SKU OTHER-ACT-18 SKU ACT-18 Species Pinus taeda"
    quote = "SKU ACT-18 Species Pinus taeda"
    assert has_complete_source_sku_token("ACT-18", full)
    assert not has_complete_source_sku_token("ACT-18", "SKU OTHER-ACT-18 Species Pinus taeda")
    original, _ = _verified(full, supplied_sku="ACT-18", table=True)
    original = replace(original, candidate=replace(original.candidate, source_text=quote))
    block = LayoutBlock("b1", 1, None, full, "TEXT_LINE", table_id="botanical", row_index=2)
    resolution = DocumentResolution(
        "synthetic.pdf", "fixture", EngineDocumentType.SPECIES_DECLARATION,
        1.0, ParsedLayout((block,), 1), {},
    )
    verified = _verify_candidates((original,), resolutions={original.document_id: resolution})[0]
    assert verified.candidate.evidence_verified
    assert verified.candidate.source_text == quote
    assert verified.source_sku_identity_blocked
    assert verified.source_line_key is None
    bound = bind_line_items((verified,))[0]
    assert bound.line_item_key.startswith("ROW:")
    assert evaluate_verified_candidate_admission((bound,)).admitted_candidates == (bound,)


def test_multi_sku_anchor_rejects_quote_cut_from_another_sku_occurrence():
    """A matching SKU elsewhere in a row cannot validate a suffix-only quote."""
    from litoral_trace.lacey_engine.sku_identity import has_aligned_source_sku_quote

    source = "SKU ACT-18 SKU OTHER-ACT-18 Species Pinus taeda"
    quote = "ACT-18 Species Pinus taeda"
    assert not has_aligned_source_sku_quote("ACT-18", quote, source)
    original, _ = _verified(source, supplied_sku="ACT-18", table=True)
    original = replace(original, candidate=replace(original.candidate, source_text=quote))
    block = LayoutBlock("b1", 1, None, source, "TEXT_LINE", table_id="botanical", row_index=2)
    resolution = DocumentResolution(
        "synthetic.pdf", "fixture", EngineDocumentType.SPECIES_DECLARATION,
        1.0, ParsedLayout((block,), 1), {},
    )
    candidate = _verify_candidates((original,), resolutions={original.document_id: resolution})[0]
    assert candidate.source_sku_identity_blocked
    assert candidate.source_line_key is None
    assert not candidate.candidate.evidence_verified
    assert bind_line_items((candidate,))[0].line_item_key.startswith("ROW:")


def test_multi_sku_aligned_quote_keeps_evidence_not_global_identity():
    from litoral_trace.lacey_engine.sku_identity import has_aligned_source_sku_quote
    source = "SKU OTHER-ACT-18 SKU ACT-18 Species Pinus taeda"
    quote = "SKU ACT-18 Species Pinus taeda"
    assert has_aligned_source_sku_quote("ACT-18", quote, source)


def test_single_source_sku_cannot_validate_quote_from_longer_legacy_alias():
    """A real SKU elsewhere in the anchor must not endorse a sliced alias."""
    from litoral_trace.lacey_engine.sku_identity import has_aligned_source_sku_quote

    source = "SKU ACT-18 Legacy OTHER-ACT-18 Species Pinus taeda"
    quote = "ACT-18 Species Pinus taeda"
    assert not has_aligned_source_sku_quote("ACT-18", quote, source)
    original, _ = _verified(source, supplied_sku="ACT-18", table=True)
    original = replace(original, candidate=replace(original.candidate, source_text=quote))
    block = LayoutBlock("b1", 1, None, source, "TEXT_LINE", table_id="botanical", row_index=2)
    resolution = DocumentResolution(
        "synthetic.pdf", "fixture", EngineDocumentType.SPECIES_DECLARATION,
        1.0, ParsedLayout((block,), 1), {},
    )
    checked = _verify_candidates((original,), resolutions={original.document_id: resolution})[0]
    assert not checked.candidate.evidence_verified
    assert checked.candidate.source_text == quote
    bound = bind_line_items((checked,))[0]
    assert bound not in evaluate_verified_candidate_admission((bound,)).admitted_candidates


@pytest.mark.parametrize("source,quote,evidence_ok", [
    (
        "SKU ACT#18 Legacy ACT-18 Species Pinus taeda",
        "ACT-18 Species Pinus taeda",
        True,
    ),
    (
        "SKU OTHER-ACT-18 Legacy ACT-18 Species Pinus taeda",
        "ACT-18 Species Pinus taeda",
        True,
    ),
    (
        "SKU OTHER-ACT-18 SKU BCT-19 Species Pinus taeda",
        "ACT-18 SKU BCT-19 Species Pinus taeda",
        False,
    ),
])
def test_multi_and_unsupported_source_sku_preserve_only_aligned_literal_quote(
    source, quote, evidence_ok,
):
    """Keep real citations, reject clipped aliases, always abstain from global SKU."""
    original, _ = _verified(source, supplied_sku="ACT-18", table=True)
    original = replace(original, candidate=replace(original.candidate, source_text=quote))
    block = LayoutBlock("b1", 1, None, source, "TEXT_LINE", table_id="botanical", row_index=2)
    resolution = DocumentResolution(
        "synthetic.pdf", "fixture", EngineDocumentType.SPECIES_DECLARATION,
        1.0, ParsedLayout((block,), 1), {},
    )
    checked = _verify_candidates((original,), resolutions={original.document_id: resolution})[0]
    assert checked.source_sku_identity_blocked
    assert checked.source_line_key is None
    assert checked.candidate.evidence_verified is evidence_ok
    assert checked.candidate.source_text == quote
    bound = bind_line_items((checked,))[0]
    assert bound.line_item_key.startswith("ROW:")
    admitted = evaluate_verified_candidate_admission((bound,)).admitted_candidates
    assert admitted == ((bound,) if evidence_ok else ())


@pytest.mark.parametrize("source,expected", [
    ("SKU ABC123 Species Pinus taeda", True),
    ("SKU ABC123 Legacy OTHER-ABC123 Species Pinus taeda", False),
])
def test_alphanumeric_sidecar_quote_needs_original_token_boundaries(source, expected):
    """ABC123 in an unsupported longer alias cannot be cited as global ABC123."""
    quote = "ABC123 Species Pinus taeda"
    original, _ = _verified(source, supplied_sku="ABC123", table=True)
    original = replace(original, candidate=replace(original.candidate, source_text=quote))
    block = LayoutBlock("b1", 1, None, source, "TEXT_LINE", table_id="botanical", row_index=2)
    resolution = DocumentResolution(
        "synthetic.pdf", "fixture", EngineDocumentType.SPECIES_DECLARATION,
        1.0, ParsedLayout((block,), 1), {},
    )
    checked = _verify_candidates((original,), resolutions={original.document_id: resolution})[0]
    assert checked.candidate.evidence_verified is expected
    bound = bind_line_items((checked,))[0]
    admitted = evaluate_verified_candidate_admission((bound,)).admitted_candidates
    if expected:
        assert bound.line_item_key == "SKU:ABC123"
        assert admitted == (bound,)
    else:
        assert bound not in admitted
