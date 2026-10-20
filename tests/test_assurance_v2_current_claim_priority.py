from litoral_trace.us_lacey.assurance_v2_case_reader import _case, _is_line_bound
from test_assurance_v2_core_memory import session, org_and_actor, supplier_product, shipment, approve_and_promote


def test_current_source_species_blocks_memory(session, monkeypatch):
    org, actor = org_and_actor(session, "AssuranceCurrentWins")
    _, product = supplier_product(session, org.id, supplier_key="MID:CURRENT")
    first, doc, _, revision, _ = shipment(session, org.id, product.id, reference="CURRENT-1", source_value="Quercus alba")
    approve_and_promote(session, org, actor, first, doc, revision)
    second, _, _, _, _ = shipment(session, org.id, product.id, reference="CURRENT-2")
    current = [{"field_name": "species", "line_reference": "1", "normalized_value": "Acer rubrum"}]
    monkeypatch.setattr("litoral_trace.us_lacey.assurance_v2_case_reader._source_claim_observations", lambda *a, **k: tuple(current))
    assert not _case(session, org.id, second.public_id).reuses
    current[0]["line_reference"] = None
    assert not _case(session, org.id, second.public_id).reuses


def test_source_claim_line_binding_recognizes_only_contract_bound_statuses():
    assert _is_line_bound({"association_status": "EXPLICIT_SKU"})
    assert _is_line_bound({"association_status": "SOURCE_LOCAL_LINE"})
    assert _is_line_bound({"association_status": "SOURCE_ROW"})
    assert not _is_line_bound({"association_status": "UNBOUND"})
