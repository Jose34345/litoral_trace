from litoral_trace.us_lacey.assurance_v2_case_reader import _case
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
