"""P0: Pack2 source candidates cross the canonical REVIEW boundary, never authority."""
from __future__ import annotations

from uuid import uuid4
import pytest
from sqlalchemy import select

from litoral_trace.db.models import (
    UsLaceyOperationField, UsLaceyOperationProductLink,
    UsLaceyPpqPlantLine, UsLaceySourceSetRevision,
    UsLaceySupplier, UsLaceySupplierProduct,
)
from litoral_trace.us_lacey.canonical_publication_support import (
    publish_source_linked_review_candidates,
)
from litoral_trace.us_lacey.source_sets import seal_current_source_set
from tests.us_lacey_engine2_postgres import (
    create_test_graph, engine2_postgres_session_factory, tenant_session,
)

TARGETS = {
    "article_component": "solid bamboo coasters",
    "plant_quantity": "288.0",
    "metric_unit": "kg",
    "entered_value": "11250",
    "merchandise_description": "retail boxed 4 piece solid bamboo coaster sets",
}


def _fixture(factory):
    org, op_id, link_id, assurance_id, _vault, _sha = create_test_graph(factory)
    db = tenant_session(factory, org)
    line = UsLaceyPpqPlantLine(
        organization_id=org, operation_id=op_id,
        line_reference="1", ordinal=1,
    )
    db.add(line)
    db.flush()
    fields = {}
    for name in TARGETS:
        shipment = name == "merchandise_description"
        row = UsLaceyOperationField(
            organization_id=org, operation_id=op_id,
            merchandise_line_reference="__shipment__" if shipment else "1",
            field_name=name, field_scope="SHIPMENT" if shipment else "PLANT_LINE",
            plant_line_id=None if shipment else line.id,
            field_status="MISSING", validation_status="MISSING", confidence=0.0,
        )
        db.add(row)
        db.flush()
        fields[name] = row.id
    supplier = UsLaceySupplier(
        organization_id=org, supplier_key="LONGSHENG-"+uuid4().hex[:12],
        display_name="Longsheng Bamboo", normalized_name="longsheng bamboo",
    )
    db.add(supplier)
    db.flush()
    product = UsLaceySupplierProduct(
        organization_id=org, supplier_id=supplier.id,
        product_key="BAM-COAST-04", sku="BAM-COAST-04",
        display_name="Bamboo coaster set", normalized_name="bamboo coaster set",
    )
    db.add(product)
    db.commit()
    db.close()
    revision = seal_current_source_set(
        organization_id=org, operation_id=op_id, session_factory=factory,
    )
    db = tenant_session(factory, org)
    db.add(UsLaceyOperationProductLink(
        organization_id=org, operation_id=op_id,
        source_set_revision_id=revision.id, line_reference="1",
        supplier_product_id=product.id, link_method="EXACT_SKU",
    ))
    db.get(UsLaceySourceSetRevision, revision.id).status = "FINALIZED"
    db.commit()
    db.close()
    return org, op_id, assurance_id, fields


def _claim(name, value, doc_id, *, sku="BAM-COAST-04", association=None, suffix=0):
    shipment = name == "merchandise_description"
    return {
        "id": ("a" * 60 + f"{suffix:04x}"),
        "field_name": name,
        "normalized_value": value, "raw_value": value,
        "entity_type": "SHIPMENT" if shipment else "PRODUCT",
        "line_reference": None if shipment else "1",
        "sku": None if shipment else sku,
        "association_status": association or (
            "UNBOUND" if shipment else "DOCUMENT_EXACT_SINGLE_SKU"
        ),
        "authority_status": "CANDIDATE", "state": "CANDIDATE",
        "support_status": "SUPPORTED", "assurance_document_id": doc_id,
        "source_locator": {"kind": "TEXT_SPAN", "page": 1,
                           "block_id": "p1-l21", "start": 10, "end": 32},
    }


def test_pack2_current_source_candidates_become_review_not_verified(
    engine2_postgres_session_factory, monkeypatch,
):
    factory = engine2_postgres_session_factory
    org, op_id, doc_id, fields = _fixture(factory)
    observations = tuple(
        _claim(name, value, doc_id, suffix=i)
        for i, (name, value) in enumerate(TARGETS.items(), 1)
    )
    monkeypatch.setattr(
        "litoral_trace.us_lacey.assurance_v2_case_reader._source_claim_observations",
        lambda *_args, **_kwargs: observations,
    )
    db = tenant_session(factory, org)
    assert publish_source_linked_review_candidates(
        db, organization_id=org, operation_id=op_id,
    ) == len(TARGETS)
    db.commit()
    for name, row_id in fields.items():
        row = db.get(UsLaceyOperationField, row_id)
        assert row.normalized_value
        assert row.source_assurance_document_id == doc_id
        assert row.field_status == "REVIEW"
        assert row.validation_status == "REVIEW_REQUIRED"
        assert row.human_value is None
        assert row.reviewed_at is None
    # Already proposed fields are not overwritten on a second call.
    assert publish_source_linked_review_candidates(
        db, organization_id=org, operation_id=op_id,
    ) == 0
    db.close()


@pytest.mark.parametrize("alteration", ["wrong_sku", "unbound", "conflict", "unsupported", "reviewed"])
def test_bridge_abstains_on_ambiguous_or_untrusted_line(
    engine2_postgres_session_factory, monkeypatch, alteration,
):
    factory = engine2_postgres_session_factory
    org, op_id, doc_id, fields = _fixture(factory)
    claim = _claim("plant_quantity", "288.0", doc_id, suffix=1)
    if alteration == "wrong_sku":
        claim["sku"] = "OTHER-SKU"
    if alteration == "unbound":
        claim["association_status"] = "UNBOUND"
    if alteration == "unsupported":
        claim["support_status"] = "UNSUPPORTED"
    sources = (claim,)
    if alteration == "conflict":
        sources += (_claim("plant_quantity", "999", doc_id, suffix=2),)
    monkeypatch.setattr(
        "litoral_trace.us_lacey.assurance_v2_case_reader._source_claim_observations",
        lambda *_args, **_kwargs: sources,
    )
    db = tenant_session(factory, org)
    if alteration == "reviewed":
        from datetime import datetime, timezone
        row = db.get(UsLaceyOperationField, fields["plant_quantity"])
        row.reviewed_at = datetime.now(timezone.utc)
        row.human_value = "5"
    assert publish_source_linked_review_candidates(
        db, organization_id=org, operation_id=op_id,
    ) == 0
    assert db.get(UsLaceyOperationField, fields["plant_quantity"]).normalized_value is None
    db.close()
