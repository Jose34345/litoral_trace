"""P0: Pack2 source candidates cross the canonical REVIEW boundary, never authority."""
from __future__ import annotations

from uuid import uuid4
import pytest
from sqlalchemy import select

from litoral_trace.db.models import (
    AssuranceDocument, VaultDocument, UsLaceyOperationDocument, UsLaceyOperation,
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


def _fixture(factory, *, extra_original=False):
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
    if extra_original:
        # Existing pending source is demonstrably a DERIVED worksheet, not
        # just any already populated canonical-review field.
        db.get(VaultDocument, _vault).original_filename = (
            "05_Entry_Preparation_Worksheet_Shipment2.pdf"
        )
        db.get(UsLaceyOperationDocument, link_id).document_role = "ENTRY_WORKSHEET"
        db.get(UsLaceyOperation, op_id).document_count = 2
        vault = VaultDocument(
            organization_id=org, original_filename="02_Packing_List_Shipment2.pdf",
            content_type="application/pdf", size_bytes=100,
            sha256="b" * 64, object_key=f"synthetic-pdf/{uuid4().hex}",
            storage_backend="s3", storage_bucket="synthetic-tests",
            document_type="OTHER_EVIDENCE", status="available",
        )
        db.add(vault)
        db.flush()
        source = AssuranceDocument(
            organization_id=org, vault_document_id=vault.id,
            semantic_document_type="UNKNOWN", type_confidence=0.0,
            processing_status="UPLOADED",
        )
        db.add(source)
        db.flush()
        db.add(UsLaceyOperationDocument(
            organization_id=org, operation_id=op_id,
            assurance_document_id=source.id, version_number=1,
            document_role="UNKNOWN", is_current=True,
        ))
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


@pytest.mark.parametrize("field_name,current_value", [
    ("plant_quantity", "288"),
    ("metric_unit", "kg"),
])
def test_pack2_original_replaces_same_value_machine_review_source_only(
    engine2_postgres_session_factory, monkeypatch, field_name, current_value,
):
    factory = engine2_postgres_session_factory
    org, op_id, worksheet_id, fields = _fixture(factory, extra_original=True)
    db = tenant_session(factory, org)
    original_id = db.scalars(
        select(UsLaceyOperationDocument.assurance_document_id).where(
            UsLaceyOperationDocument.operation_id == op_id,
            UsLaceyOperationDocument.assurance_document_id != worksheet_id,
        )
    ).one()
    row = db.get(UsLaceyOperationField, fields[field_name])
    row.original_value = current_value
    row.normalized_value = current_value
    row.field_status = "REVIEW"
    row.validation_status = "REVIEW_REQUIRED"
    row.source_assurance_document_id = worksheet_id
    row.extractor = "canonical-shipment-truth"
    observations = (
        _claim(field_name, current_value, worksheet_id, suffix=21),
        {
            **_claim(field_name, current_value, original_id, suffix=22),
            "source_claim": {"extractor_name": "packing_net_mass_exact"},
        },
    )
    monkeypatch.setattr(
        "litoral_trace.us_lacey.assurance_v2_case_reader._source_claim_observations",
        lambda *_args, **_kwargs: observations,
    )
    assert publish_source_linked_review_candidates(
        db, organization_id=org, operation_id=op_id,
    ) == 1
    db.flush()
    assert row.source_assurance_document_id == original_id
    assert row.normalized_value == current_value
    assert row.field_status == "REVIEW"
    assert row.validation_status == "REVIEW_REQUIRED"
    assert row.reviewed_at is None and row.human_value is None
    assert publish_source_linked_review_candidates(
        db, organization_id=org, operation_id=op_id,
    ) == 0
    db.rollback()
    db.close()


@pytest.mark.parametrize("alteration", [
    "conflicting_value", "unsupported", "wrong_sku", "wrong_line",
    "human_reviewed", "value_changed", "already_found", "duplicate_original",
    "current_source_is_original", "new_source_is_worksheet",
])
def test_original_provenance_never_replaces_without_exact_safe_review_conditions(
    engine2_postgres_session_factory, monkeypatch, alteration,
):
    from datetime import datetime, timezone
    factory = engine2_postgres_session_factory
    org, op_id, worksheet_id, fields = _fixture(factory, extra_original=True)
    db = tenant_session(factory, org)
    original_id = db.scalars(
        select(UsLaceyOperationDocument.assurance_document_id).where(
            UsLaceyOperationDocument.operation_id == op_id,
            UsLaceyOperationDocument.assurance_document_id != worksheet_id,
        )
    ).one()
    target = db.get(UsLaceyOperationField, fields["plant_quantity"])
    target.original_value = "288.0"
    target.normalized_value = "288"
    target.field_status = "REVIEW"
    target.validation_status = "REVIEW_REQUIRED"
    target.source_assurance_document_id = worksheet_id
    target.extractor = "canonical-shipment-truth"
    new = {
        **_claim("plant_quantity", "288", original_id, suffix=32),
        "source_claim": {"extractor_name": "packing_net_mass_exact"},
    }
    if alteration == "conflicting_value":
        new["normalized_value"] = "289"
    if alteration == "unsupported":
        new["support_status"] = "UNSUPPORTED"
    if alteration == "wrong_sku":
        new["sku"] = "OTHER-SKU"
    if alteration == "wrong_line":
        new["line_reference"] = "2"
    if alteration == "human_reviewed":
        target.reviewed_at = datetime.now(timezone.utc)
        target.human_value = "288"
    if alteration == "value_changed":
        target.normalized_value = "287"
    if alteration == "already_found":
        target.field_status = "FOUND"
    claims = (
        _claim("plant_quantity", "288", worksheet_id, suffix=31), new,
    )
    if alteration == "duplicate_original":
        claims += ({**new, "id": "f" * 64},)
    if alteration == "current_source_is_original":
        source = db.get(AssuranceDocument, worksheet_id)
        db.get(VaultDocument, source.vault_document_id).original_filename = "invoice.pdf"
        db.scalars(select(UsLaceyOperationDocument).where(
            UsLaceyOperationDocument.assurance_document_id == worksheet_id
        )).one().document_role = "COMMERCIAL_INVOICE"
    if alteration == "new_source_is_worksheet":
        source = db.get(AssuranceDocument, original_id)
        db.get(VaultDocument, source.vault_document_id).original_filename = "worksheet.pdf"
    monkeypatch.setattr(
        "litoral_trace.us_lacey.assurance_v2_case_reader._source_claim_observations",
        lambda *_args, **_kwargs: claims,
    )
    assert publish_source_linked_review_candidates(
        db, organization_id=org, operation_id=op_id,
    ) == 0
    assert target.source_assurance_document_id == worksheet_id
    db.rollback()
    db.close()


@pytest.mark.parametrize("scenario", ["atomic_success", "second_field_failure", "stale_value", "derived_source"])
def test_pack2_single_click_plant_mass_review_is_atomic_and_fail_closed(
    engine2_postgres_session_factory, monkeypatch, scenario,
):
    """PostgreSQL gate: no partial field approval and no authority from worksheet."""
    from datetime import datetime, timezone
    from litoral_trace.us_lacey import paired_review
    from litoral_trace.us_lacey.review import UsLaceyReviewError

    factory = engine2_postgres_session_factory
    org, op_id, worksheet_id, fields = _fixture(factory, extra_original=True)
    db = tenant_session(factory, org)
    original_id = db.scalars(
        select(UsLaceyOperationDocument.assurance_document_id).where(
            UsLaceyOperationDocument.operation_id == op_id,
            UsLaceyOperationDocument.assurance_document_id != worksheet_id,
        )
    ).one()
    for name, value, locator in (
        ("plant_quantity", "288", "canonical:1:quantity"),
        ("metric_unit", "kg", "canonical:1:unit"),
    ):
        field = db.get(UsLaceyOperationField, fields[name])
        field.original_value = value
        field.normalized_value = value
        field.field_status = "REVIEW"
        field.validation_status = "REVIEW_REQUIRED"
        field.extractor = "canonical-shipment-truth"
        field.source_assurance_document_id = (
            worksheet_id if scenario == "derived_source" else original_id
        )
        field.source_page = 1
        field.source_locator = locator
    op_uuid = str(db.get(UsLaceyOperation, op_id).public_id)
    db.commit()
    db.close()
    monkeypatch.setattr(paired_review, "review_journal_enabled", lambda *_: False)
    call_sessions = []

    def human_review(*, _session, _defer_commit, field_id, **_kwargs):
        assert _defer_commit
        call_sessions.append(id(_session))
        if scenario == "second_field_failure" and len(call_sessions) == 2:
            raise UsLaceyReviewError("Second field refused")
        field = _session.get(UsLaceyOperationField, field_id)
        field.field_status = "MATCHED"
        field.validation_status = "VALID"
        field.human_value = field.normalized_value
        field.reviewed_at = datetime.now(timezone.utc)
        _session.flush()

    monkeypatch.setattr(paired_review, "review_us_lacey_field", human_review)
    action = lambda: paired_review.confirm_plant_mass_pair(
        organization_id=org, operation_public_id=op_uuid, user_id=1,
        user_email="reviewer@example.test", quantity_id=fields["plant_quantity"],
        unit_id=fields["metric_unit"],
        expected_quantity="289" if scenario == "stale_value" else "288",
        expected_unit="kg", quantity_locator="canonical:1:quantity",
        unit_locator="canonical:1:unit",
        session_factory=lambda: tenant_session(factory, org),
    )
    if scenario == "atomic_success":
        assert action() == 2
        assert len(call_sessions) == 2 and len(set(call_sessions)) == 1
    else:
        with pytest.raises(UsLaceyReviewError):
            action()
    db = tenant_session(factory, org)
    statuses = {
        name: (db.get(UsLaceyOperationField, fields[name]).field_status,
               db.get(UsLaceyOperationField, fields[name]).human_value)
        for name in ("plant_quantity", "metric_unit")
    }
    db.close()
    if scenario == "atomic_success":
        assert statuses == {
            "plant_quantity": ("MATCHED", "288"),
            "metric_unit": ("MATCHED", "kg"),
        }
    else:
        assert statuses == {
            "plant_quantity": ("REVIEW", None),
            "metric_unit": ("REVIEW", None),
        }


@pytest.mark.parametrize("adversarial", [
    "valid_original", "value_disagreement", "wrong_sku", "wrong_line",
    "human_reviewed", "duplicate_original", "worksheet_only",
    "source_not_invoice", "wrong_primary_source", "already_confirmed",
])
def test_invoice_original_resourcing_preserves_human_review_and_fails_closed(
    engine2_postgres_session_factory, monkeypatch, adversarial,
):
    """Invoice review source swap: one verified line amount, no automatic approval."""
    from datetime import datetime, timezone
    factory = engine2_postgres_session_factory
    org, op_id, worksheet_id, fields = _fixture(factory, extra_original=True)
    db = tenant_session(factory, org)
    original_id = db.scalars(select(UsLaceyOperationDocument.assurance_document_id).where(
        UsLaceyOperationDocument.operation_id == op_id,
        UsLaceyOperationDocument.assurance_document_id != worksheet_id,
    )).one()
    invoice = db.get(AssuranceDocument, original_id)
    db.get(VaultDocument, invoice.vault_document_id).original_filename = (
        "01_Commercial_Invoice_Shipment2.pdf"
    )
    target = db.get(UsLaceyOperationField, fields["entered_value"])
    target.original_value = "11250"
    target.normalized_value = "11250"
    target.field_status = "REVIEW"
    target.validation_status = "REVIEW_REQUIRED"
    target.source_assurance_document_id = worksheet_id
    target.extractor = "canonical-shipment-truth"
    source = {
        **_claim("entered_value", "USD 11,250.00", original_id, suffix=502),
        "source_claim": {"extractor_name": "invoice_line_extended_value_exact"},
    }
    if adversarial == "value_disagreement":
        source["normalized_value"] = "USD 12,250.00"
    if adversarial == "wrong_sku":
        source["sku"] = "OTHER-SKU-01"
    if adversarial == "wrong_line":
        source["line_reference"] = "2"
    if adversarial == "human_reviewed":
        target.reviewed_at = datetime.now(timezone.utc)
        target.human_value = "11250"
    if adversarial == "already_confirmed":
        target.field_status = "MATCHED"
    if adversarial == "source_not_invoice":
        db.get(VaultDocument, invoice.vault_document_id).original_filename = (
            "05_Entry_Preparation_Worksheet_Shipment2.pdf"
        )
    if adversarial == "wrong_primary_source":
        target.source_assurance_document_id = original_id
    observations = (
        _claim("entered_value", "11250", worksheet_id, suffix=501),
        source,
    )
    if adversarial == "duplicate_original":
        observations += ({**source, "id": "f" * 64},)
    if adversarial == "worksheet_only":
        observations = observations[:1]
    monkeypatch.setattr(
        "litoral_trace.us_lacey.assurance_v2_case_reader._source_claim_observations",
        lambda *_args, **_kwargs: observations,
    )
    expected = 1 if adversarial == "valid_original" else 0
    assert publish_source_linked_review_candidates(
        db, organization_id=org, operation_id=op_id,
    ) == expected
    db.flush()
    assert target.field_status == (
        "MATCHED" if adversarial == "already_confirmed" else "REVIEW"
    )
    assert target.source_assurance_document_id == (
        original_id if adversarial == "valid_original" else
        original_id if adversarial == "wrong_primary_source" else worksheet_id
    )
    assert target.normalized_value == "11250"
    assert (target.reviewed_at is not None) == (adversarial == "human_reviewed")
    if adversarial == "valid_original":
        assert target.human_value is None
        assert target.validation_status == "REVIEW_REQUIRED"
        assert publish_source_linked_review_candidates(
            db, organization_id=org, operation_id=op_id,
        ) == 0
    db.rollback()
    db.close()
