from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import uuid4

from litoral_trace.db.models import (
    UsLaceyEvidenceClaim,
    UsLaceySupplier,
    UsLaceySupplierEvidence,
    UsLaceySupplierProduct,
)
from litoral_trace.us_lacey.reconciliation_schemas import (
    ReconciliationFieldInput,
    ReconciliationProvenance,
)
from litoral_trace.us_lacey.reusable_evidence import (
    ReusableEvidenceService,
    _HistoricalClaim,
    operation_field_provenance,
)


def _historical(field_name: str, value: str, *, suffix: str = "a") -> _HistoricalClaim:
    now = datetime.now(timezone.utc)
    evidence = UsLaceySupplierEvidence(
        public_id=uuid4(),
        organization_id=1,
        supplier_product_id=1,
        evidence_type="SUPPLIER_DECLARATION",
        document_hash=suffix * 64,
        valid_from=now - timedelta(days=30),
        valid_until=now + timedelta(days=30),
        verified_by_user_id=1,
        verified_at=now - timedelta(days=1),
        status="VERIFIED",
    )
    claim = UsLaceyEvidenceClaim(
        organization_id=1,
        evidence_id=1,
        field_name=field_name,
        field_value=value,
        normalized_value=value,
    )
    return _HistoricalClaim(claim=claim, evidence=evidence, validated_value=value)


def test_reusable_models_are_tenant_scoped_and_do_not_store_document_blobs():
    assert UsLaceySupplier.__table__.name == "us_lacey_supplier"
    assert UsLaceySupplierProduct.__table__.name == "us_lacey_supplier_product"
    assert UsLaceySupplierEvidence.__table__.name == "us_lacey_supplier_evidence"
    assert UsLaceyEvidenceClaim.__table__.name == "us_lacey_evidence_claim"

    evidence_columns = set(UsLaceySupplierEvidence.__table__.c.keys())
    assert {
        "organization_id",
        "supplier_product_id",
        "document_hash",
        "valid_from",
        "valid_until",
        "verified_by_user_id",
        "verified_at",
    } <= evidence_columns
    assert not {"blob", "content", "document_bytes", "raw_document"} & evidence_columns

    assert any(
        constraint.name == "fk_us_lacey_supplier_product_supplier_tenant"
        for constraint in UsLaceySupplierProduct.__table__.foreign_key_constraints
    )
    assert any(
        constraint.name == "fk_us_lacey_supplier_evidence_product_tenant"
        for constraint in UsLaceySupplierEvidence.__table__.foreign_key_constraints
    )
    assert any(
        constraint.name == "fk_us_lacey_evidence_claim_evidence_tenant"
        for constraint in UsLaceyEvidenceClaim.__table__.foreign_key_constraints
    )


def test_current_shipment_value_always_wins_over_history():
    current = ReconciliationFieldInput(
        field_name="species",
        field_value="Quercus rubra",
        validation_status="VALID",
    )
    result = ReusableEvidenceService._reuse_field(
        current,
        {"species": [_historical("species", "Quercus alba")]},
    )

    assert result.field_value == "Quercus rubra"
    assert result.provenance == ReconciliationProvenance.CURRENT_SHIPMENT
    assert result.requires_review is False
    assert result.evidence_public_id is None


def test_missing_stable_field_reuses_one_consistent_verified_value():
    missing = ReconciliationFieldInput(field_name="species", field_value=None)
    historical = _historical("species", "Quercus alba")

    result = ReusableEvidenceService._reuse_field(
        missing,
        {"species": [historical]},
    )

    assert result.field_value == "Quercus alba"
    assert result.provenance == ReconciliationProvenance.REUSED_EVIDENCE
    assert result.requires_review is False
    assert result.evidence_public_id == historical.evidence.public_id
    assert result.evidence_document_hash == "a" * 64


def test_conflicting_valid_history_fails_closed_to_review():
    missing = ReconciliationFieldInput(field_name="species", field_value=None)

    result = ReusableEvidenceService._reuse_field(
        missing,
        {
            "species": [
                _historical("species", "Quercus alba", suffix="a"),
                _historical("species", "Quercus rubra", suffix="b"),
            ]
        },
    )

    assert result.field_value is None
    assert result.provenance == ReconciliationProvenance.REVIEW_REQUIRED
    assert result.requires_review is True
    assert "disagree" in (result.reason or "")


def test_shipment_specific_quantity_is_never_reused():
    missing = ReconciliationFieldInput(field_name="plant_quantity", field_value=None)

    result = ReusableEvidenceService._reuse_field(
        missing,
        {"plant_quantity": [_historical("plant_quantity", "5000")]},
    )

    assert result.field_value is None
    assert result.provenance == ReconciliationProvenance.REVIEW_REQUIRED
    assert result.requires_review is True
    assert "shipment-specific" in (result.reason or "")


def test_operation_field_provenance_exposes_three_frontend_states():
    assert (
        operation_field_provenance(
            SimpleNamespace(extractor="reusable-supplier-evidence", field_status="FOUND")
        )
        == ReconciliationProvenance.REUSED_EVIDENCE
    )
    assert (
        operation_field_provenance(
            SimpleNamespace(extractor="engine2", field_status="MISSING")
        )
        == ReconciliationProvenance.REVIEW_REQUIRED
    )
    assert (
        operation_field_provenance(
            SimpleNamespace(extractor="engine2", field_status="FOUND")
        )
        == ReconciliationProvenance.CURRENT_SHIPMENT
    )
