from __future__ import annotations

import ast
from datetime import datetime, timedelta, timezone
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from litoral_trace.db.models import (
    ExtractedDocumentField,
    Organization,
    UsLaceyEvidenceClaim,
    UsLaceySupplier,
    UsLaceySupplierEvidence,
    UsLaceySupplierProduct,
    User,
)
from litoral_trace.us_lacey.identity_memory import (
    _line_sku_observations,
    _supplier_candidates,
)
from litoral_trace.us_lacey.reusable_evidence import ReusableEvidenceService


MIGRATION = Path(
    "alembic/versions/075_us_lacey_identity_and_product_bridge.py"
)
BACKFILL = Path("scripts/backfill_us_lacey_identities.py")


def _raw(
    *,
    header: str,
    value: str,
    column: int,
    document_id: int = 10,
) -> ExtractedDocumentField:
    return ExtractedDocumentField(
        organization_id=1,
        assurance_document_id=document_id,
        extraction_run_id=20,
        field_name=f"raw.table.1.{header}",
        original_value=value,
        normalized_value=value,
        value_type="cell",
        confidence=0.98,
        confidence_level="HIGH",
        source_page=None,
        source_locator=(
            f"csv:header_row:1;data_row:1;column:{column}"
        ),
        auto_accepted=False,
        needs_review=True,
    )


def test_supplier_discovery_needs_no_cuit_and_uses_exact_us_identifiers():
    candidates = _supplier_candidates(
        (
            _raw(header="Supplier", value="Northwoods Supplier LLC", column=1),
            _raw(
                header="Supplier Address",
                value="100 Timber Way, Portland, OR 97201",
                column=2,
            ),
            _raw(header="MID", value="USNORTIM100POR", column=3),
        )
    )

    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate.display_name == "Northwoods Supplier LLC"
    assert {
        item.identifier_type for item in candidate.identifiers
    } == {"MID", "NAME_ADDRESS"}
    assert not any(
        "cuit" in item.identifier_type.casefold()
        for item in candidate.identifiers
    )


def test_line_reference_and_sku_are_bound_from_same_source_row_not_by_equality():
    observations = _line_sku_observations(
        (
            _raw(header="Line", value="1", column=1),
            _raw(header="SKU", value="CHAIR-001", column=2),
        )
    )

    assert observations == {"1": {"CHAIR-001"}}
    assert "1" != "CHAIR-001"


def test_name_without_exact_identifier_does_not_create_supplier_candidate():
    candidates = _supplier_candidates(
        (
            _raw(
                header="Supplier",
                value="Northwoods Supplier LLC",
                column=1,
            ),
        )
    )
    assert candidates == ()


def test_075_migration_adds_tenant_hardened_identity_and_product_link():
    text = MIGRATION.read_text(encoding="utf-8")

    assert 'revision = "075_us_lacey_identity_and_product_bridge"' in text
    assert 'down_revision = "074_us_lacey_audit_identity_projection"' in text
    assert "us_lacey_supplier_identifier" in text
    assert "us_lacey_operation_product_link" in text
    assert "MID" in text
    assert "VENDOR_CODE" in text
    assert "NAME_ADDRESS" in text
    assert "MANUAL" in text
    assert "EXACT_SKU" in text
    assert "HUMAN_CONFIRMED" in text
    assert "ENABLE ROW LEVEL SECURITY" in text
    assert "FORCE ROW LEVEL SECURITY" in text
    assert "app.current_organization_id" in text
    assert "fk_us_lacey_operation_product_link_product_tenant" in text
    assert "fk_us_lacey_operation_product_link_revision_tenant" in text


def test_backfill_has_no_code_path_to_verified_reusable_evidence():
    tree = ast.parse(BACKFILL.read_text(encoding="utf-8"))
    names = {
        node.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Name)
    }
    attrs = {
        node.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Attribute)
    }
    forbidden = {
        "UsLaceySupplierEvidence",
        "UsLaceyEvidenceClaim",
        "promote_reviewed_field",
        "apply_reusable_evidence_for_operation",
    }
    assert forbidden.isdisjoint(names | attrs)


def _memory_engine():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    for table in (
        Organization.__table__,
        User.__table__,
        UsLaceySupplier.__table__,
        UsLaceySupplierProduct.__table__,
        UsLaceySupplierEvidence.__table__,
        UsLaceyEvidenceClaim.__table__,
    ):
        table.create(engine, checkfirst=True)
    return engine


def test_same_supplier_different_sku_cannot_resolve_to_reusable_product():
    engine = _memory_engine()
    with Session(engine) as session:
        org = Organization(
            name="Identity Test",
            slug="identity-test",
            tier="pro",
            is_active=True,
        )
        session.add(org)
        session.flush()
        supplier = UsLaceySupplier(
            organization_id=org.id,
            supplier_key="MID:USNORTIM100POR",
            display_name="Northwoods Supplier LLC",
            normalized_name="northwoods supplier llc",
            status="ACTIVE",
        )
        session.add(supplier)
        session.flush()
        product = UsLaceySupplierProduct(
            organization_id=org.id,
            supplier_id=supplier.id,
            product_key="SKU:CHAIR-001",
            sku="CHAIR-001",
            display_name="White Oak Chair",
            normalized_name="white oak chair",
            status="ACTIVE",
        )
        session.add(product)
        session.flush()

        exact = ReusableEvidenceService._resolve_product(
            session,
            organization_id=org.id,
            supplier_key=supplier.supplier_key,
            product_key="SKU:CHAIR-001",
        )
        different = ReusableEvidenceService._resolve_product(
            session,
            organization_id=org.id,
            supplier_key=supplier.supplier_key,
            product_key="SKU:TABLE-999",
        )

        assert exact is not None
        assert different is None


def test_expired_verified_evidence_is_not_eligible_for_reuse():
    engine = _memory_engine()
    now = datetime.now(timezone.utc)
    with Session(engine) as session:
        org = Organization(
            name="Expiry Test",
            slug="expiry-test",
            tier="pro",
            is_active=True,
        )
        session.add(org)
        session.flush()
        user = User(
            organization_id=org.id,
            email="expiry@example.com",
            username="expiry",
            password_hash="test",
            role="cliente",
            full_name="Expiry Reviewer",
            is_active=True,
        )
        supplier = UsLaceySupplier(
            organization_id=org.id,
            supplier_key="MID:EXPIRED",
            display_name="Expired Supplier",
            normalized_name="expired supplier",
            status="ACTIVE",
        )
        session.add_all([user, supplier])
        session.flush()
        product = UsLaceySupplierProduct(
            organization_id=org.id,
            supplier_id=supplier.id,
            product_key="SKU:CHAIR-001",
            sku="CHAIR-001",
            display_name="Chair",
            normalized_name="chair",
            status="ACTIVE",
        )
        session.add(product)
        session.flush()
        evidence = UsLaceySupplierEvidence(
            organization_id=org.id,
            supplier_product_id=product.id,
            evidence_type="HUMAN_VERIFIED_SUPPLIER_DOCUMENT",
            document_hash="a" * 64,
            source_reference="assurance:test",
            valid_from=now - timedelta(days=400),
            valid_until=now - timedelta(days=1),
            verified_by_user_id=user.id,
            verified_at=now - timedelta(days=400),
            status="VERIFIED",
        )
        session.add(evidence)
        session.flush()
        session.add(
            UsLaceyEvidenceClaim(
                organization_id=org.id,
                evidence_id=evidence.id,
                field_name="species",
                field_value="Quercus alba",
                normalized_value="Quercus alba",
            )
        )
        session.flush()

        historical = ReusableEvidenceService._historical_claims(
            session,
            organization_id=org.id,
            supplier_product_id=product.id,
            as_of=now,
        )
        assert historical == {}
