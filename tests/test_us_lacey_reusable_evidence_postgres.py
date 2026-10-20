from __future__ import annotations

from datetime import datetime, timedelta, timezone
import os
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, inspect, select
from sqlalchemy.orm import sessionmaker

from litoral_trace.config.settings import normalize_database_url
from litoral_trace.db.models import (
    UsLaceyEvidenceClaim,
    UsLaceySupplier,
    UsLaceySupplierEvidence,
    UsLaceySupplierProduct,
)
from tests.us_lacey_engine2_postgres import (
    create_test_graph,
    engine2_postgres_engine,
    engine2_postgres_session_factory,
    tenant_session,
)


TABLES = {
    "us_lacey_supplier",
    "us_lacey_supplier_product",
    "us_lacey_supplier_evidence",
    "us_lacey_evidence_claim",
}


def _seed_evidence(factory, *, organization_id: int, key: str) -> None:
    session = tenant_session(factory, organization_id)
    now = datetime.now(timezone.utc)
    supplier = UsLaceySupplier(
        organization_id=organization_id,
        supplier_key=f"supplier-{key}",
        display_name=f"Supplier {key}",
        normalized_name=f"supplier {key}",
        status="ACTIVE",
    )
    session.add(supplier)
    session.flush()

    product = UsLaceySupplierProduct(
        organization_id=organization_id,
        supplier_id=supplier.id,
        product_key=f"product-{key}",
        sku=f"SKU-{key}",
        display_name=f"Product {key}",
        normalized_name=f"product {key}",
        status="ACTIVE",
    )
    session.add(product)
    session.flush()

    evidence = UsLaceySupplierEvidence(
        organization_id=organization_id,
        supplier_product_id=product.id,
        evidence_type="SUPPLIER_DECLARATION",
        document_hash=(key[0].lower() if key else "a") * 64,
        source_reference=f"vault:{uuid4()}",
        valid_from=now - timedelta(days=1),
        valid_until=now + timedelta(days=365),
        verified_by_user_id=None,
        verified_at=now,
        status="VERIFIED",
    )
    session.add(evidence)
    session.flush()
    session.add(
        UsLaceyEvidenceClaim(
            organization_id=organization_id,
            evidence_id=evidence.id,
            field_name="species",
            field_value="Quercus alba",
            normalized_value="Quercus alba",
        )
    )
    session.commit()
    session.close()


def test_reusable_evidence_tables_exist_after_071(engine2_postgres_engine):
    assert TABLES <= set(inspect(engine2_postgres_engine).get_table_names())


def test_reusable_evidence_runtime_rls_isolates_tenants(
    engine2_postgres_engine,
    engine2_postgres_session_factory,
):
    if not TABLES <= set(inspect(engine2_postgres_engine).get_table_names()):
        pytest.skip("POSTGRES_SCHEMA_NOT_MIGRATED_TO_071")

    org_a, *_ = create_test_graph(
        engine2_postgres_session_factory, content=b"reuse-rls-a"
    )
    org_b, *_ = create_test_graph(
        engine2_postgres_session_factory, content=b"reuse-rls-b"
    )
    _seed_evidence(engine2_postgres_session_factory, organization_id=org_a, key="a")
    _seed_evidence(engine2_postgres_session_factory, organization_id=org_b, key="b")

    runtime_url = os.environ.get("TEST_POSTGRES_DATABASE_URL")
    assert runtime_url, "TEST_POSTGRES_DATABASE_URL is required for reusable evidence RLS acceptance"
    engine = create_engine(normalize_database_url(runtime_url), pool_pre_ping=True)
    Factory = sessionmaker(bind=engine, expire_on_commit=False)
    try:
        session = tenant_session(Factory, org_a)
        suppliers = session.scalars(select(UsLaceySupplier)).all()
        products = session.scalars(select(UsLaceySupplierProduct)).all()
        evidence = session.scalars(select(UsLaceySupplierEvidence)).all()
        claims = session.scalars(select(UsLaceyEvidenceClaim)).all()
        assert len(suppliers) == len(products) == len(evidence) == len(claims) == 1
        assert suppliers[0].organization_id == org_a
        assert products[0].organization_id == org_a
        assert evidence[0].organization_id == org_a
        assert claims[0].organization_id == org_a
        session.close()
    finally:
        engine.dispose()
