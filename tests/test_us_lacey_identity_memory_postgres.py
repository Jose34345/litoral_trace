from __future__ import annotations

import os

import pytest
from sqlalchemy import create_engine, inspect, select
from sqlalchemy.orm import sessionmaker

from litoral_trace.config.settings import normalize_database_url
from litoral_trace.db.models import (
    UsLaceyOperationProductLink,
    UsLaceySupplier,
    UsLaceySupplierIdentifier,
    UsLaceySupplierProduct,
)
from litoral_trace.us_lacey.source_sets import seal_current_source_set
from tests.us_lacey_engine2_postgres import (
    create_test_graph,
    engine2_postgres_engine,
    engine2_postgres_session_factory,
    tenant_session,
)


TABLES = {
    "us_lacey_supplier_identifier",
    "us_lacey_operation_product_link",
}


def _seed_identity(factory, *, organization_id: int, operation_id: int, token: str) -> None:
    revision = seal_current_source_set(
        organization_id=organization_id,
        operation_id=operation_id,
        session_factory=factory,
    )
    session = tenant_session(factory, organization_id)
    try:
        supplier = UsLaceySupplier(
            organization_id=organization_id,
            supplier_key=f"MID:US{token.upper()}",
            display_name=f"Supplier {token}",
            normalized_name=f"supplier {token}",
            status="ACTIVE",
        )
        session.add(supplier)
        session.flush()
        session.add(
            UsLaceySupplierIdentifier(
                organization_id=organization_id,
                supplier_id=supplier.id,
                identifier_type="MID",
                normalized_value=f"US{token.upper()}",
                display_value=f"US{token.upper()}",
                human_confirmed=False,
            )
        )
        product = UsLaceySupplierProduct(
            organization_id=organization_id,
            supplier_id=supplier.id,
            product_key=f"SKU:{token.upper()}-001",
            sku=f"{token.upper()}-001",
            display_name=f"Product {token}",
            normalized_name=f"product {token}",
            status="ACTIVE",
        )
        session.add(product)
        session.flush()
        session.add(
            UsLaceyOperationProductLink(
                organization_id=organization_id,
                operation_id=operation_id,
                source_set_revision_id=revision.id,
                line_reference="1",
                supplier_product_id=product.id,
                link_method="EXACT_SKU",
            )
        )
        session.commit()
    finally:
        session.close()


def test_identity_memory_tables_exist_after_075(engine2_postgres_engine):
    assert TABLES <= set(inspect(engine2_postgres_engine).get_table_names())


def test_identity_memory_runtime_rls_isolates_tenants(
    engine2_postgres_engine,
    engine2_postgres_session_factory,
):
    if not TABLES <= set(inspect(engine2_postgres_engine).get_table_names()):
        pytest.skip("POSTGRES_SCHEMA_NOT_MIGRATED_TO_075")

    org_a, operation_a, *_ = create_test_graph(
        engine2_postgres_session_factory,
        content=b"identity-memory-rls-a",
    )
    org_b, operation_b, *_ = create_test_graph(
        engine2_postgres_session_factory,
        content=b"identity-memory-rls-b",
    )
    _seed_identity(
        engine2_postgres_session_factory,
        organization_id=org_a,
        operation_id=operation_a,
        token="tenant-a",
    )
    _seed_identity(
        engine2_postgres_session_factory,
        organization_id=org_b,
        operation_id=operation_b,
        token="tenant-b",
    )

    runtime_url = os.environ.get("TEST_POSTGRES_DATABASE_URL")
    assert runtime_url, (
        "TEST_POSTGRES_DATABASE_URL is required for identity-memory "
        "RLS acceptance"
    )
    engine = create_engine(
        normalize_database_url(runtime_url),
        pool_pre_ping=True,
    )
    Factory = sessionmaker(bind=engine, expire_on_commit=False)
    try:
        session = tenant_session(Factory, org_a)
        identifiers = session.scalars(
            select(UsLaceySupplierIdentifier)
        ).all()
        product_links = session.scalars(
            select(UsLaceyOperationProductLink)
        ).all()
        assert len(identifiers) == 1
        assert len(product_links) == 1
        assert identifiers[0].organization_id == org_a
        assert product_links[0].organization_id == org_a
        session.close()
    finally:
        engine.dispose()
