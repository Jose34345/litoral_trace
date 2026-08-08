from __future__ import annotations

import os
from datetime import date
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError

from litoral_trace.config.settings import normalize_database_url


def _truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}


POSTGRES_RLS_TESTS_ENABLED = _truthy(os.environ.get("ENABLE_POSTGRES_TESTS"))
RUNTIME_TEST_DATABASE_URL = os.environ.get("TEST_POSTGRES_DATABASE_URL")
MIGRATION_TEST_DATABASE_URL = (
    os.environ.get("TEST_POSTGRES_MIGRATION_DATABASE_URL")
    or os.environ.get("MIGRATION_DATABASE_URL")
)

pytestmark = pytest.mark.skipif(
    not (
        POSTGRES_RLS_TESTS_ENABLED
        and RUNTIME_TEST_DATABASE_URL
        and MIGRATION_TEST_DATABASE_URL
    ),
    reason=(
        "PostgreSQL RLS tests require ENABLE_POSTGRES_TESTS=1, "
        "TEST_POSTGRES_DATABASE_URL y TEST_POSTGRES_MIGRATION_DATABASE_URL "
        "(o MIGRATION_DATABASE_URL)."
    ),
)


def _runtime_engine():
    return create_engine(
        normalize_database_url(RUNTIME_TEST_DATABASE_URL),
        pool_size=1,
        max_overflow=0,
        pool_pre_ping=True,
    )


def _owner_engine():
    return create_engine(
        normalize_database_url(MIGRATION_TEST_DATABASE_URL),
        pool_pre_ping=True,
    )


def _set_tenant_context(connection, organization_id: int) -> None:
    connection.execute(
        text(
            "SELECT set_config("
            "'app.current_organization_id', "
            ":organization_id, "
            "true"
            ")"
        ),
        {"organization_id": str(organization_id)},
    )


def _query_ids(connection, sql: str):
    return [row[0] for row in connection.execute(text(sql)).fetchall()]


@pytest.fixture(scope="module")
def rls_fixture():
    suffix = uuid4().hex[:10]
    runtime_engine = _runtime_engine()
    owner_engine = _owner_engine()

    created_ids: dict[str, int] = {}

    with owner_engine.begin() as conn:
        created_ids["org_a_id"] = conn.execute(
            text(
                """
                INSERT INTO organizations (name, slug, tax_id, tier, description, is_active)
                VALUES (:name, :slug, :tax_id, 'pro', 'P1.8B org A', true)
                RETURNING id
                """
            ),
            {
                "name": f"RLS Org A {suffix}",
                "slug": f"rls-org-a-{suffix}",
                "tax_id": f"30-9{suffix[:8]}",
            },
        ).scalar_one()
        created_ids["org_b_id"] = conn.execute(
            text(
                """
                INSERT INTO organizations (name, slug, tax_id, tier, description, is_active)
                VALUES (:name, :slug, :tax_id, 'pro', 'P1.8B org B', true)
                RETURNING id
                """
            ),
            {
                "name": f"RLS Org B {suffix}",
                "slug": f"rls-org-b-{suffix}",
                "tax_id": f"30-8{suffix[:8]}",
            },
        ).scalar_one()

        created_ids["user_a_id"] = conn.execute(
            text(
                """
                INSERT INTO users (
                    organization_id, email, username, password_hash, role, full_name, is_active
                )
                VALUES (
                    :organization_id, :email, :username, :password_hash, 'admin', 'RLS User A', true
                )
                RETURNING id
                """
            ),
            {
                "organization_id": created_ids["org_a_id"],
                "email": f"rls-a-{suffix}@example.com",
                "username": f"rls_a_{suffix}",
                "password_hash": "not-used-in-rls-tests",
            },
        ).scalar_one()
        created_ids["user_b_id"] = conn.execute(
            text(
                """
                INSERT INTO users (
                    organization_id, email, username, password_hash, role, full_name, is_active
                )
                VALUES (
                    :organization_id, :email, :username, :password_hash, 'admin', 'RLS User B', true
                )
                RETURNING id
                """
            ),
            {
                "organization_id": created_ids["org_b_id"],
                "email": f"rls-b-{suffix}@example.com",
                "username": f"rls_b_{suffix}",
                "password_hash": "not-used-in-rls-tests",
            },
        ).scalar_one()

        created_ids["license_a_id"] = conn.execute(
            text(
                """
                INSERT INTO licenses (
                    organization_id, plan_type, max_lotes, max_volume_tons, max_batch_rows, is_active
                )
                VALUES (:organization_id, 'pro', 100, 1000.0, 500, true)
                RETURNING id
                """
            ),
            {"organization_id": created_ids["org_a_id"]},
        ).scalar_one()
        created_ids["license_b_id"] = conn.execute(
            text(
                """
                INSERT INTO licenses (
                    organization_id, plan_type, max_lotes, max_volume_tons, max_batch_rows, is_active
                )
                VALUES (:organization_id, 'pro', 100, 1000.0, 500, true)
                RETURNING id
                """
            ),
            {"organization_id": created_ids["org_b_id"]},
        ).scalar_one()

        created_ids["lote_a_id"] = conn.execute(
            text(
                """
                INSERT INTO lotes (
                    organization_id, identificador, productor_id, producto_forestal,
                    hectareas, latitud, longitud, polygon_wkt, estatus,
                    volumen_ingresado_ton, volumen_exportar_ton
                )
                VALUES (
                    :organization_id, :identificador, :productor_id, 'Madera Aserrada (Pino)',
                    10.0, -27.1, -58.1, :polygon_wkt, 'Pendiente', 15.0, 5.0
                )
                RETURNING id
                """
            ),
            {
                "organization_id": created_ids["org_a_id"],
                "identificador": f"RLS-A-{suffix}",
                "productor_id": f"20-A-{suffix[:6]}",
                "polygon_wkt": "POLYGON((-58.11 -27.11, -58.09 -27.11, -58.09 -27.09, -58.11 -27.09, -58.11 -27.11))",
            },
        ).scalar_one()
        created_ids["lote_b_id"] = conn.execute(
            text(
                """
                INSERT INTO lotes (
                    organization_id, identificador, productor_id, producto_forestal,
                    hectareas, latitud, longitud, polygon_wkt, estatus,
                    volumen_ingresado_ton, volumen_exportar_ton
                )
                VALUES (
                    :organization_id, :identificador, :productor_id, 'Madera Aserrada (Pino)',
                    10.0, -27.2, -58.2, :polygon_wkt, 'Pendiente', 15.0, 5.0
                )
                RETURNING id
                """
            ),
            {
                "organization_id": created_ids["org_b_id"],
                "identificador": f"RLS-B-{suffix}",
                "productor_id": f"20-B-{suffix[:6]}",
                "polygon_wkt": "POLYGON((-58.21 -27.21, -58.19 -27.21, -58.19 -27.19, -58.21 -27.19, -58.21 -27.21))",
            },
        ).scalar_one()

        created_ids["sat_a_id"] = conn.execute(
            text(
                """
                INSERT INTO satellite_ndvi_observations (
                    organization_id, lote_id, observation_date, ndvi_mean, cloud_percentage,
                    valid_pixel_count, valid_pixel_percentage, satellite, collection,
                    geometry_hash, algorithm_version
                )
                VALUES (
                    :organization_id, :lote_id, :observation_date, 0.61, 3.0,
                    100, 98.0, 'Sentinel-2', 'COPERNICUS/S2_SR_HARMONIZED',
                    :geometry_hash, 'v-test'
                )
                RETURNING id
                """
            ),
            {
                "organization_id": created_ids["org_a_id"],
                "lote_id": created_ids["lote_a_id"],
                "observation_date": date(2026, 8, 1),
                "geometry_hash": f"hash-a-{suffix}",
            },
        ).scalar_one()
        created_ids["sat_b_id"] = conn.execute(
            text(
                """
                INSERT INTO satellite_ndvi_observations (
                    organization_id, lote_id, observation_date, ndvi_mean, cloud_percentage,
                    valid_pixel_count, valid_pixel_percentage, satellite, collection,
                    geometry_hash, algorithm_version
                )
                VALUES (
                    :organization_id, :lote_id, :observation_date, 0.44, 5.0,
                    100, 97.0, 'Sentinel-2', 'COPERNICUS/S2_SR_HARMONIZED',
                    :geometry_hash, 'v-test'
                )
                RETURNING id
                """
            ),
            {
                "organization_id": created_ids["org_b_id"],
                "lote_id": created_ids["lote_b_id"],
                "observation_date": date(2026, 8, 2),
                "geometry_hash": f"hash-b-{suffix}",
            },
        ).scalar_one()

        created_ids["api_key_a_id"] = conn.execute(
            text(
                """
                INSERT INTO api_keys (
                    organization_id, user_id, name, key_prefix, key_hash, permissions, is_active
                )
                VALUES (
                    :organization_id, :user_id, 'RLS API A', :key_prefix, :key_hash, '{}'::json, true
                )
                RETURNING id
                """
            ),
            {
                "organization_id": created_ids["org_a_id"],
                "user_id": created_ids["user_a_id"],
                "key_prefix": f"pfxa{suffix[:8]}",
                "key_hash": f"hash-api-a-{suffix}",
            },
        ).scalar_one()
        created_ids["api_key_b_id"] = conn.execute(
            text(
                """
                INSERT INTO api_keys (
                    organization_id, user_id, name, key_prefix, key_hash, permissions, is_active
                )
                VALUES (
                    :organization_id, :user_id, 'RLS API B', :key_prefix, :key_hash, '{}'::json, true
                )
                RETURNING id
                """
            ),
            {
                "organization_id": created_ids["org_b_id"],
                "user_id": created_ids["user_b_id"],
                "key_prefix": f"pfxb{suffix[:8]}",
                "key_hash": f"hash-api-b-{suffix}",
            },
        ).scalar_one()

        created_ids["audit_a_id"] = conn.execute(
            text(
                """
                INSERT INTO audit_logs (
                    organization_id, user_id, username, action, entity_type, entity_id, detail
                )
                VALUES (
                    :organization_id, :user_id, :username, 'CREATE', 'lote', :entity_id, 'RLS audit A'
                )
                RETURNING id
                """
            ),
            {
                "organization_id": created_ids["org_a_id"],
                "user_id": created_ids["user_a_id"],
                "username": f"rls_a_{suffix}",
                "entity_id": created_ids["lote_a_id"],
            },
        ).scalar_one()
        created_ids["audit_b_id"] = conn.execute(
            text(
                """
                INSERT INTO audit_logs (
                    organization_id, user_id, username, action, entity_type, entity_id, detail
                )
                VALUES (
                    :organization_id, :user_id, :username, 'CREATE', 'lote', :entity_id, 'RLS audit B'
                )
                RETURNING id
                """
            ),
            {
                "organization_id": created_ids["org_b_id"],
                "user_id": created_ids["user_b_id"],
                "username": f"rls_b_{suffix}",
                "entity_id": created_ids["lote_b_id"],
            },
        ).scalar_one()

    yield {
        **created_ids,
        "runtime_engine": runtime_engine,
        "owner_engine": owner_engine,
    }

    with owner_engine.begin() as conn:
        conn.execute(
            text("DELETE FROM audit_logs WHERE id IN (:audit_a_id, :audit_b_id)"),
            {
                "audit_a_id": created_ids["audit_a_id"],
                "audit_b_id": created_ids["audit_b_id"],
            },
        )
        conn.execute(
            text("DELETE FROM api_keys WHERE id IN (:api_key_a_id, :api_key_b_id)"),
            {
                "api_key_a_id": created_ids["api_key_a_id"],
                "api_key_b_id": created_ids["api_key_b_id"],
            },
        )
        conn.execute(
            text("DELETE FROM satellite_ndvi_observations WHERE id IN (:sat_a_id, :sat_b_id)"),
            {
                "sat_a_id": created_ids["sat_a_id"],
                "sat_b_id": created_ids["sat_b_id"],
            },
        )
        conn.execute(
            text("DELETE FROM lotes WHERE id IN (:lote_a_id, :lote_b_id)"),
            {
                "lote_a_id": created_ids["lote_a_id"],
                "lote_b_id": created_ids["lote_b_id"],
            },
        )
        conn.execute(
            text("DELETE FROM licenses WHERE id IN (:license_a_id, :license_b_id)"),
            {
                "license_a_id": created_ids["license_a_id"],
                "license_b_id": created_ids["license_b_id"],
            },
        )
        conn.execute(
            text("DELETE FROM users WHERE id IN (:user_a_id, :user_b_id)"),
            {
                "user_a_id": created_ids["user_a_id"],
                "user_b_id": created_ids["user_b_id"],
            },
        )
        conn.execute(
            text("DELETE FROM organizations WHERE id IN (:org_a_id, :org_b_id)"),
            {
                "org_a_id": created_ids["org_a_id"],
                "org_b_id": created_ids["org_b_id"],
            },
        )

    runtime_engine.dispose()
    owner_engine.dispose()


def test_runtime_role_is_rls_eligible(rls_fixture):
    with rls_fixture["runtime_engine"].connect() as conn:
        role_row = conn.execute(
            text(
                """
                SELECT current_user, r.rolsuper, r.rolbypassrls
                FROM pg_roles r
                WHERE r.rolname = current_user
                """
            )
        ).mappings().one()

    assert role_row["current_user"] == "litoral_trace_app"
    assert role_row["rolsuper"] is False
    assert role_row["rolbypassrls"] is False


def test_lotes_select_without_context_returns_no_rows(rls_fixture):
    with rls_fixture["runtime_engine"].begin() as conn:
        rows = conn.execute(text("SELECT id, organization_id FROM lotes")).fetchall()

    assert rows == []


def test_lotes_select_with_tenant_a_returns_only_tenant_a_without_where(rls_fixture):
    with rls_fixture["runtime_engine"].begin() as conn:
        _set_tenant_context(conn, rls_fixture["org_a_id"])
        rows = conn.execute(text("SELECT organization_id FROM lotes ORDER BY id")).fetchall()

    assert {row[0] for row in rows} == {rls_fixture["org_a_id"]}
    assert len(rows) >= 1


def test_lotes_select_with_tenant_b_returns_only_tenant_b_without_where(rls_fixture):
    with rls_fixture["runtime_engine"].begin() as conn:
        _set_tenant_context(conn, rls_fixture["org_b_id"])
        rows = conn.execute(text("SELECT organization_id FROM lotes ORDER BY id")).fetchall()

    assert {row[0] for row in rows} == {rls_fixture["org_b_id"]}
    assert len(rows) >= 1


def test_primary_key_cross_tenant_query_returns_no_rows(rls_fixture):
    with rls_fixture["runtime_engine"].begin() as conn:
        _set_tenant_context(conn, rls_fixture["org_a_id"])
        rows = conn.execute(
            text("SELECT id FROM lotes WHERE id = :lote_id"),
            {"lote_id": rls_fixture["lote_b_id"]},
        ).fetchall()

    assert rows == []


def test_insert_forged_tenant_is_rejected_by_postgresql_rls(rls_fixture):
    with pytest.raises(DBAPIError) as exc_info:
        with rls_fixture["runtime_engine"].begin() as conn:
            _set_tenant_context(conn, rls_fixture["org_a_id"])
            conn.execute(
                text(
                    """
                    INSERT INTO lotes (
                        organization_id, identificador, productor_id, producto_forestal,
                        hectareas, latitud, longitud, polygon_wkt, estatus,
                        volumen_ingresado_ton, volumen_exportar_ton
                    )
                    VALUES (
                        :organization_id, :identificador, :productor_id, 'Madera Aserrada (Pino)',
                        1.0, -27.3, -58.3, :polygon_wkt, 'Pendiente', 1.0, 1.0
                    )
                    """
                ),
                {
                    "organization_id": rls_fixture["org_b_id"],
                    "identificador": f"FORGED-INSERT-{uuid4().hex[:6]}",
                    "productor_id": "20-55555555-5",
                    "polygon_wkt": "POLYGON((-58.31 -27.31, -58.29 -27.31, -58.29 -27.29, -58.31 -27.29, -58.31 -27.31))",
                },
            )

    assert "row-level security" in str(exc_info.value).lower()


def test_update_forged_tenant_is_rejected_by_postgresql_rls(rls_fixture):
    with pytest.raises(DBAPIError) as exc_info:
        with rls_fixture["runtime_engine"].begin() as conn:
            _set_tenant_context(conn, rls_fixture["org_a_id"])
            conn.execute(
                text(
                    """
                    UPDATE lotes
                    SET organization_id = :target_organization_id
                    WHERE id = :lote_id
                    """
                ),
                {
                    "target_organization_id": rls_fixture["org_b_id"],
                    "lote_id": rls_fixture["lote_a_id"],
                },
            )

    assert "row-level security" in str(exc_info.value).lower()


def test_delete_cross_tenant_returns_zero_affected_rows(rls_fixture):
    with rls_fixture["runtime_engine"].begin() as conn:
        _set_tenant_context(conn, rls_fixture["org_a_id"])
        result = conn.execute(
            text("DELETE FROM lotes WHERE id = :lote_id"),
            {"lote_id": rls_fixture["lote_b_id"]},
        )

    assert result.rowcount == 0


@pytest.mark.parametrize(
    ("table_name", "tenant_column", "org_a_expected", "org_b_expected"),
    (
        ("organizations", "id", "org_a_id", "org_b_id"),
        ("licenses", "organization_id", "org_a_id", "org_b_id"),
        ("api_keys", "organization_id", "org_a_id", "org_b_id"),
        ("audit_logs", "organization_id", "org_a_id", "org_b_id"),
        ("satellite_ndvi_observations", "organization_id", "org_a_id", "org_b_id"),
    ),
)
def test_core_rls_tables_are_scoped_by_tenant_context(
    rls_fixture,
    table_name,
    tenant_column,
    org_a_expected,
    org_b_expected,
):
    with rls_fixture["runtime_engine"].begin() as conn:
        no_context_rows = _query_ids(
            conn,
            f"SELECT {tenant_column} FROM {table_name} ORDER BY 1",
        )
        _set_tenant_context(conn, rls_fixture[org_a_expected])
        tenant_a_rows = _query_ids(
            conn,
            f"SELECT {tenant_column} FROM {table_name} ORDER BY 1",
        )

    with rls_fixture["runtime_engine"].begin() as conn:
        _set_tenant_context(conn, rls_fixture[org_b_expected])
        tenant_b_rows = _query_ids(
            conn,
            f"SELECT {tenant_column} FROM {table_name} ORDER BY 1",
        )

    assert no_context_rows == []
    assert set(tenant_a_rows) == {rls_fixture[org_a_expected]}
    assert set(tenant_b_rows) == {rls_fixture[org_b_expected]}


def test_transaction_local_context_does_not_leak_across_reused_connections(rls_fixture):
    runtime_engine = rls_fixture["runtime_engine"]

    with runtime_engine.begin() as conn:
        _set_tenant_context(conn, rls_fixture["org_a_id"])
        tenant_a_rows = _query_ids(
            conn,
            "SELECT organization_id FROM lotes ORDER BY id",
        )

    with runtime_engine.begin() as conn:
        current_setting_value = conn.execute(
            text("SELECT current_setting('app.current_organization_id', true)")
        ).scalar_one()
        no_context_rows = _query_ids(
            conn,
            "SELECT organization_id FROM lotes ORDER BY id",
        )

    with runtime_engine.begin() as conn:
        _set_tenant_context(conn, rls_fixture["org_b_id"])
        tenant_b_rows = _query_ids(
            conn,
            "SELECT organization_id FROM lotes ORDER BY id",
        )

    assert set(tenant_a_rows) == {rls_fixture["org_a_id"]}
    assert current_setting_value in (None, "")
    assert no_context_rows == []
    assert set(tenant_b_rows) == {rls_fixture["org_b_id"]}
