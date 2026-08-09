from __future__ import annotations

import os
from contextlib import contextmanager
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError, IntegrityError

from litoral_trace.config.settings import normalize_database_url


def _truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}


POSTGRES_RLS_TESTS_ENABLED = _truthy(os.environ.get("ENABLE_POSTGRES_TESTS"))
RUNTIME_TEST_DATABASE_URL = os.environ.get("TEST_POSTGRES_DATABASE_URL")
MIGRATION_TEST_DATABASE_URL = os.environ.get("TEST_POSTGRES_MIGRATION_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not (
        POSTGRES_RLS_TESTS_ENABLED
        and RUNTIME_TEST_DATABASE_URL
        and MIGRATION_TEST_DATABASE_URL
    ),
    reason=(
        "PostgreSQL P2.2A tests require ENABLE_POSTGRES_TESTS=1 plus isolated "
        "TEST_POSTGRES_DATABASE_URL and TEST_POSTGRES_MIGRATION_DATABASE_URL."
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


def _set_tenant_context(conn, organization_id: int) -> None:
    conn.execute(
        text(
            "SELECT set_config('app.current_organization_id', :organization_id, true)"
        ),
        {"organization_id": str(organization_id)},
    )


@contextmanager
def _fixture_entities():
    runtime_engine = _runtime_engine()
    owner_engine = _owner_engine()
    suffix = uuid4().hex[:8]

    with owner_engine.begin() as conn:
        org_a_id = conn.execute(
            text(
                """
                INSERT INTO organizations (name, slug, tax_id, tier, is_active)
                VALUES (:name, :slug, :tax_id, 'pro', true)
                RETURNING id
                """
            ),
            {
                "name": f"P22A Org A {suffix}",
                "slug": f"p22a-org-a-{suffix}",
                "tax_id": f"60-{suffix}",
            },
        ).scalar_one()
        org_b_id = conn.execute(
            text(
                """
                INSERT INTO organizations (name, slug, tax_id, tier, is_active)
                VALUES (:name, :slug, :tax_id, 'pro', true)
                RETURNING id
                """
            ),
            {
                "name": f"P22A Org B {suffix}",
                "slug": f"p22a-org-b-{suffix}",
                "tax_id": f"61-{suffix}",
            },
        ).scalar_one()
        lote_a_id = conn.execute(
            text(
                """
                INSERT INTO lotes (
                    organization_id, identificador, productor_id, producto_forestal,
                    hectareas, latitud, longitud, polygon_wkt, estatus,
                    volumen_ingresado_ton, volumen_exportar_ton
                )
                VALUES (
                    :organization_id, :identificador, :productor_id, 'Madera Aserrada (Pino)',
                    10.0, -27.45, -58.90, :polygon_wkt, 'Pendiente', 10.0, 5.0
                )
                RETURNING id
                """
            ),
            {
                "organization_id": org_a_id,
                "identificador": f"P22A-LOTE-A-{suffix}",
                "productor_id": f"20-A-{suffix}",
                "polygon_wkt": "POLYGON((-58.91 -27.46, -58.89 -27.46, -58.89 -27.44, -58.91 -27.44, -58.91 -27.46))",
            },
        ).scalar_one()
        lote_b_id = conn.execute(
            text(
                """
                INSERT INTO lotes (
                    organization_id, identificador, productor_id, producto_forestal,
                    hectareas, latitud, longitud, polygon_wkt, estatus,
                    volumen_ingresado_ton, volumen_exportar_ton
                )
                VALUES (
                    :organization_id, :identificador, :productor_id, 'Madera Aserrada (Pino)',
                    11.0, -27.55, -58.80, :polygon_wkt, 'Pendiente', 12.0, 4.0
                )
                RETURNING id
                """
            ),
            {
                "organization_id": org_b_id,
                "identificador": f"P22A-LOTE-B-{suffix}",
                "productor_id": f"20-B-{suffix}",
                "polygon_wkt": "POLYGON((-58.81 -27.56, -58.79 -27.56, -58.79 -27.54, -58.81 -27.54, -58.81 -27.56))",
            },
        ).scalar_one()

    fixture = {
        "runtime_engine": runtime_engine,
        "owner_engine": owner_engine,
        "org_a_id": org_a_id,
        "org_b_id": org_b_id,
        "lote_a_id": lote_a_id,
        "lote_b_id": lote_b_id,
    }

    try:
        yield fixture
    finally:
        with owner_engine.begin() as conn:
            conn.execute(
                text(
                    "DELETE FROM satellite_ndvi_observations "
                    "WHERE organization_id IN (:org_a_id, :org_b_id)"
                ),
                {"org_a_id": org_a_id, "org_b_id": org_b_id},
            )
            conn.execute(
                text(
                    "DELETE FROM satellite_jobs "
                    "WHERE organization_id IN (:org_a_id, :org_b_id)"
                ),
                {"org_a_id": org_a_id, "org_b_id": org_b_id},
            )
            conn.execute(
                text("DELETE FROM lotes WHERE id IN (:lote_a_id, :lote_b_id)"),
                {"lote_a_id": lote_a_id, "lote_b_id": lote_b_id},
            )
            conn.execute(
                text(
                    "DELETE FROM organizations WHERE id IN (:org_a_id, :org_b_id)"
                ),
                {"org_a_id": org_a_id, "org_b_id": org_b_id},
            )
        runtime_engine.dispose()
        owner_engine.dispose()


def test_satellite_jobs_table_has_force_rls_enabled():
    with _fixture_entities() as fixture:
        with fixture["owner_engine"].connect() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT relrowsecurity, relforcerowsecurity
                    FROM pg_class
                    WHERE oid = 'public.satellite_jobs'::regclass
                    """
                )
            ).mappings().one()

    assert row["relrowsecurity"] is True
    assert row["relforcerowsecurity"] is True


def test_satellite_jobs_are_scoped_by_tenant_context():
    with _fixture_entities() as fixture:
        with fixture["owner_engine"].begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO satellite_jobs (
                        organization_id, lote_id, job_type, status, request_start_date,
                        request_end_date, max_cloud_pct, geometry_hash,
                        algorithm_version, polygon_wkt_snapshot
                    )
                    VALUES
                    (:org_a_id, :lote_a_id, 'ndvi_timeseries', 'queued', '2020-12-31',
                     '2026-08-09', 20.0, :hash_a, 'algo-v1', :polygon_a),
                    (:org_b_id, :lote_b_id, 'ndvi_timeseries', 'queued', '2020-12-31',
                     '2026-08-09', 20.0, :hash_b, 'algo-v1', :polygon_b)
                    """
                ),
                {
                    "org_a_id": fixture["org_a_id"],
                    "lote_a_id": fixture["lote_a_id"],
                    "hash_a": "a" * 64,
                    "polygon_a": "POLYGON((-58.91 -27.46, -58.89 -27.46, -58.89 -27.44, -58.91 -27.44, -58.91 -27.46))",
                    "org_b_id": fixture["org_b_id"],
                    "lote_b_id": fixture["lote_b_id"],
                    "hash_b": "b" * 64,
                    "polygon_b": "POLYGON((-58.81 -27.56, -58.79 -27.56, -58.79 -27.54, -58.81 -27.54, -58.81 -27.56))",
                },
            )

        with fixture["runtime_engine"].begin() as conn:
            no_context_rows = conn.execute(
                text("SELECT organization_id FROM satellite_jobs ORDER BY id")
            ).scalars().all()
            _set_tenant_context(conn, fixture["org_a_id"])
            tenant_a_rows = conn.execute(
                text("SELECT organization_id FROM satellite_jobs ORDER BY id")
            ).scalars().all()

        with fixture["runtime_engine"].begin() as conn:
            _set_tenant_context(conn, fixture["org_b_id"])
            tenant_b_rows = conn.execute(
                text("SELECT organization_id FROM satellite_jobs ORDER BY id")
            ).scalars().all()

    assert no_context_rows == []
    assert tenant_a_rows == [fixture["org_a_id"]]
    assert tenant_b_rows == [fixture["org_b_id"]]


def test_satellite_jobs_reject_cross_tenant_lote_association():
    with _fixture_entities() as fixture:
        with fixture["runtime_engine"].begin() as conn:
            _set_tenant_context(conn, fixture["org_a_id"])
            with pytest.raises((DBAPIError, IntegrityError)):
                conn.execute(
                    text(
                        """
                        INSERT INTO satellite_jobs (
                            organization_id, lote_id, job_type, status, request_start_date,
                            request_end_date, max_cloud_pct, geometry_hash,
                            algorithm_version, polygon_wkt_snapshot
                        )
                        VALUES (
                            :organization_id, :lote_id, 'ndvi_timeseries', 'queued',
                            '2020-12-31', '2026-08-09', 20.0, :geometry_hash,
                            'algo-v1', :polygon_wkt_snapshot
                        )
                        """
                    ),
                    {
                        "organization_id": fixture["org_a_id"],
                        "lote_id": fixture["lote_b_id"],
                        "geometry_hash": "c" * 64,
                        "polygon_wkt_snapshot": "POLYGON((-58.91 -27.46, -58.89 -27.46, -58.89 -27.44, -58.91 -27.44, -58.91 -27.46))",
                    },
                )


def test_satellite_observation_job_link_is_tenant_safe():
    with _fixture_entities() as fixture:
        with fixture["owner_engine"].begin() as conn:
            job_id = conn.execute(
                text(
                    """
                    INSERT INTO satellite_jobs (
                        organization_id, lote_id, job_type, status, request_start_date,
                        request_end_date, max_cloud_pct, geometry_hash,
                        algorithm_version, polygon_wkt_snapshot
                    )
                    VALUES (
                        :organization_id, :lote_id, 'ndvi_timeseries', 'succeeded',
                        '2020-12-31', '2026-08-09', 20.0, :geometry_hash,
                        'algo-v1', :polygon_wkt_snapshot
                    )
                    RETURNING id
                    """
                ),
                {
                    "organization_id": fixture["org_a_id"],
                    "lote_id": fixture["lote_a_id"],
                    "geometry_hash": "d" * 64,
                    "polygon_wkt_snapshot": "POLYGON((-58.91 -27.46, -58.89 -27.46, -58.89 -27.44, -58.91 -27.44, -58.91 -27.46))",
                },
            ).scalar_one()

            conn.execute(
                text(
                    """
                    INSERT INTO satellite_ndvi_observations (
                        organization_id, lote_id, satellite_job_id, observation_date,
                        ndvi_mean, cloud_percentage, valid_pixel_count,
                        valid_pixel_percentage, satellite, collection,
                        geometry_hash, algorithm_version
                    )
                    VALUES (
                        :organization_id, :lote_id, :satellite_job_id, '2026-08-09',
                        0.61, 5.0, 10, 98.0, 'Sentinel-2',
                        'COPERNICUS/S2_SR_HARMONIZED', :geometry_hash, 'algo-v1'
                    )
                    """
                ),
                {
                    "organization_id": fixture["org_a_id"],
                    "lote_id": fixture["lote_a_id"],
                    "satellite_job_id": job_id,
                    "geometry_hash": "d" * 64,
                },
            )

        with fixture["owner_engine"].begin() as conn:
            with pytest.raises((DBAPIError, IntegrityError)):
                conn.execute(
                    text(
                        """
                        INSERT INTO satellite_ndvi_observations (
                            organization_id, lote_id, satellite_job_id, observation_date,
                            ndvi_mean, cloud_percentage, valid_pixel_count,
                            valid_pixel_percentage, satellite, collection,
                            geometry_hash, algorithm_version
                        )
                        VALUES (
                            :organization_id, :lote_id, :satellite_job_id, '2026-08-09',
                            0.55, 4.0, 10, 98.0, 'Sentinel-2',
                            'COPERNICUS/S2_SR_HARMONIZED', :geometry_hash, 'algo-v1'
                        )
                        """
                    ),
                    {
                        "organization_id": fixture["org_b_id"],
                        "lote_id": fixture["lote_b_id"],
                        "satellite_job_id": job_id,
                        "geometry_hash": "e" * 64,
                    },
                )
