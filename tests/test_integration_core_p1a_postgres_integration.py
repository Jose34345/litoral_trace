from __future__ import annotations

from pathlib import Path
from uuid import UUID, uuid4

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import sessionmaker

from litoral_trace.config.settings import normalize_database_url
from litoral_trace.db.models import ExternalEntity, ExternalEntityVersion
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.services.integrations.canonical import GenericErpPayload
from litoral_trace.services.integrations.core import IntegrationCoreService

ROOT_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT_DIR / ".env.integration"
EXPECTED_REVISION = "022_add_integration_history"
INTEGRATION_TABLES = (
    "integration_connections",
    "integration_sync_runs",
    "external_entities",
    "external_entity_versions",
    "external_references",
    "integration_documents",
    "integration_events",
)


def _read_env() -> dict[str, str]:
    values: dict[str, str] = {}
    if not ENV_PATH.exists():
        return values
    for raw_line in ENV_PATH.read_text(encoding="utf-8-sig").splitlines():
        line = raw_line.strip()
        if line and not line.startswith("#") and "=" in line:
            name, value = line.split("=", 1)
            values[name.strip()] = value.strip()
    return values


ENV = _read_env()
ENABLED = (ENV.get("ENABLE_POSTGRES_TESTS") or "").lower() in {"1", "true", "yes", "on"}
RUNTIME_URL = ENV.get("TEST_POSTGRES_DATABASE_URL")
OWNER_URL = ENV.get("TEST_POSTGRES_MIGRATION_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not (ENABLED and RUNTIME_URL and OWNER_URL),
    reason="P1-A Integration Core PostgreSQL tests require the isolated integration contract.",
)


def _engine(url: str):
    return create_engine(normalize_database_url(url), pool_pre_ping=True, hide_parameters=True)


def _supplier_payload(*, name: str, external_id: str) -> GenericErpPayload:
    return GenericErpPayload.model_validate(
        {
            "source_system": "ERP-P1A-PG",
            "suppliers": [
                {
                    "external_id": external_id,
                    "name": name,
                    "tax_id": "30-00000000-0",
                    "country": "AR",
                    "metadata": {"source": "postgres-acceptance"},
                }
            ],
            "products": [],
            "receipts": [],
            "shipments": [],
        }
    )


@pytest.fixture()
def pg_integration_core():
    owner_engine = _engine(OWNER_URL)
    runtime_engine = _engine(RUNTIME_URL)
    # Intentionally keep SQLAlchemy's default expire_on_commit=True. This is the
    # production behavior that previously exposed transaction-local RLS/GUC
    # refresh failures after a successful commit.
    RuntimeSession = sessionmaker(bind=runtime_engine, autoflush=False)
    suffix = uuid4().hex[:10]
    org_ids: list[int] = []
    lote_ids: list[int] = []

    with owner_engine.begin() as connection:
        revision = connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
        if revision != EXPECTED_REVISION:
            raise RuntimeError(
                f"P1-A Integration Core requires {EXPECTED_REVISION}; found {revision!r}."
            )

        for label in ("A", "B"):
            org_id = int(
                connection.execute(
                    text(
                        """
                        INSERT INTO public.organizations
                            (name, slug, tax_id, tier, description, is_active)
                        VALUES
                            (:name, :slug, :tax_id, 'pro', 'P1-A ERP integration', true)
                        RETURNING id
                        """
                    ),
                    {
                        "name": f"P1A ERP Org {label} {suffix}",
                        "slug": f"p1a-erp-{label.lower()}-{suffix}",
                        "tax_id": f"P1A-ERP-{label}-{suffix}",
                    },
                ).scalar_one()
            )
            org_ids.append(org_id)
            lote_id = int(
                connection.execute(
                    text(
                        """
                        INSERT INTO public.lotes (
                            organization_id, identificador, productor_id,
                            producto_forestal, hectareas, latitud, longitud,
                            estatus, volumen_ingresado_ton, volumen_exportar_ton
                        ) VALUES (
                            :organization_id, :identificador, :productor_id,
                            'Madera Aserrada (Pino)', 10.0, -27.45, -59.05,
                            'Pendiente', 100.0, 0.0
                        ) RETURNING id
                        """
                    ),
                    {
                        "organization_id": org_id,
                        "identificador": f"P1A-ERP-{label}-{suffix}",
                        "productor_id": f"ERP-PROV-{label}-{suffix}",
                    },
                ).scalar_one()
            )
            lote_ids.append(lote_id)

    try:
        yield RuntimeSession, owner_engine, org_ids, lote_ids, suffix
    finally:
        with owner_engine.begin() as connection:
            params = {"org_a": org_ids[0], "org_b": org_ids[1]}
            for table_name in (
                "integration_events",
                "integration_documents",
                "external_references",
                "external_entity_versions",
                "external_entities",
                "integration_sync_runs",
                "integration_connections",
            ):
                connection.execute(
                    text(
                        f"DELETE FROM public.{table_name} "
                        "WHERE organization_id IN (:org_a, :org_b)"
                    ),
                    params,
                )
            connection.execute(
                text("DELETE FROM public.lotes WHERE organization_id IN (:org_a, :org_b)"),
                params,
            )
            connection.execute(
                text("DELETE FROM public.organizations WHERE id IN (:org_a, :org_b)"),
                params,
            )
        runtime_engine.dispose()
        owner_engine.dispose()


def _service_call(RuntimeSession, organization_id: int, callback):
    session = RuntimeSession()
    try:
        service = IntegrationCoreService(session=session, organization_id=organization_id)
        return callback(service)
    finally:
        session.close()


def test_p1a_postgres_runtime_handles_commit_idempotency_history_and_conflict(
    pg_integration_core,
) -> None:
    RuntimeSession, owner_engine, org_ids, lote_ids, suffix = pg_integration_core
    org_a, org_b = org_ids
    lote_identifier = f"P1A-ERP-A-{suffix}"
    external_id = f"SUP-{suffix}"

    connection = _service_call(
        RuntimeSession,
        org_a,
        lambda service: service.create_connection(
            name=f"ERP Virasoro {suffix}",
            connector_type="GENERIC_ERP",
            secret_ref="render:erp_p1a_pg",
            config_json={"mode": "staging_only", "endpoint": {"path": "/exports"}},
        ),
    )
    assert isinstance(connection.public_id, UUID)
    assert connection.status == "ACTIVE"

    payload_a = _supplier_payload(name="Proveedor A", external_id=external_id)
    first = _service_call(
        RuntimeSession,
        org_a,
        lambda service: service.stage_generic_erp(
            connection_public_id=connection.public_id,
            payload=payload_a,
            idempotency_key=f"p1a-pg-initial-{suffix}",
        ),
    )
    assert first.status == "SUCCEEDED"
    assert first.records_created == 1
    assert first.replayed is False

    replay = _service_call(
        RuntimeSession,
        org_a,
        lambda service: service.stage_generic_erp(
            connection_public_id=connection.public_id,
            payload=payload_a,
            idempotency_key=f"p1a-pg-initial-{suffix}",
        ),
    )
    assert replay.replayed is True
    assert replay.public_id == first.public_id

    def _find_external(service: IntegrationCoreService):
        snapshot = service.snapshot()
        assert len(snapshot.entities) == 1
        return snapshot.entities[0].public_id

    external_public_id = _service_call(RuntimeSession, org_a, _find_external)

    reconciliation = _service_call(
        RuntimeSession,
        org_a,
        lambda service: service.reconcile_entity(
            entity_public_id=external_public_id,
            target_type="LOTE",
            target_reference=lote_identifier,
            user_id=None,
        ),
    )
    assert isinstance(reconciliation.public_id, UUID)
    assert reconciliation.target_reference == lote_identifier

    payload_b = _supplier_payload(name="Proveedor B", external_id=external_id)
    changed = _service_call(
        RuntimeSession,
        org_a,
        lambda service: service.stage_generic_erp(
            connection_public_id=connection.public_id,
            payload=payload_b,
            idempotency_key=f"p1a-pg-change-b-{suffix}",
        ),
    )
    assert changed.status == "PARTIAL"
    assert changed.records_conflict == 1

    # Reappearance of historical payload A must reuse its immutable version,
    # not violate (external_entity_id, payload_hash), and must remain CONFLICT
    # because the reconciliation link is still stale.
    reverted = _service_call(
        RuntimeSession,
        org_a,
        lambda service: service.stage_generic_erp(
            connection_public_id=connection.public_id,
            payload=payload_a,
            idempotency_key=f"p1a-pg-revert-a-{suffix}",
        ),
    )
    assert reverted.status == "PARTIAL"
    assert reverted.records_conflict == 1

    session = RuntimeSession()
    try:
        set_tenant_db_context(session, org_a)
        entity = session.scalar(
            select(ExternalEntity).where(ExternalEntity.public_id == external_public_id)
        )
        assert entity is not None
        assert entity.status == "CONFLICT"
        assert entity.conflict_reason == "SOURCE_CHANGED_AFTER_RECONCILIATION"
        version_count = session.scalar(
            select(text("count(*)")).select_from(ExternalEntityVersion).where(
                ExternalEntityVersion.external_entity_id == entity.id
            )
        )
        assert int(version_count or 0) == 2
    finally:
        session.rollback()
        session.close()

    # A different tenant cannot observe the connection/entity through service
    # reads even though both tenants share the same physical database.
    other_snapshot = _service_call(RuntimeSession, org_b, lambda service: service.snapshot())
    assert other_snapshot.connections == ()
    assert other_snapshot.entities == ()

    with owner_engine.connect() as connection_owner:
        rls_rows = connection_owner.execute(
            text(
                """
                SELECT relname, relrowsecurity, relforcerowsecurity
                FROM pg_class
                WHERE relname = ANY(:tables)
                ORDER BY relname
                """
            ),
            {"tables": list(INTEGRATION_TABLES)},
        ).mappings().all()
        assert len(rls_rows) == len(INTEGRATION_TABLES)
        assert all(row["relrowsecurity"] and row["relforcerowsecurity"] for row in rls_rows)

        for table_name in INTEGRATION_TABLES:
            assert connection_owner.execute(
                text("SELECT has_table_privilege('litoral_trace_app', :table, 'DELETE')"),
                {"table": f"public.{table_name}"},
            ).scalar_one() is False
            for privilege in ("SELECT", "INSERT", "UPDATE", "DELETE"):
                assert connection_owner.execute(
                    text(
                        "SELECT has_table_privilege("
                        "'litoral_trace_worker_executor', :table, :privilege)"
                    ),
                    {
                        "table": f"public.{table_name}",
                        "privilege": privilege,
                    },
                ).scalar_one() is False

        for append_only_table in ("external_entity_versions", "integration_events"):
            assert connection_owner.execute(
                text("SELECT has_table_privilege('litoral_trace_app', :table, 'SELECT')"),
                {"table": f"public.{append_only_table}"},
            ).scalar_one() is True
            assert connection_owner.execute(
                text("SELECT has_table_privilege('litoral_trace_app', :table, 'INSERT')"),
                {"table": f"public.{append_only_table}"},
            ).scalar_one() is True
            assert connection_owner.execute(
                text("SELECT has_table_privilege('litoral_trace_app', :table, 'UPDATE')"),
                {"table": f"public.{append_only_table}"},
            ).scalar_one() is False
