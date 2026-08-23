from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from litoral_trace.config.settings import normalize_database_url
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.services.shipment_export_case import (
    ShipmentExportCaseNotFoundError,
    ShipmentExportCaseService,
)

ROOT_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT_DIR / ".env.integration"
EXPECTED_REVISION = "023_add_shipment_export_cases"


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
ENABLED = (ENV.get("ENABLE_POSTGRES_TESTS") or "").lower() in {
    "1", "true", "yes", "on"
}
RUNTIME_URL = ENV.get("TEST_POSTGRES_DATABASE_URL")
OWNER_URL = ENV.get("TEST_POSTGRES_MIGRATION_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not (ENABLED and RUNTIME_URL and OWNER_URL),
    reason="P1-B PostgreSQL tests require the isolated integration contract.",
)


def _engine(url: str):
    return create_engine(
        normalize_database_url(url), pool_pre_ping=True, hide_parameters=True
    )


def _runtime_call(RuntimeSession, organization_id: int, callback):
    session = RuntimeSession()
    try:
        set_tenant_db_context(session, organization_id)
        service = ShipmentExportCaseService(
            session=session,
            organization_id=organization_id,
        )
        return callback(service)
    finally:
        session.close()


def _insert_evidence(
    owner_engine,
    *,
    organization_id: int,
    shipment_id: int,
    evidence_type: str,
    suffix: str,
) -> None:
    token = uuid4().hex
    with owner_engine.begin() as connection:
        vault_id = int(
            connection.execute(
                text(
                    """
                    INSERT INTO public.vault_documents (
                        organization_id, original_filename, content_type,
                        size_bytes, sha256, object_key, storage_backend,
                        storage_bucket, document_type, status
                    ) VALUES (
                        :organization_id, :filename, 'application/pdf',
                        16, :sha256, :object_key, 's3',
                        'p1b-ci-vault', 'OTHER_EVIDENCE', 'available'
                    ) RETURNING id
                    """
                ),
                {
                    "organization_id": organization_id,
                    "filename": f"{evidence_type.lower()}-{suffix}.pdf",
                    "sha256": (token * 2)[:64],
                    "object_key": f"p1b/{suffix}/{evidence_type.lower()}-{token}.pdf",
                },
            ).scalar_one()
        )
        connection.execute(
            text(
                """
                INSERT INTO public.traceability_evidence_links (
                    organization_id, vault_document_id, shipment_id,
                    evidence_type, reference_number, issuer
                ) VALUES (
                    :organization_id, :vault_document_id, :shipment_id,
                    :evidence_type, :reference_number, :issuer
                )
                """
            ),
            {
                "organization_id": organization_id,
                "vault_document_id": vault_id,
                "shipment_id": shipment_id,
                "evidence_type": evidence_type,
                "reference_number": f"P1B-{evidence_type}-{suffix}",
                "issuer": "P1-B PostgreSQL acceptance",
            },
        )


@pytest.fixture()
def pg_export_case():
    owner_engine = _engine(OWNER_URL)
    runtime_engine = _engine(RUNTIME_URL)
    RuntimeSession = sessionmaker(bind=runtime_engine, autoflush=False)
    suffix = uuid4().hex[:10]
    org_ids: list[int] = []
    shipment_ids: list[int] = []
    shipment_codes: list[str] = []

    with owner_engine.begin() as connection:
        revision = connection.execute(
            text("SELECT version_num FROM alembic_version")
        ).scalar_one()
        if revision != EXPECTED_REVISION:
            raise RuntimeError(
                f"P1-B requires {EXPECTED_REVISION}; found {revision!r}."
            )

        for label in ("A", "B"):
            org_id = int(
                connection.execute(
                    text(
                        """
                        INSERT INTO public.organizations
                            (name, slug, tax_id, tier, description, is_active)
                        VALUES
                            (:name, :slug, :tax_id, 'pro', 'P1-B export case', true)
                        RETURNING id
                        """
                    ),
                    {
                        "name": f"P1B Export Org {label} {suffix}",
                        "slug": f"p1b-export-{label.lower()}-{suffix}",
                        "tax_id": f"P1B-EXPORT-{label}-{suffix}",
                    },
                ).scalar_one()
            )
            shipment_code = f"P1B-{label}-{suffix}"
            shipment_id = int(
                connection.execute(
                    text(
                        """
                        INSERT INTO public.shipments (
                            organization_id, shipment_code, sale_reference,
                            buyer_reference, destination_country, status
                        ) VALUES (
                            :organization_id, :shipment_code, :sale_reference,
                            'Comprador UE P1-B', 'DE', 'DISPATCHED'
                        ) RETURNING id
                        """
                    ),
                    {
                        "organization_id": org_id,
                        "shipment_code": shipment_code,
                        "sale_reference": f"SALE-{label}-{suffix}",
                    },
                ).scalar_one()
            )
            org_ids.append(org_id)
            shipment_ids.append(shipment_id)
            shipment_codes.append(shipment_code)

    try:
        yield (
            RuntimeSession,
            owner_engine,
            org_ids,
            shipment_ids,
            shipment_codes,
            suffix,
        )
    finally:
        with owner_engine.begin() as connection:
            params = {"org_a": org_ids[0], "org_b": org_ids[1]}
            connection.execute(
                text(
                    "DELETE FROM public.traceability_evidence_links "
                    "WHERE organization_id IN (:org_a, :org_b)"
                ),
                params,
            )
            connection.execute(
                text(
                    "DELETE FROM public.vault_documents "
                    "WHERE organization_id IN (:org_a, :org_b)"
                ),
                params,
            )
            connection.execute(
                text(
                    "DELETE FROM public.shipment_export_cases "
                    "WHERE organization_id IN (:org_a, :org_b)"
                ),
                params,
            )
            connection.execute(
                text(
                    "DELETE FROM public.shipments "
                    "WHERE organization_id IN (:org_a, :org_b)"
                ),
                params,
            )
            connection.execute(
                text(
                    "DELETE FROM public.organizations "
                    "WHERE id IN (:org_a, :org_b)"
                ),
                params,
            )
        runtime_engine.dispose()
        owner_engine.dispose()


def test_p1b_corrientes_arca_readiness_rls_and_least_privilege(
    pg_export_case,
) -> None:
    (
        RuntimeSession,
        owner_engine,
        org_ids,
        shipment_ids,
        shipment_codes,
        suffix,
    ) = pg_export_case
    org_a, org_b = org_ids
    shipment_a_id = shipment_ids[0]
    shipment_a = shipment_codes[0]

    # A case with no provincial/Vault evidence and no ARCA/SIM references must
    # fail closed and enumerate concrete missing requirements.
    created = _runtime_call(
        RuntimeSession,
        org_a,
        lambda service: service.upsert_case(
            shipment_code=shipment_a,
            origin_profile="CULTIVATED",
        ),
    )
    assert created.origin_profile == "CULTIVATED"

    cultivated_blocked = _runtime_call(
        RuntimeSession,
        org_a,
        lambda service: service.readiness(shipment_a),
    )
    assert cultivated_blocked.state == "BLOCKED"
    assert set(cultivated_blocked.missing) == {
        "CULTIVATED_INVOICE_OR_REMITO",
        "FRUIT_GUIDE",
        "EXPORT_INVOICE_E",
        "SIM_DESTINATION",
        "SIM_SUBREGIME",
    }

    _insert_evidence(
        owner_engine,
        organization_id=org_a,
        shipment_id=shipment_a_id,
        evidence_type="REMITO",
        suffix=suffix,
    )
    _insert_evidence(
        owner_engine,
        organization_id=org_a,
        shipment_id=shipment_a_id,
        evidence_type="FRUIT_GUIDE",
        suffix=suffix,
    )

    _runtime_call(
        RuntimeSession,
        org_a,
        lambda service: service.upsert_case(
            shipment_code=shipment_a,
            origin_profile="CULTIVATED",
            export_invoice_number="E-0001-00000001",
            export_invoice_cae="76123456789012",
            customs_destination_id=f"SIM-{suffix}",
            customs_subregime="EC01",
        ),
    )
    cultivated_ready = _runtime_call(
        RuntimeSession,
        org_a,
        lambda service: service.readiness(shipment_a),
    )
    assert cultivated_ready.state == "READY"
    assert cultivated_ready.missing == ()
    assert "FRUIT_GUIDE" in cultivated_ready.evidence_types
    assert "REMITO" in cultivated_ready.evidence_types

    # Switching the same shipment to native forest changes the documentary
    # contract immediately and must block until Guía Forestal + Vale exist.
    _runtime_call(
        RuntimeSession,
        org_a,
        lambda service: service.upsert_case(
            shipment_code=shipment_a,
            origin_profile="NATIVE",
            export_invoice_number="E-0001-00000001",
            customs_destination_id=f"SIM-{suffix}",
            customs_subregime="EC01",
        ),
    )
    native_blocked = _runtime_call(
        RuntimeSession,
        org_a,
        lambda service: service.readiness(shipment_a),
    )
    assert native_blocked.state == "BLOCKED"
    assert set(native_blocked.missing) == {
        "FOREST_GUIDE",
        "FOREST_TRANSPORT_VOUCHER",
    }

    _insert_evidence(
        owner_engine,
        organization_id=org_a,
        shipment_id=shipment_a_id,
        evidence_type="FOREST_GUIDE",
        suffix=suffix,
    )
    _insert_evidence(
        owner_engine,
        organization_id=org_a,
        shipment_id=shipment_a_id,
        evidence_type="TRANSPORT",
        suffix=suffix,
    )
    native_ready = _runtime_call(
        RuntimeSession,
        org_a,
        lambda service: service.readiness(shipment_a),
    )
    assert native_ready.state == "READY"
    assert native_ready.missing == ()

    # Tenant B cannot resolve tenant A's shipment even with the exact code.
    with pytest.raises(ShipmentExportCaseNotFoundError):
        _runtime_call(
            RuntimeSession,
            org_b,
            lambda service: service.readiness(shipment_a),
        )

    with owner_engine.connect() as connection:
        rls = connection.execute(
            text(
                """
                SELECT relrowsecurity, relforcerowsecurity
                FROM pg_class
                WHERE relname = 'shipment_export_cases'
                """
            )
        ).mappings().one()
        assert rls["relrowsecurity"] is True
        assert rls["relforcerowsecurity"] is True

        policies = connection.execute(
            text(
                """
                SELECT cmd
                FROM pg_policies
                WHERE schemaname = 'public'
                  AND tablename = 'shipment_export_cases'
                ORDER BY cmd
                """
            )
        ).scalars().all()
        assert set(policies) == {"SELECT", "INSERT", "UPDATE"}

        for privilege in ("SELECT", "INSERT", "UPDATE"):
            assert connection.execute(
                text(
                    "SELECT has_table_privilege("
                    "'litoral_trace_app', 'public.shipment_export_cases', :privilege)"
                ),
                {"privilege": privilege},
            ).scalar_one() is True
        assert connection.execute(
            text(
                "SELECT has_table_privilege("
                "'litoral_trace_app', 'public.shipment_export_cases', 'DELETE')"
            )
        ).scalar_one() is False

        for privilege in ("SELECT", "INSERT", "UPDATE", "DELETE"):
            assert connection.execute(
                text(
                    "SELECT has_table_privilege("
                    "'litoral_trace_worker_executor', "
                    "'public.shipment_export_cases', :privilege)"
                ),
                {"privilege": privilege},
            ).scalar_one() is False
