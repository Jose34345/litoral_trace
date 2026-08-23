from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from litoral_trace.config.settings import normalize_database_url
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.services.shipment_phytosanitary_case import (
    ShipmentPhytosanitaryCaseNotFoundError,
    ShipmentPhytosanitaryCaseService,
)

ROOT_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT_DIR / ".env.integration"
EXPECTED_REVISION = "024_add_shipment_phytosanitary_cases"


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
    reason="P1-C PostgreSQL tests require the isolated integration contract.",
)


def _engine(url: str):
    return create_engine(normalize_database_url(url), pool_pre_ping=True, hide_parameters=True)


def _runtime_call(RuntimeSession, organization_id: int, callback):
    session = RuntimeSession()
    try:
        set_tenant_db_context(session, organization_id)
        service = ShipmentPhytosanitaryCaseService(
            session=session, organization_id=organization_id
        )
        return callback(service)
    finally:
        session.close()


def _insert_evidence(owner_engine, *, organization_id: int, shipment_id: int, evidence_type: str, suffix: str) -> None:
    token = uuid4().hex
    content_type = "application/xml" if evidence_type == "EPHYTO_XML" else "application/pdf"
    extension = "xml" if evidence_type == "EPHYTO_XML" else "pdf"
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
                        :organization_id, :filename, :content_type,
                        32, :sha256, :object_key, 's3',
                        'p1c-ci-vault', 'OTHER_EVIDENCE', 'available'
                    ) RETURNING id
                    """
                ),
                {
                    "organization_id": organization_id,
                    "filename": f"{evidence_type.lower()}-{suffix}.{extension}",
                    "content_type": content_type,
                    "sha256": (token * 2)[:64],
                    "object_key": f"p1c/{suffix}/{evidence_type.lower()}-{token}.{extension}",
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
                    :evidence_type, :reference_number, 'SENASA P1-C acceptance'
                )
                """
            ),
            {
                "organization_id": organization_id,
                "vault_document_id": vault_id,
                "shipment_id": shipment_id,
                "evidence_type": evidence_type,
                "reference_number": f"P1C-{evidence_type}-{suffix}",
            },
        )


@pytest.fixture()
def pg_phytosanitary_case():
    owner_engine = _engine(OWNER_URL)
    runtime_engine = _engine(RUNTIME_URL)
    RuntimeSession = sessionmaker(bind=runtime_engine, autoflush=False)
    suffix = uuid4().hex[:10]
    org_ids: list[int] = []
    shipment_ids: list[int] = []
    shipment_codes: list[str] = []

    with owner_engine.begin() as connection:
        revision = connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
        if revision != EXPECTED_REVISION:
            raise RuntimeError(f"P1-C requires {EXPECTED_REVISION}; found {revision!r}.")
        for label in ("A", "B"):
            org_id = int(
                connection.execute(
                    text(
                        """
                        INSERT INTO public.organizations (name, slug, tax_id, tier, description, is_active)
                        VALUES (:name, :slug, :tax_id, 'pro', 'P1-C phytosanitary case', true)
                        RETURNING id
                        """
                    ),
                    {
                        "name": f"P1C Phyto Org {label} {suffix}",
                        "slug": f"p1c-phyto-{label.lower()}-{suffix}",
                        "tax_id": f"P1C-PHYTO-{label}-{suffix}",
                    },
                ).scalar_one()
            )
            shipment_code = f"P1C-{label}-{suffix}"
            shipment_id = int(
                connection.execute(
                    text(
                        """
                        INSERT INTO public.shipments (
                            organization_id, shipment_code, sale_reference,
                            buyer_reference, destination_country, status
                        ) VALUES (:organization_id, :shipment_code, :sale_reference,
                                  'Comprador export P1-C', 'DE', 'DISPATCHED')
                        RETURNING id
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
        yield RuntimeSession, owner_engine, org_ids, shipment_ids, shipment_codes, suffix
    finally:
        with owner_engine.begin() as connection:
            params = {"org_a": org_ids[0], "org_b": org_ids[1]}
            for table_name in (
                "traceability_evidence_links",
                "vault_documents",
                "shipment_phytosanitary_cases",
                "shipments",
            ):
                connection.execute(
                    text(f"DELETE FROM public.{table_name} WHERE organization_id IN (:org_a, :org_b)"),
                    params,
                )
            connection.execute(
                text("DELETE FROM public.organizations WHERE id IN (:org_a, :org_b)"),
                params,
            )
        runtime_engine.dispose()
        owner_engine.dispose()


def test_p1c_senasa_ephyto_readiness_rls_and_least_privilege(pg_phytosanitary_case) -> None:
    RuntimeSession, owner_engine, org_ids, shipment_ids, shipment_codes, suffix = pg_phytosanitary_case
    org_a, org_b = org_ids
    shipment_id = shipment_ids[0]
    shipment_code = shipment_codes[0]
    checked_at = datetime(2026, 8, 22, 22, 30, tzinfo=timezone.utc)

    initial = _runtime_call(RuntimeSession, org_a, lambda service: service.readiness(shipment_code))
    assert initial.state == "BLOCKED"
    assert initial.missing == ("PHYTOSANITARY_ASSESSMENT",)

    not_required_blocked = _runtime_call(
        RuntimeSession,
        org_a,
        lambda service: (
            service.upsert_case(shipment_code=shipment_code, certification_mode="NOT_REQUIRED"),
            set_tenant_db_context(service._session, org_a),
            service.readiness(shipment_code),
        )[-1],
    )
    assert set(not_required_blocked.missing) == {"REQUIREMENTS_REFERENCE", "REQUIREMENTS_CHECKED_AT"}

    _runtime_call(
        RuntimeSession,
        org_a,
        lambda service: service.upsert_case(
            shipment_code=shipment_code,
            certification_mode="NOT_REQUIRED",
            requirements_reference="ONPF destino / protocolo oficial P1-C",
            requirements_checked_at=checked_at,
        ),
    )
    not_required_ready = _runtime_call(RuntimeSession, org_a, lambda service: service.readiness(shipment_code))
    assert not_required_ready.state == "READY"

    _runtime_call(
        RuntimeSession,
        org_a,
        lambda service: service.upsert_case(
            shipment_code=shipment_code,
            certification_mode="PAPER",
            requirements_reference="Requisito oficial certificado fitosanitario",
            requirements_checked_at=checked_at,
            cert_pov_reference=f"CERTPOV-{suffix}",
            certificate_number=f"CF-{suffix}",
        ),
    )
    paper_blocked = _runtime_call(RuntimeSession, org_a, lambda service: service.readiness(shipment_code))
    assert paper_blocked.missing == ("PHYTOSANITARY_CERTIFICATE_EVIDENCE",)
    _insert_evidence(
        owner_engine,
        organization_id=org_a,
        shipment_id=shipment_id,
        evidence_type="PHYTOSANITARY_CERTIFICATE",
        suffix=suffix,
    )
    paper_ready = _runtime_call(RuntimeSession, org_a, lambda service: service.readiness(shipment_code))
    assert paper_ready.state == "READY"

    _runtime_call(
        RuntimeSession,
        org_a,
        lambda service: service.upsert_case(
            shipment_code=shipment_code,
            certification_mode="EPHYTO",
            requirements_reference="Acuerdo ePhyto / requisito ONPF destino",
            requirements_checked_at=checked_at,
            cert_pov_reference=f"CERTPOV-EPHYTO-{suffix}",
            certificate_number=f"CF-E-{suffix}",
            ephyto_reference=f"EPHYTO-{suffix}",
        ),
    )
    ephyto_blocked = _runtime_call(RuntimeSession, org_a, lambda service: service.readiness(shipment_code))
    assert ephyto_blocked.missing == ("EPHYTO_XML_EVIDENCE",)
    _insert_evidence(
        owner_engine,
        organization_id=org_a,
        shipment_id=shipment_id,
        evidence_type="EPHYTO_XML",
        suffix=suffix,
    )
    ephyto_ready = _runtime_call(RuntimeSession, org_a, lambda service: service.readiness(shipment_code))
    assert ephyto_ready.state == "READY"
    assert "EPHYTO_XML" in ephyto_ready.evidence_types

    with pytest.raises(ShipmentPhytosanitaryCaseNotFoundError):
        _runtime_call(RuntimeSession, org_b, lambda service: service.readiness(shipment_code))

    with owner_engine.connect() as connection:
        rls = connection.execute(
            text("SELECT relrowsecurity, relforcerowsecurity FROM pg_class WHERE relname='shipment_phytosanitary_cases'")
        ).mappings().one()
        assert rls["relrowsecurity"] is True
        assert rls["relforcerowsecurity"] is True
        policies = connection.execute(
            text("SELECT cmd FROM pg_policies WHERE schemaname='public' AND tablename='shipment_phytosanitary_cases'")
        ).scalars().all()
        assert set(policies) == {"SELECT", "INSERT", "UPDATE"}
        for privilege in ("SELECT", "INSERT", "UPDATE"):
            assert connection.execute(
                text("SELECT has_table_privilege('litoral_trace_app','public.shipment_phytosanitary_cases',:privilege)"),
                {"privilege": privilege},
            ).scalar_one() is True
        assert connection.execute(
            text("SELECT has_table_privilege('litoral_trace_app','public.shipment_phytosanitary_cases','DELETE')")
        ).scalar_one() is False
        for privilege in ("SELECT", "INSERT", "UPDATE", "DELETE"):
            assert connection.execute(
                text("SELECT has_table_privilege('litoral_trace_worker_executor','public.shipment_phytosanitary_cases',:privilege)"),
                {"privilege": privilege},
            ).scalar_one() is False
