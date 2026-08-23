from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from litoral_trace.config.settings import normalize_database_url
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.services.audit import AuditActor
from litoral_trace.services.eudr_dds_candidate import (
    EUDR_SPEC_FINGERPRINT_SHA256,
    EUDR_SPEC_PROFILE,
    EudrDdsCandidateNotFoundError,
    EudrDdsCandidateService,
)
from litoral_trace.services.traceability_operations import (
    ProcessInputDraft,
    ProcessOutputDraft,
    ShipmentItemDraft,
    TraceabilityOperationService,
)


ROOT_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT_DIR / ".env.integration"
EXPECTED_REVISION = "025_add_eudr_dds_candidates"


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
    "1",
    "true",
    "yes",
    "on",
}
RUNTIME_URL = ENV.get("TEST_POSTGRES_DATABASE_URL")
OWNER_URL = ENV.get("TEST_POSTGRES_MIGRATION_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not (ENABLED and RUNTIME_URL and OWNER_URL),
    reason="P1-D PostgreSQL tests require the isolated integration contract.",
)


def _engine(url: str):
    return create_engine(
        normalize_database_url(url),
        pool_pre_ping=True,
        hide_parameters=True,
    )


def _actor(org_id: int) -> AuditActor:
    return AuditActor(
        organization_id=org_id,
        user_id=None,
        username="p1d.eudr@acceptance.test",
        role="admin",
    )


def _runtime_candidate(RuntimeSession, org_id: int, callback):
    session = RuntimeSession()
    try:
        set_tenant_db_context(session, org_id)
        service = EudrDdsCandidateService(
            session=session,
            organization_id=org_id,
        )
        return callback(service)
    finally:
        session.close()


@pytest.fixture()
def p1d_pg():
    owner_engine = _engine(OWNER_URL)
    runtime_engine = _engine(RUNTIME_URL)
    RuntimeSession = sessionmaker(bind=runtime_engine, autoflush=False)
    suffix = uuid4().hex[:10]
    org_ids: list[int] = []

    with owner_engine.begin() as connection:
        revision = connection.execute(
            text("SELECT version_num FROM alembic_version")
        ).scalar_one()
        if revision != EXPECTED_REVISION:
            raise RuntimeError(
                f"P1-D requires {EXPECTED_REVISION}; found {revision!r}."
            )

        for label in ("A", "B"):
            org_id = int(
                connection.execute(
                    text(
                        """
                        INSERT INTO public.organizations
                            (name, slug, tax_id, tier, description, is_active)
                        VALUES
                            (:name, :slug, :tax_id, 'pro', 'P1-D EUDR candidate', true)
                        RETURNING id
                        """
                    ),
                    {
                        "name": f"P1D EUDR Org {label} {suffix}",
                        "slug": f"p1d-eudr-{label.lower()}-{suffix}",
                        "tax_id": f"P1D-EUDR-{label}-{suffix}",
                    },
                ).scalar_one()
            )
            org_ids.append(org_id)

        connection.execute(
            text(
                """
                INSERT INTO public.lotes (
                    organization_id, identificador, productor_id,
                    producto_forestal, hectareas, latitud, longitud,
                    polygon_wkt, geom, estatus,
                    volumen_ingresado_ton, volumen_exportar_ton
                ) VALUES (
                    :organization_id, :identificador, :productor_id,
                    'Pino taeda', 12.0, -28.05, -56.03,
                    :polygon_wkt,
                    ST_SetSRID(ST_GeomFromText(:polygon_wkt), 4326),
                    'Pendiente', 0.0, 0.0
                )
                """
            ),
            {
                "organization_id": org_ids[0],
                "identificador": f"RODAL-P1D-{suffix}",
                "productor_id": f"PROV-P1D-{suffix}",
                "polygon_wkt": (
                    "POLYGON((-56.04 -28.06,-56.02 -28.06,-56.02 -28.04,"
                    "-56.04 -28.04,-56.04 -28.06))"
                ),
            },
        )

    operation_service = TraceabilityOperationService(session_factory=RuntimeSession)
    actor = _actor(org_ids[0])
    t0 = datetime.now(timezone.utc) - timedelta(hours=4)

    receipt = operation_service.create_receipt_draft(
        organization_id=org_ids[0],
        actor=actor,
        source_identifier=f"RODAL-P1D-{suffix}",
        event_code=f"REC-P1D-{suffix}",
        batch_code=f"MP-P1D-{suffix}",
        product_name="Madera rolliza Pino taeda",
        quantity="100",
        unit="M3",
        occurred_at=t0,
        facility_reference="Planta P1-D Corrientes",
    )
    operation_service.post_event(
        organization_id=org_ids[0],
        event_public_id=receipt.event_public_id,
        actor=actor,
    )
    process = operation_service.create_process_draft(
        organization_id=org_ids[0],
        actor=actor,
        event_code=f"PROC-P1D-{suffix}",
        event_type="TRANSFORMATION",
        occurred_at=t0 + timedelta(hours=1),
        inputs=(
            ProcessInputDraft(
                batch_public_id=receipt.output_batch_public_ids[0],
                quantity=Decimal("70"),
            ),
        ),
        outputs=(
            ProcessOutputDraft(
                code=f"ASERRADO-P1D-{suffix}",
                product_name="Madera aserrada Pino taeda",
                stage="FINISHED_GOOD",
                unit="M3",
                quantity=Decimal("65"),
            ),
        ),
        facility_reference="Planta P1-D Corrientes",
    )
    operation_service.post_event(
        organization_id=org_ids[0],
        event_public_id=process.event_public_id,
        actor=actor,
    )
    shipment = operation_service.create_shipment_draft(
        organization_id=org_ids[0],
        actor=actor,
        shipment_code=f"EXP-P1D-{suffix}",
        sale_reference=f"FAC-P1D-{suffix}",
        buyer_reference="Importador UE P1-D",
        destination_country="DE",
        items=(
            ShipmentItemDraft(
                batch_public_id=process.output_batch_public_ids[0],
                quantity=Decimal("60"),
            ),
        ),
    )
    operation_service.dispatch_shipment(
        organization_id=org_ids[0],
        shipment_public_id=shipment.shipment_public_id,
        actor=actor,
    )

    try:
        yield RuntimeSession, owner_engine, org_ids, shipment.shipment_code, suffix
    finally:
        with owner_engine.begin() as connection:
            params = {"org_a": org_ids[0], "org_b": org_ids[1]}
            for table_name in (
                "eudr_dds_candidates",
                "audit_logs",
                "traceability_evidence_links",
                "shipment_items",
                "shipments",
                "traceability_event_inputs",
                "traceability_event_outputs",
                "traceability_events",
                "traceability_batches",
            ):
                connection.execute(
                    text(
                        f"DELETE FROM public.{table_name} "
                        "WHERE organization_id IN (:org_a, :org_b)"
                    ),
                    params,
                )
            connection.execute(
                text(
                    "DELETE FROM public.lotes WHERE organization_id IN (:org_a, :org_b)"
                ),
                params,
            )
            connection.execute(
                text(
                    "DELETE FROM public.organizations WHERE id IN (:org_a, :org_b)"
                ),
                params,
            )
        runtime_engine.dispose()
        owner_engine.dispose()


def test_p1d_candidate_conformance_determinism_rls_and_least_privilege(p1d_pg) -> None:
    RuntimeSession, owner_engine, org_ids, shipment_code, suffix = p1d_pg
    org_a, org_b = org_ids

    initial = _runtime_candidate(
        RuntimeSession,
        org_a,
        lambda service: service.conformance(shipment_code),
    )
    assert initial.state == "DRAFT"
    assert "DDS_CANDIDATE" in initial.missing
    assert initial.lineage_complete is True
    assert len(initial.plots) == 1
    assert initial.plots[0]["geojson"]["type"] == "Polygon"

    _runtime_candidate(
        RuntimeSession,
        org_a,
        lambda service: service.upsert_candidate(
            shipment_code=shipment_code,
            activity_type="IMPORT",
            commodity_profile="WOOD",
            production_country_code="AR",
            risk_conclusion="UNASSESSED",
        ),
    )
    blocked = _runtime_candidate(
        RuntimeSession,
        org_a,
        lambda service: service.conformance(shipment_code),
    )
    assert blocked.state == "BLOCKED"
    assert {
        "OPERATOR_NAME",
        "OPERATOR_ADDRESS",
        "OPERATOR_COUNTRY",
        "OPERATOR_EORI",
        "HS_CODE",
        "TRADE_NAME",
        "PRODUCT_DESCRIPTION",
        "NET_MASS_KG",
        "PRODUCTION_DATES",
        "WOOD_COMMON_SPECIES",
        "WOOD_SCIENTIFIC_SPECIES",
        "RISK_CONCLUSION",
        "RISK_ASSESSMENT_REFERENCE",
        "RISK_ASSESSED_AT",
    }.issubset(set(blocked.missing))

    _runtime_candidate(
        RuntimeSession,
        org_a,
        lambda service: service.upsert_candidate(
            shipment_code=shipment_code,
            activity_type="IMPORT",
            commodity_profile="WOOD",
            operator_name="EU Importer GmbH",
            operator_address="Musterstrasse 1, Hamburg, Germany",
            operator_country_code="DE",
            operator_eori="DE123456789012345",
            hs_code="4407",
            trade_name="Sawn pine timber",
            product_description="Madera aserrada de pino para uso industrial",
            common_species_name="Pino taeda",
            scientific_species_name="Pinus taeda",
            net_mass_kg="32500.000",
            production_country_code="AR",
            production_date_from=date(2026, 7, 1),
            production_date_to=date(2026, 7, 31),
            risk_conclusion="NO_OR_NEGLIGIBLE_RISK",
            risk_assessment_reference=f"RISK-P1D-{suffix}",
            risk_assessed_at=datetime(2026, 8, 22, 18, 0, tzinfo=timezone.utc),
        ),
    )
    ready_a = _runtime_candidate(
        RuntimeSession,
        org_a,
        lambda service: service.conformance(shipment_code),
    )
    ready_b = _runtime_candidate(
        RuntimeSession,
        org_a,
        lambda service: service.conformance(shipment_code),
    )
    assert ready_a.state == "CONFORMANCE_READY"
    assert ready_a.ready is True
    assert ready_a.missing == ()
    assert ready_a.payload_sha256 == ready_b.payload_sha256
    assert len(ready_a.payload_sha256 or "") == 64
    assert ready_a.candidate is not None
    assert ready_a.candidate.spec_profile == EUDR_SPEC_PROFILE
    assert ready_a.candidate.spec_fingerprint_sha256 == EUDR_SPEC_FINGERPRINT_SHA256
    assert ready_a.payload["legal_effect"] == "NONE_LOCAL_CANDIDATE"
    assert ready_a.payload["target"]["environment"] == "ACCEPTANCE"
    assert ready_a.payload["due_diligence"]["automatic_compliance_claim"] is False
    assert ready_a.payload["production"]["plots"][0]["geojson"]["type"] == "Polygon"

    with pytest.raises(EudrDdsCandidateNotFoundError):
        _runtime_candidate(
            RuntimeSession,
            org_b,
            lambda service: service.conformance(shipment_code),
        )

    with owner_engine.connect() as connection:
        rls = connection.execute(
            text(
                """
                SELECT relrowsecurity, relforcerowsecurity
                FROM pg_class
                WHERE relname = 'eudr_dds_candidates'
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
                  AND tablename = 'eudr_dds_candidates'
                ORDER BY cmd
                """
            )
        ).scalars().all()
        assert set(policies) == {"SELECT", "INSERT", "UPDATE"}

        for privilege in ("SELECT", "INSERT", "UPDATE"):
            assert connection.execute(
                text(
                    "SELECT has_table_privilege("
                    "'litoral_trace_app', 'public.eudr_dds_candidates', :privilege)"
                ),
                {"privilege": privilege},
            ).scalar_one() is True
        assert connection.execute(
            text(
                "SELECT has_table_privilege("
                "'litoral_trace_app', 'public.eudr_dds_candidates', 'DELETE')"
            )
        ).scalar_one() is False

        for privilege in ("SELECT", "INSERT", "UPDATE", "DELETE"):
            assert connection.execute(
                text(
                    "SELECT has_table_privilege("
                    "'litoral_trace_worker_executor', 'public.eudr_dds_candidates', :privilege)"
                ),
                {"privilege": privilege},
            ).scalar_one() is False
