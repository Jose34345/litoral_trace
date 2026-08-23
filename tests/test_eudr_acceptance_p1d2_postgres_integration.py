from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from litoral_trace.config.eudr_acceptance import (
    ACCEPTANCE_HOST,
    DDS_V3_SERVICE_PATH,
    EudrAcceptanceSettings,
)
from litoral_trace.config.settings import normalize_database_url
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.services.audit import AuditActor
from litoral_trace.services.eudr_acceptance_submission import (
    EudrAcceptanceSubmissionError,
    EudrAcceptanceSubmissionService,
)
from litoral_trace.services.eudr_acceptance_transport import (
    EudrAcceptanceResponse,
    EudrAcceptanceTransportError,
)
from litoral_trace.services.eudr_dds_candidate import EudrDdsCandidateService
from litoral_trace.services.traceability_operations import (
    ProcessInputDraft,
    ProcessOutputDraft,
    ShipmentItemDraft,
    TraceabilityOperationService,
)


ROOT_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT_DIR / ".env.integration"
EXPECTED_REVISION = "026_add_eudr_acceptance_attempts"


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
    reason="P1-D2 PostgreSQL tests require the isolated integration contract.",
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
        username="p1d2.acceptance@test.invalid",
        role="admin",
    )


def _settings() -> EudrAcceptanceSettings:
    return EudrAcceptanceSettings(
        enabled=True,
        endpoint_url=f"https://{ACCEPTANCE_HOST}{DDS_V3_SERVICE_PATH}",
        username="acceptance-test-user",
        authentication_key="ACCEPTANCE-TEST-PRIVATE-KEY",
        web_service_client_id="litoral-trace-ci",
    )


class _SuccessTransport:
    def __init__(self) -> None:
        self.envelopes: list[bytes] = []

    def send(self, *, envelope: bytes, settings: EudrAcceptanceSettings) -> EudrAcceptanceResponse:
        self.envelopes.append(envelope)
        return EudrAcceptanceResponse(
            http_status=200,
            body=b'''<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
              xmlns:dds="http://ec.europa.eu/tracesnt/certificate/eudr/due-diligence-statement/v3">
              <soapenv:Body><dds:SubmitDdsResponse><dds:uuid>CI-REMOTE-UUID</dds:uuid></dds:SubmitDdsResponse></soapenv:Body>
            </soapenv:Envelope>''',
        )


class _FaultTransport:
    def send(self, *, envelope: bytes, settings: EudrAcceptanceSettings) -> EudrAcceptanceResponse:
        return EudrAcceptanceResponse(
            http_status=500,
            body=b'''<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
              <soapenv:Body><soapenv:Fault><faultcode>VALIDATION</faultcode><faultstring>Rejected fixture</faultstring></soapenv:Fault></soapenv:Body>
            </soapenv:Envelope>''',
        )


class _ErrorTransport:
    def send(self, *, envelope: bytes, settings: EudrAcceptanceSettings) -> EudrAcceptanceResponse:
        raise EudrAcceptanceTransportError(
            "ACCEPTANCE_TRANSPORT_ERROR",
            "Fixture transport failure",
        )


@pytest.fixture()
def p1d2_pg():
    owner_engine = _engine(OWNER_URL)
    runtime_engine = _engine(RUNTIME_URL)
    RuntimeSession = sessionmaker(bind=runtime_engine, autoflush=False)
    suffix = uuid4().hex[:10]
    org_ids: list[int] = []

    with owner_engine.begin() as connection:
        revision = connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
        if revision != EXPECTED_REVISION:
            raise RuntimeError(f"P1-D2 requires {EXPECTED_REVISION}; found {revision!r}.")

        for label in ("A", "B"):
            org_id = int(
                connection.execute(
                    text(
                        """
                        INSERT INTO public.organizations
                            (name, slug, tax_id, tier, description, is_active)
                        VALUES
                            (:name, :slug, :tax_id, 'pro', 'P1-D2 ACCEPTANCE', true)
                        RETURNING id
                        """
                    ),
                    {
                        "name": f"P1D2 Acceptance Org {label} {suffix}",
                        "slug": f"p1d2-acceptance-{label.lower()}-{suffix}",
                        "tax_id": f"P1D2-{label}-{suffix}",
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
                "identificador": f"RODAL-P1D2-{suffix}",
                "productor_id": f"PROV-P1D2-{suffix}",
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
        source_identifier=f"RODAL-P1D2-{suffix}",
        event_code=f"REC-P1D2-{suffix}",
        batch_code=f"MP-P1D2-{suffix}",
        product_name="Madera rolliza Pino taeda",
        quantity="100",
        unit="M3",
        occurred_at=t0,
        facility_reference="Planta P1-D2 Corrientes",
    )
    operation_service.post_event(
        organization_id=org_ids[0],
        event_public_id=receipt.event_public_id,
        actor=actor,
    )
    process = operation_service.create_process_draft(
        organization_id=org_ids[0],
        actor=actor,
        event_code=f"PROC-P1D2-{suffix}",
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
                code=f"ASERRADO-P1D2-{suffix}",
                product_name="Madera aserrada Pino taeda",
                stage="FINISHED_GOOD",
                unit="M3",
                quantity=Decimal("65"),
            ),
        ),
        facility_reference="Planta P1-D2 Corrientes",
    )
    operation_service.post_event(
        organization_id=org_ids[0],
        event_public_id=process.event_public_id,
        actor=actor,
    )
    shipment = operation_service.create_shipment_draft(
        organization_id=org_ids[0],
        actor=actor,
        shipment_code=f"EXP-P1D2-{suffix}",
        sale_reference=f"FAC-P1D2-{suffix}",
        buyer_reference="Importador UE P1-D2",
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

    session = RuntimeSession()
    try:
        set_tenant_db_context(session, org_ids[0])
        EudrDdsCandidateService(session=session, organization_id=org_ids[0]).upsert_candidate(
            shipment_code=shipment.shipment_code,
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
            risk_assessment_reference=f"RISK-P1D2-{suffix}",
            risk_assessed_at=datetime(2026, 8, 22, 18, 0, tzinfo=timezone.utc),
        )
    finally:
        session.close()

    try:
        yield RuntimeSession, owner_engine, org_ids, shipment.shipment_code, suffix
    finally:
        with owner_engine.begin() as connection:
            params = {"org_a": org_ids[0], "org_b": org_ids[1]}
            for table_name in (
                "eudr_acceptance_attempts",
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
                text("DELETE FROM public.lotes WHERE organization_id IN (:org_a, :org_b)"),
                params,
            )
            connection.execute(
                text("DELETE FROM public.organizations WHERE id IN (:org_a, :org_b)"),
                params,
            )
        runtime_engine.dispose()
        owner_engine.dispose()


def _service(RuntimeSession, org_id: int, transport) -> tuple:
    session = RuntimeSession()
    set_tenant_db_context(session, org_id)
    service = EudrAcceptanceSubmissionService(
        session=session,
        organization_id=org_id,
        settings=_settings(),
        transport=transport,
    )
    return session, service


def test_p1d2_prepare_submit_idempotency_rls_and_least_privilege(p1d2_pg) -> None:
    RuntimeSession, owner_engine, org_ids, shipment_code, suffix = p1d2_pg
    org_a, org_b = org_ids

    with owner_engine.connect() as connection:
        ledger_before = connection.execute(
            text(
                "SELECT (SELECT count(*) FROM public.traceability_events WHERE organization_id=:org) AS events, "
                "(SELECT count(*) FROM public.traceability_batches WHERE organization_id=:org) AS batches"
            ),
            {"org": org_a},
        ).mappings().one()

    success_transport = _SuccessTransport()
    session, service = _service(RuntimeSession, org_a, success_transport)
    try:
        prepared_a = service.prepare(
            shipment_code=shipment_code,
            operator_role="OPERATOR",
            country_of_activity="DE",
            border_cross_country="DE",
            internal_reference_number=f"LT-{suffix}-A",
        )
        prepared_b = service.prepare(
            shipment_code=shipment_code.lower(),
            operator_role="OPERATOR",
            country_of_activity="DE",
            border_cross_country="DE",
            internal_reference_number=f"LT-{suffix}-A",
        )
        assert prepared_a.created is True
        assert prepared_b.created is False
        assert prepared_a.attempt.public_id == prepared_b.attempt.public_id
        assert prepared_a.attempt.state == "PREPARED"
        assert prepared_a.attempt.environment == "ACCEPTANCE"
        assert prepared_a.attempt.legal_effect == "NONE_NON_LEGAL_ACCEPTANCE"
        assert prepared_a.attempt.ledger_mutated is False

        accepted = service.submit(
            attempt_public_id=prepared_a.attempt.public_id,
            shipment_code=shipment_code.lower(),
        )
        assert accepted.state == "REMOTE_ACCEPTED"
        assert accepted.remote_uuid == "CI-REMOTE-UUID"
        assert accepted.remote_status == "SUBMITTED"
        assert accepted.http_status == 200
        assert accepted.response_sha256 and len(accepted.response_sha256) == 64
        assert accepted.envelope_sha256 and len(accepted.envelope_sha256) == 64
        assert len(success_transport.envelopes) == 1
        assert b"ACCEPTANCE-TEST-PRIVATE-KEY" not in success_transport.envelopes[0]

        # Terminal results are idempotent and never submit twice.
        accepted_again = service.submit(
            attempt_public_id=prepared_a.attempt.public_id,
            shipment_code=shipment_code,
        )
        assert accepted_again.state == "REMOTE_ACCEPTED"
        assert len(success_transport.envelopes) == 1
    finally:
        session.close()

    fault_session, fault_service = _service(RuntimeSession, org_a, _FaultTransport())
    try:
        fault_prepared = fault_service.prepare(
            shipment_code=shipment_code,
            operator_role="OPERATOR",
            country_of_activity="DE",
            border_cross_country="DE",
            internal_reference_number=f"LT-{suffix}-FAULT",
        )
        rejected = fault_service.submit(
            attempt_public_id=fault_prepared.attempt.public_id,
            shipment_code=shipment_code,
        )
        assert rejected.state == "REMOTE_REJECTED"
        assert rejected.error_code == "VALIDATION"
        assert rejected.error_summary == "Rejected fixture"
    finally:
        fault_session.close()

    error_session, error_service = _service(RuntimeSession, org_a, _ErrorTransport())
    try:
        error_prepared = error_service.prepare(
            shipment_code=shipment_code,
            operator_role="OPERATOR",
            country_of_activity="DE",
            border_cross_country="DE",
            internal_reference_number=f"LT-{suffix}-ERROR",
        )
        errored = error_service.submit(
            attempt_public_id=error_prepared.attempt.public_id,
            shipment_code=shipment_code,
        )
        assert errored.state == "TRANSPORT_ERROR"
        with pytest.raises(EudrAcceptanceSubmissionError) as retry:
            error_service.submit(
                attempt_public_id=error_prepared.attempt.public_id,
                shipment_code=shipment_code,
            )
        assert retry.value.code == "ACCEPTANCE_RETRY_REQUIRES_EXPLICIT_OVERRIDE"
    finally:
        error_session.close()

    # Tenant B cannot read tenant A's attempt through FORCE RLS/service lookup.
    other_session, other_service = _service(RuntimeSession, org_b, _SuccessTransport())
    try:
        with pytest.raises(EudrAcceptanceSubmissionError) as hidden:
            other_service.get_attempt(prepared_a.attempt.public_id)
        assert hidden.value.code == "ACCEPTANCE_ATTEMPT_NOT_FOUND"
    finally:
        other_session.close()

    with owner_engine.connect() as connection:
        ledger_after = connection.execute(
            text(
                "SELECT (SELECT count(*) FROM public.traceability_events WHERE organization_id=:org) AS events, "
                "(SELECT count(*) FROM public.traceability_batches WHERE organization_id=:org) AS batches"
            ),
            {"org": org_a},
        ).mappings().one()
        assert dict(ledger_after) == dict(ledger_before)

        rows = connection.execute(
            text(
                "SELECT state, request_body_sha256, envelope_sha256, response_sha256 "
                "FROM public.eudr_acceptance_attempts WHERE organization_id=:org ORDER BY created_at"
            ),
            {"org": org_a},
        ).mappings().all()
        assert {row["state"] for row in rows} == {
            "REMOTE_ACCEPTED",
            "REMOTE_REJECTED",
            "TRANSPORT_ERROR",
        }
        assert all(len(row["request_body_sha256"]) == 64 for row in rows)

        rls = connection.execute(
            text(
                "SELECT relrowsecurity, relforcerowsecurity FROM pg_class "
                "WHERE relname='eudr_acceptance_attempts'"
            )
        ).mappings().one()
        assert rls["relrowsecurity"] is True
        assert rls["relforcerowsecurity"] is True

        policies = set(
            connection.execute(
                text(
                    "SELECT cmd FROM pg_policies WHERE schemaname='public' "
                    "AND tablename='eudr_acceptance_attempts'"
                )
            ).scalars().all()
        )
        assert policies == {"SELECT", "INSERT", "UPDATE"}

        for privilege in ("SELECT", "INSERT", "UPDATE"):
            assert connection.execute(
                text(
                    "SELECT has_table_privilege('litoral_trace_app', "
                    "'public.eudr_acceptance_attempts', :p)"
                ),
                {"p": privilege},
            ).scalar_one() is True
        assert connection.execute(
            text(
                "SELECT has_table_privilege('litoral_trace_app', "
                "'public.eudr_acceptance_attempts', 'DELETE')"
            )
        ).scalar_one() is False

        for privilege in ("SELECT", "INSERT", "UPDATE", "DELETE"):
            assert connection.execute(
                text(
                    "SELECT has_table_privilege('litoral_trace_worker_executor', "
                    "'public.eudr_acceptance_attempts', :p)"
                ),
                {"p": privilege},
            ).scalar_one() is False
