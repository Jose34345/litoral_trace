"""PostgreSQL acceptance for P1C reverse genealogy under tenant RLS."""
from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from litoral_trace.config.settings import normalize_database_url
from litoral_trace.services.traceability_lineage import TraceabilityLineageService


ROOT_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT_DIR / ".env.integration"
EXPECTED_REVISION = "019_add_traceability_genealogy"


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
    reason="P1C PostgreSQL tests require the isolated integration contract.",
)


def _engine(url: str):
    return create_engine(
        normalize_database_url(url),
        pool_pre_ping=True,
        hide_parameters=True,
    )


@pytest.fixture()
def pg_p1c():
    owner_engine = _engine(OWNER_URL)
    runtime_engine = _engine(RUNTIME_URL)
    RuntimeSession = sessionmaker(bind=runtime_engine, autoflush=False, expire_on_commit=False)
    suffix = uuid4().hex[:10]
    org_ids: list[int] = []

    with owner_engine.begin() as connection:
        revision = connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
        if revision != EXPECTED_REVISION:
            raise RuntimeError(f"P1C requires {EXPECTED_REVISION}; found {revision!r}.")

        for label in ("A", "B"):
            org_id = int(
                connection.execute(
                    text(
                        """
                        INSERT INTO public.organizations
                            (name, slug, tax_id, tier, description, is_active)
                        VALUES
                            (:name, :slug, :tax_id, 'pro', 'P1C integration', true)
                        RETURNING id
                        """
                    ),
                    {
                        "name": f"P1C Org {label} {suffix}",
                        "slug": f"p1c-{label.lower()}-{suffix}",
                        "tax_id": f"P1C-{label}-{suffix}",
                    },
                ).scalar_one()
            )
            org_ids.append(org_id)

            lote_a = int(
                connection.execute(
                    text(
                        """
                        INSERT INTO public.lotes (
                            organization_id, identificador, productor_id,
                            producto_forestal, hectareas, latitud, longitud,
                            polygon_wkt, estatus, volumen_ingresado_ton,
                            volumen_exportar_ton
                        ) VALUES (
                            :organization_id, :identificador, :productor_id,
                            'Pino resinoso', 50.0, -28.05, -56.03,
                            'POLYGON((-56.04 -28.06,-56.02 -28.06,-56.02 -28.04,-56.04 -28.04,-56.04 -28.06))',
                            'Verde', 0.0, 0.0
                        ) RETURNING id
                        """
                    ),
                    {
                        "organization_id": org_id,
                        "identificador": f"RODAL-{label}-1-{suffix}",
                        "productor_id": f"PROVEEDOR-{label}-1-{suffix}",
                    },
                ).scalar_one()
            )
            lote_b = int(
                connection.execute(
                    text(
                        """
                        INSERT INTO public.lotes (
                            organization_id, identificador, productor_id,
                            producto_forestal, hectareas, latitud, longitud,
                            polygon_wkt, estatus, volumen_ingresado_ton,
                            volumen_exportar_ton
                        ) VALUES (
                            :organization_id, :identificador, :productor_id,
                            'Pino resinoso', 60.0, -28.06, -56.04,
                            'POLYGON((-56.05 -28.07,-56.03 -28.07,-56.03 -28.05,-56.05 -28.05,-56.05 -28.07))',
                            'Verde', 0.0, 0.0
                        ) RETURNING id
                        """
                    ),
                    {
                        "organization_id": org_id,
                        "identificador": f"RODAL-{label}-2-{suffix}",
                        "productor_id": f"PROVEEDOR-{label}-2-{suffix}",
                    },
                ).scalar_one()
            )

            batch_ids: dict[str, int] = {}
            for code, product, stage, source_lote_id in (
                (f"RAW-{label}-1-{suffix}", "Rollizo de pino", "RAW_MATERIAL", lote_a),
                (f"RAW-{label}-2-{suffix}", "Rollizo de pino", "RAW_MATERIAL", lote_b),
                (f"FIN-{label}-{suffix}", "Madera aserrada", "FINISHED_GOOD", None),
            ):
                batch_ids[code] = int(
                    connection.execute(
                        text(
                            """
                            INSERT INTO public.traceability_batches (
                                organization_id, code, product_name, stage,
                                unit, status, source_lote_id
                            ) VALUES (
                                :organization_id, :code, :product_name, :stage,
                                'M3', 'ACTIVE', :source_lote_id
                            ) RETURNING id
                            """
                        ),
                        {
                            "organization_id": org_id,
                            "code": code,
                            "product_name": product,
                            "stage": stage,
                            "source_lote_id": source_lote_id,
                        },
                    ).scalar_one()
                )

            raw_1 = batch_ids[f"RAW-{label}-1-{suffix}"]
            raw_2 = batch_ids[f"RAW-{label}-2-{suffix}"]
            finished = batch_ids[f"FIN-{label}-{suffix}"]

            for event_code, event_type, inputs, outputs in (
                (
                    f"RECEIPT-{label}-1-{suffix}",
                    "RECEIPT",
                    (),
                    ((raw_1, "100.000000"),),
                ),
                (
                    f"RECEIPT-{label}-2-{suffix}",
                    "RECEIPT",
                    (),
                    ((raw_2, "80.000000"),),
                ),
                (
                    f"SAW-{label}-{suffix}",
                    "TRANSFORMATION",
                    ((raw_1, "70.000000"), (raw_2, "30.000000")),
                    ((finished, "65.000000"),),
                ),
            ):
                event_id = int(
                    connection.execute(
                        text(
                            """
                            INSERT INTO public.traceability_events (
                                organization_id, event_code, event_type, status,
                                occurred_at, facility_reference
                            ) VALUES (
                                :organization_id, :event_code, :event_type,
                                'POSTED', now(), 'Planta Virasoro'
                            ) RETURNING id
                            """
                        ),
                        {
                            "organization_id": org_id,
                            "event_code": event_code,
                            "event_type": event_type,
                        },
                    ).scalar_one()
                )
                for batch_id, quantity in inputs:
                    connection.execute(
                        text(
                            """
                            INSERT INTO public.traceability_event_inputs (
                                organization_id, event_id, batch_id, quantity, unit
                            ) VALUES (
                                :organization_id, :event_id, :batch_id, :quantity, 'M3'
                            )
                            """
                        ),
                        {
                            "organization_id": org_id,
                            "event_id": event_id,
                            "batch_id": batch_id,
                            "quantity": quantity,
                        },
                    )
                for batch_id, quantity in outputs:
                    connection.execute(
                        text(
                            """
                            INSERT INTO public.traceability_event_outputs (
                                organization_id, event_id, batch_id, quantity, unit
                            ) VALUES (
                                :organization_id, :event_id, :batch_id, :quantity, 'M3'
                            )
                            """
                        ),
                        {
                            "organization_id": org_id,
                            "event_id": event_id,
                            "batch_id": batch_id,
                            "quantity": quantity,
                        },
                    )

            shipment_id = int(
                connection.execute(
                    text(
                        """
                        INSERT INTO public.shipments (
                            organization_id, shipment_code, sale_reference,
                            buyer_reference, destination_country, shipped_at, status
                        ) VALUES (
                            :organization_id, :shipment_code, :sale_reference,
                            :buyer_reference, 'DE', now(), 'DISPATCHED'
                        ) RETURNING id
                        """
                    ),
                    {
                        "organization_id": org_id,
                        "shipment_code": f"EXP-SHARED-{suffix}",
                        "sale_reference": f"SALE-{label}-{suffix}",
                        "buyer_reference": f"BUYER-{label}-{suffix}",
                    },
                ).scalar_one()
            )
            connection.execute(
                text(
                    """
                    INSERT INTO public.shipment_items (
                        organization_id, shipment_id, batch_id, quantity, unit
                    ) VALUES (
                        :organization_id, :shipment_id, :batch_id, 60.000000, 'M3'
                    )
                    """
                ),
                {
                    "organization_id": org_id,
                    "shipment_id": shipment_id,
                    "batch_id": finished,
                },
            )

    try:
        yield RuntimeSession, org_ids, suffix
    finally:
        with owner_engine.begin() as connection:
            params = {"org_a": org_ids[0], "org_b": org_ids[1]}
            for table_name in (
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


def test_p1c_reverse_lineage_respects_tenant_and_attributes_source_volume(pg_p1c) -> None:
    RuntimeSession, org_ids, suffix = pg_p1c
    org_a, org_b = org_ids
    shipment_code = f"EXP-SHARED-{suffix}"

    with RuntimeSession() as session_a:
        payload_a = TraceabilityLineageService(
            session=session_a,
            organization_id=org_a,
        ).trace_shipment(shipment_code)

    with RuntimeSession() as session_b:
        payload_b = TraceabilityLineageService(
            session=session_b,
            organization_id=org_b,
        ).trace_shipment(shipment_code)

    assert payload_a["complete"] is True
    assert payload_b["complete"] is True
    assert payload_a["shipment"]["sale_reference"] == f"SALE-A-{suffix}"
    assert payload_b["shipment"]["sale_reference"] == f"SALE-B-{suffix}"

    sources_a = {
        item["lote"]["identificador"]: item
        for item in payload_a["source_lotes"]
    }
    assert sources_a[f"RODAL-A-1-{suffix}"]["attributed_shipment_quantity"] == "42.000000"
    assert sources_a[f"RODAL-A-2-{suffix}"]["attributed_shipment_quantity"] == "18.000000"
    assert all(
        source["lote"]["productor_id"].startswith("PROVEEDOR-A-")
        for source in payload_a["source_lotes"]
    )
    assert all(
        source["lote"]["productor_id"].startswith("PROVEEDOR-B-")
        for source in payload_b["source_lotes"]
    )
