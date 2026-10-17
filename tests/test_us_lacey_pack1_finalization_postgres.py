from __future__ import annotations

import os

import pytest
from sqlalchemy import select

from litoral_trace.db.models import (
    UsLaceyFieldCandidate,
    UsLaceyOperation,
    UsLaceyOperationField,
    UsLaceyPpqPlantLine,
)
from litoral_trace.us_lacey.canonical_publication_support import (
    prepare_canonical_publication,
    publish_derived_article_components,
)
from litoral_trace.us_lacey.canonical_shipment_truth import publish_canonical_shipment_truth
from litoral_trace.us_lacey.ppq505 import PPQ505_PLANT_FIELDS
from tests.test_us_lacey_canonical_shipment_truth_postgres import _field, _seed_two_lines
from tests.us_lacey_engine2_postgres import tenant_session


pytestmark = pytest.mark.skipif(
    os.environ.get("ENABLE_POSTGRES_TESTS") != "1",
    reason="Pack 1 canonical finalization requires the isolated PostgreSQL gate.",
)


def _add_provisional_machine_line(
    session,
    *,
    org: int,
    operation_id: int,
    assurance_id: int,
    ordinal: int,
    populated_field: str,
    value: str,
) -> None:
    reference = str(ordinal)
    line = UsLaceyPpqPlantLine(
        organization_id=org,
        operation_id=operation_id,
        line_reference=reference,
        ordinal=ordinal,
    )
    session.add(line)
    session.flush()
    for contract in PPQ505_PLANT_FIELDS:
        kwargs = {}
        if contract.key == populated_field:
            kwargs = {
                "original_value": value,
                "normalized_value": value,
                "field_status": "REVIEW",
                "validation_status": "REVIEW_REQUIRED",
                "source_assurance_document_id": assurance_id,
                "extractor": "assurance-deterministic-parser",
                "extractor_version": "test",
                "confidence": 0.8,
            }
        session.add(
            UsLaceyOperationField(
                organization_id=org,
                operation_id=operation_id,
                merchandise_line_reference=reference,
                field_name=contract.key,
                field_scope="PLANT_LINE",
                plant_line_id=line.id,
                field_status=kwargs.pop("field_status", "MISSING"),
                validation_status=kwargs.pop("validation_status", "MISSING"),
                confidence=kwargs.pop("confidence", 0.0),
                **kwargs,
            )
        )


def test_pack1_style_numeric_machine_surplus_is_compacted_before_canonical_publish(
    engine2_postgres_session_factory,
):
    factory = engine2_postgres_session_factory
    org, operation_id, assurance_id = _seed_two_lines(factory)
    session = tenant_session(factory, org)
    operation = session.get(UsLaceyOperation, operation_id)
    _add_provisional_machine_line(
        session,
        org=org,
        operation_id=operation_id,
        assurance_id=assurance_id,
        ordinal=3,
        populated_field="genus",
        value="Eucalyptus",
    )
    _add_provisional_machine_line(
        session,
        org=org,
        operation_id=operation_id,
        assurance_id=assurance_id,
        ordinal=4,
        populated_field="species",
        value="grandis",
    )
    operation.merchandise_line_count = 4
    session.commit()

    truth = prepare_canonical_publication(
        session,
        organization_id=org,
        operation_id=operation_id,
    )
    result = publish_canonical_shipment_truth(
        session,
        organization_id=org,
        operation_id=operation_id,
    )
    session.commit()

    remaining = session.scalars(
        select(UsLaceyPpqPlantLine)
        .where(
            UsLaceyPpqPlantLine.organization_id == org,
            UsLaceyPpqPlantLine.operation_id == operation_id,
        )
        .order_by(UsLaceyPpqPlantLine.ordinal.asc())
    ).all()
    session.refresh(operation)

    assert len(truth.plant_lines) == 2
    assert result.line_count == 2
    assert [line.line_reference for line in remaining] == ["1", "2"]
    assert operation.merchandise_line_count == 2
    session.close()


def test_article_component_and_species_stay_line_local_after_canonical_publish(
    engine2_postgres_session_factory,
):
    factory = engine2_postgres_session_factory
    org, operation_id, _ = _seed_two_lines(factory)
    session = tenant_session(factory, org)

    truth = prepare_canonical_publication(
        session,
        organization_id=org,
        operation_id=operation_id,
    )
    publish_canonical_shipment_truth(
        session,
        organization_id=org,
        operation_id=operation_id,
    )
    derived_count = publish_derived_article_components(
        session,
        organization_id=org,
        operation_id=operation_id,
        truth=truth,
    )
    session.commit()

    assert derived_count == 2
    for line_reference, expected_species in (("1", "Pinus taeda"), ("2", "Eucalyptus grandis")):
        component = _field(session, org, operation_id, line_reference, "article_component")
        species = _field(session, org, operation_id, line_reference, "species")
        assert component is not None
        assert component.normalized_value == "KD sawn boards"
        assert component.field_status == "FOUND"
        assert component.extractor == "canonical-shipment-truth"
        assert species is not None
        assert species.normalized_value == expected_species

        pending_species = session.scalars(
            select(UsLaceyFieldCandidate).where(
                UsLaceyFieldCandidate.organization_id == org,
                UsLaceyFieldCandidate.operation_field_id == species.id,
                UsLaceyFieldCandidate.decision == "PENDING",
            )
        ).all()
        assert {row.normalized_value for row in pending_species} == {expected_species}

    session.close()
