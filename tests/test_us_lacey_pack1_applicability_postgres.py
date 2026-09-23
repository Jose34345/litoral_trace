from __future__ import annotations

from sqlalchemy import select

from litoral_trace.db.models import (
    ReconciliationIssue,
    UsLaceyOperation,
    UsLaceyOperationField,
    UsLaceyPpqPlantLine,
)
from litoral_trace.us_lacey.projection import (
    _materialize_applicable_plant_lines,
    _sync_applicability_review_issue,
)
from litoral_trace.us_lacey.regulatory.applicability import (
    DeclarationApplicabilityService,
    DeclarationScope,
    MerchandiseLineFacts,
    PlantMaterialEvidence,
)
from tests.us_lacey_engine2_postgres import (
    create_test_graph,
    engine2_postgres_engine,
    engine2_postgres_session_factory,
    tenant_session,
)


def _fact(line_key: str, hts10: str) -> MerchandiseLineFacts:
    return MerchandiseLineFacts(
        line_key=line_key,
        hts10=hts10,
        description=f"Pack 1 merchandise line {line_key}",
        entered_value="100.00",
        plant_material=PlantMaterialEvidence.UNKNOWN,
        evidence_refs=(f"pack-1:line:{line_key}",),
    )


def test_pack1_non_lacey_hts_rows_create_zero_botanical_lines(
    engine2_postgres_session_factory,
):
    org, operation_id, _link, assurance_id, _vault, _sha = create_test_graph(
        engine2_postgres_session_factory,
        content=b"pack-1-applicability",
    )
    service = DeclarationApplicabilityService()
    facts = (
        _fact("1", "7613000000"),
        _fact("2", "8424890000"),
        _fact("3", "8716805070"),
    )
    decisions = tuple(service.evaluate(item) for item in facts)

    assert all(
        decision.scope in {
            DeclarationScope.NOT_REQUIRED,
            DeclarationScope.REVIEW_REQUIRED,
        }
        for decision in decisions
    )
    assert all(decision.requires_botanical_fields is False for decision in decisions)

    session = tenant_session(engine2_postgres_session_factory, org)
    operation = session.scalar(
        select(UsLaceyOperation).where(UsLaceyOperation.id == operation_id)
    )
    assert operation is not None

    for item, decision in zip(facts, decisions, strict=True):
        _sync_applicability_review_issue(
            session,
            organization_id=org,
            operation=operation,
            document_id=assurance_id,
            facts=item,
            decision=decision,
        )

    line_references = _materialize_applicable_plant_lines(
        session,
        organization_id=org,
        operation=operation,
        applicable_line_keys=tuple(
            item.line_key
            for item, decision in zip(facts, decisions, strict=True)
            if decision.requires_botanical_fields
        ),
    )
    session.commit()

    assert line_references == ()
    assert (
        session.query(UsLaceyPpqPlantLine)
        .filter_by(organization_id=org, operation_id=operation_id)
        .count()
        == 0
    )
    assert (
        session.query(UsLaceyOperationField)
        .filter(
            UsLaceyOperationField.organization_id == org,
            UsLaceyOperationField.operation_id == operation_id,
            UsLaceyOperationField.field_name.in_(("genus", "species")),
        )
        .count()
        == 0
    )
    session.close()


def test_unknown_plant_material_creates_workspace_question_without_botanical_fields(
    engine2_postgres_session_factory,
):
    org, operation_id, _link, assurance_id, _vault, _sha = create_test_graph(
        engine2_postgres_session_factory,
        content=b"scheduled-unknown-plant",
    )
    facts = MerchandiseLineFacts(
        line_key="1",
        hts10="4407990190",
        description="Sawn wood product",
        entered_value="100.00",
        plant_material=PlantMaterialEvidence.UNKNOWN,
        evidence_refs=("fixture:line:1",),
    )
    decision = DeclarationApplicabilityService().evaluate(facts)
    assert decision.scope is DeclarationScope.REVIEW_REQUIRED
    assert decision.reason_codes == ("PLANT_MATERIAL_NOT_ESTABLISHED",)
    assert decision.requires_botanical_fields is False

    session = tenant_session(engine2_postgres_session_factory, org)
    operation = session.get(UsLaceyOperation, operation_id)
    _sync_applicability_review_issue(
        session,
        organization_id=org,
        operation=operation,
        document_id=assurance_id,
        facts=facts,
        decision=decision,
    )
    _materialize_applicable_plant_lines(
        session,
        organization_id=org,
        operation=operation,
        applicable_line_keys=(),
    )
    session.commit()

    issue = (
        session.query(ReconciliationIssue)
        .filter_by(
            organization_id=org,
            operation_reference=f"us_lacey:{operation.public_id}",
            rule_code="US_LACEY_DECLARATION_APPLICABILITY",
            status="OPEN",
        )
        .one()
    )
    assert issue.explanation == "Does this product contain plant material?"
    assert issue.field_name == "plant_material"
    assert issue.evidence_json["requires_botanical_fields"] is False
    assert issue.evidence_json["reason_codes"] == [
        "PLANT_MATERIAL_NOT_ESTABLISHED"
    ]
    assert (
        session.query(UsLaceyPpqPlantLine)
        .filter_by(organization_id=org, operation_id=operation_id)
        .count()
        == 0
    )
    assert (
        session.query(UsLaceyOperationField)
        .filter(
            UsLaceyOperationField.organization_id == org,
            UsLaceyOperationField.operation_id == operation_id,
            UsLaceyOperationField.field_name.in_(("genus", "species")),
        )
        .count()
        == 0
    )
    session.close()
