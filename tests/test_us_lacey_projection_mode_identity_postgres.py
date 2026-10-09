from __future__ import annotations

from litoral_trace.db.models import UsLaceyEngineDocumentRun, UsLaceyOperationField
from litoral_trace.us_lacey import lacey_engine_service as service_module
from litoral_trace.us_lacey import specialized_shadow as specialized_module
from litoral_trace.us_lacey.lacey_engine_service import UsLaceyEngine2Service
from litoral_trace.us_lacey.specialized_shadow import SPECIALIZED_SHADOW_SCHEMA_VERSION
from tests.test_us_lacey_shadow_dispatcher_postgres import (
    _add_missing_field,
    _configure_shadow,
    _shadow_run_count,
    _specialized_success,
    _wait_until,
)
from tests.us_lacey_engine2_postgres import (
    FakeVault,
    create_test_graph,
    engine2_postgres_engine,
    engine2_postgres_session_factory,
    tenant_session,
)


def test_projection_mode_transition_creates_new_auditable_specialized_run(
    monkeypatch,
    engine2_postgres_session_factory,
) -> None:
    content = b"projection-mode-transition"
    org, operation, _, _, _, _ = create_test_graph(
        engine2_postgres_session_factory,
        content=content,
    )
    _add_missing_field(
        engine2_postgres_session_factory,
        organization_id=org,
        operation_id=operation,
        field_name="bill_of_lading",
    )
    _configure_shadow(monkeypatch, engine2_postgres_session_factory)
    monkeypatch.setenv("LT_AI_ARCHITECTURE", "specialized")
    monkeypatch.setattr(
        specialized_module,
        "run_specialized_shadow_operation",
        _specialized_success,
    )

    service = UsLaceyEngine2Service(
        session_factory=engine2_postgres_session_factory,
        vault_service=FakeVault(content),
    )

    monkeypatch.setenv("LT_AI_SPECIALIZED_PROJECTION_MODE", "off")
    first = service.resolve_operation_with_engine2(
        organization_id=org,
        operation_id=operation,
    )
    assert first.status == "SUCCEEDED"
    _wait_until(
        lambda: _shadow_run_count(
            engine2_postgres_session_factory,
            organization_id=org,
            operation_id=operation,
            schema=SPECIALIZED_SHADOW_SCHEMA_VERSION,
        )
        == 1
    )
    _wait_until(lambda: not service_module._AI_BACKGROUND_INFLIGHT)

    session = tenant_session(engine2_postgres_session_factory, org)
    first_runs = (
        session.query(UsLaceyEngineDocumentRun)
        .filter_by(
            operation_id=operation,
            schema_version=SPECIALIZED_SHADOW_SCHEMA_VERSION,
            status="SUCCEEDED",
        )
        .order_by(UsLaceyEngineDocumentRun.id.asc())
        .all()
    )
    assert len(first_runs) == 1
    assert "projection" not in first_runs[0].resolution_json
    first_engine_version = first_runs[0].engine_version
    session.close()

    monkeypatch.setenv("LT_AI_SPECIALIZED_PROJECTION_MODE", "shadow")
    second = service.resolve_operation_with_engine2(
        organization_id=org,
        operation_id=operation,
    )
    assert second.status == "SUCCEEDED"
    _wait_until(
        lambda: _shadow_run_count(
            engine2_postgres_session_factory,
            organization_id=org,
            operation_id=operation,
            schema=SPECIALIZED_SHADOW_SCHEMA_VERSION,
        )
        == 2
    )

    session = tenant_session(engine2_postgres_session_factory, org)
    runs = (
        session.query(UsLaceyEngineDocumentRun)
        .filter_by(
            operation_id=operation,
            schema_version=SPECIALIZED_SHADOW_SCHEMA_VERSION,
            status="SUCCEEDED",
        )
        .order_by(UsLaceyEngineDocumentRun.id.asc())
        .all()
    )
    field = session.query(UsLaceyOperationField).filter_by(
        operation_id=operation,
        field_name="bill_of_lading",
    ).one()

    assert len(runs) == 2
    assert runs[1].engine_version != first_engine_version
    assert runs[1].resolution_json["projection"]["mode"] == "shadow"
    assert runs[1].resolution_json["projection"]["eligible_count"] == 1
    assert runs[1].resolution_json["projection"]["projected_count"] == 0
    assert field.field_status == "MISSING"
    assert field.normalized_value is None
    session.close()
