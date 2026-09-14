from __future__ import annotations

from litoral_trace.db.models import UsLaceyEngineDocumentRun
from litoral_trace.us_lacey import specialized_shadow as specialized_module
from litoral_trace.us_lacey.lacey_engine_service import UsLaceyEngine2Service
from litoral_trace.us_lacey.specialized_shadow import SPECIALIZED_SHADOW_SCHEMA_VERSION
from tests.test_us_lacey_shadow_dispatcher_postgres import _specialized_success
from tests.test_us_lacey_specialized_projection_postgres import _configure_projection
from tests.us_lacey_engine2_postgres import (
    FakeVault,
    create_test_graph,
    engine2_postgres_engine,
    engine2_postgres_session_factory,
    tenant_session,
)


def test_specialized_same_immutable_source_set_is_not_reexecuted(
    monkeypatch,
    engine2_postgres_session_factory,
) -> None:
    org, operation, _, _, _, _ = create_test_graph(
        engine2_postgres_session_factory,
        content=b"specialized-source-set-idempotency",
    )
    _configure_projection(monkeypatch, engine2_postgres_session_factory, mode="shadow")

    calls = 0

    def counted_specialized_run(**kwargs):
        nonlocal calls
        calls += 1
        return _specialized_success(**kwargs)

    monkeypatch.setattr(
        specialized_module,
        "run_specialized_shadow_operation",
        counted_specialized_run,
    )

    service = UsLaceyEngine2Service(
        session_factory=engine2_postgres_session_factory,
        vault_service=FakeVault(b"specialized-source-set-idempotency"),
    )
    first = service.resolve_operation_with_engine2(
        organization_id=org,
        operation_id=operation,
    )
    second = service.resolve_operation_with_engine2(
        organization_id=org,
        operation_id=operation,
    )

    assert first.status == "SUCCEEDED"
    assert second.status == "SUCCEEDED"
    assert calls == 1

    session = tenant_session(engine2_postgres_session_factory, org)
    assert (
        session.query(UsLaceyEngineDocumentRun)
        .filter_by(
            operation_id=operation,
            schema_version=SPECIALIZED_SHADOW_SCHEMA_VERSION,
            status="SUCCEEDED",
        )
        .count()
        == 1
    )
    session.close()
