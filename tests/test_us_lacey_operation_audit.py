from __future__ import annotations

from pathlib import Path
from uuid import uuid4

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from litoral_trace.db.models import UsLaceyOperationEvent
from litoral_trace.us_lacey import audit_trail
from litoral_trace.us_lacey.audit_trail import (
    OperationActorType,
    OperationEventType,
    append_operation_event,
    list_operation_events,
)


def _factory(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE us_lacey_operations (
                    id INTEGER PRIMARY KEY,
                    public_id CHAR(32) NOT NULL,
                    organization_id INTEGER NOT NULL,
                    UNIQUE(id, organization_id)
                )
                """
            )
        )
    UsLaceyOperationEvent.__table__.create(engine)
    monkeypatch.setattr(
        audit_trail,
        "set_tenant_db_context",
        lambda _session, _organization_id: None,
    )
    return engine, sessionmaker(bind=engine, expire_on_commit=False)


def test_operation_audit_events_append_and_return_newest_first(monkeypatch):
    engine, factory = _factory(monkeypatch)
    public_id = uuid4()
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO us_lacey_operations (id, public_id, organization_id)
                VALUES (101, :public_id, 7)
                """
            ),
            {"public_id": public_id.hex},
        )

    session = factory()
    try:
        created = append_operation_event(
            session,
            organization_id=7,
            operation_id=101,
            actor_type=OperationActorType.USER,
            actor_identity="user:42",
            event_type=OperationEventType.CREATED,
            event_key="operation:created",
            details={"client_reference": "ENTRY-100"},
        )
        uploaded = append_operation_event(
            session,
            organization_id=7,
            operation_id=101,
            actor_type=OperationActorType.USER,
            actor_identity="user:42",
            event_type=OperationEventType.DOCUMENT_UPLOADED,
            event_key="document:501:uploaded",
            details={
                "filename": "supplier-declaration.pdf",
                "document_role": "SUPPLIER_DECLARATION",
                "version_number": 1,
            },
        )
        session.commit()
        assert created.id < uploaded.id
    finally:
        session.close()

    events = list_operation_events(
        organization_id=7,
        operation_public_id=public_id,
        session_factory=factory,
    )

    assert [event.event_type for event in events] == [
        "DOCUMENT_UPLOADED",
        "CREATED",
    ]
    assert events[0].actor_identity == "user:42"
    assert "supplier-declaration.pdf" in events[0].detail_text

    migration = (
        Path(__file__).resolve().parents[1]
        / "alembic"
        / "versions"
        / "073_us_lacey_operation_audit_trail.py"
    ).read_text(encoding="utf-8")
    assert "FORCE ROW LEVEL SECURITY" in migration
    assert "GRANT SELECT, INSERT" in migration
    assert "REVOKE UPDATE, DELETE" in migration
    assert "FOR UPDATE TO litoral_trace_app" not in migration
    assert "FOR DELETE TO litoral_trace_app" not in migration


def test_operation_audit_template_is_dense_system_of_record_surface():
    root = Path(__file__).resolve().parents[1]
    fragment = (
        root
        / "src"
        / "litoral_trace"
        / "templates"
        / "us_lacey"
        / "fragments"
        / "activity_audit_log.html"
    ).read_text(encoding="utf-8")

    assert "Activity &amp; Audit Log" in fragment
    assert "System of record" in fragment
    assert "fa-robot" in fragment
    assert "fa-user" in fragment
    assert "border-l-2 border-slate-200" in fragment
    assert 'hx-trigger="every 15s"' in fragment
