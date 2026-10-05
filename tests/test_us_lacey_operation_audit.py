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
    list_organization_audit_events,
)


def _factory(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE users (
                    id INTEGER PRIMARY KEY,
                    organization_id INTEGER NOT NULL,
                    email VARCHAR(255) NOT NULL,
                    username VARCHAR(100) NOT NULL,
                    full_name VARCHAR(255)
                )
                """
            )
        )
        connection.execute(
            text(
                """
                CREATE TABLE us_lacey_operations (
                    id INTEGER PRIMARY KEY,
                    public_id CHAR(32) NOT NULL,
                    organization_id INTEGER NOT NULL,
                    client_reference VARCHAR(255) NOT NULL DEFAULT '',
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
                INSERT INTO users (
                    id, organization_id, email, username, full_name
                ) VALUES (
                    42, 7, 'jane@example.com', 'jane', 'Jane Reviewer'
                )
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO us_lacey_operations (
                    id, public_id, organization_id, client_reference
                )
                VALUES (101, :public_id, 7, 'ENTRY-100')
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
                "document_role": "UNKNOWN",
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
    assert events[0].actor_identity == "Jane Reviewer"
    assert "supplier-declaration.pdf" in events[0].detail_text
    assert "Unknown" not in events[0].detail_text

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

    assert "Recent activity" in fragment
    assert "System of record" in fragment
    assert "View full audit log" in fragment
    assert "fa-robot" in fragment
    assert "fa-user" in fragment
    assert "border-l-2 border-slate-200" in fragment
    assert 'hx-trigger="every 15s"' in fragment


def test_global_audit_log_resolves_users_and_filters_operation(monkeypatch):
    engine, factory = _factory(monkeypatch)
    operation_public_id = uuid4()
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO users (
                    id, organization_id, email, username, full_name
                ) VALUES (
                    7, 9, 'david@example.com', 'david', 'David Lezcano'
                )
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO us_lacey_operations (
                    id, public_id, organization_id, client_reference
                ) VALUES (
                    201, :public_id, 9, 'SHIPMENT-AUDIT-201'
                )
                """
            ),
            {"public_id": operation_public_id.hex},
        )

    session = factory()
    try:
        append_operation_event(
            session,
            organization_id=9,
            operation_id=201,
            actor_type=OperationActorType.USER,
            actor_identity="user:7",
            event_type=OperationEventType.HUMAN_REVIEW,
            event_key="review:one",
            details={
                "action": "accept",
                "field_name": "species",
                "before_value": "Quercus rubra",
                "after_value": "Quercus alba",
            },
        )
        append_operation_event(
            session,
            organization_id=9,
            operation_id=201,
            actor_type=OperationActorType.SYSTEM,
            actor_identity="Litoral Trace",
            event_type=OperationEventType.EXTRACTED,
            event_key="extract:one",
            details={"projected_field_count": 4, "conflict_count": 0},
        )
        session.commit()
    finally:
        session.close()

    events = list_organization_audit_events(
        organization_id=9,
        operation_query="AUDIT-201",
        actor_query="David",
        event_type="HUMAN_REVIEW",
        date_range="all",
        session_factory=factory,
    )

    assert len(events) == 1
    assert events[0].operation_public_id == operation_public_id
    assert events[0].operation_reference == "SHIPMENT-AUDIT-201"
    assert events[0].actor_identity == "David Lezcano"
    assert events[0].action_label == "Human review"
    assert "Quercus rubra" in events[0].detail_text
    assert "Quercus alba" in events[0].detail_text


def test_global_audit_log_navigation_and_operation_summary_contract():
    root = Path(__file__).resolve().parents[1]
    base = (
        root / "src" / "litoral_trace" / "templates" / "us_lacey" / "base.html"
    ).read_text(encoding="utf-8")
    template = (
        root
        / "src"
        / "litoral_trace"
        / "templates"
        / "us_lacey"
        / "audit_log_list.html"
    ).read_text(encoding="utf-8")
    fragment = (
        root
        / "src"
        / "litoral_trace"
        / "templates"
        / "us_lacey"
        / "fragments"
        / "activity_audit_log.html"
    ).read_text(encoding="utf-8")
    app = (
        root / "src" / "litoral_trace" / "web" / "us_lacey_pilot_app.py"
    ).read_text(encoding="utf-8")

    assert 'href="/audit-log"' in base
    assert ">Audit Log<" in base
    assert "fa-clock-rotate-left" in base
    assert '@app.get("/audit-log"' in app
    for column in ("Time", "Operation", "Actor", "Event", "Details"):
        assert column in template
    for field in ("operation_id", "actor", "event_type", "date_range"):
        assert f'name="{field}"' in template
    assert "Recent activity" in fragment
    assert "View full audit log" in fragment
    assert "?operation_id={{ detail.public_id }}" in fragment
