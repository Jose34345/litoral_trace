"""PostgreSQL/RLS acceptance for the Assurance V2 authority journal.

Requires a disposable, fully migrated PostgreSQL test database and distinct
migration-owner / litoral_trace_app runtime connections. Never run on production.
"""
from __future__ import annotations

import os
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.pool import NullPool

from litoral_trace.config.settings import normalize_database_url


ENABLED = os.environ.get("ENABLE_POSTGRES_TESTS") == "1"
OWNER_URL = os.environ.get("TEST_POSTGRES_MIGRATION_DATABASE_URL")
RUNTIME_URL = os.environ.get("TEST_POSTGRES_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not (ENABLED and OWNER_URL and RUNTIME_URL),
    reason="Assurance V2 PostgreSQL/RLS gate requires isolated owner/runtime test URLs.",
)
TABLES = (
    "us_lacey_assurance_v2_decisions",
    "us_lacey_assurance_v2_decision_sources",
    "us_lacey_assurance_v2_memory_links",
    "us_lacey_assurance_v2_identity_events",
)


def _connect(url: str):
    return create_engine(normalize_database_url(url), poolclass=NullPool)


def _tenant(conn, org_id: int):
    conn.execute(
        text("SELECT set_config('app.current_organization_id', :org, true)"),
        {"org": str(org_id)},
    )


@pytest.fixture
def pg_case():
    owner = _connect(OWNER_URL)
    runtime = _connect(RUNTIME_URL)
    suffix = uuid4().hex[:10]
    values: dict[str, int] = {}
    try:
        with owner.connect() as conn:
            names = set(conn.execute(text(
                "SELECT tablename FROM pg_tables "
                "WHERE schemaname = 'public' AND tablename LIKE 'us_lacey_assurance_v2_%'"
            )).scalars().all())
        assert set(TABLES) <= names, (
            "078 not applied to isolated PostgreSQL test DB: run alembic upgrade head first."
        )
        with owner.begin() as conn:
            for label in ("a", "b"):
                org_id = int(conn.execute(text(
                    "SELECT nextval(pg_get_serial_sequence('public.organizations','id'))"
                )).scalar_one())
                _tenant(conn, org_id)
                conn.execute(text(
                    "INSERT INTO organizations "
                    "(id, name, slug, tax_id, tier, description, is_active) "
                    "VALUES (:id, :name, :slug, :tax_id, 'pro', 'V2 RLS fixture', true)"
                ), {
                    "id": org_id, "name": f"Assurance V2 {label.upper()} {suffix}",
                    "slug": f"v2-{label}-{suffix}", "tax_id": f"V2-{label}-{suffix}",
                })
                user_id = int(conn.execute(text(
                    "INSERT INTO users (organization_id,email,username,password_hash,role,is_active) "
                    "VALUES (:org,:email,:username,'test-fixture','cliente',true) RETURNING id"
                ), {
                    "org": org_id, "email": f"v2-{label}-{suffix}@example.com",
                    "username": f"v2-{label}-{suffix}",
                }).scalar_one())
                operation_id = int(conn.execute(text(
                    "INSERT INTO us_lacey_operations "
                    "(organization_id,client_reference,status,document_count,merchandise_line_count) "
                    "VALUES (:org,:ref,'NEW',0,0) RETURNING id"
                ), {"org": org_id, "ref": f"V2-{label}-{suffix}"}).scalar_one())
                revision_id = int(conn.execute(text(
                    "INSERT INTO us_lacey_source_set_revisions "
                    "(organization_id,operation_id,generation,source_set_fingerprint,"
                    "document_count,status,is_current) "
                    "VALUES (:org,:op,1,:fingerprint,1,'FINALIZED',true) RETURNING id"
                ), {
                    "org": org_id, "op": operation_id, "fingerprint": (label * 64),
                }).scalar_one())
                audit_id = int(conn.execute(text(
                    "INSERT INTO us_lacey_operation_events "
                    "(public_id,organization_id,operation_id,actor_type,actor_identity,event_type,event_key,details) "
                    "VALUES (CAST(:public_id AS UUID),:org,:op,'USER',:identity,"
                    "'HUMAN_REVIEW',:key,CAST('{}' AS JSONB)) RETURNING id"
                ), {
                    "public_id": str(uuid4()), "org": org_id, "op": operation_id,
                    "identity": f"user:{user_id}", "key": f"v2fixture:{label}:{suffix}",
                }).scalar_one())
                values.update({
                    f"org_{label}": org_id, f"user_{label}": user_id,
                    f"op_{label}": operation_id, f"revision_{label}": revision_id,
                    f"audit_{label}": audit_id,
                })
        # Verify fixture runtime is unprivileged; a superuser would fake RLS green.
        with runtime.begin() as conn:
            runtime_identity = conn.execute(text(
                "SELECT current_user, rolbypassrls, rolsuper "
                "FROM pg_roles WHERE rolname=current_user"
            )).one()
            assert runtime_identity[0] == "litoral_trace_app"
            assert runtime_identity[1] is False and runtime_identity[2] is False
        yield values, owner, runtime
    finally:
        if values:
            with owner.begin() as conn:
                conn.execute(text(
                    "DELETE FROM us_lacey_operations "
                    "WHERE organization_id IN (:a,:b)"
                ), {"a": values["org_a"], "b": values["org_b"]})
                conn.execute(text(
                    "DELETE FROM organizations WHERE id IN (:a,:b)"
                ), {"a": values["org_a"], "b": values["org_b"]})
        runtime.dispose()
        owner.dispose()


def _insert_decision(conn, *, org_id, op_id, revision_id, user_id, audit_id, key):
    return int(conn.execute(text(
        "INSERT INTO us_lacey_assurance_v2_decisions "
        "(public_id, organization_id, operation_id, source_set_revision_id, "
        "source_set_fingerprint, generation, actor_user_id, action, field_name, "
        "reason, audit_event_id, idempotency_key, context_json) VALUES "
        "(CAST(:public_id AS UUID),:org,:op,:revision,:fingerprint,1,:user,"
        "'REJECT','species','Fixture rejected',:audit,:key,CAST('{}' AS JSONB)) "
        "RETURNING id"
    ), {
        "public_id": str(uuid4()), "org": org_id, "op": op_id,
        "revision": revision_id, "fingerprint": "a" * 64 if key.startswith("a") else "b" * 64,
        "user": user_id, "audit": audit_id, "key": key,
    }).scalar_one())


def test_assurance_v2_rls_force_and_append_only(pg_case):
    vals, owner, runtime = pg_case
    with owner.connect() as conn:
        entries = conn.execute(text(
            "SELECT c.relname, c.relrowsecurity, c.relforcerowsecurity "
            "FROM pg_class c JOIN pg_namespace ns ON ns.oid = c.relnamespace "
            "WHERE ns.nspname = 'public' AND c.relname IN "
            "('us_lacey_assurance_v2_decisions', 'us_lacey_assurance_v2_decision_sources',"
            "'us_lacey_assurance_v2_memory_links','us_lacey_assurance_v2_identity_events')"
        )).all()
        assert len(entries) == 4
        assert all(row[1] is True and row[2] is True for row in entries)

    with runtime.begin() as conn:
        _tenant(conn, vals["org_a"])
        a = _insert_decision(
            conn, org_id=vals["org_a"], op_id=vals["op_a"],
            revision_id=vals["revision_a"], user_id=vals["user_a"],
            audit_id=vals["audit_a"], key="a:reject",
        )
    with runtime.begin() as conn:
        _tenant(conn, vals["org_b"])
        b = _insert_decision(
            conn, org_id=vals["org_b"], op_id=vals["op_b"],
            revision_id=vals["revision_b"], user_id=vals["user_b"],
            audit_id=vals["audit_b"], key="b:reject",
        )
    assert a != b

    with runtime.begin() as conn:
        _tenant(conn, vals["org_a"])
        rows = conn.execute(text(
            "SELECT id, organization_id FROM us_lacey_assurance_v2_decisions"
        )).all()
        assert len(rows) == 1
        assert rows[0].id == a and rows[0].organization_id == vals["org_a"]
    with runtime.begin() as conn:
        _tenant(conn, vals["org_b"])
        assert conn.execute(text(
            "SELECT count(*) FROM us_lacey_assurance_v2_decisions"
        )).scalar_one() == 1

    with pytest.raises(DBAPIError):
        with runtime.begin() as conn:
            _tenant(conn, vals["org_a"])
            conn.execute(text(
                "UPDATE us_lacey_assurance_v2_decisions "
                "SET reason='rewritten' WHERE id=:id"
            ), {"id": a})
    with pytest.raises(DBAPIError):
        with runtime.begin() as conn:
            _tenant(conn, vals["org_a"])
            conn.execute(text(
                "DELETE FROM us_lacey_assurance_v2_decisions WHERE id=:id"
            ), {"id": a})

    with pytest.raises(DBAPIError):
        with runtime.begin() as conn:
            _tenant(conn, vals["org_a"])
            _insert_decision(
                conn, org_id=vals["org_a"], op_id=vals["op_a"],
                revision_id=vals["revision_a"], user_id=vals["user_b"],
                audit_id=vals["audit_a"], key="a:wrong-tenant-actor",
            )
    with pytest.raises(DBAPIError):
        with runtime.begin() as conn:
            _tenant(conn, vals["org_a"])
            _insert_decision(
                conn, org_id=vals["org_b"], op_id=vals["op_b"],
                revision_id=vals["revision_b"], user_id=vals["user_b"],
                audit_id=vals["audit_b"], key="b:wrong-tenant-insert",
            )


def test_tenant_context_required_for_reads(pg_case):
    vals, _, runtime = pg_case
    with runtime.begin() as conn:
        _tenant(conn, vals["org_a"])
        _insert_decision(
            conn, org_id=vals["org_a"], op_id=vals["op_a"],
            revision_id=vals["revision_a"], user_id=vals["user_a"],
            audit_id=vals["audit_a"], key="a:no-context",
        )
    with runtime.begin() as conn:
        assert conn.execute(text(
            "SELECT count(*) FROM us_lacey_assurance_v2_decisions"
        )).scalar_one() == 0
