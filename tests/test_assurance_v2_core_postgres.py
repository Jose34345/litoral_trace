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
    "us_lacey_assurance_v2_claim_snapshots",
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
            "079 not applied to isolated PostgreSQL test DB: run alembic upgrade head first."
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
                values.update({
                    f"org_{label}": org_id, f"user_{label}": user_id,
                    f"op_{label}": operation_id, f"revision_{label}": revision_id,
                })
        # Operation events use FORCE RLS and deliberately permit INSERT only
        # to the tenant-scoped application role. Do not bypass that authority
        # boundary with the migration-owner fixture connection.
        for label in ("a", "b"):
            with runtime.begin() as conn:
                org_id = values[f"org_{label}"]
                _tenant(conn, org_id)
                audit_id = int(conn.execute(text(
                    "INSERT INTO us_lacey_operation_events "
                    "(public_id,organization_id,operation_id,actor_type,actor_identity,event_type,event_key,details) "
                    "VALUES (CAST(:public_id AS UUID),:org,:op,'USER',:identity,"
                    "'HUMAN_REVIEW',:key,CAST('{}' AS JSONB)) RETURNING id"
                ), {
                    "public_id": str(uuid4()), "org": org_id,
                    "op": values[f"op_{label}"],
                    "identity": f"user:{values[f'user_{label}']}",
                    "key": f"v2fixture:{label}:{suffix}",
                }).scalar_one())
                values[f"audit_{label}"] = audit_id
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
            "('us_lacey_assurance_v2_claim_snapshots', 'us_lacey_assurance_v2_decisions', 'us_lacey_assurance_v2_decision_sources',"
            "'us_lacey_assurance_v2_memory_links','us_lacey_assurance_v2_identity_events')"
        )).all()
        assert len(entries) == 5
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


def test_claim_snapshot_is_tenant_scoped_immutable_and_operation_bound(pg_case):
    vals, _, runtime = pg_case
    table = "us_lacey_assurance_v2_claim_snapshots"

    def insert(conn, org, op, revision, fingerprint):
        return conn.execute(text(
            f"INSERT INTO {table} (organization_id, operation_id, "
            "source_set_revision_id, source_set_fingerprint, status, "
            "document_count, claim_count, snapshot_json) "
            "VALUES (:org,:op,:revision,:fingerprint,'CANDIDATE_ONLY',1,0, "
            "CAST(:snapshot AS JSONB)) RETURNING id"
        ), {"org": org, "op": op, "revision": revision,
            "fingerprint": fingerprint,
            "snapshot": '{"schema_version":"assurance.claim.snapshot.v1","documents":[]}',
            }).scalar_one()

    with runtime.begin() as conn:
        _tenant(conn, vals["org_a"])
        snap_id = insert(conn, vals["org_a"], vals["op_a"], vals["revision_a"], "a" * 64)
        assert conn.execute(text(f"SELECT count(*) FROM {table}")).scalar_one() == 1
    with runtime.begin() as conn:
        _tenant(conn, vals["org_b"])
        assert conn.execute(text(f"SELECT count(*) FROM {table}")).scalar_one() == 0
    with runtime.begin() as conn:
        assert conn.execute(text(f"SELECT count(*) FROM {table}")).scalar_one() == 0

    with pytest.raises(DBAPIError):
        with runtime.begin() as conn:
            _tenant(conn, vals["org_a"])
            insert(conn, vals["org_a"], vals["op_a"], vals["revision_a"], "a" * 64)
    with pytest.raises(DBAPIError):
        with runtime.begin() as conn:
            _tenant(conn, vals["org_a"])
            insert(conn, vals["org_a"], vals["op_b"], vals["revision_a"], "b" * 64)
    with pytest.raises(DBAPIError):
        with runtime.begin() as conn:
            _tenant(conn, vals["org_a"])
            insert(conn, vals["org_b"], vals["op_b"], vals["revision_b"], "b" * 64)
    with pytest.raises(DBAPIError):
        with runtime.begin() as conn:
            _tenant(conn, vals["org_a"])
            conn.execute(text(f"UPDATE {table} SET claim_count=7 WHERE id=:id"), {"id": snap_id})
    with pytest.raises(DBAPIError):
        with runtime.begin() as conn:
            _tenant(conn, vals["org_a"])
            conn.execute(text(f"DELETE FROM {table} WHERE id=:id"), {"id": snap_id})


def test_legacy_export_guard_blocks_v2_adopted_operation_only(pg_case):
    from sqlalchemy.orm import Session
    from litoral_trace.us_lacey.assurance_v2_legacy_export_gate import (
        AssuranceV2LegacyExportBlocked, legacy_export_guard,
    )

    vals, _, runtime = pg_case
    factory = lambda: Session(bind=runtime, expire_on_commit=False)
    with runtime.connect() as conn:
        _tenant(conn, vals["org_a"])
        public_a = str(conn.execute(text(
            "SELECT public_id FROM us_lacey_operations WHERE id=:op"
        ), {"op": vals["op_a"]}).scalar_one())
    with runtime.connect() as conn:
        _tenant(conn, vals["org_b"])
        public_b = str(conn.execute(text(
            "SELECT public_id FROM us_lacey_operations WHERE id=:op"
        ), {"op": vals["op_b"]}).scalar_one())

    with legacy_export_guard(organization_id=vals["org_a"],
                             operation_public_id=public_a, session_factory=factory):
        pass  # Historical V1 operation remains available.
    with runtime.begin() as conn:
        _tenant(conn, vals["org_a"])
        _insert_decision(conn, org_id=vals["org_a"], op_id=vals["op_a"],
                         revision_id=vals["revision_a"], user_id=vals["user_a"],
                         audit_id=vals["audit_a"], key="a:legacy-gate")
    with pytest.raises(AssuranceV2LegacyExportBlocked):
        with legacy_export_guard(organization_id=vals["org_a"],
                                 operation_public_id=public_a, session_factory=factory):
            raise AssertionError("V2 operation reached legacy export")
    with legacy_export_guard(organization_id=vals["org_b"],
                             operation_public_id=public_b, session_factory=factory):
        pass  # Another tenant's V2 decision cannot affect this operation.



def test_explicit_export_authorization_uses_runtime_postgres_rls(pg_case):
    """Real app-role PostgreSQL write/read. Only the CASE is synthetic.

    This proves append-only tenant isolation and actor checks. It is not a
    documentary golden test or an actual CBP/APHIS export.
    """
    from dataclasses import replace
    from sqlalchemy.orm import Session
    from litoral_trace.us_lacey.assurance_v2_export_authorization import (
        authorization_from_event, explicitly_authorize_assurance_export,
    )
    from litoral_trace.us_lacey.exporters.assurance_v2 import evaluate_filing_gate
    from test_assurance_v2_outputs_core import make_case

    vals, owner, runtime = pg_case
    with owner.connect() as conn:
        _tenant(conn, vals["org_a"])
        operation_public_id = str(conn.execute(text(
            "SELECT public_id FROM us_lacey_operations WHERE id=:op AND organization_id=:org"
        ), {"org": vals["org_a"], "op": vals["op_a"]}).scalar_one())

    case = replace(
        make_case(organization_id=vals["org_a"], approved=False),
        operation_id=operation_public_id,
        source_set_revision=vals["revision_a"],
        current_source_set_revision=vals["revision_a"],
        source_set_fingerprint="a" * 64,
        current_source_set_fingerprint="a" * 64,
    )
    assert {b.code for b in evaluate_filing_gate(case).blockers} == {"EXPORT_NOT_AUTHORIZED"}
    factory = lambda: Session(bind=runtime, expire_on_commit=False)
    auth = explicitly_authorize_assurance_export(
        organization_id=vals["org_a"], operation_public_id=operation_public_id,
        authenticated_user_id=vals["user_a"],
        expected_source_set_fingerprint="a" * 64,
        expected_case_fingerprint=case.authority_fingerprint(),
        explicit_confirmation=True,
        reason="Verified all evidence for this exact synthetic test case",
        session_factory=factory, case_loader=lambda *_: case,
    )
    assert auth["actor_id"] == f"user:{vals['user_a']}"
    assert auth["status"] == "AUTHORIZED"

    with factory() as session:
        _tenant(session, vals["org_a"])
        assert authorization_from_event(
            session, organization_id=vals["org_a"],
            operation_id=vals["op_a"], case=case,
        ) == auth
    with factory() as session:
        _tenant(session, vals["org_b"])
        assert not authorization_from_event(
            session, organization_id=vals["org_b"],
            operation_id=vals["op_b"], case=case,
        )
    # A changed field or document invalidates the authorization; no blanket
    # permission is recorded for future source sets.
    fields = list(case.fields)
    fields[0] = {**fields[0], "value": "Changed source after approval"}
    changed = replace(case, fields=tuple(fields))
    with factory() as session:
        _tenant(session, vals["org_a"])
        assert not authorization_from_event(
            session, organization_id=vals["org_a"],
            operation_id=vals["op_a"], case=changed,
        )
