"""Static and PostgreSQL-gated contracts for the Engine 2 shadow schema."""
from __future__ import annotations
from pathlib import Path
from types import SimpleNamespace
import pytest
from sqlalchemy import text
from litoral_trace.us_lacey.lacey_engine_service import source_set_fingerprint
from litoral_trace.us_lacey.lacey_engine_service import UsLaceyEngine2Service
from litoral_trace.lacey_engine.pipeline import ENGINE_VERSION
from litoral_trace.lacey_engine.shipment import LaceyRuleset
from litoral_trace.db.models import UsLaceyEngineDocumentRun
from tests.us_lacey_engine2_postgres import engine2_postgres_engine

MIGRATION = Path(__file__).parents[1] / "alembic/versions/043_add_us_lacey_engine2_shadow.py"

def test_engine2_migration_is_single_canonical_successor():
    source = MIGRATION.read_text(encoding="utf-8")
    assert 'revision = "043_us_lacey_engine2_shadow"' in source
    assert '"042_us_lacey_owner_admin"' in source
    assert "us_lacey_engine_document_runs" in source and "us_lacey_engine_shipment_runs" in source
    assert len(list(MIGRATION.parent.glob("043_*.py"))) == 1
    for table in ("us_lacey_engine_document_runs", "us_lacey_engine_shipment_runs"):
        assert "ENABLE ROW LEVEL SECURITY" in source
        assert "FORCE ROW LEVEL SECURITY" in source
        for action in ("select", "insert", "update", "delete"):
            assert f'("{action}",' in source
        assert "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.{table}" in source


def test_source_set_fingerprint_is_order_independent_and_version_sensitive():
    first = (SimpleNamespace(id=8, assurance_document_id=4, version_number=2), SimpleNamespace(sha256="a" * 64))
    second = (SimpleNamespace(id=3, assurance_document_id=2, version_number=1), SimpleNamespace(sha256="b" * 64))
    baseline = source_set_fingerprint(organization_id=1, operation_id=9, documents=[first, second])
    assert baseline == source_set_fingerprint(organization_id=1, operation_id=9, documents=[second, first])
    changed = (SimpleNamespace(id=8, assurance_document_id=4, version_number=3), SimpleNamespace(sha256="a" * 64))
    assert baseline != source_set_fingerprint(organization_id=1, operation_id=9, documents=[changed, second])
    assert baseline != source_set_fingerprint(organization_id=1, operation_id=9, documents=[first, second], shipment_schema_version="lacey_shipment_resolution_v2")


def test_engine2_orm_and_migration_share_status_scoped_document_identity():
    source = MIGRATION.read_text(encoding="utf-8")
    expected = ("organization_id", "assurance_document_id", "source_sha256", "engine_version", "schema_version", "role_hint", "status")
    unique = next(item for item in UsLaceyEngineDocumentRun.__table_args__ if getattr(item, "name", None) == "uq_lacey_e2_docrun_identity")
    assert tuple(column.name for column in unique.columns) == expected
    assert 'sa.UniqueConstraint("organization_id", "assurance_document_id", "source_sha256", "engine_version", "schema_version", "role_hint", "status", name="uq_lacey_e2_docrun_identity")' in source
    # Retry policy intentionally retains one FAILED and one SUCCEEDED row per identity.


def test_engine2_service_defaults_use_canonical_version_boundaries():
    service = UsLaceyEngine2Service(vault_service=SimpleNamespace())
    assert service._engine_version == ENGINE_VERSION
    assert service._ruleset.version == LaceyRuleset().version == "lacey_ruleset_2026_01"


def test_engine2_tables_have_forced_rls_and_tenant_policies(engine2_postgres_engine):
    with engine2_postgres_engine.connect() as conn:
        rows = conn.execute(text("SELECT relname, relrowsecurity, relforcerowsecurity FROM pg_class WHERE relname IN ('us_lacey_engine_document_runs', 'us_lacey_engine_shipment_runs')")).all()
        assert {(row[0], row[1], row[2]) for row in rows} == {("us_lacey_engine_document_runs", True, True), ("us_lacey_engine_shipment_runs", True, True)}
        policies = conn.execute(text("SELECT tablename, count(*) FROM pg_policies WHERE tablename IN ('us_lacey_engine_document_runs', 'us_lacey_engine_shipment_runs') GROUP BY tablename")).all()
        assert dict(policies) == {"us_lacey_engine_document_runs": 4, "us_lacey_engine_shipment_runs": 4}
