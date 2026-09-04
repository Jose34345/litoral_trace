"""Explicit isolated PostgreSQL fixtures for substantive Engine 2 tests."""
from __future__ import annotations
import hashlib, os
from contextlib import contextmanager
from uuid import uuid4
import pytest
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker
from litoral_trace.config.settings import normalize_database_url
from litoral_trace.db.models import Organization, UsLaceyOperation, UsLaceyOperationDocument, AssuranceDocument, VaultDocument
from litoral_trace.db.tenant import set_tenant_db_context

ENV = "US_LACEY_POSTGRES_TEST_DATABASE_URL"

@pytest.fixture
def engine2_postgres_engine():
    url = os.environ.get(ENV)
    if not url: pytest.skip(f"BLOCKED_ENVIRONMENT: {ENV} is required for isolated PostgreSQL acceptance.")
    if not url.startswith(("postgresql://", "postgresql+psycopg://")): raise RuntimeError(f"{ENV} must be an explicit PostgreSQL URL.")
    engine = create_engine(normalize_database_url(url), pool_pre_ping=True)
    if not {"us_lacey_engine_document_runs", "us_lacey_engine_shipment_runs"}.issubset(inspect(engine).get_table_names()):
        engine.dispose(); pytest.skip("POSTGRES_SCHEMA_NOT_MIGRATED_TO_043")
    yield engine; engine.dispose()

@pytest.fixture
def engine2_postgres_session_factory(engine2_postgres_engine):
    return sessionmaker(bind=engine2_postgres_engine, expire_on_commit=False)

def tenant_session(factory, organization_id):
    session = factory(); set_tenant_db_context(session, organization_id); return session

def create_test_graph(factory, *, role="BILL_OF_LADING", content=b"engine2-test"):
    suffix = uuid4().hex; session = factory()
    org = Organization(name=f"Engine2 {suffix}", slug=f"engine2-{suffix}", tax_id=f"e2-{suffix}", tier="pro", is_active=True); session.add(org); session.flush(); set_tenant_db_context(session, org.id)
    vault = VaultDocument(organization_id=org.id, original_filename="bill.pdf", content_type="application/pdf", size_bytes=len(content), sha256=hashlib.sha256(content).hexdigest(), object_key=f"tests/{suffix}", storage_backend="s3", storage_bucket="tests", document_type="OTHER_EVIDENCE", status="available")
    session.add(vault); session.flush()
    assurance = AssuranceDocument(organization_id=org.id, vault_document_id=vault.id, semantic_document_type="UNKNOWN", processing_status="EXTRACTED"); session.add(assurance); session.flush()
    operation = UsLaceyOperation(organization_id=org.id, client_reference=f"engine2-{suffix}", status="NEW", document_count=1, merchandise_line_count=0); session.add(operation); session.flush()
    link = UsLaceyOperationDocument(organization_id=org.id, operation_id=operation.id, assurance_document_id=assurance.id, document_role=role, version_number=1, is_current=True); session.add(link); session.commit()
    return org.id, operation.id, link.id, assurance.id, vault.id, vault.sha256


def add_test_document(factory, *, organization_id, operation_id, role, filename,
                      content, version_number=1, is_current=True):
    """Persist one real Assurance/Vault/operation-document prerequisite."""
    session = tenant_session(factory, organization_id)
    suffix = uuid4().hex
    vault = VaultDocument(organization_id=organization_id, original_filename=filename,
        content_type="application/pdf", size_bytes=len(content),
        sha256=hashlib.sha256(content).hexdigest(), object_key=f"tests/{suffix}",
        storage_backend="s3", storage_bucket="tests", document_type="OTHER_EVIDENCE",
        status="available")
    session.add(vault); session.flush()
    assurance = AssuranceDocument(organization_id=organization_id, vault_document_id=vault.id,
        semantic_document_type="UNKNOWN", processing_status="EXTRACTED")
    session.add(assurance); session.flush()
    link = UsLaceyOperationDocument(organization_id=organization_id, operation_id=operation_id,
        assurance_document_id=assurance.id, document_role=role,
        version_number=version_number, is_current=is_current)
    session.add(link); session.commit()
    result = (link.id, assurance.id, vault.id, vault.sha256)
    session.close()
    return result

class FakeVault:
    def __init__(self, content): self.content = content
    @contextmanager
    def materialize_verified_download(self, **_):
        yield type("Download", (), {"iter_chunks": lambda self: iter((self.content,))})()
