from __future__ import annotations

from sqlalchemy import select

from litoral_trace.assurance.processing import AssuranceProcessingService
from litoral_trace.db.models import DocumentExtractionRun, VaultDocument
from tests.test_assurance_processing_safety import (
    FakeVaultService, factory, patch_successful_pipeline, seed,
)


def test_extraction_cache_reuses_only_compatible_version_without_duplicating_blob(monkeypatch):
    sessions = factory()
    public_id = seed(sessions, status="UPLOADED")
    patch_successful_pipeline(monkeypatch, version="cache-test-v1")
    service = AssuranceProcessingService(session_factory=sessions, vault_service=FakeVaultService())
    arguments = {"organization_id": 42, "assurance_public_id": public_id}
    assert service.process(**arguments) == "EXTRACTED"
    assert service.process(**arguments) == "EXTRACTED"
    with sessions() as session:
        assert len(session.scalars(select(DocumentExtractionRun)).all()) == 1

    patch_successful_pipeline(monkeypatch, version="cache-test-v2")
    assert service.process(**arguments) == "EXTRACTED"
    with sessions() as session:
        runs = session.scalars(select(DocumentExtractionRun).order_by(DocumentExtractionRun.id)).all()
        assert [run.engine_version for run in runs] == ["cache-test-v1", "cache-test-v2"]
        assert all(run.status == "SUCCEEDED" for run in runs)
        assert len(session.scalars(select(VaultDocument)).all()) == 1


def test_terminal_document_without_versioned_run_is_not_a_cache_hit(monkeypatch):
    sessions = factory()
    public_id = seed(sessions, status="EXTRACTED")
    patch_successful_pipeline(monkeypatch, version="cache-test-v2")
    service = AssuranceProcessingService(session_factory=sessions, vault_service=FakeVaultService())
    assert service.process(organization_id=42, assurance_public_id=public_id) == "EXTRACTED"
    with sessions() as session:
        runs = session.scalars(select(DocumentExtractionRun)).all()
        assert len(runs) == 1
        assert runs[0].engine_version == "cache-test-v2"


def test_material_extraction_config_change_invalidates_reuse(monkeypatch):
    sessions = factory()
    public_id = seed(sessions, status="UPLOADED")
    patch_successful_pipeline(monkeypatch, version="cache-test-v1")
    monkeypatch.setenv("LT_ASSURANCE_RAW_CELL_PERSIST_LIMIT", "2000")
    service = AssuranceProcessingService(session_factory=sessions, vault_service=FakeVaultService())
    arguments = {"organization_id": 42, "assurance_public_id": public_id}
    assert service.process(**arguments) == "EXTRACTED"
    monkeypatch.setenv("LT_ASSURANCE_RAW_CELL_PERSIST_LIMIT", "3000")
    assert service.process(**arguments) == "EXTRACTED"
    assert service.process(**arguments) == "EXTRACTED"
    with sessions() as session:
        runs = session.scalars(select(DocumentExtractionRun).order_by(DocumentExtractionRun.id)).all()
        assert len(runs) == 2
        assert runs[0].extraction_metadata["cache_identity"] != runs[1].extraction_metadata["cache_identity"]
        assert len(session.scalars(select(VaultDocument)).all()) == 1
