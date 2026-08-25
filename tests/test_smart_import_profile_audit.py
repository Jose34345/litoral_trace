from __future__ import annotations

from io import BytesIO
from unittest.mock import MagicMock, patch
from uuid import uuid4

from openpyxl import Workbook
from sqlalchemy.exc import SQLAlchemyError
import pytest

from litoral_trace.db.models.smart_import_profile import SmartImportProfile
from litoral_trace.services.audit import AuditAction, AuditOutcome
from litoral_trace.services.smart_import import SmartImportEngine, default_confirmed_mapping
from litoral_trace.services.smart_import.profiles import (
    SmartImportProfilePersistenceError,
    SmartImportProfileService,
)


def _candidate_and_mappings():
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Recepciones"
    sheet.append(
        [
            "Rodal",
            "Productor",
            "Especie",
            "Sup. ha",
            "LAT",
            "LONG",
            "Tn recibidas",
            "Stock exportable",
        ]
    )
    sheet.append(
        [
            "R-001",
            "P-001",
            "Pino",
            12.0,
            -27.4,
            -58.8,
            30.0,
            18.0,
        ]
    )
    buffer = BytesIO()
    workbook.save(buffer)
    workbook.close()

    analysis = SmartImportEngine().analyze(
        buffer.getvalue(),
        filename="cliente.xlsx",
    )
    candidate = analysis.best_candidate
    assert candidate is not None
    mappings = default_confirmed_mapping(candidate)
    assert len(mappings) == 8
    return candidate, mappings


def _new_profile_session() -> MagicMock:
    session = MagicMock()
    query_result = MagicMock()
    query_result.scalar_one_or_none.return_value = None
    session.execute.return_value = query_result

    def assign_identity(objects=None):
        if objects:
            profile = list(objects)[0]
            profile.id = 77
            profile.public_id = uuid4()

    session.flush.side_effect = assign_identity
    return session


def _existing_profile_session() -> tuple[MagicMock, SmartImportProfile]:
    session = MagicMock()
    profile = SmartImportProfile(
        id=77,
        public_id=uuid4(),
        organization_id=123,
        created_by_user_id=111,
        updated_by_user_id=111,
        name="Recepciones planta",
        schema_kind="lotes",
        sheet_name="Formato anterior",
        header_fingerprint="a" * 64,
        header_signature=["old-header"],
        mapping_json={},
        version=4,
        use_count=9,
        active=True,
    )
    query_result = MagicMock()
    query_result.scalar_one_or_none.return_value = profile
    session.execute.return_value = query_result
    return session, profile


def test_profile_create_audit_is_in_same_transaction_and_excludes_business_values() -> None:
    candidate, mappings = _candidate_and_mappings()
    session = _new_profile_session()
    service = SmartImportProfileService(session_factory=lambda: session)
    observed: dict[str, object] = {}

    def capture_audit(db_session, **kwargs):
        assert db_session is session
        assert session.commit.called is False
        observed.update(kwargs)
        return MagicMock()

    with (
        patch(
            "litoral_trace.services.smart_import.profiles.set_tenant_db_context",
            return_value=None,
        ),
        patch(
            "litoral_trace.services.smart_import.profiles.record_audit_event",
            side_effect=capture_audit,
        ),
    ):
        profile = service.remember(
            organization_id=123,
            user_id=456,
            candidate=candidate,
            mappings=mappings,
            name="Recepciones planta",
        )

    assert profile.id == 77
    assert session.commit.call_count == 1
    assert observed["action"] == AuditAction.SMART_IMPORT_PROFILE_CREATE
    assert observed["outcome"] == AuditOutcome.SUCCESS
    assert observed["entity_type"] == "smart_import_profile"
    assert observed["entity_id"] == 77
    assert observed["metadata"]["mapping_field_count"] == 8

    audit_payload = repr(observed)
    assert "R-001" not in audit_payload
    assert "P-001" not in audit_payload
    assert "Pino" not in audit_payload
    assert "tn recibidas" not in audit_payload.lower()


def test_profile_update_audit_tracks_version_without_business_values() -> None:
    candidate, mappings = _candidate_and_mappings()
    session, existing = _existing_profile_session()
    service = SmartImportProfileService(session_factory=lambda: session)
    observed: dict[str, object] = {}

    def capture_audit(db_session, **kwargs):
        assert db_session is session
        assert session.commit.called is False
        observed.update(kwargs)
        return MagicMock()

    with (
        patch(
            "litoral_trace.services.smart_import.profiles.set_tenant_db_context",
            return_value=None,
        ),
        patch(
            "litoral_trace.services.smart_import.profiles.record_audit_event",
            side_effect=capture_audit,
        ),
    ):
        profile = service.remember(
            organization_id=123,
            user_id=456,
            candidate=candidate,
            mappings=mappings,
            name="Recepciones planta",
        )

    assert profile is existing
    assert profile.version == 5
    assert profile.use_count == 10
    assert session.commit.call_count == 1
    assert observed["action"] == AuditAction.SMART_IMPORT_PROFILE_UPDATE
    assert observed["outcome"] == AuditOutcome.SUCCESS
    assert observed["before_data"]["version"] == 4
    assert observed["before_data"]["use_count"] == 9
    assert observed["after_data"]["version"] == 5
    assert observed["after_data"]["use_count"] == 10

    audit_payload = repr(observed)
    assert "R-001" not in audit_payload
    assert "P-001" not in audit_payload
    assert "Pino" not in audit_payload
    assert "rodal" not in audit_payload.lower()
    assert "tn recibidas" not in audit_payload.lower()


def test_profile_mutation_rolls_back_when_atomic_audit_write_fails() -> None:
    candidate, mappings = _candidate_and_mappings()
    session = _new_profile_session()
    service = SmartImportProfileService(session_factory=lambda: session)

    with (
        patch(
            "litoral_trace.services.smart_import.profiles.set_tenant_db_context",
            return_value=None,
        ),
        patch(
            "litoral_trace.services.smart_import.profiles.record_audit_event",
            side_effect=SQLAlchemyError("audit unavailable"),
        ),
        pytest.raises(SmartImportProfilePersistenceError),
    ):
        service.remember(
            organization_id=123,
            user_id=456,
            candidate=candidate,
            mappings=mappings,
            name="Recepciones planta",
        )

    assert session.commit.called is False
    assert session.rollback.call_count == 1
    assert session.close.call_count == 1
