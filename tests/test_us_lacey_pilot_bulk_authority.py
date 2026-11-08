"""Pilot bulk review enforces current source authority before confirming fields."""
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from litoral_trace.db.models import (
    AssuranceDocument, ReconciliationIssue, UsLaceyFieldCandidate, UsLaceyOperation,
    UsLaceyOperationDocument, UsLaceyOperationField, UsLaceyPlantDeclaration,
    UsLaceyPpqPlantLine, UsLaceyProcessingJob, VaultDocument,
)
from litoral_trace.us_lacey import review


@pytest.mark.parametrize(
    "field_name,value,role,filename,is_current,source_page,conflict,expected",
    [
        ("hts_code", "4407110000", "SUPPLIER_SHEET",
         "original-data.csv", True, None, False, "MATCHED"),
        ("bill_of_lading", "MAEU-TEST-260921-01", "SUPPLIER_SHEET",
         "original-data.csv", True, None, False, "FOUND"),
        ("bill_of_lading", "MAEU-TEST-260921-01", "SUPPLIER_DECLARATION",
         "02_Botanical_Supplier_Declaration.pdf", True, 1, False, "FOUND"),
        ("estimated_arrival_date", "2026-11-15", "COMMERCIAL_INVOICE",
         "05_Entry_Preparation_Worksheet.pdf", True, 1, False, "FOUND"),
        ("estimated_arrival_date", "2026-11-15", "SUPPLIER_DECLARATION",
         "vendor_record.pdf", True, 1, False, "FOUND"),
        ("estimated_arrival_date", "2026-11-15", "ARRIVAL_NOTICE",
         "02_Botanical_Supplier_Declaration.pdf", True, 1, False, "FOUND"),
        ("estimated_arrival_date", "2026-11-15", "ARRIVAL_NOTICE",
         "arrival-notice.pdf", True, 1, False, "MATCHED"),
        ("bill_of_lading", "MAEU-TEST-260921-01", "BILL_OF_LADING",
         "03_Bill_of_Lading.pdf", False, 1, False, "FOUND"),
        ("bill_of_lading", "MAEU-TEST-260921-01", "BILL_OF_LADING",
         "03_Bill_of_Lading.pdf", True, None, False, "FOUND"),
        ("bill_of_lading", "MAEU-TEST-260921-01", "BILL_OF_LADING",
         "03_Bill_of_Lading.pdf", True, 1, True, "FOUND"),
        ("bill_of_lading", "MAEU-TEST-260921-01", "BILL_OF_LADING",
         "03_Bill_of_Lading.pdf", True, 1, False, "MATCHED"),
    ],
    ids=[
        "tabular-hts", "tabular-bl", "botanical-bl", "derived-eta", "supplier-eta", "role-conflict-eta", "carrier-eta",
        "superseded-bl", "unanchored-bl", "conflicting-bl", "carrier-bl",
    ],
)
def test_bulk_review_only_matches_authoritative_anchored_values(
    monkeypatch, field_name, value, role, filename, is_current, source_page, conflict,
    expected,
):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    try:
        for model in (
            UsLaceyOperation, VaultDocument, AssuranceDocument,
            UsLaceyOperationDocument, UsLaceyOperationField, UsLaceyPpqPlantLine, ReconciliationIssue,
        ):
            model.__table__.create(engine)
        factory = sessionmaker(bind=engine, expire_on_commit=False)
        with factory.begin() as session:
            operation = UsLaceyOperation(organization_id=1, client_reference="SYNTHETIC")
            tabular = role == "SUPPLIER_SHEET"
            vault = VaultDocument(
                organization_id=1, original_filename=filename, size_bytes=128,
                sha256="a" * 64, content_type="text/csv" if tabular else "application/pdf",
                object_key="synthetic",
                storage_bucket="synthetic", document_type="OTHER_EVIDENCE", status="available",
            )
            session.add_all((operation, vault))
            session.flush()
            document = AssuranceDocument(
                organization_id=1, vault_document_id=vault.id,
                semantic_document_type="SPREADSHEET" if tabular else "UNKNOWN",
            )
            session.add(document)
            session.flush()
            session.add(UsLaceyOperationDocument(
                organization_id=1, operation_id=operation.id,
                assurance_document_id=document.id, document_role=role, is_current=is_current,
            ))
            plant_line_id = None
            if field_name == "hts_code":
                plant_line = UsLaceyPpqPlantLine(
                    organization_id=1, operation_id=operation.id, line_reference="1", ordinal=1,
                )
                session.add(plant_line)
                session.flush()
                plant_line_id = plant_line.id
            field = UsLaceyOperationField(
                organization_id=1, operation_id=operation.id, field_scope="PLANT_LINE" if field_name == "hts_code" else "SHIPMENT",
                field_name=field_name, plant_line_id=plant_line_id, normalized_value=value, field_status="FOUND",
                validation_status="VALID", source_assurance_document_id=document.id,
                source_page=source_page,
                source_locator="csv:header_row:1;data_row:1;column:1" if tabular else "p1:block:4",
                extractor=review.PARSER_ENGINE if tabular else None,
            )
            session.add(field)
            session.flush()
            if conflict:
                session.add(ReconciliationIssue(
                    organization_id=1, operation_reference=f"us_lacey:{operation.public_id}",
                    fingerprint="b" * 64, rule_code="SYNTHETIC", left_source="first",
                    explanation="Conflicting carrier reference", us_lacey_operation_field_id=field.id,
                ))

        audit_events = []
        monkeypatch.setattr(review, "get_us_lacey_db_session", factory)
        monkeypatch.setattr(review, "review_journal_enabled", lambda *_args: False)
        monkeypatch.setattr(review, "_ensure_documentary_identity_for_review", lambda *_args, **_kwargs: 0)
        monkeypatch.setattr(review, "reconcile_entered_value_invariant", lambda *_args, **_kwargs: None)
        monkeypatch.setattr(review, "refresh_regulatory_assessment_snapshot_for_review", lambda *_args, **_kwargs: None)
        monkeypatch.setattr(review, "refresh_us_lacey_operation_status", lambda *_args, **_kwargs: "REVIEW_REQUIRED")
        monkeypatch.setattr(review, "promote_reviewed_field", lambda *_args, **_kwargs: SimpleNamespace(promoted=False))
        monkeypatch.setattr(review, "record_audit_event", lambda *_args, **kwargs: audit_events.append(kwargs))
        monkeypatch.setattr(review, "append_operation_event", lambda *_args, **_kwargs: None)

        result = review.accept_supported_us_lacey_fields(
            organization_id=1, operation_public_id=operation.public_id,
            user_id=17, user_email="reviewer@example.test",
        )

        with factory() as session:
            saved = session.scalar(select(UsLaceyOperationField))
            assert saved.field_status == expected
            assert saved.source_assurance_document_id == document.id
            assert saved.source_page == source_page
            if expected == "MATCHED":
                assert saved.human_value == value
                assert saved.reviewed_by_user_id == 17
                assert saved.reviewed_at is not None
                assert result.accepted_count == 1
                assert audit_events[0]["metadata"]["accepted_field_ids"] == [saved.id]
            else:
                assert saved.human_value is None
                assert saved.reviewed_by_user_id is None
                assert saved.reviewed_at is None
                assert result.accepted_count == 0
                assert audit_events == []
            if conflict:
                assert session.scalar(select(ReconciliationIssue.status)) == "OPEN"
    finally:
        engine.dispose()

@pytest.mark.parametrize("entrypoint", ["finalize", "completed_export", "completed_conflict"])
def test_unresolved_value_blocks_filing_review_and_legacy_completed_export(monkeypatch, entrypoint):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    try:
        for model in (
            UsLaceyOperation, UsLaceyOperationField, ReconciliationIssue,
            UsLaceyProcessingJob, UsLaceyPpqPlantLine, UsLaceyPlantDeclaration,
            UsLaceyFieldCandidate, AssuranceDocument,
        ):
            model.__table__.create(engine)
        factory = sessionmaker(bind=engine, expire_on_commit=False)
        before_status = "READY_FOR_REVIEW" if entrypoint == "finalize" else "COMPLETED"
        before_field_status = "MATCHED" if entrypoint == "completed_conflict" else "FOUND"
        with factory.begin() as session:
            operation = UsLaceyOperation(
                organization_id=1, client_reference="SYNTHETIC",
                status=before_status, document_count=1,
            )
            session.add(operation)
            session.flush()
            plant_line = UsLaceyPpqPlantLine(
                organization_id=1, operation_id=operation.id, line_reference="1", ordinal=1,
            )
            session.add(plant_line)
            session.flush()
            session.add(UsLaceyPlantDeclaration(
                organization_id=1, plant_line_id=plant_line.id, ordinal=1,
            ))
            session.add(UsLaceyOperationField(
                organization_id=1, operation_id=operation.id, field_scope="SHIPMENT",
                field_name="filing_entry_reference", merchandise_line_reference="__shipment__",
                normalized_value="12345678901", field_status=before_field_status, validation_status="VALID",
            ))

            if entrypoint == "completed_conflict":
                session.add(ReconciliationIssue(
                    organization_id=1, operation_reference=f"us_lacey:{operation.public_id}",
                    fingerprint="b" * 64, rule_code="SYNTHETIC", left_source="first",
                    explanation="Conflicting entry information",
                ))

        monkeypatch.setattr(review, "get_us_lacey_db_session", factory)
        monkeypatch.setattr(
            review, "reconcile_entered_value_invariant",
            lambda *_args, **_kwargs: SimpleNamespace(evaluated=False),
        )
        monkeypatch.setattr(review, "refresh_regulatory_assessment_snapshot_for_review", lambda *_args, **_kwargs: None)
        monkeypatch.setattr(review, "record_audit_event", lambda *_args, **_kwargs: None)
        monkeypatch.setattr(review, "append_operation_event", lambda *_args, **_kwargs: None)

        expected_error = ("unresolved fields or conflicts" if entrypoint == "completed_conflict"
                          else "Confirm the auto-resolved values first")
        with pytest.raises(review.UsLaceyReviewError, match=expected_error):
            if entrypoint == "finalize":
                review.finalize_us_lacey_review(
                    organization_id=1, operation_public_id=operation.public_id,
                    user_id=17, user_email="reviewer@example.test",
                )
            else:
                review.export_us_lacey_csv(
                    organization_id=1, operation_public_id=operation.public_id,
                )

        with factory() as session:
            assert session.scalar(select(UsLaceyOperation.status)) == before_status
            assert session.scalar(select(UsLaceyOperationField.field_status)) == before_field_status
    finally:
        engine.dispose()
