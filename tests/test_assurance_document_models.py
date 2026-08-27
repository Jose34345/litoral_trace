from litoral_trace.assurance.domain import (
    AssuranceDocumentType,
    DocumentProcessingStatus,
)
from litoral_trace.db.models import (
    AssuranceDocument,
    DocumentClaim,
    DocumentEntityLink,
    DocumentExtractionRun,
    ExtractedDocumentField,
    VaultDocument,
)


def test_semantic_document_catalog_covers_pilot_inputs():
    assert {value.value for value in AssuranceDocumentType} == {
        "INVOICE",
        "DELIVERY_NOTE",
        "FOREST_GUIDE",
        "PHYTOSANITARY_CERTIFICATE",
        "CUSTOMS_DOCUMENT",
        "SPREADSHEET",
        "UNKNOWN",
    }


def test_assurance_document_is_vault_backed_and_tenant_scoped():
    columns = AssuranceDocument.__table__.c
    assert columns.organization_id.nullable is False
    assert columns.vault_document_id.nullable is False
    assert columns.semantic_document_type.nullable is False
    assert columns.processing_status.nullable is False
    assert "valid_from" in columns
    assert "valid_until" in columns

    constraints = {
        constraint.name: {column.name for column in constraint.columns}
        for constraint in AssuranceDocument.__table__.constraints
        if constraint.name and hasattr(constraint, "columns")
    }
    assert constraints["uq_assurance_documents_tenant_vault_document"] == {
        "organization_id",
        "vault_document_id",
    }


def test_vault_already_owns_original_integrity_hash():
    columns = VaultDocument.__table__.c
    assert columns.sha256.nullable is False
    assert columns.object_key.nullable is False
    assert columns.content_type.nullable is False


def test_extraction_run_records_engine_version_status_and_errors():
    columns = DocumentExtractionRun.__table__.c
    assert {
        "organization_id",
        "assurance_document_id",
        "engine",
        "engine_version",
        "status",
        "started_at",
        "completed_at",
        "extraction_metadata",
        "error_code",
        "error_detail",
    }.issubset(columns.keys())


def test_extracted_field_keeps_original_normalized_confidence_and_provenance():
    columns = ExtractedDocumentField.__table__.c
    assert {
        "field_name",
        "original_value",
        "normalized_value",
        "value_type",
        "confidence",
        "confidence_level",
        "source_page",
        "source_locator",
        "auto_accepted",
        "needs_review",
    }.issubset(columns.keys())


def test_claim_represents_assertion_scope_validity_and_integrity():
    columns = DocumentClaim.__table__.c
    assert {
        "claim_type",
        "issuer",
        "subject_type",
        "subject_reference",
        "statement",
        "scope_json",
        "valid_from",
        "valid_until",
        "integrity_hash",
    }.issubset(columns.keys())


def test_entity_link_supports_supplier_lot_order_shipment_and_operation():
    columns = DocumentEntityLink.__table__.c
    assert {
        "entity_type",
        "entity_reference",
        "link_confidence",
        "link_method",
        "human_confirmed",
    }.issubset(columns.keys())


def test_processing_statuses_include_human_review_state():
    assert {value.value for value in DocumentProcessingStatus} == {
        "UPLOADED",
        "PROCESSING",
        "EXTRACTED",
        "NEEDS_REVIEW",
        "FAILED",
    }
