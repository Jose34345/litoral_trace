from litoral_trace.assurance.domain import DocumentProcessingStatus, DocumentType
from litoral_trace.db.models import (
    AssuranceDocument,
    DocumentClaim,
    DocumentEntityLink,
    DocumentExtractionRun,
    ExtractedField,
)


def test_document_catalog_contains_pilot_types():
    assert DocumentType.INVOICE.value == "INVOICE"
    assert DocumentType.DELIVERY_NOTE.value == "DELIVERY_NOTE"
    assert DocumentType.FOREST_GUIDE.value == "FOREST_GUIDE"
    assert DocumentType.PHYTOSANITARY_CERTIFICATE.value == "PHYTOSANITARY_CERTIFICATE"
    assert DocumentType.CUSTOMS_DOCUMENT.value == "CUSTOMS_DOCUMENT"
    assert DocumentType.SPREADSHEET.value == "SPREADSHEET"
    assert DocumentType.UNKNOWN.value == "UNKNOWN"


def test_document_has_tenant_hash_processing_and_validity_fields():
    columns = AssuranceDocument.__table__.c

    assert columns.organization_id.nullable is False
    assert columns.sha256.nullable is False
    assert columns.filename.nullable is False
    assert columns.storage_key.nullable is False
    assert columns.document_type.nullable is False
    assert columns.processing_status.nullable is False
    assert "valid_from" in columns
    assert "valid_until" in columns


def test_document_hash_is_unique_inside_tenant():
    unique_constraints = {
        constraint.name: {column.name for column in constraint.columns}
        for constraint in AssuranceDocument.__table__.constraints
        if constraint.name
    }

    assert unique_constraints["uq_assurance_document_org_sha256"] == {
        "organization_id",
        "sha256",
    }


def test_extraction_run_records_engine_and_outcome():
    columns = DocumentExtractionRun.__table__.c

    assert columns.organization_id.nullable is False
    assert columns.document_id.nullable is False
    assert columns.engine.nullable is False
    assert columns.engine_version.nullable is False
    assert columns.status.nullable is False
    assert "error_code" in columns
    assert "error_detail" in columns


def test_extracted_field_keeps_original_normalized_confidence_and_provenance():
    columns = ExtractedField.__table__.c

    required = {
        "organization_id",
        "document_id",
        "extraction_run_id",
        "field_name",
        "original_value",
        "normalized_value",
        "confidence",
        "confidence_level",
        "source_page",
        "source_locator",
        "auto_accepted",
        "needs_review",
    }
    assert required.issubset(columns.keys())


def test_claim_models_assertion_scope_validity_and_integrity():
    columns = DocumentClaim.__table__.c

    assert {
        "claim_type",
        "issuer",
        "subject_type",
        "subject_id",
        "statement",
        "scope",
        "valid_from",
        "valid_until",
        "integrity_hash",
    }.issubset(columns.keys())


def test_entity_link_supports_operational_binding():
    columns = DocumentEntityLink.__table__.c

    assert {
        "organization_id",
        "document_id",
        "entity_type",
        "entity_id",
        "link_confidence",
        "link_method",
        "human_confirmed",
    }.issubset(columns.keys())


def test_processing_status_catalog_is_stable():
    assert {status.value for status in DocumentProcessingStatus} == {
        "UPLOADED",
        "PROCESSING",
        "EXTRACTED",
        "NEEDS_REVIEW",
        "FAILED",
    }
