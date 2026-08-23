from __future__ import annotations

from types import SimpleNamespace

import pytest

from litoral_trace.config.settings import StorageSettings
from litoral_trace.services.traceability_evidence import (
    TraceabilityEvidenceValidationError,
    _validate_evidence_document_content_type,
)
from litoral_trace.services.vault import (
    VaultValidationError,
    validate_vault_upload,
)


def _settings() -> StorageSettings:
    return StorageSettings(
        backend="s3",
        bucket_name="p1c-vault-test",
        max_upload_bytes=1024 * 1024,
    )


def test_p1c_vault_accepts_well_formed_utf8_xml() -> None:
    payload = b'<?xml version="1.0" encoding="UTF-8"?><ePhyto><certificate>AR-001</certificate></ePhyto>'

    upload = validate_vault_upload(
        filename="ephyto-ar-001.xml",
        document_type="OTHER_EVIDENCE",
        content_type="application/xml; charset=utf-8",
        content=payload,
        settings=_settings(),
    )

    assert upload.filename == "ephyto-ar-001.xml"
    assert upload.content_type == "application/xml"
    assert upload.content == payload
    assert len(upload.sha256) == 64


def test_p1c_vault_rejects_xml_dtd_and_entity_declarations() -> None:
    payload = b'<?xml version="1.0"?><!DOCTYPE x [<!ENTITY a "boom">]><ePhyto>&a;</ePhyto>'

    with pytest.raises(VaultValidationError, match="DTD"):
        validate_vault_upload(
            filename="unsafe.xml",
            document_type="OTHER_EVIDENCE",
            content_type="application/xml",
            content=payload,
            settings=_settings(),
        )


def test_p1c_vault_rejects_xml_with_non_xml_extension() -> None:
    with pytest.raises(VaultValidationError, match="extension"):
        validate_vault_upload(
            filename="ephyto.pdf",
            document_type="OTHER_EVIDENCE",
            content_type="application/xml",
            content=b"<ePhyto />",
            settings=_settings(),
        )


def test_p1c_evidence_semantics_require_real_xml_for_ephyto() -> None:
    with pytest.raises(TraceabilityEvidenceValidationError) as exc_info:
        _validate_evidence_document_content_type(
            "EPHYTO_XML",
            SimpleNamespace(content_type="application/pdf"),
        )
    assert exc_info.value.code == "EVIDENCE_CONTENT_TYPE_MISMATCH"

    _validate_evidence_document_content_type(
        "EPHYTO_XML",
        SimpleNamespace(content_type="application/xml"),
    )


def test_p1c_paper_certificate_semantics_require_pdf() -> None:
    with pytest.raises(TraceabilityEvidenceValidationError):
        _validate_evidence_document_content_type(
            "PHYTOSANITARY_CERTIFICATE",
            SimpleNamespace(content_type="application/xml"),
        )

    _validate_evidence_document_content_type(
        "PHYTOSANITARY_CERTIFICATE",
        SimpleNamespace(content_type="application/pdf"),
    )
