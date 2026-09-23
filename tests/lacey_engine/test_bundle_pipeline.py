from __future__ import annotations

import pytest

from litoral_trace.lacey_engine.domain import (
    BundleResolution,
    DocumentResolution,
    DocumentSection,
    DocumentType,
    LayoutBlock,
    LogicalDocumentResolution,
    ParsedLayout,
)
from litoral_trace.lacey_engine.errors import (
    LaceyEngineError,
    UnsupportedDocumentDomainError,
)
from litoral_trace.lacey_engine.pipeline import (
    _slice_layout,
    process_bundle,
    process_document,
)
from litoral_trace.lacey_engine.serialization import (
    BUNDLE_RESOLUTION_SCHEMA_VERSION,
    DOCUMENT_RESOLUTION_SCHEMA_VERSION,
    deserialize_bundle_resolution,
    serialize_bundle_resolution,
    serialize_document_resolution,
)


def _logical(
    *,
    logical_id: str,
    filename: str = "bundle.pdf",
    page_start: int,
    page_end: int,
    physical_page_count: int = 4,
    document_type: DocumentType = DocumentType.COMMERCIAL_INVOICE,
) -> LogicalDocumentResolution:
    blocks = tuple(
        LayoutBlock(
            block_id=f"p{page}-l1",
            page=page,
            bbox=None,
            text=f"page {page}",
            block_type="TEXT_LINE",
        )
        for page in range(page_start, page_end + 1)
    )
    section = DocumentSection(
        section_id=logical_id,
        page_start=page_start,
        page_end=page_end,
        document_type=document_type,
        confidence=1.0,
        block_ids=tuple(block.block_id for block in blocks),
    )
    resolution = DocumentResolution(
        filename=filename,
        engine_version="test",
        document_type=document_type,
        type_confidence=1.0,
        layout=ParsedLayout(
            blocks=blocks,
            page_count=physical_page_count,
        ),
        sections=(section,),
        fields={},
    )
    return LogicalDocumentResolution(
        logical_document_id=logical_id,
        parent_filename=filename,
        page_start=page_start,
        page_end=page_end,
        document_type=document_type,
        type_confidence=1.0,
        resolution=resolution,
    )


def test_bundle_requires_unique_logical_document_ids():
    first = _logical(
        logical_id="same",
        page_start=1,
        page_end=2,
    )
    second = _logical(
        logical_id="same",
        page_start=3,
        page_end=4,
    )

    with pytest.raises(ValueError, match="must be unique"):
        BundleResolution(
            filename="bundle.pdf",
            engine_version="test",
            page_count=4,
            documents=(first, second),
        )


def test_bundle_rejects_page_gap():
    first = _logical(
        logical_id="one",
        page_start=1,
        page_end=1,
    )
    second = _logical(
        logical_id="two",
        page_start=3,
        page_end=4,
    )

    with pytest.raises(ValueError, match="page gap"):
        BundleResolution(
            filename="bundle.pdf",
            engine_version="test",
            page_count=4,
            documents=(first, second),
        )


def test_bundle_rejects_page_overlap():
    first = _logical(
        logical_id="one",
        page_start=1,
        page_end=3,
    )
    second = _logical(
        logical_id="two",
        page_start=3,
        page_end=4,
    )

    with pytest.raises(ValueError, match="overlap"):
        BundleResolution(
            filename="bundle.pdf",
            engine_version="test",
            page_count=4,
            documents=(first, second),
        )


def test_bundle_requires_complete_physical_page_coverage():
    only = _logical(
        logical_id="one",
        page_start=1,
        page_end=3,
    )

    with pytest.raises(ValueError, match="every physical page"):
        BundleResolution(
            filename="bundle.pdf",
            engine_version="test",
            page_count=4,
            documents=(only,),
        )


def test_logical_document_rejects_block_outside_physical_range():
    block = LayoutBlock(
        "p4",
        4,
        None,
        "outside",
        "TEXT_LINE",
    )
    section = DocumentSection(
        "logical-001",
        1,
        2,
        DocumentType.COMMERCIAL_INVOICE,
        1.0,
        ("p4",),
    )
    resolution = DocumentResolution(
        "bundle.pdf",
        "test",
        DocumentType.COMMERCIAL_INVOICE,
        1.0,
        ParsedLayout((block,), 4),
        (section,),
        {},
    )

    with pytest.raises(ValueError, match="original physical page"):
        LogicalDocumentResolution(
            "logical-001",
            "bundle.pdf",
            1,
            2,
            DocumentType.COMMERCIAL_INVOICE,
            1.0,
            resolution,
        )


def test_slice_layout_preserves_physical_page_numbers_and_page_count():
    layout = ParsedLayout(
        blocks=(
            LayoutBlock("p1", 1, None, "page one", "TEXT_LINE"),
            LayoutBlock("p3", 3, None, "page three", "TEXT_LINE"),
            LayoutBlock("p4", 4, None, "page four", "TEXT_LINE"),
            LayoutBlock("p5", 5, None, "page five", "TEXT_LINE"),
        ),
        page_count=5,
    )

    sliced = _slice_layout(
        layout,
        page_start=3,
        page_end=4,
    )

    assert sliced.page_count == 5
    assert [block.page for block in sliced.blocks] == [3, 4]
    assert [block.block_id for block in sliced.blocks] == ["p3", "p4"]


def test_process_document_remains_compatible_for_single_logical_document(
    monkeypatch,
):
    layout = ParsedLayout(
        blocks=(
            LayoutBlock(
                "p1",
                1,
                None,
                "COMMERCIAL INVOICE",
                "TEXT_LINE",
            ),
        ),
        page_count=1,
    )
    monkeypatch.setattr(
        "litoral_trace.lacey_engine.pipeline.parse_layout",
        lambda *_args, **_kwargs: layout,
    )

    resolution = process_document(
        filename="invoice.pdf",
        content=b"unused",
    )

    assert resolution.document_type is DocumentType.COMMERCIAL_INVOICE
    assert resolution.filename == "invoice.pdf"


def test_process_document_fails_closed_for_multi_document_bundle(
    monkeypatch,
):
    layout = ParsedLayout(
        blocks=(
            LayoutBlock(
                "p1",
                1,
                None,
                "COMMERCIAL INVOICE",
                "TEXT_LINE",
            ),
            LayoutBlock(
                "p2",
                2,
                None,
                "OCEAN BILL OF LADING",
                "TEXT_LINE",
            ),
        ),
        page_count=2,
    )
    sections = (
        DocumentSection(
            "section-1",
            1,
            1,
            DocumentType.COMMERCIAL_INVOICE,
            1.0,
            ("p1",),
        ),
        DocumentSection(
            "section-2",
            2,
            2,
            DocumentType.BILL_OF_LADING,
            1.0,
            ("p2",),
        ),
    )
    monkeypatch.setattr(
        "litoral_trace.lacey_engine.pipeline.parse_layout",
        lambda *_args, **_kwargs: layout,
    )
    monkeypatch.setattr(
        "litoral_trace.lacey_engine.pipeline.segment",
        lambda *_args, **_kwargs: sections,
    )

    bundle = process_bundle(
        filename="bundle.pdf",
        content=b"unused",
    )
    assert len(bundle.documents) == 2

    with pytest.raises(LaceyEngineError, match="must use process_bundle"):
        process_document(
            filename="bundle.pdf",
            content=b"unused",
        )


def test_bundle_serialization_round_trip_preserves_logical_pages():
    bundle = BundleResolution(
        filename="bundle.pdf",
        engine_version="test",
        page_count=4,
        documents=(
            _logical(
                logical_id="logical-001",
                page_start=1,
                page_end=2,
            ),
            _logical(
                logical_id="logical-002",
                page_start=3,
                page_end=4,
                document_type=DocumentType.BILL_OF_LADING,
            ),
        ),
    )

    payload = serialize_bundle_resolution(bundle)
    assert (
        payload["schema_version"]
        == BUNDLE_RESOLUTION_SCHEMA_VERSION
        == "lacey_bundle_resolution_v1"
    )

    restored = deserialize_bundle_resolution(payload)
    assert restored == bundle


def test_existing_document_schema_version_remains_unchanged():
    document = _logical(
        logical_id="logical-001",
        page_start=1,
        page_end=1,
        physical_page_count=1,
    ).resolution

    payload = serialize_document_resolution(document)

    assert (
        payload["schema_version"]
        == DOCUMENT_RESOLUTION_SCHEMA_VERSION
        == "lacey_document_resolution_v1"
    )



def test_process_bundle_rejects_when_segmentation_finds_no_supported_logical_document(
    monkeypatch,
):
    layout = ParsedLayout(
        blocks=(
            LayoutBlock(
                "p1",
                1,
                None,
                "Administrative memorandum with no shipment document identity.",
                "TEXT_LINE",
            ),
        ),
        page_count=1,
    )
    monkeypatch.setattr(
        "litoral_trace.lacey_engine.pipeline.parse_layout",
        lambda *_args, **_kwargs: layout,
    )

    with pytest.raises(UnsupportedDocumentDomainError) as excinfo:
        process_bundle(
            filename="unsupported.pdf",
            content=b"%PDF-stubbed-for-domain-fallback",
        )

    assert excinfo.value.code == "UNSUPPORTED_DOMAIN"
    assert excinfo.value.domain == "UNSUPPORTED"
