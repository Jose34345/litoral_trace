from __future__ import annotations
from .domain import DocumentSection, DocumentType, ParsedLayout


def segment(layout: ParsedLayout, document_type: DocumentType) -> tuple[DocumentSection, ...]:
    relevant = tuple(block.block_id for block in layout.blocks)
    return (DocumentSection("section-1", 1, layout.page_count, document_type, 1.0, relevant),)
