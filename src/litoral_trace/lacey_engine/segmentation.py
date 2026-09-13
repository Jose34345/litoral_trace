from __future__ import annotations
from .classifier import classify_text
from .domain import DocumentSection, DocumentType, ParsedLayout


def segment(layout: ParsedLayout, document_type: DocumentType) -> tuple[DocumentSection, ...]:
    sections: list[DocumentSection] = []
    previous = document_type
    for page in range(1, layout.page_count + 1):
        blocks = tuple(block for block in layout.blocks if block.page == page)
        kind, confidence = classify_text(" ".join(block.text for block in blocks))
        if kind is DocumentType.UNKNOWN: kind, confidence = previous, 0.5
        else: previous = kind
        ids = tuple(block.block_id for block in blocks)
        if sections and sections[-1].document_type is kind:
            old = sections[-1]; sections[-1] = DocumentSection(old.section_id, old.page_start, page, kind, max(old.confidence, confidence), old.block_ids + ids)
        else: sections.append(DocumentSection(f"section-{len(sections)+1}", page, page, kind, confidence, ids))
    return tuple(sections)
