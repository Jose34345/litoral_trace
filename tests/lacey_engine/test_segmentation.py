from __future__ import annotations

from litoral_trace.lacey_engine.domain import (
    DocumentType,
    LayoutBlock,
    ParsedLayout,
)
from litoral_trace.lacey_engine.segmentation import segment


def test_strong_anchor_beats_shared_bill_fingerprint_across_broker_packet():
    shared_bill = "B/L No: MBL-GOLDEN-2026"
    layout = ParsedLayout(
        blocks=(
            LayoutBlock(
                "p1",
                1,
                None,
                f"COMMERCIAL INVOICE\n{shared_bill}\nInvoice Number: INV-GOLDEN-001",
                "TEXT_LINE",
            ),
            LayoutBlock(
                "p2",
                2,
                None,
                f"PACKING LIST\n{shared_bill}\nContainer Number: MSKU0857658",
                "TEXT_LINE",
            ),
            LayoutBlock(
                "p3",
                3,
                None,
                f"OCEAN BILL OF LADING\n{shared_bill}\nContainer Number: MSKU0857658",
                "TEXT_LINE",
            ),
            LayoutBlock(
                "p4",
                4,
                None,
                f"BOTANICAL DECLARATION\n{shared_bill}\nGenus: Tectona\nSpecies: grandis",
                "TEXT_LINE",
            ),
        ),
        page_count=4,
    )

    sections = segment(layout, DocumentType.COMMERCIAL_INVOICE)

    assert [
        (section.document_type, section.page_start, section.page_end)
        for section in sections
    ] == [
        (DocumentType.COMMERCIAL_INVOICE, 1, 1),
        (DocumentType.PACKING_LIST, 2, 2),
        (DocumentType.BILL_OF_LADING, 3, 3),
        (DocumentType.SPECIES_DECLARATION, 4, 4),
    ]
