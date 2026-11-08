"""Resolve duplicate physical renderings of an explicitly labeled party cell."""
from __future__ import annotations

from .domain import LayoutBlock, LayoutStructureType, ParsedLayout


def complete_party_source(
    layout: ParsedLayout, block: LayoutBlock, *, label: str, value: str,
) -> tuple[str, LayoutBlock]:
    """Use a complete cell only when geometry and literal text prove same origin.

    Do not join arbitrary adjacent lines, infer postal components, or reconcile
    distinct addresses by prefix similarity. Original layout blocks stay intact.
    Multiple possible cells abstain, preserving every independent candidate.
    """
    if block.block_type != "TEXT_LINE" or block.bbox is None:
        return value, block
    matches: list[LayoutBlock] = []
    for cell in layout.blocks:
        if (
            cell.structure_type is not LayoutStructureType.KEY_VALUE_TABLE
            or cell.page != block.page
            or cell.bbox is None
            or str(cell.key_text or "").strip().casefold() != label.strip().casefold()
        ):
            continue
        full_value = " ".join(str(cell.value_text or "").split())
        if not full_value.startswith(value) or full_value == value:
            continue
        # Only a complete word/value prefix can be a wrapped rendering.
        if not full_value[len(value):].startswith(" "):
            continue
        source, rendered = cell.bbox, block.bbox
        if (
            source.x0 <= rendered.x0 and source.top <= rendered.top
            and source.x1 >= rendered.x1 and source.bottom >= rendered.bottom
        ):
            matches.append(cell)
    if len(matches) != 1:
        return value, block
    source = matches[0]
    return " ".join(str(source.value_text).split()), source
