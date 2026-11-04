"""One-SKU packing-list plant *net* mass observations, never gross or inferred.

Only admit literal source text when one merchandise line, material and arithmetic
are explicit on the same packing list. Downstream reconciliation and review
retain final authority; this module does not publish shipment truth.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import re

from litoral_trace.lacey_engine.domain import DocumentType, LayoutBlock, ParsedLayout


_HEADER = re.compile(r"^CARTON MARK\s+SKU\s+CARTONS\s+SETS\s*/\s*CARTON\s+TOTAL SETS$", re.I)
_ROW = re.compile(
    r"(?P<sku>[A-Z0-9]+(?:-[A-Z0-9]+)+)\s+"
    r"(?P<cartons>[0-9]{1,6})\s+(?P<sets_per_carton>[0-9]{1,6})\s+"
    r"(?P<sets_total>[0-9]{1,9})$",
    re.I,
)
_MASS = re.compile(
    r"^Total bamboo net mass\s+(?P<amount>[0-9]+(?:\.[0-9]{1,3})?)\s+(?P<unit>KG)$",
    re.I,
)
_CARTON_MASS = re.compile(
    r"^Carton weights\s+Net bamboo\s+(?P<amount>[0-9]+(?:\.[0-9]{1,3})?)"
    r"\s+KG per carton;\s*gross\s+(?P<gross>[0-9]+(?:\.[0-9]{1,3})?)"
    r"\s+KG per carton$",
    re.I,
)
_MATERIAL = re.compile(r"^Product material\s+Solid bamboo(?:;\s*natural finish)?$", re.I)


@dataclass(frozen=True, slots=True)
class PackingPlantNetMass:
    sku: str
    amount: str
    unit: str
    source_block: LayoutBlock


def extract_packing_plant_net_mass(
    layout: ParsedLayout, *, document_type: DocumentType,
) -> PackingPlantNetMass | None:
    """Do not turn consolidated logistics mass into SKU evidence by guessing."""
    if document_type is not DocumentType.PACKING_LIST:
        return None

    lines = [b for b in layout.blocks if b.block_type == "TEXT_LINE"]
    heads = [i for i, block in enumerate(lines) if _HEADER.fullmatch(block.text.strip())]
    if len(heads) != 1:
        return None
    start = heads[0]
    # One (and only one) row before product details; extra/unknown table rows
    # invalidate single-SKU authority rather than letting the first row win.
    details = next(
        (i for i in range(start + 1, len(lines)) if lines[i].text.startswith("Goods ")),
        None,
    )
    if details is None or details <= start + 1 or details - start > 5:
        return None
    raw_rows = lines[start + 1:details]
    # Printed carton marks sometimes wrap their numeric range onto the next
    # physical text line. It is not a second merchandise row.
    if len(raw_rows) == 2 and re.fullmatch(
        r"[0-9]{1,6}-[0-9]{1,6}", raw_rows[1].text.strip(),
    ):
        raw_rows = raw_rows[:1]
    if len(raw_rows) != 1:
        return None
    row = _ROW.search(raw_rows[0].text.strip())
    if row is None:
        return None

    mass_matches = [(b, _MASS.fullmatch(b.text.strip())) for b in lines]
    mass_matches = [(b, m) for b, m in mass_matches if m is not None]
    carton_matches = [_CARTON_MASS.fullmatch(b.text.strip()) for b in lines]
    carton_matches = [m for m in carton_matches if m is not None]
    if (
        len(mass_matches) != 1
        or len(carton_matches) != 1
        or sum(bool(_MATERIAL.fullmatch(b.text.strip())) for b in lines) != 1
    ):
        return None

    block, total = mass_matches[0]
    carton = carton_matches[0]
    try:
        cartons = Decimal(row.group("cartons"))
        sets_per_carton = Decimal(row.group("sets_per_carton"))
        sets_total = Decimal(row.group("sets_total"))
        mass = Decimal(total.group("amount"))
        per_carton = Decimal(carton.group("amount"))
        gross = Decimal(carton.group("gross"))
    except InvalidOperation:
        return None
    if (
        cartons <= 0 or sets_per_carton <= 0 or sets_total != cartons * sets_per_carton
        or mass <= 0 or per_carton <= 0 or gross < per_carton
        or mass != cartons * per_carton
    ):
        return None

    return PackingPlantNetMass(
        sku=row.group("sku").upper(),
        amount=total.group("amount"),
        unit=total.group("unit").upper(),
        source_block=block,
    )
