"""Commercial line specialist."""
from __future__ import annotations

from ..contracts import SpecialistRole
from ..specialist_runtime import BaseSpecialistExtractor


class CommercialLineExtractor(BaseSpecialistExtractor):
    role = SpecialistRole.COMMERCIAL_LINES
    # The legacy AI-shadow contract does not yet expose sku/line_number/commercial_quantity
    # as AICandidate field keys. Preserve that safety contract here: the prompt requires the
    # complete row (including those locators/quantities) in source_text, and Phase 3 derives
    # line_item_key deterministically from that evidence instead of adding unsupported fields.
    allowed_fields = frozenset(
        {
            "description",
            "article_component",
            "hts_code",
            "entered_value",
            "invoice_total",
        }
    )
    prompt = """
Extract commercial merchandise facts row by row. Emit separate candidates for description,
article/component, HTS code, and entered value; never merge multiple merchandise rows.
For every candidate from a table, preserve the complete commercial row in source_text when
possible, including explicit SKU, line number, quantity and unit. Those row locators are
evidence for the later deterministic line-binding stage, not free-form fields to invent.
The shipment-level invoice_total is distinct from row-level entered_value: extract it only
from an explicit Invoice Total, Grand Total, Total Amount, or Amount Due label on a
Commercial Invoice, and only as a valid financial numeric amount. Never substitute claim,
loss, damage, freight narrative, weights, or OCR garbage for invoice_total.
Packing Lists are corroborating evidence only and must never create botanical taxonomy.
Ignore packaging/admin rows such as PAL, pallet, AUX, carton, box and standalone row numbers.
Do not extract importer/consignee identity, shipment logistics, genus/species, or harvest country.
"""
