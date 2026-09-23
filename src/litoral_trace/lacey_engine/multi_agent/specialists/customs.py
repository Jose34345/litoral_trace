"""Customs identity specialist."""
from __future__ import annotations

from ..contracts import SpecialistRole
from ..specialist_runtime import BaseSpecialistExtractor


class CustomsIdentityExtractor(BaseSpecialistExtractor):
    role = SpecialistRole.CUSTOMS_IDENTITY
    allowed_fields = frozenset(
        {
            "importer_name",
            "importer_address",
            "consignee_name",
            "consignee_address",
            "filing_entry_reference",
            "manufacturer_id",
            "supplier_name",
        }
    )
    prompt = """
Extract only customs-party and filing identity facts explicitly supported by the page.
Prefer Entry Worksheets for importer, filing reference, and manufacturer ID; Commercial
Invoices and Bills of Lading may corroborate parties. MID/manufacturer_id must be
explicitly labeled as a Manufacturer Identification Code; a carrier SCAC is never a MID.
Reject phone numbers and isolated one/two-letter OCR fragments as importer or supplier
names. Do not extract logistics, HTS, commercial values, taxonomy, or harvest-origin
fields. Keep exact party/address evidence separate rather than combining unrelated blocks.
"""
