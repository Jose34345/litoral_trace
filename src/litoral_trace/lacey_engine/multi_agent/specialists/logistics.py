"""Shipment logistics specialist."""
from __future__ import annotations

from ..contracts import SpecialistRole
from ..specialist_runtime import BaseSpecialistExtractor


class LogisticsExtractor(BaseSpecialistExtractor):
    role = SpecialistRole.LOGISTICS
    allowed_fields = frozenset(
        {
            "bill_of_lading",
            "container_number",
            "seal_number",
            "estimated_arrival_date",
        }
    )
    prompt = """
Extract only shipment logistics identifiers and ETA explicitly supported by the page.
Bills of Lading are primary for B/L, ISO 6346 freight-container identifiers, and seal
numbers. A container must be four letters plus seven digits with a valid ISO 6346 check
digit. Extract a seal only from an explicit Seal/Seal Number label and never interchange
seal, vessel, equipment type, SCAC, or container identifiers. Arrival Notices and Bills
of Lading may support ETA. Do not extract customs-party identities, HTS, commercial
values, taxonomy, or harvest origin.
"""
