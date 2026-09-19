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
            "estimated_arrival_date",
        }
    )
    prompt = """
Extract only shipment logistics identifiers and ETA explicitly supported by the page.
Bills of Lading are primary for B/L and container. Arrival Notices and Bills of Lading
may support ETA. Do not extract customs-party identities, HTS, commercial values,
taxonomy, or harvest origin. Never mistake seal/equipment identifiers for containers.
"""
