"""Botanical evidence specialist."""
from __future__ import annotations

from ..contracts import SpecialistRole
from ..specialist_runtime import BaseSpecialistExtractor


class BotanicalExtractor(BaseSpecialistExtractor):
    role = SpecialistRole.BOTANICAL
    allowed_fields = frozenset(
        {
            "genus",
            "species",
            "country_of_harvest",
            "plant_quantity",
            "metric_unit",
        }
    )
    prompt = """
Extract only botanical merchandise evidence: genus, species, harvest country, plant-material
quantity, and metric unit. Botanical Declarations are primary; Supplier Origin Statements may
corroborate. Keep each botanical merchandise row separate and preserve the complete row in
source_text when possible, especially explicit SKU/line locators. Never infer harvest country
from customs origin, exporter/manufacturer address, routing, or port. Never use shipment gross
weight as plant_quantity unless the document explicitly identifies it as plant material.
Never take taxonomy from Packing Lists or packaging rows.
"""
