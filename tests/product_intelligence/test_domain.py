from dataclasses import FrozenInstanceError
from decimal import Decimal

import pytest

from litoral_trace.product_intelligence.domain import MassValue, SourceAnchor


def test_source_anchor_and_mass_value_are_immutable():
    anchor = SourceAnchor(table_name="BOM", sheet="BOM", row=2)
    mass = MassValue(raw_value="500", raw_unit="g", kilograms=Decimal("0.5"))

    with pytest.raises(FrozenInstanceError):
        anchor.row = 3
    with pytest.raises(FrozenInstanceError):
        mass.raw_unit = "kg"


def test_source_anchor_keeps_transport_provenance_fields():
    anchor = SourceAnchor(
        document_id="doc-123",
        table_name="BOM",
        sheet="Components",
        row=7,
        column=4,
        locator="sheet:Components;header_row:2",
    )

    assert anchor.document_id == "doc-123"
    assert anchor.table_name == "BOM"
    assert anchor.sheet == "Components"
    assert anchor.row == 7
    assert anchor.column == 4
    assert anchor.locator == "sheet:Components;header_row:2"
