from decimal import Decimal

from litoral_trace.assurance.parsers import ParsedTable, SourceLocation
from litoral_trace.product_intelligence.bom_ingestion import ingest_bom_table


def _table(rows):
    return ParsedTable(
        name="BOM",
        headers=("SKU", "Product", "Component", "Material", "Qty", "Weight", "UOM"),
        rows=tuple(rows),
        source=SourceLocation(sheet="BOM", row=1, locator="sheet:BOM;header_row:1"),
    )


def test_ingest_simple_bom_preserves_source_and_normalizes_material():
    result = ingest_bom_table(
        _table(
            [
                {
                    "SKU": "CHAIR-1",
                    "Product": "Dining Chair",
                    "Component": "Front leg",
                    "Material": "  Rubber Wood  ",
                    "Qty": "2",
                    "Weight": "500",
                    "UOM": "g",
                }
            ]
        ),
        document_id="doc-1",
    )

    assert result.issues == ()
    assert len(result.compositions) == 1
    composition = result.compositions[0]
    assert composition.sku == "CHAIR-1"
    assert composition.product_name == "Dining Chair"
    component = composition.components[0]
    assert component.description_raw == "Front leg"
    assert component.quantity == Decimal("2")
    assert component.material.name_raw == "Rubber Wood"
    assert component.material.name_normalized == "rubber wood"
    assert component.material.mass.kilograms == Decimal("0.500")
    assert component.source.document_id == "doc-1"
    assert component.source.sheet == "BOM"
    assert component.source.row == 2


def test_multi_sku_rows_never_cross_composition_boundaries():
    result = ingest_bom_table(
        _table(
            [
                {"SKU": "SKU-A", "Product": "A", "Component": "Leg", "Material": "Oak", "Qty": "4", "Weight": "2", "UOM": "lb"},
                {"SKU": "SKU-A", "Product": "A", "Component": "Seat", "Material": "MDF", "Qty": "1", "Weight": "1", "UOM": "kg"},
                {"SKU": "SKU-B", "Product": "B", "Component": "Handle", "Material": "Beech", "Qty": "1", "Weight": "4", "UOM": "oz"},
            ]
        )
    )

    by_sku = {item.sku: item for item in result.compositions}
    assert set(by_sku) == {"SKU-A", "SKU-B"}
    assert [item.description_raw for item in by_sku["SKU-A"].components] == ["Leg", "Seat"]
    assert [item.description_raw for item in by_sku["SKU-B"].components] == ["Handle"]
    assert all(component.component_key.startswith("SKU-A:") for component in by_sku["SKU-A"].components)
    assert all(component.component_key.startswith("SKU-B:") for component in by_sku["SKU-B"].components)


def test_invalid_rows_are_reported_without_discarding_independent_valid_rows():
    result = ingest_bom_table(
        _table(
            [
                {"SKU": "GOOD", "Product": "Good", "Component": "Leg", "Material": "Oak", "Qty": "1", "Weight": "1", "UOM": "kg"},
                {"SKU": "", "Product": "Bad", "Component": "Seat", "Material": "Oak", "Qty": "1", "Weight": "1", "UOM": "kg"},
                {"SKU": "BAD-QTY", "Product": "Bad", "Component": "Seat", "Material": "Oak", "Qty": "abc", "Weight": "1", "UOM": "kg"},
                {"SKU": "BAD-MASS", "Product": "Bad", "Component": "Seat", "Material": "Oak", "Qty": "1", "Weight": "5", "UOM": ""},
            ]
        )
    )

    assert [item.sku for item in result.compositions] == ["GOOD"]
    codes = {issue.code for issue in result.issues}
    assert "MISSING_SKU" in codes
    assert "INVALID_QUANTITY" in codes
    assert "INVALID_MASS" in codes
