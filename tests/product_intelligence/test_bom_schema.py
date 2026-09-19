import pytest

from litoral_trace.product_intelligence.bom_schema import BomSchemaError, bind_bom_headers


def test_bind_bom_headers_supports_canonical_and_known_aliases():
    binding = bind_bom_headers(
        ("Item Number", "Product", "Part", "Material Description", "Qty", "Weight", "UOM")
    )

    assert binding.sku == "Item Number"
    assert binding.product_name == "Product"
    assert binding.component == "Part"
    assert binding.material == "Material Description"
    assert binding.quantity == "Qty"
    assert binding.mass_value == "Weight"
    assert binding.mass_unit == "UOM"


def test_bind_bom_headers_rejects_ambiguous_aliases():
    with pytest.raises(BomSchemaError, match="sku"):
        bind_bom_headers(("SKU", "Item Number", "Component", "Material"))


def test_bind_bom_headers_rejects_missing_required_material():
    with pytest.raises(BomSchemaError, match="material"):
        bind_bom_headers(("SKU", "Component", "Quantity"))
