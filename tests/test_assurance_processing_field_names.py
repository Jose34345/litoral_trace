from litoral_trace.assurance.processing import _raw_table_field_name


def test_raw_table_field_name_preserves_normal_headers() -> None:
    assert _raw_table_field_name(2, "HTS Code") == "raw.table.2.HTS Code"


def test_raw_table_field_name_bounds_pathological_pdf_headers() -> None:
    header = "TERMS AND CONDITIONS " * 40

    value = _raw_table_field_name(7, header)

    assert value.startswith("raw.table.7.TERMS AND CONDITIONS ")
    assert len(value) == 255
    assert "~" in value
    assert value == _raw_table_field_name(7, header)


def test_raw_table_field_name_hash_distinguishes_long_headers() -> None:
    common = "A" * 400

    first = _raw_table_field_name(1, common + "X")
    second = _raw_table_field_name(1, common + "Y")

    assert len(first) <= 255
    assert len(second) <= 255
    assert first != second
