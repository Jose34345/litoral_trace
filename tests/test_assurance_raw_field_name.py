from litoral_trace.assurance.processing import _raw_table_field_name


def test_raw_table_field_name_preserves_normal_header() -> None:
    assert (
        _raw_table_field_name(table_index=3, header="HTS Code")
        == "raw.table.3.HTS Code"
    )


def test_raw_table_field_name_bounds_oversized_pdf_header_deterministically() -> None:
    header = "NOISY OCR HEADER " + ("X" * 600)

    first = _raw_table_field_name(table_index=12, header=header)
    second = _raw_table_field_name(table_index=12, header=header)

    assert first == second
    assert len(first) <= 255
    assert first.startswith("raw.table.12.NOISY OCR HEADER ")
    assert "~" in first[-13:]
