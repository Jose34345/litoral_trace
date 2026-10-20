from litoral_trace.us_lacey.regulatory.applicability.triage import (
    NOT_REQUIRED_TYPES,
    REQUIRED_TYPES,
    SPECIAL_TYPES,
)


def test_entry_type_options_preserve_every_supported_regulatory_code() -> None:
    from litoral_trace.web.us_lacey_entry_type_options import entry_type_options

    options = entry_type_options()

    assert {option.code for option in options} == (
        REQUIRED_TYPES | NOT_REQUIRED_TYPES | SPECIAL_TYPES
    )
    assert all(option.code and option.official_title for option in options)


def test_ftz_and_de_minimis_options_keep_their_required_guidance() -> None:
    from litoral_trace.web.us_lacey_entry_type_options import entry_type_options

    options_by_code = {option.code: option for option in entry_type_options()}

    assert options_by_code["06"].group == "Foreign-trade zone"
    assert "PGA" in options_by_code["06"].caution
    assert "eligibility" in options_by_code["86"].caution.lower()
    assert "de minimis" in options_by_code["86"].official_title.lower()
