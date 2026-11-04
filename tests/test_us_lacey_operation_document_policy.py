from __future__ import annotations

import pytest

from litoral_trace.us_lacey.operations import _document_role_replaces_current


@pytest.mark.parametrize(
    ("role", "expected"),
    [
        ("UNKNOWN", False),
        ("unknown", False),
        ("  OTHER  ", False),
        ("BILL_OF_LADING", True),
        ("COMMERCIAL_INVOICE", True),
        ("PACKING_LIST", True),
        ("SUPPLIER_DECLARATION", True),
        ("CERTIFICATE", True),
    ],
)
def test_only_explicit_semantic_roles_replace_the_current_document(
    role: str,
    expected: bool,
) -> None:
    assert _document_role_replaces_current(role) is expected


def test_blank_role_is_fail_safe_non_versioned() -> None:
    assert _document_role_replaces_current("") is False
