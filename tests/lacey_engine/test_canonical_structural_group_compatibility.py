from __future__ import annotations

from itertools import permutations

import pytest

from litoral_trace.us_lacey.canonical_shipment_truth import build_canonical_shipment_truth
from tests.lacey_engine.test_canonical_shipment_truth import _evidence, _field


def _payload(specs):
    rows = {"description": [], "hts_code": [], "entered_value": []}
    for document, table, ordinal, description, amount in specs:
        line_key = f"{document}:table:{table}:row:{ordinal}"
        for field, value in (
            ("description", description),
            ("hts_code", "4419199010"),
            ("entered_value", amount),
        ):
            if value is not None:
                rows[field].append(_evidence(
                    field=field, value=value, document_id=document,
                    text=value, line_key=line_key,
                ))
    return {
        "canonical_fields": {
            field: _field(field, "SUPPORTED_MULTIPLE", evidence)
            for field, evidence in rows.items() if evidence
        },
        "issues": [],
    }


def _truth(specs):
    return build_canonical_shipment_truth(_payload(specs))


@pytest.mark.parametrize("order", tuple(permutations(range(3))))
def test_transitive_class_preserves_distinct_same_document_rows(order):
    # Recovered audit case: both tables in document 1 have row ordinal 1.
    specs = (
        (1, 1, 1, "Solid wood trays", None),
        (1, 2, 1, "Wooden chopping boards", None),
        (2, 1, 1, "Solid wood trays", None),
    )
    truth = _truth([specs[index] for index in order])

    # The third row could match either local identity by ordinal/HTS alone.
    # Keep all three; do not choose a partial matching by input order.
    assert {line.entity_key for line in truth.plant_lines} == {
        "1:table:1:row:1", "1:table:2:row:1", "2:table:1:row:1",
    }
    assert {
        line.entity_key: line.fields["merchandise_description"].values
        for line in truth.plant_lines
    } == {
        "1:table:1:row:1": ("Solid wood trays",),
        "1:table:2:row:1": ("Wooden chopping boards",),
        "2:table:1:row:1": ("Solid wood trays",),
    }
    assert {
        (e.document_id, e.line_key)
        for line in truth.plant_lines for e in line.fields["hts_code"].evidence
    } == {
        ("1", "1:table:1:row:1"),
        ("1", "1:table:2:row:1"),
        ("2", "2:table:1:row:1"),
    }


def test_identical_rows_from_distinct_documents_still_join():
    truth = _truth((
        (1, 1, 1, "Solid wood trays", "100"),
        (2, 2, 1, "Solid wood trays", "100"),
    ))
    assert len(truth.plant_lines) == 1
    line = truth.plant_lines[0]
    assert not line.identity_review_required
    assert line.fields["merchandise_description"].values == ("Solid wood trays",)
    assert {e.document_id for e in line.fields["entered_value"].evidence} == {"1", "2"}


@pytest.mark.parametrize("reverse", (False, True))
def test_multiple_legitimate_lines_with_same_hts_keep_cardinality(reverse):
    specs = [
        (1, 1, 1, "Solid wood trays", "100"),
        (1, 1, 2, "Wooden chopping boards", "200"),
        (2, 2, 1, "Solid wood trays", "100"),
        (2, 2, 2, "Wooden chopping boards", "200"),
    ]
    truth = _truth(list(reversed(specs)) if reverse else specs)
    assert len(truth.plant_lines) == 2
    assert {
        line.fields["entered_value"].values for line in truth.plant_lines
    } == {("100",), ("200",)}
    for line in truth.plant_lines:
        assert line.fields["hts_code"].values == ("4419199010",)
        assert {e.document_id for e in line.fields["hts_code"].evidence} == {"1", "2"}


@pytest.mark.parametrize("reverse", (False, True))
def test_repeated_values_do_not_select_arbitrary_cross_document_matching(reverse):
    specs = [
        (1, 1, 1, "Solid wood trays", "100"),
        (1, 1, 2, "Wooden chopping boards", "100"),
        (2, 2, 1, "Solid wood trays", "100"),
        (2, 2, 2, "Wooden chopping boards", "100"),
    ]
    truth = _truth(list(reversed(specs)) if reverse else specs)
    assert len(truth.plant_lines) == 4
    for line in truth.plant_lines:
        assert len({e.line_key for e in line.fields["hts_code"].evidence}) == 1


def test_compatible_class_keeps_existing_field_conflict_and_all_evidence():
    truth = _truth((
        (1, 1, 1, "Solid wood trays", "100"),
        (2, 2, 1, "Solid wood trays", "200"),
    ))
    assert len(truth.plant_lines) == 1
    field = truth.plant_lines[0].fields["entered_value"]
    assert field.state == "CONFLICT"
    assert set(field.values) == {"100", "200"}
    assert {e.document_id for e in field.evidence} == {"1", "2"}


def test_ambiguous_class_does_not_discard_unrelated_compatible_class():
    truth = _truth((
        (1, 1, 1, "Solid wood trays", None),
        (1, 2, 1, "Wooden chopping boards", None),
        (2, 1, 1, "Solid wood trays", None),
        (3, 1, 2, "Wooden spoons", "300"),
        (4, 1, 2, "Wooden spoons", "300"),
    ))
    assert len(truth.plant_lines) == 4
    spoons = [
        line for line in truth.plant_lines
        if line.fields["merchandise_description"].values == ("Wooden spoons",)
    ]
    assert len(spoons) == 1
    assert not spoons[0].identity_review_required
    assert spoons[0].fields["hts_code"].state == "SUPPORTED_MULTIPLE"
    assert {e.document_id for e in spoons[0].fields["hts_code"].evidence} == {"3", "4"}


def test_rejected_class_retains_identity_review_and_cannot_resolve_conflicts():
    from litoral_trace.us_lacey.canonical_shipment_truth import _resolvable_field_names

    truth = _truth((
        (1, 1, 1, "Solid wood trays", None),
        (1, 2, 1, "Wooden chopping boards", None),
        (2, 1, 1, "Solid wood trays", None),
    ))
    assert {
        field.state.value
        for line in truth.plant_lines for field in line.fields.values()
    } == {"REVIEW_REQUIRED"}
    assert all(line.identity_review_required for line in truth.plant_lines)
    assert "hts_code" not in _resolvable_field_names(truth)
    assert "merchandise_description" not in _resolvable_field_names(truth)


def test_shipment_description_cannot_hide_rejected_line_identity():
    truth = _truth((
        (1, 1, 1, "Solid wood trays", None),
        (1, 2, 1, "Wooden chopping boards", None),
        (2, 1, 1, "Solid wood trays", None),
    ))
    description = truth.shipment_fields["merchandise_description"]
    assert description.state == "REVIEW_REQUIRED"
    assert {e.line_key for e in description.evidence} == {
        "1:table:1:row:1", "1:table:2:row:1", "2:table:1:row:1",
    }


def test_rejected_class_keeps_local_value_conflict_visible():
    truth = _truth((
        (1, 1, 1, "Solid wood trays", "100"),
        (1, 1, 1, "Solid wood trays", "200"),
        (1, 2, 1, "Wooden chopping boards", None),
        (2, 1, 1, "Solid wood trays", None),
    ))
    line = next(line for line in truth.plant_lines if line.entity_key == "1:table:1:row:1")
    assert line.fields["entered_value"].state == "CONFLICT"
    assert set(line.fields["entered_value"].values) == {"100", "200"}
    assert line.identity_review_required


def test_reordered_same_hts_rows_abstain_instead_of_crossing_identity():
    truth = _truth((
        (1, 1, 1, "Solid wood trays", "100"),
        (1, 1, 2, "Wooden chopping boards", "200"),
        (2, 2, 1, "Wooden chopping boards", "200"),
        (2, 2, 2, "Solid wood trays", "100"),
    ))
    assert len(truth.plant_lines) == 4
    assert all(line.identity_review_required for line in truth.plant_lines)
    assert {
        (line.entity_key, line.fields["entered_value"].values)
        for line in truth.plant_lines
    } == {
        ("1:table:1:row:1", ("100",)), ("1:table:1:row:2", ("200",)),
        ("2:table:2:row:1", ("200",)), ("2:table:2:row:2", ("100",)),
    }


def test_compatible_rows_can_join_across_different_ordinals():
    truth = _truth((
        (1, 1, 1, "Solid wood trays", "100"),
        (2, 2, 3, "Solid wood trays", "100"),
        (3, 1, 2, "Solid wood trays", "100"),
    ))
    assert len(truth.plant_lines) == 1
    line = truth.plant_lines[0]
    assert not line.identity_review_required
    assert {e.document_id for e in line.fields["entered_value"].evidence} == {"1", "2", "3"}
