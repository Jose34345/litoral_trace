"""Exception-first UX: grouping does not change regulatory or reviewer authority."""
from litoral_trace.us_lacey.assurance_v2_simplified_review import build_review_workspace

class Blocker:
    def __init__(self, code, subject):
        self.code, self.subject = code, subject

class Gate:
    blockers = [Blocker("BLOCKING_EXCEPTION", "field:1"),
                Blocker("UNDECIDED_CONFLICT", "field:1"),
                Blocker("MISSING_EVIDENCE_REF", "1:genus")]


def test_pack2_triage_groups_actions_without_claiming_authority():
    case = {
        "documents": [{"id": str(i)} for i in range(6)],
        "identities": [{"line_reference":"1", "sku":"BAM-COAST-04",
                        "supplier_name":"LONGSHENG", "status":"REVIEW_REQUIRED"}],
        "reuses": [
            {"status":"ELIGIBLE", "field_name":name, "line_reference":"1"}
            for name in ("genus","species","country_of_harvest")
        ],
    }
    required = {"verified":0, "review":15, "rows":[
        {"field_name":"filing_entry_reference", "line_reference":"__shipment__",
         "status":"MISSING_EVIDENCE", "label":"Entry Number", "value":None},
        *[{"field_name":f"field{i}","line_reference":"1","status":"REVIEW_SOURCE",
           "label":f"Field {i}", "value":str(i)} for i in range(15)],
    ]}
    x = build_review_workspace(case, required, Gate())
    assert x["found_count"] == 15
    assert x["missing_count"] == 1
    assert x["source_count"] == 6
    assert [a["key"] for a in x["tasks"]] == [
        "identity:1","botanical-memory",
        "missing:__shipment__:filing_entry_reference","batch-review"
    ]
    assert x["verified_count"] == 0
    assert x["technical_finding_count"] == 3
    assert x["regulatory_blocks"] == {}


def test_missing_sources_are_not_silently_reclassified_as_found():
    x = build_review_workspace(
        {"documents":[], "identities":[], "reuses":[]},
        {"verified":0, "review":0,
         "rows":[{"status":"MISSING_EVIDENCE","value":"","field_name":"genus",
                  "line_reference":"1","label":"Genus"}]},
        Gate(),
    )
    assert x["found_count"] == 0
    assert x["missing_count"] == 1
    assert len(x["tasks"]) == 1


def test_pack2_missing_supplier_evidence_is_one_task_per_line_not_per_field():
    rows = [
        {"status":"MISSING_EVIDENCE", "field_name":"entry_reference",
         "line_reference":"__shipment__", "label":"Entry", "value":None},
        *[
            {"status":"MISSING_EVIDENCE", "field_name":field,
             "line_reference":"1", "label":label, "value":None}
            for field, label in (
                ("genus", "Genus"), ("species", "Species"),
                ("country_of_harvest", "Harvest country"),
                ("quantity", "Plant material quantity"),
            )
        ],
        {"status":"MISSING_EVIDENCE", "field_name":"genus",
         "line_reference":"2", "label":"Genus", "value":None},
        {"status":"VERIFIED", "field_name":"species",
         "line_reference":"2", "label":"Species", "value":"Pinus"},
    ]
    required = {"verified":1, "review":0, "rows":rows}
    case = {"documents":[{"id":"doc-a"}], "identities":[], "reuses":[]}
    x = build_review_workspace(case, required, Gate())
    assert x["missing_count"] == 6
    assert x["verified_count"] == 1
    assert len(x["tasks"]) == 3
    assert [task["key"] for task in x["tasks"]] == [
        "missing:__shipment__:entry_reference",
        "missing:1:group", "missing:2:genus",
    ]
    assert "4 missing inputs for line 1" in x["tasks"][1]["detail"]
    assert "Genus, Species, Harvest country, and 1 more" in x["tasks"][1]["detail"]
    assert x["tasks"][1]["target"] == "#missing-evidence-panel"
    assert x["technical_finding_count"] == 3
    assert rows[1]["status"] == "MISSING_EVIDENCE"
    assert rows[2]["status"] == "MISSING_EVIDENCE"


def test_missing_evidence_grouping_does_not_mix_lines_or_hide_many_requirements():
    rows = [
        {"status":"MISSING_EVIDENCE", "field_name":f"field_{i}",
         "line_reference":"L1", "label":f"Input {i}", "value":""}
        for i in range(12)
    ]
    rows.append({"status":"MISSING_EVIDENCE", "field_name":"field_0",
                 "line_reference":"L2", "label":"Input 0", "value":""})
    x = build_review_workspace(
        {"documents":[], "identities":[], "reuses":[]},
        {"verified":0, "review":0, "rows":rows}, Gate(),
    )
    assert x["missing_count"] == 13
    assert len(x["tasks"]) == 2
    assert "and 9 more" in x["tasks"][0]["detail"]
    assert "line L1" in x["tasks"][0]["detail"]
    assert "line L2" in x["tasks"][1]["detail"]
    assert all(task["target"] == "#missing-evidence-panel" for task in x["tasks"])


def test_pack2_actions_route_to_real_first_screen_review_forms():
    case = {
        "documents": [{"id": str(i)} for i in range(6)],
        "identities": [{"line_reference": "1", "supplier_name": "LONGSHENG",
                        "sku": "BAM-COAST-04", "status": "REVIEW_REQUIRED"}],
        "reuses": [{"status": "ELIGIBLE", "field_name": name, "line_reference": "1"}
                   for name in ("genus", "species", "country_of_harvest")],
    }
    rows = [{"field_name": "filing_entry_reference", "line_reference": "__shipment__",
             "status": "MISSING_EVIDENCE", "label": "Entry Number", "value": None}]
    rows += [{"field_name": f"field_{i}", "line_reference": "1",
              "status": "REVIEW_SOURCE", "label": f"Value {i}", "value": str(i)}
             for i in range(15)]
    x = build_review_workspace(case, {"rows": rows, "verified": 0, "review": 15}, Gate())
    assert x["found_count"] == 15
    assert x["missing_count"] == 1
    assert len(x["tasks"]) == 4
    assert [task["target"] for task in x["tasks"]] == [
        "#identity-action-panel", "#identity-action-panel",
        "#missing-evidence-panel", "#batch-review-panel",
    ]
    assert x["technical_finding_count"] == 3
    # No single task can be interpreted as regulatory approval.
    assert x["verified_count"] == 0


def test_botanical_review_routes_to_primary_form_only_after_identity_confirmation():
    case = {
        "documents": [{"id": "1"}],
        "identities": [{"line_reference": "1", "status": "HUMAN_CONFIRMED"}],
        "reuses": [{"status": "ELIGIBLE", "field_name": "genus", "line_reference": "1"}],
    }
    x = build_review_workspace(case, {"rows": (), "verified": 0, "review": 0}, Gate())
    assert [task["target"] for task in x["tasks"]] == ["#botanical-group-panel"]
