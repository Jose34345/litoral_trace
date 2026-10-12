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
