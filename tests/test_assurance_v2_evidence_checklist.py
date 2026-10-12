"""Actionable required-evidence work queue cannot manufacture regulatory readiness."""
from __future__ import annotations

from dataclasses import replace

from litoral_trace.us_lacey.assurance_v2_evidence_checklist import (
    required_evidence_checklist, required_evidence_summary,
)
from test_assurance_v2_outputs_core import make_case


def test_required_rows_dont_confuse_parser_support_with_authority():
    case = make_case(approved=False)
    checklist = required_evidence_checklist(case)
    assert len(checklist) == 16
    assert {row.status for row in checklist} == {"REVIEW_VALUE"}
    assert required_evidence_summary(case)["verified"] == 0


def test_current_document_candidate_becomes_reviewable_but_not_verified():
    case = make_case(approved=False)
    fields = tuple(
        {**field, "value": "", "authority": "CANDIDATE"}
        if field["name"] == "article_component" else field
        for field in case.fields
    )
    exceptions = ({
        "id": "field:13", "field_name": "article_component",
        "line_reference": "LINE-1", "status": "OPEN", "blocking": True,
        "candidates": [{"id": "source:" + "a" * 64, "value": "sawn oak boards"}],
    },)
    summary = required_evidence_summary(replace(case, fields=fields, exceptions=exceptions))
    article = next(row for row in summary["rows"] if row.field_name == "article_component")
    assert article.status == "REVIEW_SOURCE"
    assert summary["verified"] == 0


def test_missing_source_is_not_called_human_reviewable():
    case = make_case(approved=False)
    missing = tuple(
        {**field, "value": ""} if field["name"] == "filing_entry_reference" else field
        for field in case.fields
    )
    summary = required_evidence_summary(replace(case, fields=missing))
    entry = next(row for row in summary["rows"] if row.field_name == "filing_entry_reference")
    assert entry.status == "MISSING_EVIDENCE"
    assert summary["missing"] == 1


def test_verified_human_and_history_are_distinct_from_candidate():
    case = make_case(approved=False)
    reviewed = tuple({
        **field,
        "authority": ("HUMAN_CONFIRMED" if field["name"] == "article_component"
                      else "VERIFIED_REUSE" if field["name"] == "species"
                      else field["authority"]),
    } for field in case.fields)
    summary = required_evidence_summary(replace(case, fields=reviewed))
    assert summary["verified"] == 2
