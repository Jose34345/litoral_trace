"""Tenant-scoped persistence boundary for deterministic regulatory assessments."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Mapping

from litoral_trace.us_lacey.regulatory.rules import RULESET_VERSION


SNAPSHOT_SCHEMA_VERSION = "regulatory-assessment-snapshot-v1"


@dataclass(frozen=True, slots=True)
class RegulatoryAssessmentView:
    status: str
    generation: int
    source_set_fingerprint: str
    ruleset_version: str
    input_fingerprint: str
    assessment_count: int
    indeterminate_count: int
    payload: dict[str, Any]


def fingerprint_rule_inputs(payload: Mapping[str, Any]) -> str:
    """Return the stable SHA-256 identity of exact rule-relevant inputs."""
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


__all__ = [
    "RULESET_VERSION",
    "SNAPSHOT_SCHEMA_VERSION",
    "RegulatoryAssessmentView",
    "fingerprint_rule_inputs",
]
