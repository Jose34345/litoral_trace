"""Assurance V2 authority gate and immutable source-linked case contract.

This is an integration boundary, not another decision engine. The caller must
supply state already determined by the authority/identity service (Agent 2).
No AI candidate, regulatory assessment or historical match grants authority here.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from hashlib import sha256
import json
import re
from typing import Any, Mapping

from litoral_trace.us_lacey.ppq505 import (
    PPQ505_SHIPMENT_REFERENCE, PPQ505_FIELDS_BY_KEY,
    PpqValidationStatus, validate_ppq_value,
)
from litoral_trace.us_lacey.regulatory.assurance_v2_adapter import LACEY_ADAPTER
from litoral_trace.us_lacey.exporters.export_snapshot import (
    LaceyExportHeader, LaceyExportPlantLine, LaceyExportSnapshot,
)

_AUTHORITIES = frozenset({"SUPPORTED", "HUMAN_CONFIRMED", "VERIFIED_REUSE"})
_REVIEW_ACTIONS = frozenset({"ACCEPT", "CORRECT", "REJECT", "REQUEST_EVIDENCE", "PENDING"})
_FINAL_DECISIONS = frozenset({"ACCEPT", "CORRECT"})
_REUSE_GOOD = frozenset({"ACTIVE", "VERIFIED"})
_RULE_GOOD = frozenset({"PASS", "NOT_APPLICABLE"})


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      default=str, allow_nan=False).encode("utf-8")


def _fingerprint(value: Any) -> str:
    return sha256(_canonical(value)).hexdigest()


def _utc(value: str) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)
    except (ValueError, TypeError):
        return None


@dataclass(frozen=True)
class AssuranceCase:
    organization_id: int
    operation_id: str
    source_set_fingerprint: str
    source_set_revision: int
    current_source_set_fingerprint: str
    current_source_set_revision: int
    ruleset_version: str
    documents: tuple[dict[str, Any], ...] = ()
    lines: tuple[str, ...] = ()
    fields: tuple[dict[str, Any], ...] = ()
    exceptions: tuple[dict[str, Any], ...] = ()
    decisions: tuple[dict[str, Any], ...] = ()
    reuses: tuple[dict[str, Any], ...] = ()
    identities: tuple[dict[str, Any], ...] = ()
    rules: tuple[dict[str, Any], ...] = ()
    export_authorization: dict[str, Any] = field(default_factory=dict)

    def snapshot(self) -> dict[str, Any]:
        # A JSON round-trip detaches the package from mutable caller dictionaries.
        return json.loads(_canonical(asdict(self)))

    def authority_fingerprint(self) -> str:
        payload = self.snapshot()
        payload.pop("export_authorization", None)
        # The current pointer is a freshness fence, not a source assertion.
        payload.pop("current_source_set_fingerprint", None)
        payload.pop("current_source_set_revision", None)
        return _fingerprint(payload)


def case_from_snapshot(payload: Mapping[str, Any]) -> AssuranceCase:
    def collection(name: str) -> tuple[dict[str, Any], ...]:
        return tuple(dict(item) for item in payload.get(name, ()) if isinstance(item, Mapping))
    return AssuranceCase(
        organization_id=int(payload["organization_id"]),
        operation_id=str(payload["operation_id"]),
        source_set_fingerprint=str(payload["source_set_fingerprint"]),
        source_set_revision=int(payload["source_set_revision"]),
        current_source_set_fingerprint=str(payload["current_source_set_fingerprint"]),
        current_source_set_revision=int(payload["current_source_set_revision"]),
        ruleset_version=str(payload["ruleset_version"]),
        documents=collection("documents"),
        lines=tuple(str(line) for line in payload.get("lines", ())),
        fields=collection("fields"),
        exceptions=collection("exceptions"),
        decisions=collection("decisions"),
        reuses=collection("reuses"),
        identities=collection("identities"),
        rules=collection("rules"),
        export_authorization=dict(payload.get("export_authorization") or {}),
    )


@dataclass(frozen=True)
class GateBlocker:
    code: str
    subject: str


@dataclass(frozen=True)
class GateResult:
    ready: bool
    blockers: tuple[GateBlocker, ...]
    case_fingerprint: str

    @property
    def status(self) -> str:
        return "FILING_READY" if self.ready else "BLOCKED"


def evaluate_filing_gate(case: AssuranceCase, *, now: datetime | None = None) -> GateResult:
    """Deny-by-default gate; export authorization binds to the exact case digest."""
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    blockers: list[GateBlocker] = []
    def block(code: str, subject: object) -> None:
        blockers.append(GateBlocker(code, str(subject)))

    if not case.source_set_fingerprint or (
        case.source_set_fingerprint != case.current_source_set_fingerprint
        or case.source_set_revision != case.current_source_set_revision
        or case.source_set_revision < 1
    ):
        block("STALE_SOURCE_SET", case.operation_id)

    ids: set[str] = set()
    for doc in case.documents:
        identity = str(doc.get("id") or "")
        if not identity or identity in ids or not re.fullmatch(r"[a-f0-9]{64}", str(doc.get("sha256") or "")) or not doc.get("version"):
            block("INVALID_DOCUMENT_MANIFEST", identity or "unknown")
        ids.add(identity)
    if not ids:
        block("NO_SOURCE_DOCUMENTS", case.operation_id)
    if not case.lines or len(case.lines) != len(set(case.lines)) or any(not x for x in case.lines):
        block("INVALID_LINE_IDENTITY", case.operation_id)
    if len(case.identities) != len(case.lines) or any(
        str(identity.get("status") or "").upper() not in {"VERIFIED", "HUMAN_CONFIRMED"}
        or not identity.get("supplier_id") or not identity.get("product_id")
        or not identity.get("line_reference") or identity.get("line_reference") not in case.lines
        for identity in case.identities
    ) or set(str(identity.get("line_reference")) for identity in case.identities) != set(case.lines):
        block("UNRESOLVED_ENTITY_IDENTITY", case.operation_id)

    identities_by_line = {str(item.get("line_reference")): item for item in case.identities}
    decisions = {str(item.get("id")): item for item in case.decisions if item.get("id")}
    reuses = {str(item.get("id")): item for item in case.reuses if item.get("id")}
    by_key: dict[tuple[str, str], dict[str, Any]] = {}
    for f in case.fields:
        key = (str(f.get("line_reference") or ""), str(f.get("name") or ""))
        if key in by_key:
            block("DUPLICATE_FIELD", ":".join(key))
        by_key[key] = f
        value = str(f.get("value") or "").strip()
        if not value:
            continue
        authority = str(f.get("authority") or "").upper()
        evidence_ids = set(str(x) for x in f.get("evidence_ids", ()) if x)
        source_document_ids = set(str(x) for x in f.get("document_ids", ()) if x)
        if authority not in _AUTHORITIES:
            block("UNSUPPORTED_CLAIM", ":".join(key))
        # Reuse references a historical document (outside this source set);
        # the verified memory record must preserve that document's hash.
        if authority != "VERIFIED_REUSE" and (
            not source_document_ids or not source_document_ids <= ids
        ):
            block("MISSING_SOURCE_LINK", ":".join(key))
        if not evidence_ids:
            block("MISSING_EVIDENCE_REF", ":".join(key))
        if authority == "HUMAN_CONFIRMED":
            decision = decisions.get(str(f.get("decision_id") or ""))
            if not decision or str(decision.get("action") or "").upper() not in _FINAL_DECISIONS or (
                str(decision.get("field_name")) != key[1]
                or str(decision.get("line_reference")) != key[0]
                or str(decision.get("value") or "").strip() != value
                or not decision.get("actor_id") or not str(decision.get("reason") or "").strip()
                or not _utc(str(decision.get("decided_at") or ""))
            ):
                block("MISSING_REVIEW_DECISION", ":".join(key))
        if authority == "VERIFIED_REUSE":
            reuse = reuses.get(str(f.get("reuse_id") or ""))
            if not reuse or str(reuse.get("status") or "").upper() not in _REUSE_GOOD or (
                not reuse.get("verified_by") or not reuse.get("evidence_id")
                or str(reuse.get("evidence_id")) not in evidence_ids
                or not reuse.get("source_document_hash")
                or str(reuse.get("line_reference")) != key[0]
                or str(reuse.get("supplier_id")) != str(identities_by_line.get(key[0], {}).get("supplier_id"))
                or str(reuse.get("product_id")) != str(identities_by_line.get(key[0], {}).get("product_id"))
                or (valid := _utc(str(reuse.get("valid_until") or ""))) is None or valid <= now
                or reuse.get("revoked_at")
            ):
                block("INVALID_REUSED_EVIDENCE", ":".join(key))

    required = LACEY_ADAPTER.required_fields(case.lines)
    for line, specification in required:
        key = (line, specification.key)
        f = by_key.get(key)
        if not f or not str(f.get("value") or "").strip():
            block("MISSING_REQUIRED_FIELD", ":".join(key))
            continue
        validation = validate_ppq_value(specification.key, f["value"])
        if validation.status != PpqValidationStatus.VALID:
            block("INVALID_PPQ_VALUE", ":".join(key))
    required_keys = {(line, specification.key) for line, specification in required}
    for (line, name), f in by_key.items():
        if (line, name) not in required_keys and name in PPQ505_FIELDS_BY_KEY and (
            str(f.get("value") or "").strip()
        ) and validate_ppq_value(name, f["value"]).status != PpqValidationStatus.VALID:
            block("INVALID_PPQ_VALUE", f"{line}:{name}")

    for exception in case.exceptions:
        if str(exception.get("status") or "").upper() not in {"RESOLVED", "DISMISSED"}:
            block("BLOCKING_EXCEPTION", exception.get("id") or "unknown")
        if bool(exception.get("blocking")) and not exception.get("resolution_decision_id"):
            block("UNDECIDED_CONFLICT", exception.get("id") or "unknown")
        if exception.get("resolution_decision_id") and str(exception.get("resolution_decision_id")) not in decisions:
            block("UNVERIFIED_EXCEPTION_DECISION", exception.get("id") or "unknown")

    for regulatory_blocker in LACEY_ADAPTER.assess(case.rules, case.ruleset_version):
        block(regulatory_blocker.code, regulatory_blocker.subject)

    digest = case.authority_fingerprint()
    authorization = case.export_authorization
    if (
        not authorization.get("id")
        or not authorization.get("actor_id")
        or not _utc(str(authorization.get("authorized_at") or ""))
        or authorization.get("case_fingerprint") != digest
        or authorization.get("source_set_fingerprint") != case.source_set_fingerprint
        or authorization.get("status") != "AUTHORIZED"
    ):
        block("EXPORT_NOT_AUTHORIZED", case.operation_id)

    return GateResult(not blockers, tuple(blockers), digest)


def export_snapshot_from_case(
    case: AssuranceCase, *, now: datetime | None = None,
) -> LaceyExportSnapshot:
    """Refuse direct projection unless all source/review/export gates passed."""
    gate = evaluate_filing_gate(case, now=now)
    if not gate.ready:
        raise ValueError("Cannot project blocked Assurance V2 case for final export")
    values = {
        (str(f.get("line_reference") or ""), str(f.get("name") or "")): str(f.get("value") or "")
        for f in case.fields
        if str(f.get("authority") or "").upper() in _AUTHORITIES
    }
    def read(line: str, key: str) -> str:
        return values.get((line, key), "")

    return LaceyExportSnapshot(
        operation_id=case.operation_id, client_reference="",
        header=LaceyExportHeader(
            entry_number=read(PPQ505_SHIPMENT_REFERENCE, "filing_entry_reference"),
            importer=read(PPQ505_SHIPMENT_REFERENCE, "importer_name"),
            estimated_date_of_arrival=read(PPQ505_SHIPMENT_REFERENCE, "estimated_arrival_date"),
        ),
        plant_lines=tuple(LaceyExportPlantLine(
            line_reference=line, hts_number=read(line, "hts_code"),
            entered_value=read(line, "entered_value"),
            article_component=read(line, "article_component"),
            genus=read(line, "genus"), species=read(line, "species"),
            country_of_harvest=read(line, "country_of_harvest"),
            quantity=read(line, "plant_quantity"), unit=read(line, "metric_unit"),
            percent_recycled=read(line, "percent_recycled"),
        ) for line in case.lines),
    )
