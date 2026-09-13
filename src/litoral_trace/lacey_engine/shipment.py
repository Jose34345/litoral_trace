"""Pure, deterministic shipment-level evidence reconciliation for Gate 2."""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from enum import Enum
import re

from .domain import AdmittedCandidate, DocumentResolution, EvidenceClass, FieldStatus
from .pipeline import ENGINE_VERSION, process_document
from .semantic_graph import association_key, is_out_of_scope_context, semantic_normalize
from .source_authority import authority


class ReconciliationState(str, Enum):
    SUPPORTED = "SUPPORTED"
    SUPPORTED_MULTIPLE = "SUPPORTED_MULTIPLE"
    NEAR_MATCH = "NEAR_MATCH"
    CONFLICT = "CONFLICT"
    MISSING = "MISSING"
    REVIEW_REQUIRED = "REVIEW_REQUIRED"


class ShipmentReadiness(str, Enum):
    READY = "READY"
    REVIEW_REQUIRED = "REVIEW_REQUIRED"
    BLOCKED = "BLOCKED"


class FieldCardinality(str, Enum):
    SCALAR = "SCALAR"
    SET = "SET"
    PER_MERCHANDISE_LINE = "PER_MERCHANDISE_LINE"
    PER_PLANT_COMPONENT = "PER_PLANT_COMPONENT"


class PartyRole(str, Enum):
    IMPORTER = "IMPORTER"
    CONSIGNEE = "CONSIGNEE"
    SHIPPER = "SHIPPER"
    SUPPLIER = "SUPPLIER"
    MANUFACTURER = "MANUFACTURER"
    NOTIFY_PARTY = "NOTIFY_PARTY"


class EvidenceScope(str, Enum):
    SHIPMENT = "SHIPMENT"
    MERCHANDISE_LINE = "MERCHANDISE_LINE"
    PLANT_COMPONENT = "PLANT_COMPONENT"


class AssociationState(str, Enum):
    ASSOCIATED = "ASSOCIATED"
    UNASSOCIATED = "UNASSOCIATED"
    AMBIGUOUS = "AMBIGUOUS"


class QuantitySemanticType(str, Enum):
    PLANT_MATERIAL_QUANTITY = "PLANT_MATERIAL_QUANTITY"
    GROSS_WEIGHT = "GROSS_WEIGHT"
    NET_WEIGHT = "NET_WEIGHT"
    PACKAGE_COUNT = "PACKAGE_COUNT"
    PIECE_COUNT = "PIECE_COUNT"
    VOLUME = "VOLUME"
    OTHER = "OTHER"


# Entered value has two deliberately different semantic roles.  The PPQ review
# field remains line-scoped; an unassociated overall total is retained only as a
# shipment reconciliation fact and must never be projected onto line 1.
SHIPMENT_TOTAL_ENTERED_VALUE = "shipment_total_entered_value"
PLANT_LINE_ENTERED_VALUE = "entered_value"


@dataclass(frozen=True, slots=True)
class ShipmentDocumentInput:
    document_id: str
    filename: str
    content: bytes | None = None
    role_hint: str | None = None
    resolution: DocumentResolution | None = None


@dataclass(frozen=True, slots=True)
class ShipmentDocumentResolution:
    document_id: str
    filename: str
    resolution: DocumentResolution


@dataclass(frozen=True, slots=True)
class ShipmentEvidence:
    candidate_id: str
    document_id: str
    field_key: str
    normalized_value: str
    candidate: AdmittedCandidate
    candidate_score: float
    source_authority: float
    scope: EvidenceScope = EvidenceScope.SHIPMENT
    line_key: str | None = None
    component_key: str | None = None
    quantity_semantic_type: QuantitySemanticType = QuantitySemanticType.OTHER

    @property
    def authority(self) -> float:
        return self.source_authority


@dataclass(frozen=True, slots=True)
class CanonicalFieldCandidate:
    value: str
    evidence_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ReconciliationResult:
    field_key: str
    state: ReconciliationState
    values: tuple[CanonicalFieldCandidate, ...]
    supporting_evidence: tuple[ShipmentEvidence, ...]


@dataclass(frozen=True, slots=True)
class ShipmentIssue:
    issue_id: str
    field_key: str
    scope: str
    severity: str
    issue_type: str
    message: str
    candidate_ids: tuple[str, ...]
    document_ids: tuple[str, ...]
    requires_human_review: bool = True
    line_key: str | None = None
    component_key: str | None = None


@dataclass(frozen=True, slots=True)
class PreparationRequirement:
    requirement_id: str
    acceptable_fields: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ShipmentPreparationContext:
    requirements: tuple[PreparationRequirement, ...] = (
        PreparationRequirement(
            "BILL_OF_LADING_PRESENT",
            ("master_bill_of_lading", "bill_of_lading", "house_bill_of_lading"),
        ),
    )


@dataclass(frozen=True, slots=True)
class LaceyRuleset:
    version: str = "lacey_ruleset_2026_02_semantic_graph"
    preparation: ShipmentPreparationContext = ShipmentPreparationContext()


@dataclass(frozen=True, slots=True)
class ShipmentResolution:
    engine_version: str
    documents: tuple[ShipmentDocumentResolution, ...]
    canonical_fields: dict[str, ReconciliationResult]
    issues: tuple[ShipmentIssue, ...]
    readiness: ShipmentReadiness
    metrics: dict[str, int]
    ruleset_version: str = "lacey_ruleset_2026_02_semantic_graph"


def normalize_mass(value: str, unit: str) -> Decimal | None:
    try:
        amount = Decimal(value.replace(",", ""))
    except (InvalidOperation, AttributeError):
        return None
    factor = {
        "g": Decimal(".001"), "kg": Decimal(1), "metric ton": Decimal(1000),
        "metric tons": Decimal(1000), "tonne": Decimal(1000), "tonnes": Decimal(1000),
        "lb": Decimal(".45359237"),
    }.get(unit.strip().casefold())
    return amount * factor if factor is not None else None


def normalize_money(value: str) -> tuple[str | None, Decimal | None]:
    match = re.fullmatch(r"\s*(USD|EUR|CAD|GBP|AUD|JPY)\s*([0-9][0-9,]*(?:\.[0-9]+)?)\s*", value.upper())
    if not match:
        return None, None
    try:
        return match.group(1), Decimal(match.group(2).replace(",", "")).normalize()
    except InvalidOperation:
        return None, None


def _money_signature(value: str) -> tuple[str | None, Decimal | None]:
    """Normalize explicit or extractor-stripped monetary values for arithmetic only."""
    raw = str(value or "").strip()
    match = re.fullmatch(
        r"\s*(?:(USD|EUR|CAD|GBP|AUD|JPY)\s*|\$\s*)?([0-9][0-9,]*(?:\.[0-9]+)?)\s*",
        raw,
        re.I,
    )
    if not match:
        return None, None
    try:
        amount = Decimal(match.group(2).replace(",", "")).normalize()
    except InvalidOperation:
        return None, None
    currency = match.group(1).upper() if match.group(1) else ("USD" if raw.startswith("$") else None)
    return currency, amount


def normalize_quantity(value: str) -> Decimal | None:
    match = re.fullmatch(r"\s*([0-9][0-9,]*(?:\.[0-9]+)?)\s*(g|kg|lb|metric tons?|tonnes?)\s*", value, re.I)
    return normalize_mass(match.group(1), match.group(2)) if match else None


_CATALOG = {key: FieldCardinality.SCALAR for key in (
    "estimated_arrival_date", "master_bill_of_lading", "house_bill_of_lading", "bill_of_lading",
    "importer_name", "importer_address", "consignee_name", "consignee_address", "manufacturer_id",
    "filing_entry_reference", "currency", "notify_party_name", "shipper_name", "supplier_name",
    "manufacturer_name",
)}
_CATALOG.update({
    "container_number": FieldCardinality.SET,
    "country_of_origin": FieldCardinality.SET,
    "article_component": FieldCardinality.PER_PLANT_COMPONENT,
    "genus": FieldCardinality.PER_PLANT_COMPONENT,
    "species": FieldCardinality.PER_PLANT_COMPONENT,
    "country_of_harvest": FieldCardinality.PER_PLANT_COMPONENT,
    "plant_quantity": FieldCardinality.PER_PLANT_COMPONENT,
    "metric_unit": FieldCardinality.PER_PLANT_COMPONENT,
    "percent_recycled": FieldCardinality.PER_PLANT_COMPONENT,
})
for _key in ("hts_code", "description", PLANT_LINE_ENTERED_VALUE):
    _CATALOG[_key] = FieldCardinality.PER_MERCHANDISE_LINE
_PARTY_KEYS = {"importer_name", "consignee_name", "shipper_name", "supplier_name", "manufacturer_name", "notify_party_name"}
_IMPORTANT = frozenset({"bill_of_lading", "master_bill_of_lading", "house_bill_of_lading", "country_of_harvest", "species", "genus"})


def _cardinality(field_key: str) -> FieldCardinality:
    if field_key == SHIPMENT_TOTAL_ENTERED_VALUE:
        return FieldCardinality.SCALAR
    return _CATALOG[field_key]


def _normal(field_key: str, value: str) -> str:
    semantic_key = PLANT_LINE_ENTERED_VALUE if field_key == SHIPMENT_TOTAL_ENTERED_VALUE else field_key
    normalized = semantic_normalize(semantic_key, value)
    return normalized.upper() if semantic_key not in {"entered_value", "plant_quantity", "percent_recycled"} else normalized


def _scope(cardinality: FieldCardinality) -> EvidenceScope:
    if cardinality is FieldCardinality.PER_PLANT_COMPONENT:
        return EvidenceScope.PLANT_COMPONENT
    if cardinality is FieldCardinality.PER_MERCHANDISE_LINE:
        return EvidenceScope.MERCHANDISE_LINE
    return EvidenceScope.SHIPMENT


def _quantity_semantic_type(label: str) -> QuantitySemanticType:
    label = label.casefold()
    for phrase, semantic in (
        ("plant quantity", QuantitySemanticType.PLANT_MATERIAL_QUANTITY),
        ("plant material", QuantitySemanticType.PLANT_MATERIAL_QUANTITY),
        ("gross weight", QuantitySemanticType.GROSS_WEIGHT),
        ("net weight", QuantitySemanticType.NET_WEIGHT),
        ("package", QuantitySemanticType.PACKAGE_COUNT),
        ("piece", QuantitySemanticType.PIECE_COUNT),
        ("volume", QuantitySemanticType.VOLUME),
    ):
        if phrase in label:
            return semantic
    return QuantitySemanticType.OTHER


def _atomic_state(field_key: str, evidence: list[ShipmentEvidence]) -> ReconciliationState:
    groups = {_normal(field_key, item.normalized_value) for item in evidence}
    if len(groups) == 1:
        return ReconciliationState.SUPPORTED_MULTIPLE if len(evidence) > 1 else ReconciliationState.SUPPORTED
    if field_key in {PLANT_LINE_ENTERED_VALUE, SHIPMENT_TOTAL_ENTERED_VALUE}:
        currencies = {_money_signature(item.normalized_value)[0] for item in evidence}
        currencies.discard(None)
        if len(currencies) > 1:
            return ReconciliationState.REVIEW_REQUIRED
    return ReconciliationState.REVIEW_REQUIRED if len({item.source_authority for item in evidence}) > 1 else ReconciliationState.CONFLICT


def _reconcile(field_key: str, evidence: list[ShipmentEvidence]) -> ReconciliationResult:
    if not evidence:
        return ReconciliationResult(field_key, ReconciliationState.MISSING, (), ())
    groups: dict[str, list[ShipmentEvidence]] = {}
    for item in evidence:
        groups.setdefault(_normal(field_key, item.normalized_value), []).append(item)
    values = tuple(
        CanonicalFieldCandidate(value, tuple(item.candidate_id for item in items))
        for value, items in groups.items()
    )
    cardinality = _cardinality(field_key)
    if cardinality is FieldCardinality.SET:
        return ReconciliationResult(
            field_key,
            ReconciliationState.SUPPORTED_MULTIPLE if len(groups) > 1 or len(evidence) > 1 else ReconciliationState.SUPPORTED,
            values,
            tuple(evidence),
        )
    if cardinality in {FieldCardinality.PER_MERCHANDISE_LINE, FieldCardinality.PER_PLANT_COMPONENT}:
        association_keys = [
            item.line_key if cardinality is FieldCardinality.PER_MERCHANDISE_LINE else item.component_key
            for item in evidence
        ]
        if None in association_keys:
            state = ReconciliationState.REVIEW_REQUIRED if len(groups) > 1 else _atomic_state(field_key, evidence)
            return ReconciliationResult(field_key, state, values, tuple(evidence))
        partitions: dict[str, list[ShipmentEvidence]] = {}
        for item, key in zip(evidence, association_keys):
            partitions.setdefault(key or "", []).append(item)
        states = [_atomic_state(field_key, items) for items in partitions.values()]
        if ReconciliationState.CONFLICT in states:
            state = ReconciliationState.CONFLICT
        elif ReconciliationState.REVIEW_REQUIRED in states:
            state = ReconciliationState.REVIEW_REQUIRED
        elif len(partitions) > 1:
            # Multiple component rows are parallel valid facts, not a conflict.
            state = ReconciliationState.SUPPORTED_MULTIPLE
        else:
            state = states[0]
        return ReconciliationResult(field_key, state, values, tuple(evidence))
    if len(groups) == 1:
        raw_values = {item.normalized_value for item in evidence}
        state = (
            ReconciliationState.NEAR_MATCH
            if field_key in _PARTY_KEYS and len(raw_values) > 1
            else (ReconciliationState.SUPPORTED_MULTIPLE if len(evidence) > 1 else ReconciliationState.SUPPORTED)
        )
        return ReconciliationResult(field_key, state, values, tuple(evidence))
    strengths = {item.source_authority for item in evidence}
    state = ReconciliationState.REVIEW_REQUIRED if len(strengths) > 1 else ReconciliationState.CONFLICT
    return ReconciliationResult(field_key, state, values, tuple(evidence))


def _typed_key(field_key: str, label: str) -> str:
    if field_key in {"party", "party_name", "name"}:
        label = label.casefold()
        for role, key in (
            ("importer", "importer_name"), ("consignee", "consignee_name"),
            ("shipper", "shipper_name"), ("supplier", "supplier_name"),
            ("manufacturer", "manufacturer_name"), ("notify", "notify_party_name"),
        ):
            if role in label:
                return key
    if field_key != "bill_of_lading":
        return field_key
    label = label.casefold()
    if "master" in label and any(token in label for token in ("b/l", "bol", "bill of lading")):
        return "master_bill_of_lading"
    if "house" in label and any(token in label for token in ("b/l", "bol", "bill of lading")):
        return "house_bill_of_lading"
    return field_key


def _entered_value_allocation_issue(fields: dict[str, ReconciliationResult]) -> ShipmentIssue | None:
    total = fields.get(SHIPMENT_TOTAL_ENTERED_VALUE)
    lines = fields.get(PLANT_LINE_ENTERED_VALUE)
    if total is None or lines is None or not total.supporting_evidence or not lines.supporting_evidence:
        return None

    total_signatures = {
        _money_signature(item.normalized_value)
        for item in total.supporting_evidence
        if _money_signature(item.normalized_value)[1] is not None
    }
    if len(total_signatures) != 1:
        return None
    total_currency, total_amount = next(iter(total_signatures))
    if total_amount is None:
        return None

    line_signatures: dict[str, set[tuple[str | None, Decimal | None]]] = {}
    for item in lines.supporting_evidence:
        if not item.line_key:
            continue
        signature = _money_signature(item.normalized_value)
        if signature[1] is not None:
            line_signatures.setdefault(item.line_key, set()).add(signature)
    if not line_signatures or any(len(signatures) != 1 for signatures in line_signatures.values()):
        return None

    selected = [next(iter(signatures)) for signatures in line_signatures.values()]
    currencies = {currency for currency, _amount in selected if currency}
    if total_currency:
        currencies.add(total_currency)
    all_evidence = tuple(total.supporting_evidence) + tuple(lines.supporting_evidence)
    candidate_ids = tuple(item.candidate_id for item in all_evidence)
    document_ids = tuple(sorted({item.document_id for item in all_evidence}))
    if len(currencies) > 1:
        return ShipmentIssue(
            "entered-value-allocation-currency-mismatch",
            PLANT_LINE_ENTERED_VALUE,
            EvidenceScope.SHIPMENT.value,
            "HIGH",
            "ENTERED_VALUE_ALLOCATION_CURRENCY_MISMATCH",
            "Plant-line entered-value allocations and the shipment total use incompatible currencies.",
            candidate_ids,
            document_ids,
        )

    line_total = sum((amount for _currency, amount in selected if amount is not None), Decimal(0))
    if line_total == total_amount:
        return None
    return ShipmentIssue(
        "entered-value-allocation-mismatch",
        PLANT_LINE_ENTERED_VALUE,
        EvidenceScope.SHIPMENT.value,
        "HIGH",
        "ENTERED_VALUE_ALLOCATION_MISMATCH",
        f"Plant-line entered-value allocations total {line_total} but shipment total is {total_amount}.",
        candidate_ids,
        document_ids,
    )


def process_shipment(*, documents: list[ShipmentDocumentInput], ruleset: LaceyRuleset = LaceyRuleset()) -> ShipmentResolution:
    """Reconcile evidence by semantic entity before declaring contradictions."""
    identifiers = [item.document_id for item in documents]
    if len(identifiers) != len(set(identifiers)):
        raise ValueError("Shipment document_id values must be unique.")
    resolved: list[ShipmentDocumentResolution] = []
    evidence_by_field: dict[str, list[ShipmentEvidence]] = {}
    inferred_not_used = 0
    out_of_scope_not_used = 0
    unresolved_roles: list[tuple[str, AdmittedCandidate]] = []
    for item in documents:
        if item.resolution is None and not item.content:
            raise ValueError("ShipmentDocumentInput requires resolution or non-empty content.")
        resolution = item.resolution or process_document(
            filename=item.filename,
            content=item.content or b"",
            role_hint=item.role_hint,
        )
        resolved.append(ShipmentDocumentResolution(item.document_id, item.filename, resolution))
        for original_key, field in resolution.fields.items():
            candidates = (
                field.candidates
                if field.status is FieldStatus.CONFLICT
                else ((field.winning_candidate,) if field.status is FieldStatus.MATCHED and field.winning_candidate else ())
            )
            for index, candidate in enumerate(candidates):
                if candidate.raw.evidence_class is EvidenceClass.INFERRED:
                    inferred_not_used += 1
                    continue
                if is_out_of_scope_context(candidate.provenance.source_text):
                    out_of_scope_not_used += 1
                    continue
                field_key = _typed_key(original_key, candidate.raw.label or "")
                if original_key in {"party", "party_name", "name"} and field_key == original_key:
                    unresolved_roles.append((item.document_id, candidate))
                    continue
                if field_key not in _CATALOG:
                    continue
                scope = _scope(_CATALOG[field_key])
                row_key = association_key(candidate, scope.value, item.document_id)
                if field_key == PLANT_LINE_ENTERED_VALUE and row_key is None:
                    field_key = SHIPMENT_TOTAL_ENTERED_VALUE
                    scope = EvidenceScope.SHIPMENT
                component_key = row_key if scope is EvidenceScope.PLANT_COMPONENT else None
                line_key = row_key if scope is EvidenceScope.MERCHANDISE_LINE else None
                authority_key = (
                    "bill_of_lading"
                    if field_key in {"master_bill_of_lading", "house_bill_of_lading"}
                    else (PLANT_LINE_ENTERED_VALUE if field_key == SHIPMENT_TOTAL_ENTERED_VALUE else field_key)
                )
                semantic = _quantity_semantic_type(candidate.raw.label or "")
                if field_key == "plant_quantity" and semantic is not QuantitySemanticType.PLANT_MATERIAL_QUANTITY:
                    continue
                record = ShipmentEvidence(
                    f"{item.document_id}:{field_key}:{candidate.provenance.block_id}:{index}",
                    item.document_id,
                    field_key,
                    candidate.raw.normalized_value,
                    candidate,
                    candidate.score,
                    authority(authority_key, candidate.document_type),
                    scope,
                    line_key,
                    component_key,
                    semantic,
                )
                evidence_by_field.setdefault(field_key, []).append(record)

    fields = {key: _reconcile(key, evidence_by_field.get(key, [])) for key in _CATALOG}
    if evidence_by_field.get(SHIPMENT_TOTAL_ENTERED_VALUE):
        fields[SHIPMENT_TOTAL_ENTERED_VALUE] = _reconcile(
            SHIPMENT_TOTAL_ENTERED_VALUE,
            evidence_by_field[SHIPMENT_TOTAL_ENTERED_VALUE],
        )
    issues: list[ShipmentIssue] = []
    allocation_issue = _entered_value_allocation_issue(fields)
    if allocation_issue is not None:
        issues.append(allocation_issue)
    for document_id, candidate in unresolved_roles:
        issues.append(ShipmentIssue(
            f"unresolved-role:{document_id}:{candidate.provenance.block_id}",
            "party_name", EvidenceScope.SHIPMENT.value, "MEDIUM", "UNRESOLVED_ROLE",
            "Party evidence has no explicit supported role.",
            (f"{document_id}:party:{candidate.provenance.block_id}",), (document_id,),
        ))
    for key, result in fields.items():
        evidence = result.supporting_evidence
        ids = tuple(item.candidate_id for item in evidence)
        docs = tuple(sorted({item.document_id for item in evidence}))
        cardinality = _cardinality(key)
        if result.state is ReconciliationState.CONFLICT:
            issues.append(ShipmentIssue(f"conflict:{key}", key, _scope(cardinality).value, "HIGH", "CONFLICT", f"Conflicting admissible {key} evidence for the same semantic entity.", ids, docs))
        elif result.state is ReconciliationState.REVIEW_REQUIRED:
            kind = "AMBIGUOUS_ASSOCIATION" if cardinality in {FieldCardinality.PER_PLANT_COMPONENT, FieldCardinality.PER_MERCHANDISE_LINE} else "INCONSISTENT_SET"
            issues.append(ShipmentIssue(f"review:{key}", key, _scope(cardinality).value, "MEDIUM", kind, f"{key} requires semantic review.", ids, docs))
        if key in _IMPORTANT and evidence and max(item.source_authority for item in evidence) < 10:
            issues.append(ShipmentIssue(f"low-authority:{key}", key, _scope(cardinality).value, "MEDIUM", "LOW_AUTHORITY_ONLY", f"Only low-authority evidence is available for {key}.", ids, docs))
            fields[key] = ReconciliationResult(key, ReconciliationState.REVIEW_REQUIRED, (), evidence)
    for requirement in ruleset.preparation.requirements:
        acceptable = [fields[key] for key in requirement.acceptable_fields]
        if not any(result.state is not ReconciliationState.MISSING for result in acceptable):
            issues.append(ShipmentIssue(f"missing:{requirement.requirement_id}", "bill_of_lading", EvidenceScope.SHIPMENT.value, "HIGH", "MISSING_REQUIRED", f"Required preparation evidence missing for {requirement.requirement_id}.", (), ()))
    blocking = any(issue.severity == "HIGH" for issue in issues)
    review = any(issue.severity == "MEDIUM" for issue in issues) or any(
        result.state in {ReconciliationState.NEAR_MATCH, ReconciliationState.REVIEW_REQUIRED}
        for result in fields.values()
    )
    readiness = ShipmentReadiness.BLOCKED if blocking else (ShipmentReadiness.REVIEW_REQUIRED if review else ShipmentReadiness.READY)
    metric_fields = [
        result for key, result in fields.items()
        if key != SHIPMENT_TOTAL_ENTERED_VALUE
    ]
    metrics = {
        "documents_processed": len(resolved),
        "fields_supported": sum(result.state in {ReconciliationState.SUPPORTED, ReconciliationState.SUPPORTED_MULTIPLE, ReconciliationState.NEAR_MATCH} for result in metric_fields),
        "fields_conflicting": sum(result.state is ReconciliationState.CONFLICT for result in metric_fields),
        "fields_missing": sum(result.state is ReconciliationState.MISSING for result in metric_fields),
        "fields_review_required": sum(result.state is ReconciliationState.REVIEW_REQUIRED for result in metric_fields),
        "inferred_candidates_not_used": inferred_not_used,
        "out_of_scope_candidates_not_used": out_of_scope_not_used,
        "issues_total": len(issues),
    }
    return ShipmentResolution(ENGINE_VERSION, tuple(resolved), fields, tuple(issues), readiness, metrics, ruleset.version)
