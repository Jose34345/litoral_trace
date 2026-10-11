# Assurance V2 — shared authority contract

**Contract:** `assurance.v2/1.0.0` · **Status:** proposed, additive · **Owner:** Agent 2 (Entity Identity / Evidence / Human Decisions / Memory).

This is an integration contract, **not** a second source of truth or a new extraction engine. `lacey_engine` produces observations; `us_lacey` resolves authority, using existing semantic evidence, supplier/product identity, audit events and source-set revisions. No V2 record alone authorizes canonical export.

## Authority and state vocabulary

| State | Meaning | May become canonical? |
|---|---|---|
| `CANDIDATE` | Raw AI/deterministic suggestion, source-referenced if available; not adjudicated | No |
| `SUPPORTED` | Source-backed assertion with proven entity binding; no known contradiction | Only through existing publication gate |
| `CONFLICTED` | Two applicable assertions disagree; no implicit winner | No |
| `REVIEW_REQUIRED` | Identity, source, context or evidence insufficient | No |
| `HUMAN_AUTHORIZED` | Authenticated reviewer chose a supported/corrected value in a fenced context | Through existing publication gate, never by writing the source node |
| `REJECTED` | Explicitly rejected; permanently barred from memory promotion as that decision version | No |
| `SUPERSEDED` | Replaced by subsequent decision/document/source set; history retained | No |
| `REVOKED` | Verification withdrawn; no further reuse | No |

The **claim** is a statement from a source, not an approved field. `SUPPORTED` is not synonymous with `HUMAN_AUTHORIZED` or `READY`. Model confidence is non-authoritative; comparison/normalization cannot destroy source values.

## Common envelope

All interfaces include `contract_version="assurance.v2/1.0.0"`, `organization_id` (tenant int), `operation_id` (shipment int), `source_set_revision_id`, `source_set_fingerprint` and `generation` whenever shipment-scoped. Public/external IDs are opaque UUIDs; persistent references use actual tenant-bound internal FK IDs. A stale revision cannot create new authoritative decisions/current snapshots. Timestamps are UTC ISO-8601. Idempotency keys are scoped by tenant and source-set revision; retries must not produce duplicate events.

### `SourceLinkedClaim`

`claim_id: UUID`, `subject: EntityIdentity`, `field_name: str`, `raw_value: str | null`, `normalized_value: str | null`, `candidate_origin: DOCUMENT | DETERMINISTIC | AI | HUMAN`, `state: CANDIDATE | SUPPORTED | CONFLICTED | REVIEW_REQUIRED | REJECTED | SUPERSEDED`, `semantic_evidence_node_ids: list[int]`, `assurance_document_id: int | null`, `document_version_id: int | null`, `source_span_ids: list[int]`, `source_set_revision_id: int`, `source_set_fingerprint: str`, `generation: int`, `observed_at: datetime`.

Support requires a resolvable original document/span or explicit parser source anchor, a matching subject, and a non-superseded source set. AI-only claims remain `CANDIDATE`.

### `EntityIdentity`

`entity_type: SUPPLIER | SUPPLIER_PRODUCT | SHIPMENT | SHIPMENT_LINE | DOCUMENT | DOCUMENT_VERSION | SOURCE_SET_REVISION`, `entity_id: int | UUID`, `organization_id: int`, `identity_status: DISCOVERED | ACTIVE | VERIFIED | REVOKED`, `exact_identifier_kind: MID | VENDOR_CODE | NAME_ADDRESS | SKU | LINE_REFERENCE | DOCUMENT_ID | REVISION_ID | HUMAN_CONFIRMED | null`, `exact_identifier: str | null`, `parent_entity_id: int | UUID | null`, `identity_event_id: int | null`.

Supplier, supplier product and shipment line are distinct entities. Supplier-product identity is `(tenant, supplier_id, exact SKU)`; line-to-product is an **explicit versioned link**, not line-reference equality. HTS, fuzzy names and model similarity must not merge entities. Human alias/merge/unmerge events are append-only and logical (no destructive key rewrites).

### `EvidenceAssertion`

`assertion_id: UUID`, `claim_ids: list[UUID]`, `subject: EntityIdentity`, `field_name: str`, `value: str`, `state: SUPPORTED | CONFLICTED | REVOKED | SUPERSEDED`, `semantic_evidence_node_ids: list[int]`, `source_document_ids: list[int]`, `decision_id: UUID | null`, `valid_from: datetime | null`, `valid_until: datetime | null`, `created_at: datetime`.

This is an authority **view over original claims**: references existing `semantic_evidence_nodes` and/or `us_lacey_evidence_claim`, never a copied original text corpus. Unsupported claims cannot become assertions.

### `EvidenceConflict`

`conflict_id: UUID`, `subject: EntityIdentity`, `field_name: str`, `assertion_ids: list[UUID]`, `classification: CORROBORATION | CONTRADICTION | OTHER_ENTITY | HISTORICAL | INSUFFICIENT_IDENTITY | ABSENT | REVOKED | OBSOLETE`, `state: OPEN | RESOLVED | SUPERSEDED`, `resolution_decision_id: UUID | null`, `reason_codes: list[str]`.

Only evidence for the **same exact entity, field, applicable period and context** can corroborate or contradict. A human decision (not confidence alone) resolves contradictions.

### `HumanDecision`

`decision_id: UUID`, `actor_user_id: int`, `action: ACCEPT | REJECT | CORRECT | SUPERSEDE`, `subject: EntityIdentity`, `field_name: str`, `selected_value: str | null`, `claim_ids: list[UUID]`, `evidence_assertion_ids: list[UUID]`, `reason: str`, `context: dict`, `source_set_revision_id: int`, `source_set_fingerprint: str`, `generation: int`, `document_versions: list[int]`, `supersedes_decision_id: UUID | null`, `decided_at: datetime`, `idempotency_key: str`, `audit_operation_event_id: int`.

A trusted actor is resolved from the authenticated user/session; it is **never** supplied by AI. Accept/correct are human-authorized only when evidence/context are explicit. Reject forbids promotion. Supersession creates a new event rather than editing an old decision. The existing `us_lacey_operation_events` system of record must record the decision atomically.

### `MemoryRecord`

`memory_id: UUID`, `supplier_id: int`, `supplier_product_id: int`, `field_name: str`, `normalized_value: str`, `origin_claim_ids: list[UUID]`, `origin_document_ids: list[int]`, `decision_id: UUID`, `source_set_revision_id: int`, `scope: SUPPLIER_PRODUCT`, `valid_from: datetime`, `valid_until: datetime`, `status: VERIFIED | REVOKED | SUPERSEDED | EXPIRED`, `regulatory_context: dict | null`, `created_at: datetime`.

Default memory fields are `genus`, `species`, `country_of_harvest` only; stability is conditional, not perpetual. `quantity`, `entered_value`, `arrival_date`, `line_reference`, `HTS` and shipment identifiers are **never** automatically promoted/reused. Existing `us_lacey_supplier_evidence` / `us_lacey_evidence_claim` hold verified claims; V2 adds authority links/lifecycle metadata rather than duplicating facts.

### `ReuseEligibility`

`eligible: bool`, `status: ELIGIBLE | BLOCKED | REVIEW_REQUIRED`, `reason_codes: list[str]`, `memory_id: UUID | null`, `target_operation_id: int`, `target_source_set_revision_id: int`, `target_line_reference: str`, `target_supplier_product_id: int`, `field_name: str`, `evaluated_at: datetime`, `regulatory_context_version: str | null`.

Every reuse must pass same tenant; exact ACTIVE/VERIFIED supplier and SKU; explicit line binding; allowlisted field; validity; unrevoked verification; retained source/decision provenance; compatible context/ruleset; and no contradiction or authoritative current-shipment value. The current shipment wins. Reused outputs must be explicitly labeled `REUSED_HISTORICAL` and retain origin document/decision references; never pretend historical text came from the current document.

### `AssuranceCaseSnapshot`

`snapshot_id: UUID`, `organization_id: int`, `operation_id: int`, `source_set_revision_id: int`, `source_set_fingerprint: str`, `generation: int`, `state: CURRENT | STALE`, `entity_refs: list[EntityIdentity]`, `claim_refs: list[UUID]`, `assertion_refs: list[UUID]`, `open_conflict_refs: list[UUID]`, `decision_refs: list[UUID]`, `reuse_results: list[ReuseEligibility]`, `created_at: datetime`.

Snapshot is a **read model**, not canonical publication. It is CURRENT only when its operation source-set revision/generation/fingerprint still matches the current finalized source set.

## Transitions and invariants

`CANDIDATE → SUPPORTED` requires existing source provenance and exact identity; `SUPPORTED → CONFLICTED` on applicable contradictory current evidence; `SUPPORTED/CONFLICTED/REVIEW_REQUIRED → HUMAN_AUTHORIZED` requires a persisted authenticated decision. `HUMAN_AUTHORIZED → VERIFIED memory` is a separate allowlisted promotion gate. `REJECTED` cannot promote; new decisions may supersede but cannot erase rejection. New document version/revision or revocation triggers re-evaluation, not deletion.

**Fail closed:** no source / incomplete identity / competing values / revoked or missing original support / expired evidence / stale generation / cross-tenant refs → `REVIEW_REQUIRED` or `BLOCKED`, never automatic reuse.

## Integration and compatibility

- **Consume** `semantic_evidence_nodes`, `document_text_spans`, `assurance_documents`, `us_lacey_source_set_revisions/members`; do not fork provenance.
- **Extend** `us_lacey_supplier*`, `us_lacey_operation_product_link`, `us_lacey_supplier_evidence`, `us_lacey_evidence_claim` conservatively. Keep legacy readers/writers running while introducing V2 records.
- **Emit** atomic `us_lacey_operation_events` of type `HUMAN_REVIEW` or `EVIDENCE_REUSED` (the event_type check is constrained); detailed action/reason goes in `details`, with deterministic `event_key`.
- **Other agents:** extraction emits source-linked candidates but must not decide verified authority; presentation consumes `AssuranceCaseSnapshot` and `ReuseEligibility` without manufacturing verdicts. The eventual review/worker hook must call V2 authority services inside the existing source-set fence and same DB transaction; no edits to worker/UI/export are part of Agent 2.
- No direct write to `projection.py`, `lacey_engine/`, canonical shipment truth, PPQ505, LAWGS or ACE.
- Storage uses composite `(organization_id,id)` FKs, FORCE RLS and tenant GUC `app.current_organization_id`. Every decision/retry is idempotent; SQL migration must branch from the **actual current single Alembic head** and be PostgreSQL-tested before approval.

**Change policy:** bump major if meanings/authority differ, minor for additive optional fields, patch for editorial clarification. Consumers must reject unknown major versions rather than silently infer semantics.
