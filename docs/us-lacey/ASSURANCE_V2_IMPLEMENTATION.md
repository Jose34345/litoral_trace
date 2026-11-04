# Assurance V2 — Agent 2 Implementation and Integration Runbook

**Contract:** `assurance.v2/1.0.0`. **Base:** `0f8398de9132c5cea442590438f8b7245e56f18b`. **Migration:** `078_us_lacey_assurance_v2_authority` (after `077_us_lacey_outreach_human_signals`).

## Scope and compatibility

- Existing `UsLaceySupplier`, `UsLaceySupplierProduct`, `UsLaceyOperationProductLink` remain exact, tenant-scoped identity authorities. Supplier ≠ supplier product ≠ shipment line. No fuzzy, HTS, or similarity merges.
- Existing source documents, document versions (`UsLaceyOperationDocument`), `UsLaceySourceSetRevision/Member`, `SemanticEvidenceNode`, `UsLaceyOperationField`, `UsLaceySupplierEvidence`, `UsLaceyEvidenceClaim` and `UsLaceyOperationEvent` remain the original sources of truth. No duplicate document text, extraction engine or second provenance graph is introduced.
- New `AssuranceV2Decision` and `AssuranceV2DecisionSource` are append-only human authority and provenance overlays; `AssuranceV2MemoryLink` binds existing verified evidence claims to the exact decision/document/revision, without replacing original values; `AssuranceV2IdentityEvent` captures human logical identity activity without physical FK rewrites.
- The migration adds `UNIQUE(users.id, users.organization_id)` to enforce a composite **tenant-aware foreign key** for the authenticated reviewer. All four V2 tables use FORCE RLS and only SELECT/INSERT policies for `litoral_trace_app`; no UPDATE/DELETE grants. All write APIs require an already-authenticated tenant user and must execute inside a transaction with tenant DB context.
- **Opt-in, backward compatible:** the legacy review/worker/reusable evidence pipeline is left untouched; V2 authority endpoints are **not automatically live**. This PR cannot be called end-to-end integrated until the designated view/worker owners wire the adapters, CI accepts PostgreSQL/RLS tests, and a real migrated test DB passes.

## State map and transitions

| From | To | Gate |
|---|---|---|
| AI/deterministic `CANDIDATE` | `SUPPORTED` | Existing source document plus field locator or semantic evidence span, exact entity/line binding |
| `SUPPORTED` | `CONFLICTED` | Same exact entity + field + applicable period/context with inconsistent source-backed values |
| `SUPPORTED / CONFLICTED / REVIEW_REQUIRED` | `HUMAN_AUTHORIZED` | Authenticated actor, explicit `ACCEPT`/`CORRECT`, reason, original document, current finalized source set, immutable audit event |
| Any unresolved candidate | `REJECTED` | Authenticated `REJECT` decision, never promoted |
| Prior decision | `SUPERSEDED` | New `SUPERSEDE` or correction referencing prior decision; prior row remains |
| `HUMAN_AUTHORIZED` | `VERIFIED memory` | Separate field allowlist, exact supplier + SKU and line linkage, source available, original document still current, valid supported value |
| Verified memory | Blocked | Evidence status `REVOKED`, expiry, source document/version supersession, origin decision superseded, context mismatch or new current-shipment evidence |
| Verified memory | `REUSED_HISTORICAL` proposal | All eligibility gates pass; no rewrite into original source evidence and no direct canonical publication |

An AI candidate is never a human decision. A field with a high confidence score but without original traceable support remains advisory. `CONTRADICTION` is never resolved by confidence alone.

`classify_evidence` distinguishes `CORROBORATION`, `CONTRADICTION`, `OTHER_ENTITY`, `HISTORICAL`, `INSUFFICIENT_IDENTITY`, `ABSENT`, `REVOKED` and `OBSOLETE`. It has no fuzzy/scoring authority.

## API surfaces (all operate on a caller-owned SQLAlchemy transaction)

```python
# Caller: authenticated session, tenant context already set.
decision = record_human_decision(
    session,
    organization_id=org_id, operation_id=operation_id,
    source_set_revision_id=current_finalized_revision_id,
    authenticated_user_id=trusted_session_user_id,
    action="ACCEPT", field_name="species", line_reference="1",
    selected_value="Quercus alba", reason="Supplier source reviewed",
    assurance_document_ids=(original_document_id,),
    semantic_evidence_node_ids=(source_node_id,),
    idempotency_key="review:operation:field:revision:decision",
)
memory_link = promote_decision_to_memory(
    session, organization_id=org_id, decision_id=decision.id
)
proposed = evaluate_reuse(
    session, organization_id=org_id, operation_id=next_operation_id,
    source_set_revision_id=next_finalized_revision_id,
    line_reference="1", field_name="species",
    regulatory_context={},
    current_claim_values=tuple(current_source_values),
)
# Caller commits only when all invariants hold and then uses the EXISTING
# canonical review/publication gate. proposed.eligible is NOT a filing verdict.
```

`record_human_decision` supports `ACCEPT`, `REJECT`, `CORRECT`, `SUPERSEDE`. The audit event is appended in the *same database transaction* under `HUMAN_REVIEW`, with a deterministic key and secure actor projection. The decision's reason/selected value/context live in immutable decision rows, not in source nodes. Caller retries must use the same key and payload; a mismatched payload is a hard collision. SQL uniqueness is the concurrent deduplication backstop; any concurrent unique race must retry in a fresh transaction.

`record_identity_event` records `ALIAS_ADD`, `ALIAS_REMOVE`, `MERGE`, `UNMERGE` with a trusted actor, reason, explicit source/target IDs and a reversal pointer. **No automatic merge or reuse through human merge history is enabled in V2.** A separate audited, deterministic consumer must interpret the logical merge graph and reject cycles/multiple targets before authorizing cross-identifier operations.

`build_case_snapshot` is a deterministic read model for the given revision, with `CURRENT` only after finalized/current source-set state and `STALE` otherwise; it must not be treated as a canonical regulatory result. This V2 implementation contains decision IDs and verified memory link IDs, not a complete derived case projection; Agent 3 must compose source-linked claims, conflict queues, rules and eligibility on its own read path.

## Controlled reuse policy

Positive gates:

1. Tenant GUC + tenant-bound supplier, product, source, decision and evidence references.
2. Unique exact ACTIVE/VERIFIED supplier identity, nonblank supplier SKU, explicit current `UsLaceyOperationProductLink` for the line/revision; same SKU under a different supplier is **another entity**.
3. Only `genus`, `species`, `country_of_harvest` (existing `REUSABLE_FIELD_NAMES`). Quantities, shipment line IDs, totals, values, HTS, arrival dates, customs status are never eligible.
4. Human ACCEPT/CORRECT, current origin document version and current origin source-set fingerprint/generation; underlying `us_lacey_supplier_evidence.status=VERIFIED`, active validity window and original source metadata intact.
5. Exact documented context equality (including regulatory version where supplied). Missing/changed context blocks.
6. No current-shipment conflict, human value, or any other source-backed current value; if the current fields have not been materialized, integrating caller **must supply all reconciled current source claims** via `current_claim_values` and only call after finalized source sets. Never use an empty tuple to represent an unknown or partially processed current packet.
7. No contradictory historical verified memory claims from the same exact supplier/product/field.

`ReuseEligibility` reports `BLOCKED` with reason codes, or `ELIGIBLE` with `REUSED_HISTORICAL`, origin `assurance_document_id`, origin `decision_public_id`, link public ID, and original existing evidence-claim ID. It does **not** insert a new raw candidate or mutate original extraction.

Important: original source metadata remains referenceable while derived data exists, but raw Vault bytes may be purged after 4 hours under the evaluation retention policy. If a customer requires later reinspection of original raw files, obtain explicit authorized retention before treating that document as durable inspectable evidence; a hash alone is not a replacement for accessible source contents.

## Integration adapters owned by the other agents

- **Candidate/engine owner:** supply exact semantic node IDs, corresponding source document/version, explicit entity/line scope, and all current evidence values. Do not promote AI hypotheses; do not rewrite original semantic nodes.
- **Human review owner:** on authenticated accept/reject/correct/supersede, provide trusted user ID, current finalized source-set revision, original document IDs and explicit reason; invoke `record_human_decision` and (only if eligible) `promote_decision_to_memory` in a transaction, then existing canonical review/publication. Never accept user-submitted actor IDs as authoritative.
- **Worker/processing owner:** invoke `evaluate_reuse` only after current source-set has finalized and all current candidates/conflicts are materialized; persist the returned *historical* provenance (not "extracted from current document"), and use existing status/readiness gates. This agent has **not modified worker.py, projection.py, web views or canonical publication**.
- **UI/Case owner:** show separate `CANDIDATE`, `SUPPORTED`, `CONFLICTED`, `HUMAN_AUTHORIZED`, `REUSED_HISTORICAL` states; preserve the trace back to each original source. `build_case_snapshot` is an authority skeleton, not a full case UI payload.
- **CI owner:** add `tests/test_assurance_v2_core_postgres.py` to PostgreSQL migration/RLS acceptance after `alembic upgrade head` in a disposable database; run the full test matrix and ensure the canonical Alembic head remains singular.

## Verification

```bash
python -m alembic heads       # expect 078_us_lacey_assurance_v2_authority
python -m pytest -q tests/test_assurance_v2_core_memory.py
python -m pytest -q tests/test_assurance_v2_core_postgres.py
python -m pytest -q tests/test_us_lacey_identity_memory.py tests/test_us_lacey_reusable_evidence.py tests/test_us_lacey_shipment_product_bridge.py
```

PostgreSQL requires disposable test URLs for *separate owner and unprivileged runtime roles*: `ENABLE_POSTGRES_TESTS=1`, `TEST_POSTGRES_MIGRATION_DATABASE_URL` and `TEST_POSTGRES_DATABASE_URL`. Do **not** run migration against a live production database. PostgreSQL/RLS tests must fail (not silently skip) if enabled but revision 078 is missing.

Before approving migration: staging `alembic upgrade head`, validate FORCE RLS (all four tables), reject cross-tenant actor FK inserts, reject cross-tenant writes, deny UPDATE/DELETE, run old migration regression and down-revision checks in the disposable test database.

## Acceptance checklist

- Source-linked verified stable fields from shipment 1 are reusable on shipment 2 for the identical supplier/SKU; provenance references shipment 1.
- Same SKU under another supplier and same supplier in a separate tenant are **not** reusable.
- Any authoritative current-shipment value or contradiction blocks history.
- Superseded original source set/document blocks all derived memory and old worker generations.
- `REJECT` never promotes; `SUPERSEDE` preserves original decision/event and blocks obsolete memory.
- Decisions are linked to authenticated tenant actors, immutable audit events and original document/source evidence.
- Repeated identical decision, audit and memory promotion is idempotent; mismatched repeat fails.
- Version/context/scope changes fail closed.

**Release blocker:** PostgreSQL upgraded-test migration and FORCE RLS integration must be demonstrated before merging. This is an implementation PR only, intentionally not merged or deployed.
