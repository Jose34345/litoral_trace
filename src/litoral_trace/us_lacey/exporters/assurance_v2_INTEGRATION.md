# Assurance V2 Outputs — Agent 3 integration contract

**Scope:** standalone, unmounted adapter. Branch `feat/assurance-v2-outputs`,
base `37d92856d86b4d4adbb72059e7a99f51e27b968a`.
Do not merge independently of Agent 1 (source-linked claims) and Agent 2
(entity authority, decision lifecycle and persistent memory).

## Input: AssuranceCase v1

Agent 2's authoritative read adapter must construct
`exporters.assurance_v2.AssuranceCase` for ONE authenticated organization and
operation, using immutable current source-set reads. The UI NEVER chooses which
candidate becomes fact.

- `organization_id, operation_id`: authenticated tenant and shipment public ID.
- `source_set_fingerprint, source_set_revision`: snapshot's source generation.
- `current_source_set_fingerprint, current_source_set_revision`: current authoritative pointer.
- `documents[]`: `id, name, sha256, version, role`. A document ID must be stable and
  must resolve via a separately authenticated document controller.
- `lines[]`: stable shipment line references; `identities[]`: verified
  `line_reference, supplier_id, product_id, sku, status`. A line ref is not a SKU.
- `fields[]`: `line_reference, name, value, authority, evidence_ids[],
  document_ids[], decision_id?, reuse_id?`. Authority values are EXACT:
  `SUPPORTED | HUMAN_CONFIRMED | VERIFIED_REUSE | CANDIDATE`.
  Candidates must not be promoted by projection or the browser.
- `exceptions[]`: `id, field_name, line_reference, blocking, status,
  reason, entity_ref, candidates[]`. Each candidate may carry
  `id, value, document_id, page, locator, evidence_id, document_name`.
- `decisions[]`: immutable reviewer events with `id, field_name,
  line_reference, action (ACCEPT/CORRECT), value, actor_id, decided_at, reason`.
- `reuses[]`: immutable history events with `id, evidence_id, verified_by,
  status, valid_until, revoked_at?, source_document_hash, supplier_id,
  product_id, line_reference`.
- `ruleset_version, rules[]`: deterministic rule outputs with
  `id, version, inputs_fingerprint, status, blocking`. PASS is not a shipment
  compliance certification.
- `export_authorization`: `id, actor_id, authorized_at, status=AUTHORIZED,
  case_fingerprint, source_set_fingerprint`.
  The `case_fingerprint` must match `AssuranceCase.authority_fingerprint()`
  AFTER all state and decisions are frozen. It changes on new claims, identities,
  evidence, rules, document versions or reviewer events.

**Important:** this is an adapter boundary, not a contract claiming that
Agent 2 has already delivered these exact object keys. Integration should
map final authority events, not introduce another decision table or new model.

## Output gate

`evaluate_filing_gate(case)` denies when:
- source set stale, missing/duplicated documents/hashes/versions;
- shipment line/product identities unresolved or inconsistent;
- unsupported or unlinked values, missing authoritative evidence IDs;
- human-confirmed values without matching authenticated decision events;
- reuses with missing source hash, expired/revoked state or wrong supplier/product binding;
- PPQ505-required values missing/invalid (including two-letter country review);
- unresolved exceptions/undecided blocking contradictions;
- rules missing, unversioned or blocking non-PASS/non-NOT_APPLICABLE;
- explicit export authorization missing or bound to another case/source set.

A stale package remains readable as historical evidence, but cannot be
*reissued as a current filing-ready package*.

`issue_assurance_package` captures:
- exact authority snapshot (documents/hashes/versions/claims/decisions/memory/rules),
- source-set and case fingerprints,
- generation time, authorized export values,
- **exact output bytes** (LAWGS XML, Excel, PPQ505 preparation JSON),
- package SHA-256 over the canonical snapshot.

The metadata expressly states `submitted_to_agency=false`. PPQ505 JSON is
a preparation record, NOT a fillable federal PDF, nor proof of CBP/APHIS acceptance.
The LAWGS XML writer supports merchandise rows, not transmission.

`draft_assurance_excel` exports ONLY visibly marked blockers, never an XML
that could be confused with a final LAWGS-ready file.

### Existing Phase E limitation

Legacy `/operations/{id}/export/lawgs-xml` and `.../excel` currently use
the old routers and are **not** wired to the new V2 gate. This agent cannot
modify shared routes; the integration PR MUST either route *filing-ready*
exports exclusively through an authorized V2 package or label existing paths
as draft/legacy, and preserve compatibility tests. The Phase E fallback to
`proposed_value` was removed, and semantic display values can no longer
override unresolved or empty `effective_value`.

## New endpoints (not mounted)

A trusted integrator must call `create_assurance_v2_router` with explicit
dependencies `principal_for(request)`, `case_for(principal, op)`,
`decision_gateway(principal, op, command)`, `csrf_verify(principal, token)`,
`package_store` and `document_link_for(principal, op, document, page)`.

| Method | Route | Behavior |
|---|---|---|
| GET | `/assurance-v2/operations/{operation_id}` | exception-first assurance case; read only |
| POST | `/.../{operation_id}/decisions` | sends action to Agent 2 gateway; 202 receipt; no UI-owned state |
| POST | `/.../{operation_id}/issue` | current, CSRF-bound case; 409 when blocked; persists immutable package |
| GET | `/.../{operation_id}/draft.xlsx` | marked incomplete workbook; never ready XML |
| GET | `/.../{operation_id}/packages/{fingerprint}/manifest` | historical immutable manifest scoped by tenant |
| GET | `/.../{operation_id}/packages/{fingerprint}/ppq505_preparation_json` | structured preparation data, not submitted |
| GET | `/.../{operation_id}/packages/{fingerprint}/lacey_excel` | frozen XLSX bytes |
| GET | `/.../{operation_id}/packages/{fingerprint}/lawgs_xml` | frozen LAWGS XML merchandise rows |

`PostgresOperationEventPackageStore` uses the existing immutable, RLS/FORCE-RLS
`us_lacey_operation_events` table (`PACKAGE_GENERATED`) to store the entire
tenant-owned package as JSONB. This is a transitional persistence adapter,
not a new multi-tenant schema. It must be load-tested for package size and
atomicity/idempotency under concurrency before being exposed publicly.
No new schema or migrations are introduced by this agent.

## Demonstration

From the isolated worktree with Python dependencies installed:

```powershell
$env:PYTHONPATH="src"
python -m pytest -q tests/test_assurance_v2_outputs_core.py tests/test_assurance_v2_ui_contract.py
python -m litoral_trace.web.us_lacey_assurance_v2_demo
```

Open locally:
`http://127.0.0.1:8765/assurance-v2/operations/11111111-2222-3333-4444-555555555555`

The synthetic demo binds only localhost, has deliberately fixture-only
authentication, never sends any real review command and cannot issue final
filing-ready outputs while the fixture conflict remains OPEN.

Dedicated PostgreSQL acceptance:
```powershell
$env:ENABLE_POSTGRES_TESTS="1"
$env:TEST_POSTGRES_DATABASE_URL="<dedicated migrated runtime database>"
$env:TEST_POSTGRES_MIGRATION_DATABASE_URL="<dedicated migrated owner database>"
python -m pytest -q tests/test_assurance_v2_outputs_postgres.py
```
Do NOT point these variables to production.

## Pending integration gates

1. Confirm Agent 1 provenance IDs and source-document permissioned deep links.
2. Bind Agent 2's finalized command/event schema and review version/ETag checks.
3. Bind current source-set reads under a single transaction and protect
   issue/package-write TOCTOU with source-set fence/advisory lock.
4. Adapt PPQ505 conditional requirements by actual rule applicability, not a
   simulated compliance judgment.
5. Integrate tenant auth, permissioned principal, CSRF, web router and static assets
   in shared application entrypoint (owned by integration agent).
6. Execute PostgreSQL/RLS acceptance and full CI on the merged integration branch.
7. Deprecate or relabel legacy Phase E export endpoints, so a non-gated legacy
   XML cannot be mistaken for an authorized Assurance V2 filing artifact.

No merge, no production deployment and no government submission occurs here.
