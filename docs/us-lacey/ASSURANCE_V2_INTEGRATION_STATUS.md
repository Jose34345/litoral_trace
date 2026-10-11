# Assurance V2 integration — release gate

The integration branch combines the four independently green PRs #396–#399
on the production target commit `0f8398de`. This is **not a GA release**.

## Implemented in this integration branch

- Real shared-contract, source-linked-claim, V2 authority journal and
  export-gated workspace modules coexist without import/test regressions.
- `assurance_v2_claim_bridge.py`: converts source-linked claims only when
  tenant, operation, source-set, original SHA-256 and source document version
  match a database-resolved binding. SOURCE SUPPORT NEVER GRANTS HUMAN AUTHORITY.
- `assurance_v2_case_reader.py`: constructs a tenant-scoped, read-only
  AssuranceCase from current DB objects; legacy MATCHED is not V2 authority.
  Eligible historical memory is presented as a proposal, not copied into
  current document evidence or treated as an approved filing input.
- `US_LACEY_ASSURANCE_V2_MODE=OFF` (default) installs no V2 portal routes.
  `SHADOW` installs the existing authenticated, CSRF-aware V2 case view;
  its decision gateway refuses mutation and its read model issues no export
  authorization. ENABLED is rejected until release acceptance.
- Legacy operations, review, storage and filing flows remain unchanged.

## Critical blockers for enabled/live rollout

1. The engine source-linked claim representation still requires an explicit
   ingestion-time adapter bound to original parser output and raw source bytes,
   with durable, source-set-fenced provenance persistence.
2. Agent 2's decision journal is not wired transactionally to existing review
   and canonical publication. Agent 3's final export authorization must be
   committed atomically with current source-set and authority checks.
3. The original synthetic seven-PDF Shipment 1 and six-PDF Shipment 2
   ZIPs have been located in the owner's ChatGPT Library and inspected by
   opening the PDF bytes directly. Those bytes are not yet present on the
   remote CI runner or in the repository; an authorized, controlled fixture
   transfer and unmodified PDF-to-worker E2E are still required. Tests built
   from equivalent extracted source text are NOT byte-for-byte PDF acceptance.
4. Golden integration must assert shipment 1 verified evidence is retained,
   shipment 2 supplier/SKU/line resolves independently of genus/species,
   controlled historical reuse occurs, UI shows accurate provenance and
   a conflict blocks filing. Measure coverage *and* projection loss.
5. PostgreSQL upgrade/RLS, source-set concurrency, original file lifecycle,
   endpoint authorization and export-gate bypass tests must pass on the
   combined branch, not merely on each source PR.
6. Before enabling V2: deploy to staging, inspect representative documents,
   compare V1 versus V2 via shadow mode, and only then canary and promote.

No deployment, migration or source document changes are authorized by this file.
