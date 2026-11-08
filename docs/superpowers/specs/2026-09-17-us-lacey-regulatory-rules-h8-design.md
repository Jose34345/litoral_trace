# U.S. Lacey Hito 8 — Deterministic Regulatory Rules Design

## Goal
Add a deterministic, versioned, auditable U.S. Lacey regulatory-rules layer that consumes source-backed Product Intelligence, taxonomy and operation context and produces rule-scoped `PASS` / `FAIL` / `INDETERMINATE` assessments without creating canonical filing truth.

## Authority and safety boundary
- A rule `PASS` means only that the named rule's criteria are satisfied by the supplied evidence. It never means the shipment is globally compliant.
- Missing, ambiguous, conflicting or unsupported inputs yield `INDETERMINATE`, never a guessed PASS.
- Rule output is non-canonical and must not write to `canonical_shipment_truth`, PPQ505, LAWGS or ACE.
- AI/model output cannot be an authoritative rule input unless separately confirmed by an allowed authority path.
- Every assessment records ruleset id/version, reason codes, calculation trace and evidence/input references.
- Historical source values remain untouched.

## Current official rule basis
Validated against APHIS guidance on 2026-09-17.

### De minimis
APHIS states that the declaration exception applies when both conditions are met:
1. plant material is no more than 5 percent of the total weight of each individual product unit; and
2. total plant material in an entry of products in the same 10-digit HTS provision does not exceed 2.9 kilograms.

The exception does not apply when the product contains plant material protected under the CITES / ESA / applicable State-law categories described by APHIS.

Official source: https://www.aphis.usda.gov/plant-imports/file-lacey-act-declaration/requirements

### SPECIAL / COMPOSITE
APHIS allows `SPECIAL / COMPOSITE` only when the material is manufactured from small fibers of more than one kind of plant, is mechanically processed/mixed/chemically bonded, and the scientific name cannot be determined after exercising due care. Thin plies or layers of solid wood do not qualify. When scientific names are known, the SUD should not be used.

Official sources:
- https://www.aphis.usda.gov/plant-imports/file-lacey-act-declaration/requirements
- https://www.aphis.usda.gov/plant-imports/file-lacey-act-declaration/special-use-designations

## Architecture
Create a focused package:

`src/litoral_trace/us_lacey/regulatory/rules/`

It contains pure immutable contracts and deterministic evaluators. Persistence and source-set lifecycle live in a separate U.S. Lacey integration module, not in the pure rules package.

Flow:

```text
Product Intelligence snapshot + taxonomy + exact operation context
    -> regulatory input adapter
    -> versioned deterministic rules
       -> DE_MINIMIS
       -> SPECIAL_COMPOSITE
    -> regulatory assessment snapshot (non-canonical)
    -> future exception-first human review
```

## Rule result contract
Each rule produces:
- `rule_id`
- `ruleset_version`
- `status`: `PASS | FAIL | INDETERMINATE`
- stable `reason_codes`
- human-readable explanation safe for customer display
- calculation trace with original and normalized numeric values
- evidence/input references
- `review_required` (`true` for every `INDETERMINATE`; may also be true for advisory PASS/FAIL where policy requires review)

No aggregate shipment-level PASS is introduced.

## De minimis input contract
The pure evaluator receives explicit facts; it does not scrape arbitrary payload dictionaries internally:
- product/SKU reference
- exact 10-digit HTS provision
- plant-material mass per individual product unit in kg
- total individual product-unit mass in kg
- aggregate plant-material mass for the entry and same 10-digit HTS provision in kg
- protected-plant status: `CLEAR | PRESENT | UNKNOWN`
- source/evidence references for each supplied fact

Rules:
- any required numeric/context fact absent or invalid => `INDETERMINATE`
- protected status `UNKNOWN` => `INDETERMINATE`
- protected status `PRESENT` => `FAIL`
- plant percentage `<= 5.00%` and same-HTS entry plant mass `<= 2.900 kg`, with protected status `CLEAR` => `PASS`
- either numeric threshold exceeded => `FAIL`
- exact boundaries 5.00% and 2.900 kg are inclusive
- use `Decimal`, never binary float, for regulatory arithmetic

The adapter may derive plant-unit mass from source-backed BOM components only when every contributing value and plant/non-plant classification needed for the calculation is known. Otherwise it supplies no derived value and the rule remains `INDETERMINATE`.

## SPECIAL / COMPOSITE input contract
The evaluator receives explicit evidence-backed facts:
- material/component reference
- `small_fibers_more_than_one_plant_kind`: `YES | NO | UNKNOWN`
- `mechanically_processed_mixed_chemically_bonded`: `YES | NO | UNKNOWN`
- `thin_solid_plies_or_layers`: `YES | NO | UNKNOWN`
- `species_determinable_after_due_care`: `YES | NO | UNKNOWN`
- evidence references

Rules:
- any required fact `UNKNOWN` => `INDETERMINATE`
- thin solid plies/layers `YES` => `FAIL`
- either composite construction criterion `NO` => `FAIL`
- species determinable after due care `YES` => `FAIL`
- all positive composite conditions satisfied, thin solid plies `NO`, species determinable after due care `NO` => `PASS`

A small deterministic material-name classifier may populate obvious construction facts for exact curated aliases such as MDF/HDF/OSB/particle board/paper/paperboard/cardboard, but it must never fabricate the due-care fact. Plywood/layered solid-wood aliases may deterministically establish the disqualifying thin-ply fact. Unknown names remain unknown.

## Persistence
Add Alembic revision `050_lacey_regulatory_assessment_snapshots` and model `UsLaceyRegulatoryAssessmentSnapshot`.

Tenant-scoped table fields:
- id/public_id
- organization_id
- operation_id
- source_set_revision_id
- generation
- source_set_fingerprint
- ruleset_version
- input_fingerprint
- status: `CURRENT | STALE`
- assessment_count
- indeterminate_count
- payload_json
- created_at/finalized_at

Constraints:
- tenant-aware foreign keys to operation/source-set revision
- one regulatory snapshot per organization + source-set revision + ruleset version
- RLS + FORCE RLS
- runtime role gets scoped CRUD; worker executor receives no direct table privileges, matching Product Intelligence
- stale source generations may never be returned as current
- historical migration 049 remains unchanged

## Input fingerprint
Create a deterministic SHA-256 fingerprint over the exact rule-relevant inputs plus ruleset version. JSON must be serialized with stable key ordering and normalized decimal strings. The fingerprint makes recalculation reproducible and detects rule/input drift without altering the source evidence.

## Integration
Add `src/litoral_trace/us_lacey/regulatory_assessment_snapshot.py` as the application boundary.

It:
1. verifies the claimed current `FINALIZING` source-set generation;
2. loads the current Product Intelligence snapshot and exact operation/plant-line context through existing tenant-scoped models;
3. converts only supported values into explicit rule inputs;
4. evaluates all applicable rules;
5. persists one immutable assessment snapshot;
6. marks older snapshots `STALE` when a source set is superseded;
7. exposes current-only read helpers for UI/review.

Worker ordering becomes:

```text
canonical publication
-> Product Intelligence snapshot
-> regulatory assessment snapshot
-> multilingual shadow
-> source-set finalize
-> job COMPLETED
```

Failure of the non-canonical regulatory stage must fail closed: it may not manufacture a safe assessment. The worker must retain an observable stage/error rather than silently presenting old/current regulatory results.

## UI
Add a compact non-canonical Regulatory Assessment panel to the existing operation workspace. It displays rule name, rule-scoped status, reason and calculation trace/evidence summary, with explicit copy that this is an assessment aid and not a final filing determination.

No export integration in Hito 8.

## Tests
### Pure rules
Golden tests include:
- de minimis exactly 5.00% + exactly 2.900 kg + CLEAR => PASS
- 5.01% => FAIL
- 2.901 kg => FAIL
- missing/zero-invalid total unit mass => INDETERMINATE
- protected UNKNOWN => INDETERMINATE
- protected PRESENT => FAIL
- composite qualifying facts + due-care species not determinable => PASS
- thin solid plies => FAIL
- species known/determinable => FAIL
- unknown construction/due-care fact => INDETERMINATE
- no fuzzy/material-name guessing

### Integration
- source-set generation fencing
- snapshot idempotency
- old snapshot becomes STALE on supersession
- current-only reads
- ruleset/input fingerprint determinism
- tenant A cannot read/write tenant B
- PostgreSQL RLS/FORCE RLS and privilege contracts
- worker stage ordering and failure behavior
- UI rendering of PASS/FAIL/INDETERMINATE without global compliance wording

## Out of scope
- automatic CITES/ESA/State-law database synchronization; Hito 8 accepts an explicit protected-status fact and fails closed when unknown
- live ACE/LAWGS submission
- PPQ505 mutation
- canonical shipment truth mutation
- AI adjudication
- broad product-material ontology
- exception-first reviewer actions (next milestone)

## Success criteria
Hito 8 is complete when pure rules are deterministic and versioned, snapshot 050 is tenant-isolated/source-set-fenced, worker and UI integration are regression-protected, canonical/export boundaries remain untouched, and full CI plus the U.S. Lacey PostgreSQL Gate are green.