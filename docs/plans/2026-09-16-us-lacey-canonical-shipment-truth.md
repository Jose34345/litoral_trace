# Canonical shipment truth refactor — implementation plan

**Goal:** make one deterministic canonical shipment/plant-line model the final publisher of machine-derived U.S. Lacey review state, while preserving human decisions and existing exports.

## Task 1 — Lock the canonical contract with RED tests

Create `tests/lacey_engine/test_canonical_shipment_truth.py` covering the seven-document line semantics in infrastructure-free payloads:
- two merchandise lines remain parallel, not conflicting;
- taxon components join only their explicit description line;
- Pinus/Eucalyptus evidence cannot cross lines;
- low-authority harvest-country evidence remains `REVIEW_REQUIRED` with a visible value;
- ambiguous plant-component joins fail closed.

## Task 2 — Build the pure canonical model

Create `src/litoral_trace/us_lacey/canonical_shipment_truth.py` with immutable canonical evidence, field and plant-line structures plus `build_canonical_shipment_truth(payload)`.

The builder consumes the serialized Engine 2 shipment contract, partitions by association, links taxon components to merchandise lines deterministically, computes per-entity states and produces stable line ordering.

## Task 3 — Add one canonical database publisher

Add `publish_canonical_shipment_truth(organization_id, operation_id)` in the same module:
- load latest shipment resolution;
- ensure missing PPQ line skeletons only when canonical truth proves them;
- preserve all human-reviewed fields;
- supersede previous machine candidates on line-scoped fields;
- write only entity-local canonical candidates;
- rewrite unreviewed line field state/provenance from canonical truth;
- resolve stale legacy conflicts where canonical semantics prove parallel entities;
- retain/create blocking state for true same-entity conflicts;
- refresh operation readiness atomically.

## Task 4 — Retire parallel final authority

- Change `project_engine2_supported_suggestions()` to delegate final line publication to canonical truth while retaining its pure compatibility helpers/tests.
- Prevent `ai_suggestions.py` from independently publishing plant-line fields.
- Filter `REJECTED` candidates from operation detail so superseded historical machine candidates cannot contaminate review cards.
- Keep per-document projection as provisional ingestion compatibility only; canonical publication wins after source-set finalization.

## Task 5 — PostgreSQL regression

Add/extend database tests proving:
- canonical publication preserves reviewed/human values;
- old pending candidates become `REJECTED` and disappear from customer candidate views;
- country-of-harvest candidate is `REVIEW`, not `MISSING`;
- false HTS/description conflicts are resolved after canonical publication;
- rerunning publication is idempotent.

## Task 6 — Verification and release

Run targeted canonical tests, affected U.S. Lacey tests, full CI and U.S. Lacey PostgreSQL Gate. Review the PR patch and open threads. Merge only on exact green head SHA. Then verify Render deploys the merge SHA, `/health` is 200, runtime startup is clean, and the service is `live`.