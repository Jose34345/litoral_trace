# Lacey Taxonomy Reference Layer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an offline, versioned USDA GRIN/AP​HIS taxonomy reference layer and make the Golden Benchmark compare botanical fields by canonical taxon identity rather than raw strings.

**Architecture:** Keep all new behavior inside `litoral_trace.lacey_benchmark`. Load a checksum-verified CSV snapshot into immutable indexes, resolve exact accepted names/synonyms/genus+species pairs deterministically, model APHIS special-use designations separately, and add an optional taxonomy-aware preprocessing policy before the existing semantic evaluator. No Engine 2 runtime imports or network calls.

**Tech Stack:** Python 3.11, Pydantic 2, stdlib `csv`, `hashlib`, `json`, `pathlib`, pytest.

**Spec:** `docs/superpowers/specs/2026-09-16-lacey-taxonomy-reference-design.md`

## Global Constraints

- No external HTTP calls during benchmark evaluation or customer shipment processing.
- No fuzzy match may become authoritative.
- USDA GRIN is botanical master data; APHIS SUDs are filing designations and remain distinct from botanical taxa.
- Golden Corpus remains the shipment-level source of expected truth.
- No Engine 2 production behavior changes in this PR.
- Snapshot checksum validation is mandatory and fail-closed.
- Matching remains deterministic.

---

### Task 1: Taxonomy contracts and checksum-verified snapshot loader

**Files:**
- Create: `src/litoral_trace/lacey_benchmark/taxonomy_contracts.py`
- Create: `src/litoral_trace/lacey_benchmark/taxonomy_snapshot.py`
- Create: `tests/lacey_benchmark/test_taxonomy_snapshot.py`

**Interfaces:**
- Produces `TaxonRecord`, `TaxonomySnapshotManifest`, `TaxonomySnapshot`, `TaxonomySnapshotError`.
- `TaxonomySnapshot.load(csv_path: Path, manifest_path: Path) -> TaxonomySnapshot`.

- [ ] Write failing tests covering valid load, SHA-256 mismatch, duplicate source id, conflicting canonical target, and immutable exact-name/genus-species indexes.
- [ ] Run `python -m pytest -q tests/lacey_benchmark/test_taxonomy_snapshot.py` and verify RED due to missing modules/classes.
- [ ] Implement strict Pydantic contracts and CSV loader with stdlib only.
- [ ] Run the focused tests and verify GREEN.
- [ ] Commit `feat: add checksum-verified taxonomy snapshot loader`.

### Task 2: Deterministic taxon resolver and APHIS SUD reference

**Files:**
- Create: `src/litoral_trace/lacey_benchmark/taxonomy_resolver.py`
- Create: `src/litoral_trace/lacey_benchmark/special_use.py`
- Create: `tests/lacey_benchmark/test_taxonomy_resolver.py`
- Create: `benchmarks/lacey/reference/aphis/sud.json`

**Interfaces:**
- `TaxonomyResolver(snapshot: TaxonomySnapshot, special_use: SpecialUseRegistry)`.
- `resolve(*, scientific_name: str | None = None, genus: str | None = None, species: str | None = None) -> TaxonResolution`.
- `TaxonResolutionStatus`: `EXACT_ACCEPTED`, `EXACT_SYNONYM`, `GENUS_SPECIES`, `AMBIGUOUS`, `NOT_FOUND`, `SPECIAL_USE`.

- [ ] Write failing tests for accepted name, exact synonym, `genus + epithet`, full binomial in species field, missing-genus epithet fail-closed, unknown name, and APHIS SUD separation.
- [ ] Run `python -m pytest -q tests/lacey_benchmark/test_taxonomy_resolver.py` and verify RED.
- [ ] Implement normalized exact matching (casefold + whitespace collapse only), accepted-target dereferencing, ambiguity handling, and static SUD registry loading.
- [ ] Run focused tests and verify GREEN.
- [ ] Commit `feat: add deterministic Lacey taxonomy resolver`.

### Task 3: Taxonomy-aware benchmark comparison

**Files:**
- Create: `src/litoral_trace/lacey_benchmark/taxonomy_benchmark.py`
- Modify: `src/litoral_trace/lacey_benchmark/evaluator.py` only if a narrow comparison hook is required; otherwise wrap it without changes.
- Create: `tests/lacey_benchmark/test_taxonomy_benchmark.py`

**Interfaces:**
- `TaxonomyAwareEvaluator(resolver: TaxonomyResolver)`.
- `evaluate(expected: ShipmentTruth, actual: ShipmentTruth, *, case_id: str | None = None) -> Scorecard`.

- [ ] Write failing tests proving `Eucalyptus + grandis` equals `Eucalyptus grandis`, while a canonical taxon present on the wrong `line_id` still produces `WRONG_ENTITY_ASSOCIATION`.
- [ ] Add a test that unresolved or ambiguous names fall back to existing raw-field semantics rather than being auto-corrected.
- [ ] Run the focused test file and verify RED.
- [ ] Implement a narrow preprocessing/comparison policy that derives canonical taxon keys without mutating input models.
- [ ] Run focused tests and verify GREEN.
- [ ] Commit `feat: make golden benchmark taxonomy-aware`.

### Task 4: Versioned GRIN reference fixture and manifest contract

**Files:**
- Create: `benchmarks/lacey/reference/grin/taxa.csv`
- Create: `benchmarks/lacey/reference/grin/manifest.json`
- Create: `tests/lacey_benchmark/test_reference_fixture.py`

**Interfaces:**
- The fixture uses the same import schema as future full GRIN CSV exports: `source_id,scientific_name,genus,species,is_accepted,accepted_source_id`.

- [ ] Write a failing test loading the repository fixture and resolving `Pinus taeda` and `Eucalyptus grandis`.
- [ ] Create a deliberately small, source-attributed fixture containing only benchmark taxa plus at least one synonym record; compute and record the exact SHA-256.
- [ ] Run the fixture test and verify GREEN.
- [ ] Confirm no pack/vendor/file-name specific rules exist in resolver code.
- [ ] Commit `test: add versioned taxonomy reference fixture`.

### Task 5: CLI integration without runtime coupling

**Files:**
- Modify: `src/litoral_trace/lacey_benchmark/run_benchmark.py`
- Modify: `tests/lacey_benchmark/test_golden_harness.py`

**Interfaces:**
- Add optional flags `--taxonomy-csv`, `--taxonomy-manifest`, and `--special-use-json`.
- When all taxonomy flags are supplied, runner uses `TaxonomyAwareEvaluator`; otherwise current `SemanticEvaluator` behavior is unchanged.

- [ ] Write failing CLI-level tests for taxonomy-enabled evaluation and unchanged default behavior.
- [ ] Run focused CLI/harness tests and verify RED.
- [ ] Implement optional resolver construction with explicit all-or-none argument validation.
- [ ] Run focused tests and verify GREEN.
- [ ] Commit `feat: expose taxonomy-aware golden benchmark CLI`.

### Task 6: Full verification and architectural guardrails

**Files:**
- Create: `tests/lacey_benchmark/test_taxonomy_architecture.py`
- Update PR description if needed.

**Interfaces:**
- Architecture test statically asserts new benchmark modules do not import `litoral_trace.lacey_engine`, `litoral_trace.us_lacey`, SQLAlchemy, FastAPI, or DB packages.

- [ ] Write and run architecture guard test.
- [ ] Run `python -m compileall src/litoral_trace/lacey_benchmark tests/lacey_benchmark`.
- [ ] Run `python -m pytest -q tests/lacey_benchmark`.
- [ ] Push and wait for full repository CI on the exact PR HEAD.
- [ ] Read full pytest summary and confirm zero failures.
- [ ] Keep PR draft/unmerged; report exact SHA, CI run, and remaining next step (building curated Golden shipment JSONs).
