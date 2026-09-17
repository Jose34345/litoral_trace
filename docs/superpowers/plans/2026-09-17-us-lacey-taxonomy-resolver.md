# U.S. Lacey Taxonomy Resolver Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a deterministic, versioned, fail-closed taxonomy resolver and enrich U.S. Lacey Product Intelligence BOM materials with non-canonical taxonomic candidates.

**Architecture:** Keep reusable BOM contracts regulation-agnostic. Implement U.S.-specific taxonomy under `src/litoral_trace/us_lacey/regulatory/taxonomy/`, with a small curated v1 catalog and deterministic exact matching. Integrate only at Product Intelligence snapshot serialization so existing source-set fencing, persistence, canonical truth, PPQ505 and LAWGS remain unchanged.

**Tech Stack:** Python 3.11, dataclasses, `StrEnum`, `Decimal`, existing Product Intelligence snapshot JSON payload, pytest.

**Spec:** `docs/superpowers/specs/2026-09-17-us-lacey-taxonomy-resolver.md`

## Global Constraints
- Taxonomy results are advisory/non-canonical.
- No fuzzy matching in v1.
- Genus-only/common/commercial aliases must not be promoted to species facts.
- No database migration in Hito 7.
- Do not change canonical truth, PPQ505, LAWGS, auth, billing, source-set fencing or worker completion ordering.
- Full CI and U.S. Lacey PostgreSQL Gate must be green before ready-for-review.

---

### Task 1: Domain contract and fail-closed resolver tests

**Files:**
- Create: `tests/test_us_lacey_taxonomy_resolver.py`
- Create: `src/litoral_trace/us_lacey/regulatory/__init__.py`
- Create: `src/litoral_trace/us_lacey/regulatory/taxonomy/__init__.py`
- Create: `src/litoral_trace/us_lacey/regulatory/taxonomy/domain.py`
- Create: `src/litoral_trace/us_lacey/regulatory/taxonomy/catalog.py`
- Create: `src/litoral_trace/us_lacey/regulatory/taxonomy/resolver.py`

**Interfaces:**
- Produces: `resolve_taxonomy(name: str) -> TaxonomyResolution`
- Produces: `TaxonomyStatus`, `TaxonomicRank`, `TaxonomyMatchKind`, `TaxonomyCandidate`, `TaxonomyResolution`

- [ ] **Step 1: Write failing tests**

Cover these exact behaviors:
```python
assert resolve_taxonomy("Hevea brasiliensis").status is TaxonomyStatus.RESOLVED
assert resolve_taxonomy("rubberwood").status is TaxonomyStatus.REVIEW_REQUIRED
assert resolve_taxonomy("hevea wood").candidates[0].scientific_name == "Hevea"
assert resolve_taxonomy("hevea wood").candidates[0].rank is TaxonomicRank.GENUS
assert resolve_taxonomy("plywood").status is TaxonomyStatus.NO_MATCH
assert resolve_taxonomy("rubberwod").status is TaxonomyStatus.NO_MATCH
```
Also assert that no `hevea wood` result contains species epithet `brasiliensis`.

- [ ] **Step 2: Run tests and verify RED**

Run:
```bash
python -m pytest -q tests/test_us_lacey_taxonomy_resolver.py
```
Expected: collection/import failure because the taxonomy package does not yet exist.

- [ ] **Step 3: Implement minimal immutable domain contracts**

Use frozen slot dataclasses and `StrEnum`. Candidate confidence is a `Decimal`; serializer later emits a string.

- [ ] **Step 4: Implement the v1 curated catalog**

Catalog version: `2026-09-17-v1`.

Records must include:
- accepted species `Hevea brasiliensis` with POWO URL `https://powo.science.kew.org/taxon/349913-1`;
- synonym `Siphonia brasiliensis` -> `Hevea brasiliensis`;
- commercial aliases `rubberwood`, `rubber wood` -> `Hevea brasiliensis`, review required;
- `hevea wood` -> genus `Hevea`, review required;
- `oak` -> genus `Quercus`, review required;
- `pine` -> genus `Pinus`, review required.

- [ ] **Step 5: Implement deterministic normalization and exact lookup**

Normalization may casefold, Unicode-normalize, trim whitespace, collapse repeated whitespace and normalize simple separators. It must not perform edit-distance/fuzzy matching.

- [ ] **Step 6: Run resolver tests and verify GREEN**

Run:
```bash
python -m pytest -q tests/test_us_lacey_taxonomy_resolver.py
```
Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add tests/test_us_lacey_taxonomy_resolver.py src/litoral_trace/us_lacey/regulatory
git commit -m "feat(lacey): add fail-closed taxonomy resolver"
```

### Task 2: Product Intelligence enrichment

**Files:**
- Modify: `src/litoral_trace/us_lacey/product_intelligence_snapshot.py`
- Modify: `tests/test_us_lacey_product_intelligence_snapshot.py`

**Interfaces:**
- Consumes: `resolve_taxonomy(name: str) -> TaxonomyResolution`
- Produces: material JSON containing existing fields plus `taxonomy`.

- [ ] **Step 1: Write failing Product Intelligence test**

Create/extend a BOM test with materials `Rubberwood`, `Hevea wood`, and `Plywood` and assert:
```python
rubberwood["taxonomy"]["status"] == "REVIEW_REQUIRED"
rubberwood["taxonomy"]["candidates"][0]["scientific_name"] == "Hevea brasiliensis"
hevea_wood["taxonomy"]["candidates"][0]["scientific_name"] == "Hevea"
hevea_wood["taxonomy"]["candidates"][0]["rank"] == "GENUS"
plywood["taxonomy"]["status"] == "NO_MATCH"
```
Also assert source row/sheet/document provenance is unchanged.

- [ ] **Step 2: Run targeted test and verify RED**

Run:
```bash
python -m pytest -q tests/test_us_lacey_product_intelligence_snapshot.py -k taxonomy
```
Expected: fail because material payload has no `taxonomy` key.

- [ ] **Step 3: Add a pure serializer for taxonomy results**

Serialize only deterministic fields from the domain result:
`catalog_version`, `query_normalized`, `status`, `reason`, `review_required`, `candidates`.

- [ ] **Step 4: Enrich `_material()`**

Call the resolver with `value.name_raw` and add the serialized result under `taxonomy`. Preserve all existing keys unchanged.

- [ ] **Step 5: Run targeted Product Intelligence tests**

Run:
```bash
python -m pytest -q tests/test_us_lacey_product_intelligence_snapshot.py tests/test_us_lacey_product_intelligence_worker.py
```
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add src/litoral_trace/us_lacey/product_intelligence_snapshot.py tests/test_us_lacey_product_intelligence_snapshot.py
git commit -m "feat(lacey): enrich BOM materials with taxonomy candidates"
```

### Task 3: Architecture documentation and regression protection

**Files:**
- Modify: `docs/us-lacey/ROADMAP.md`
- Modify: `docs/us-lacey/CAPABILITIES.toml`
- Modify: `docs/us-lacey/TEST_MATRIX.md`
- Modify: `src/litoral_trace/us_lacey/AGENTS.md`

**Interfaces:**
- Documents taxonomy as ACTIVE but NON_CANONICAL.

- [ ] **Step 1: Update capability map**

Record package ownership, status, non-canonical authority, tests, no-fuzzy invariant and Product Intelligence integration point.

- [ ] **Step 2: Update roadmap**

Move Taxonomy Resolver from NEXT to DELIVERED only after implementation is green; leave Deterministic Regulatory Rules as next.

- [ ] **Step 3: Update test matrix/agent guide**

Add exact tests to run when taxonomy/catalog/Product Intelligence serialization changes.

- [ ] **Step 4: Run architecture-doc validator**

Run:
```bash
python scripts/validate_us_lacey_architecture_docs.py
python -m pytest -q tests/test_us_lacey_architecture_docs.py
```
Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add docs/us-lacey src/litoral_trace/us_lacey/AGENTS.md
git commit -m "docs(lacey): register taxonomy resolver architecture"
```

### Task 4: Full verification and PR gate

**Files:** none unless failures reveal a contract regression.

- [ ] **Step 1: Run targeted suite**

```bash
python -m pytest -q tests/test_us_lacey_taxonomy_resolver.py tests/test_us_lacey_product_intelligence_snapshot.py tests/test_us_lacey_product_intelligence_worker.py tests/test_us_lacey_taxonomy_sanitization.py
```

- [ ] **Step 2: Run full suite**

```bash
python -m pytest -q -rs
```

- [ ] **Step 3: Verify Alembic remains at 049**

```bash
alembic heads
```
Expected one head: `049_lacey_product_intelligence_snapshots`.

- [ ] **Step 4: Require fresh CI and U.S. Lacey PostgreSQL Gate**

No reuse of #241 results. Both gates must execute against the Taxonomy Resolver PR head and finish green.

- [ ] **Step 5: Review the PR**

Confirm no change to canonical truth, PPQ505, LAWGS, auth, billing, source-set lifecycle, tenant isolation, or worker completion ordering.

- [ ] **Step 6: Mark ready only after all barriers pass**

Leave the PR draft while any test/gate/review item is unresolved.