# U.S. Lacey Taxonomy Resolver Hito 7 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a deterministic, versioned, fail-closed taxonomy resolver and enrich U.S. Lacey Product Intelligence BOM materials with non-canonical taxonomic candidates.

**Architecture:** Keep reusable BOM contracts regulation-agnostic. Implement U.S.-specific taxonomy under `src/litoral_trace/us_lacey/regulatory/taxonomy/`, using a deliberately small curated catalog and exact deterministic matching. Integrate only when Product Intelligence serializes BOM materials, preserving the existing immutable source-set snapshot, provenance, worker ordering and canonical boundaries.

**Tech Stack:** Python 3.11, frozen dataclasses, `StrEnum`, `Decimal`, existing Product Intelligence JSON snapshots, pytest.

**Spec:** `docs/superpowers/specs/2026-09-17-us-lacey-taxonomy-resolver-h7.md`

## Global Constraints
- Taxonomy is advisory/non-canonical.
- No fuzzy matching in v1.
- Genus/common/commercial input never becomes an exact species fact automatically.
- No database migration in Hito 7.
- Preserve existing BOM source anchors and source-set fencing.
- Do not change canonical shipment truth, PPQ505, LAWGS, auth, billing, RLS, or worker completion ordering.
- Fresh full CI and U.S. Lacey PostgreSQL Gate must be green before ready-for-review.

---

### Task 1: Resolver contract and RED tests

**Files:**
- Create: `tests/test_us_lacey_taxonomy_resolver.py`
- Create: `src/litoral_trace/us_lacey/regulatory/__init__.py`
- Create: `src/litoral_trace/us_lacey/regulatory/taxonomy/__init__.py`
- Create: `src/litoral_trace/us_lacey/regulatory/taxonomy/domain.py`
- Create: `src/litoral_trace/us_lacey/regulatory/taxonomy/catalog.py`
- Create: `src/litoral_trace/us_lacey/regulatory/taxonomy/resolver.py`

**Interfaces:**
- Produces: `resolve_taxonomy(name: str) -> TaxonomyResolution`
- Produces: immutable `TaxonomyStatus`, `TaxonomicRank`, `TaxonomyMatchKind`, `TaxonomyCandidate`, `TaxonomyResolution`.

- [ ] **Step 1: Write failing tests** for accepted `Hevea brasiliensis`, `Siphonia brasiliensis`, `rubber tree`, `rubberwood`, genus-only `hevea wood`, `oak`, `pine`, unknown `plywood`, near miss `rubberwod`, and an explicitly ambiguous catalog alias.
- [ ] **Step 2: Run** `python -m pytest -q tests/test_us_lacey_taxonomy_resolver.py` and confirm RED because the package does not exist.
- [ ] **Step 3: Implement immutable domain contracts** with statuses `RESOLVED`, `REVIEW_REQUIRED`, `AMBIGUOUS`, `NO_MATCH`, ranks `SPECIES`/`GENUS`, explicit reason and review flag.
- [ ] **Step 4: Implement versioned catalog** `2026-09-17-v1` with authority metadata and separate common/commercial alias classifications.
- [ ] **Step 5: Implement exact normalizer/resolver** with no fuzzy matching and stable candidate ordering.
- [ ] **Step 6: Re-run targeted resolver tests** and require GREEN.

### Task 2: Product Intelligence enrichment

**Files:**
- Modify: `src/litoral_trace/us_lacey/product_intelligence_snapshot.py`
- Modify: `tests/test_us_lacey_product_intelligence_snapshot.py`

**Interfaces:**
- Consumes: `resolve_taxonomy(name_raw)`.
- Produces: existing material JSON plus a `taxonomy` object; existing keys/provenance unchanged.

- [ ] **Step 1: Add failing snapshot test** proving `Rubberwood` yields a species candidate with `REVIEW_REQUIRED`, `Hevea wood` yields genus `Hevea` only, and `Plywood` yields `NO_MATCH`; assert source document/sheet/row remain unchanged.
- [ ] **Step 2: Run targeted snapshot test** and confirm RED because `taxonomy` is absent.
- [ ] **Step 3: Add a pure serializer** for catalog version, normalized query, status, reason, review flag and candidate authority metadata.
- [ ] **Step 4: Enrich `_material()`** without changing Product Intelligence readiness or persistence semantics.
- [ ] **Step 5: Run Product Intelligence snapshot/worker tests** and require GREEN.

### Task 3: Architecture/control-plane update

**Files:**
- Modify: `docs/us-lacey/CAPABILITIES.toml`
- Modify: `docs/us-lacey/ROADMAP.md`
- Modify: `docs/us-lacey/TEST_MATRIX.md`
- Modify if required: `src/litoral_trace/us_lacey/AGENTS.md`

- [ ] **Step 1: Mark Taxonomy Resolver ACTIVE/non-canonical** with implementation/tests/invariants.
- [ ] **Step 2: Move roadmap next item** to deterministic regulatory rules only after implementation is green.
- [ ] **Step 3: Register exact test routing** for taxonomy/catalog/Product Intelligence serialization.
- [ ] **Step 4: Run** `python scripts/validate_us_lacey_architecture_docs.py` and `python -m pytest -q tests/test_us_lacey_architecture_docs.py`.

### Task 4: Full verification and PR gate

- [ ] **Step 1: Run targeted regression suite**: resolver, Product Intelligence snapshot/worker, and existing `test_us_lacey_taxonomy_sanitization.py`.
- [ ] **Step 2: Run full** `python -m pytest -q -rs`.
- [ ] **Step 3: Verify one Alembic head remains** `049_lacey_product_intelligence_snapshots`.
- [ ] **Step 4: Open/keep PR Draft** and require fresh CI + U.S. Lacey PostgreSQL Gate on exact PR head.
- [ ] **Step 5: Review diff for prohibited changes** to canonical truth/PPQ505/LAWGS/auth/billing/RLS/source-set/worker ordering.
- [ ] **Step 6: Mark Ready only when all gates and review are green. Do not merge automatically without explicit authorization for Hito 7.**
