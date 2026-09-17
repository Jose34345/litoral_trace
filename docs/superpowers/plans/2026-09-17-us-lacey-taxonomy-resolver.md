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
- Full CI and U.S. Lacey PostgreSQL Gate must be green before merge.

---

### Task 1: Domain contract and fail-closed resolver

**Files:**
- `tests/test_us_lacey_taxonomy_resolver.py`
- `src/litoral_trace/us_lacey/regulatory/__init__.py`
- `src/litoral_trace/us_lacey/regulatory/taxonomy/__init__.py`
- `src/litoral_trace/us_lacey/regulatory/taxonomy/domain.py`
- `src/litoral_trace/us_lacey/regulatory/taxonomy/catalog.py`
- `src/litoral_trace/us_lacey/regulatory/taxonomy/resolver.py`

**Acceptance:** exact accepted scientific names resolve as advisory evidence; aliases require review; ambiguous aliases preserve all candidates; unknown and near-match strings fail closed.

### Task 2: Product Intelligence enrichment

**Files:**
- `src/litoral_trace/us_lacey/product_intelligence_snapshot.py`
- `tests/test_us_lacey_product_intelligence_taxonomy.py`

**Acceptance:** each serialized BOM material contains a taxonomy object while existing source provenance and BOM readiness semantics remain unchanged.

### Task 3: Architecture documentation

**Files:**
- `docs/us-lacey/ROADMAP.md`
- `docs/us-lacey/CAPABILITIES.toml`
- `docs/us-lacey/TEST_MATRIX.md`
- `src/litoral_trace/us_lacey/AGENTS.md`

**Acceptance:** Taxonomy Resolver is documented as ACTIVE and NON_CANONICAL; Deterministic Regulatory Rules becomes the next roadmap capability.

### Task 4: Full verification and PR gate

Run the repository's existing Python 3.11 general CI and U.S. Lacey PostgreSQL Gate against the exact PR head. Require zero failures, inspect any new skips/warnings, verify Alembic remains at `049_lacey_product_intelligence_snapshots`, check review threads, then merge only after all barriers are green.