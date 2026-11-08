# U.S. Lacey AI Development Control Plane — Design

**Date:** 2026-09-16

## Problem
The U.S. Lacey codebase has matured into a substantial modular subsystem. Its runtime is spread across `lacey_engine`, `us_lacey`, persistence models, web entrypoints, migrations, tests and workflows. That architecture is functional, but a new human or AI agent must rediscover ownership, authority and invariants before making a safe change.

The immediate risk is not insufficient functionality; it is cognitive entropy: adding new logic to convenient hotspots, duplicating evidence/review systems, misunderstanding shadow vs canonical state, or following historical docs as if they were current architecture.

## Goal
Create a low-risk, machine-readable navigation/control layer that lets agents determine where to work, what to preserve, what tests to run and what the current commercial roadmap is, without changing production behavior.

## Design
1. Add root and package-local `AGENTS.md` files defining bounded-context ownership and guardrails.
2. Add `docs/us-lacey/` as the canonical current architecture directory.
3. Use `CAPABILITIES.toml` as a dependency-free machine-readable map. TOML is preferred over YAML here because Python 3.11 provides `tomllib`, avoiding a new runtime/dev dependency solely for documentation validation.
4. Add a small validator and pytest contract that verify referenced paths in the capability map exist.
5. Record cleanup candidates without deleting or moving runtime files in this change.
6. Preserve all existing U.S. Lacey behavior, schemas, RLS, workers, canonical publication, review and exports.

## Non-goals
- no package reorganization;
- no runtime refactor;
- no schema/migration changes;
- no RLS/auth/billing changes;
- no new regulatory logic;
- no removal of historical code/tests/workflows;
- no Product Intelligence/BOM/Taxonomy implementation yet.

## Success criteria
- A new agent can find the responsible module and authority level for major U.S. Lacey capabilities from a small documented entrypoint set.
- Planned BOM/taxonomy/regulatory work has explicit extension seams rather than encouraging edits to hotspots.
- The capability map is machine-readable and validated in pytest.
- Existing product behavior is unchanged.
