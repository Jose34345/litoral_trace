# Litoral Trace Agent Guide

## Current priority
The commercial priority is the U.S. Lacey workflow. Preserve the stable document/evidence pipeline while evolving the product toward Product Intelligence, BOM/composition, taxonomy resolution, deterministic regulatory rules, exception-first human review, and source-linked review packages.

## Read this before changing U.S. Lacey
1. `docs/us-lacey/README.md`
2. `docs/us-lacey/ARCHITECTURE.md`
3. `docs/us-lacey/CAPABILITIES.toml`
4. `docs/us-lacey/INVARIANTS.md`
5. `docs/us-lacey/PIPELINE.md`
6. `docs/us-lacey/TEST_MATRIX.md`
7. `docs/us-lacey/ROADMAP.md`
8. the nearest nested `AGENTS.md`

## Repository authority map
- `src/litoral_trace/lacey_engine/`: document understanding, routing, extraction candidates, source authority, specialist orchestration, line binding and fusion.
- `src/litoral_trace/us_lacey/`: U.S. Lacey application workflow, jobs/workers, source-set lifecycle, reconciliation, canonical publication, review and exports.
- `src/litoral_trace/db/models/`: persistent models.
- `src/litoral_trace/web/`: presentation/controllers.
- `alembic/versions/`: immutable schema history. Never edit historical migrations.
- `tests/`: executable behavior contracts.
- `.github/workflows/`: CI/release gates.
- `docs/us-lacey/`: canonical current architecture documentation for agents.

## Non-negotiable engineering rules
1. False-safe outcomes are unacceptable: uncertainty must remain visible.
2. No evidence means no supported regulatory claim.
3. AI suggestions are not automatically canonical regulatory truth.
4. Ambiguity must not become certainty without additional evidence or explicit human review.
5. Preserve source provenance through canonical publication and review.
6. Preserve organization isolation and PostgreSQL RLS semantics.
7. Preserve source-set generation/finalization semantics; stale generations must not overwrite current truth.
8. Preserve auditable human review.
9. Do not log full customer source-document contents.
10. Do not add new domain logic to large legacy hotspots merely because they are convenient.
11. Prefer focused modules with explicit ownership over more logic in `projection.py`, `_operations_core.py`, `worker.py`, `review.py`, or `canonical_shipment_truth.py`.
12. Do not introduce microservices, queues, databases, AI agents, or dependencies unless the current commercial goal requires them.

## Change discipline
Before coding, identify the capability in `CAPABILITIES.toml`, its owner, dependencies, authority level and required tests. If the capability is absent, add it to the architecture docs as part of the same change.

For U.S. Lacey changes, use a dedicated branch/worktree. Do not implement directly on the target production branch. Keep changes small enough to review and revert independently.

## Validation
CI currently uses Python 3.11 and runs syntax compilation, Alembic head validation and the pytest suite. Run the narrowest relevant tests first, then the broader U.S. Lacey/PostgreSQL gates for changes affecting persistence, RLS, workers, source sets, canonical truth or exports.

## Documentation authority
`docs/us-lacey/` describes the current architecture. Historical plans elsewhere in the repository may explain why something exists, but they do not override current code, migrations, tests, or the canonical documentation in `docs/us-lacey/`.