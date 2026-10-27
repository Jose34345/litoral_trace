# Litoral Trace Agent Guide

## Current priority
The commercial priority is the U.S. Lacey workflow. Preserve the stable document/evidence pipeline while evolving the product toward Product Intelligence, BOM/composition, taxonomy resolution, deterministic regulatory rules, exception-first human review, and source-linked review packages.

## Selective context loading
Read this guide and the nearest nested `AGENTS.md`. Search relevant code/tests first; never load all canonical docs by default.
- Behavior/authority change: inspect the matching `[capabilities.*]` entry in `docs/us-lacey/CAPABILITIES.toml`, relevant `INVARIANTS.md` clauses and `TEST_MATRIX.md` row.
- Ownership or architecture: consult relevant `ARCHITECTURE.md` sections. Processing/source-set flow: `PIPELINE.md`. Schema/RLS: `DATA_MODEL.md`. Priority: `ROADMAP.md`. Other docs: `README.md`.
- Assurance V2: read its contracts/runbook only when changing V2; do not activate experimental features. Production-state/release docs: only for deployment work.
- Localized UI, tests or docs: inspect the affected files and direct contracts, not the entire architecture. All safety rules below still apply.

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
For domain/authority changes, identify the owning capability, dependencies and required tests. Update the capability map/docs only when changing contracts or adding capabilities, not for unrelated small fixes.
Use a dedicated branch/worktree for U.S. Lacey work. Never implement directly on the target production branch; keep diffs reviewable and reversible.

## Validation
CI uses Python 3.11, syntax compilation, Alembic head and pytest. Run targeted tests first; broaden by impact. Persistence, RLS, workers, source sets, canonical truth and exports require their PostgreSQL/release gates before merge. Documentation-only changes need structural review, not full runtime tests.

## Efficient tools and reporting
Search by symbol/path before reading large files; bound logs and traces. Avoid rereading unchanged content and duplicating these instructions in prompts. Use subagents only for independent work. Report diagnosis, changed paths, actual tests and blockers (GO/NO-GO) concisely in Spanish. Never claim unrun tests or unverified LIVE deployments.

## Documentation authority
`docs/us-lacey/` describes the current architecture. Historical plans elsewhere in the repository may explain why something exists, but they do not override current code, migrations, tests, or the canonical documentation in `docs/us-lacey/`.