# Litoral Trace Agent Guide

## Current priority
The commercial priority is the U.S. Lacey workflow. Preserve the stable document/evidence pipeline while evolving the product toward Product Intelligence, BOM/composition, taxonomy resolution, deterministic regulatory rules, exception-first human review, and source-linked review packages.

## Task-scoped context loading
- Always follow this file and the nearest nested `AGENTS.md` for files being changed. Start with the relevant code, existing tests and a scoped search; do not pre-read the entire documentation tree for every task.
- For changes to U.S. Lacey behavior or authority, look up the affected capability/owner in the relevant `[capabilities.*]` section of `docs/us-lacey/CAPABILITIES.toml`; consult the applicable `docs/us-lacey/INVARIANTS.md` rules and the relevant row of `docs/us-lacey/TEST_MATRIX.md`.
- Read `docs/us-lacey/ARCHITECTURE.md` when ownership or module boundaries are in question; `PIPELINE.md` for processing flow or source-set changes; `DATA_MODEL.md` for persistence/RLS changes; `ROADMAP.md` only for prioritization; `README.md` to locate other canonical documents.
- Read Assurance V2 contracts/runbooks only for an Assurance V2 change. Read the production-state audit and release documentation when diagnosing or authorizing a deployment. Do not activate experimental V2 functionality as part of unrelated tasks.
- For isolated presentation, tests-only or documentation work, inspect direct dependencies and applicable guardrails; do not automatically load all architecture documents.
- Read only relevant sections when a document is large. These routing rules never relax the non-negotiable engineering rules below.

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
For a change to domain behavior, data contracts or authority, identify its capability owner, dependencies, authority level and relevant tests before implementation. Update the capability map and canonical docs when introducing a genuinely new capability or changing an existing contract; do not rewrite architecture docs for an unrelated small fix.

For U.S. Lacey changes, use a dedicated branch/worktree. Do not implement directly on the target production branch. Keep changes small enough to review and revert independently.

## Validation
CI currently uses Python 3.11 and runs syntax compilation, Alembic head validation and the pytest suite. Run the narrowest relevant tests first; expand coverage according to affected contracts and risk. Before merge, require the broader U.S. Lacey/PostgreSQL gates for changes affecting persistence, RLS, workers, source sets, canonical truth or exports. Docs-only edits need structural review, not a gratuitous full runtime test run.

## Context and output efficiency
- Use repository search to locate owners and tests before opening large files; prefer bounded file ranges and concise error traces to full log dumps.
- Do not re-read unchanged material or repeat instructions already in these guides. Reuse established code and tests; introduce subagents only for genuinely independent investigations.
- In the final response, report concise findings, changed paths, verification performed and blockers (GO/NO-GO). Never claim unrun tests, releases or LIVE status. Keep user-facing reports in Spanish.

## Documentation authority
`docs/us-lacey/` describes the current architecture. Historical plans elsewhere in the repository may explain why something exists, but they do not override current code, migrations, tests, or the canonical documentation in `docs/us-lacey/`.