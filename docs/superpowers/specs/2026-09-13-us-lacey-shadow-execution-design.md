# U.S. Lacey Shadow Execution Design

## Goal
Run the existing monolithic AI extractor and the new specialized multi-agent extractor side by side in production without changing the authoritative customer-facing result.

## Branch and authority boundary
This work is based on `feature/us-lacey-pilot-platform` after PR #221, commit `2b1763fe8dd82f21715c4845db01a7091403d6ea`.

`legacy` remains authoritative during shadow rollout. `specialized` output is persisted only for comparison and must not feed the current UI, `project_verified_ai_suggestions`, or human reconciliation while `LT_AI_ARCHITECTURE=shadow`.

## Runtime modes
`LT_AI_ARCHITECTURE` accepts exactly:
- `legacy`: execute/persist only the existing monolithic AI shadow extractor.
- `specialized`: execute/persist only the specialized multi-agent extractor, still non-authoritative for customer-facing projection in this rollout.
- `shadow`: execute legacy first, then execute specialized best-effort for the same current operation source set; persist both, but legacy remains the only output eligible for existing projection.

Unset or invalid values resolve to `legacy`; invalid values emit a warning.

## Specialized execution
The specialized runner loads the current operation documents, materializes each source document, derives deterministic page text from existing Engine 2 `DocumentResolution` layout blocks, routes pages with `route_document`, builds one `RoutingPlan`, and executes Customs, Logistics, Commercial, and Botanical through the Phase 6 `orchestrate_specialists` boundary.

Before line binding/fusion, specialized candidates must be evidence-verified against the corresponding Engine 2 `DocumentResolution`; this closes the known Phase 2/6 shadow-verification gap. The specialized path remains best-effort: provider/network failures are persisted or logged and cannot fail the owned legacy worker job in `shadow` mode.

## Persistence
Reuse `UsLaceyEngineDocumentRun` to avoid a schema migration. Architecture is distinguished by schema/version metadata:
- legacy: existing `AI_SHADOW_SCHEMA_VERSION` and `resolution_json.architecture = "legacy"`.
- specialized: new `SPECIALIZED_SHADOW_SCHEMA_VERSION = "lacey_multi_agent_shadow_v1"` and `resolution_json.architecture = "specialized"`.

Specialized operation-level results are persisted as one immutable run per current source document containing only candidates sourced from that document plus operation-level execution metadata. Different schema versions allow legacy and specialized runs to coexist for the same source SHA.

The current UI bridge continues filtering only `AI_SHADOW_SCHEMA_VERSION`; therefore specialized runs are invisible to authoritative projection by construction.

## Telemetry
Both architectures persist/log:
- `latency_ms`
- `input_tokens`
- `output_tokens`
- `total_tokens`
- provider/model identity
- candidate count

Provider adapters parse usage counters when the upstream response exposes them. Missing usage remains `null`/zero rather than estimated. Specialized totals are sums across specialist calls.

## Failure semantics
In `shadow` mode:
- legacy failure keeps existing legacy failure semantics;
- specialized failure after a successful legacy run is isolated and cannot alter queue/document/operation authoritative state;
- worker completion and UI projection depend only on legacy.

## Tests
TDD coverage must prove:
1. architecture parsing: default/invalid -> legacy; explicit legacy/specialized/shadow accepted;
2. shadow dispatcher invokes legacy then specialized;
3. specialized failure does not fail a successful legacy worker path;
4. legacy and specialized persistence can coexist and carry architecture metadata;
5. UI projection query continues selecting only legacy schema version;
6. token and latency telemetry serialize for both paths;
7. specialized evidence is verified before fusion/persistence.
