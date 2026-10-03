# U.S. Lacey Runtime Pipeline

This is the canonical navigation view of the current processing flow. Verify exact call chains in code before modifying runtime behavior.

## Main path

```text
customer upload
  -> ingestion/storage
  -> operation
  -> source-set revision
  -> job creation / lock / worker claim
  -> document engine
      -> admission/classification
      -> layout/segmentation
      -> source authority
      -> specialist routing
      -> line binding
      -> candidate fusion/resolution
  -> application reconciliation
  -> canonical publication support
  -> canonical shipment truth
  -> Product Intelligence snapshot (non-canonical)
  -> Regulatory Assessment snapshot (non-canonical, rule-scoped)
  -> multilingual shadow snapshot
  -> source-set finalize
  -> job COMPLETED
  -> operation refresh
  -> projection
  -> human review where required
  -> export/review output
```

## Product Intelligence path
Explicit BOM files have a deterministic, persisted, non-canonical composition path:

```text
CSV/XLS/XLSX bytes
  -> existing Assurance parse_csv / parse_xlsx
  -> ParsedTable
  -> Product Intelligence explicit header binding
  -> SKU -> Component -> Material composition
  -> Decimal quantity/mass normalization
  -> source anchors + row issues
  -> source-set-scoped Product Intelligence snapshot
      READY / PARTIAL / FAILED / NOT_APPLICABLE / STALE
  -> current-only customer read
  -> taxonomy enrichment / regulatory assessment inputs
```

The worker invokes Product Intelligence only when it owns source-set finalization, after canonical publication for that source set and before the regulatory assessment, multilingual shadow and source-set finalization. A Product Intelligence snapshot is evidence about product composition, not canonical Lacey declaration truth.

## Regulatory Assessment path
The active deterministic rules path consumes supported Product Intelligence facts and produces a separate source-set/ruleset-versioned work product:

```text
current Product Intelligence snapshot
  -> read-side taxonomy enrichment where available
  -> exact rule inputs
  -> regulatory/rules deterministic evaluators
      -> DE_MINIMIS
      -> SPECIAL_COMPOSITE
  -> PASS / FAIL / INDETERMINATE per rule only
  -> reason codes + calculation trace + evidence refs
  -> input fingerprint + ruleset version
  -> tenant/source-set-scoped Regulatory Assessment snapshot
      CURRENT / STALE
  -> current-ruleset/current-source-set customer read
```

Missing required facts remain `INDETERMINATE`; Product Intelligence/taxonomy observations are not silently promoted into unsupported HTS, weight, protected-status or due-care facts. A rule result is not an overall shipment compliance verdict and does not mutate canonical shipment truth, PPQ505, LAWGS or ACE data.

## Source-set and supersession behavior
Product Intelligence and Regulatory Assessment are generation-fenced. Each snapshot records the source-set revision/fingerprint used to compute it. When a newer source-set generation supersedes the prior one, both derived snapshot families become `STALE`; current read paths must not present the superseded generation as current evidence or rule output.

## Deterministic path
Deterministic logic includes operation/source-set identity, worker/job lifecycle, explicit normalization/reconciliation invariants, Product Intelligence BOM header/unit normalization, regulatory rule evaluation, input/ruleset fingerprinting, snapshot generation fencing, canonical publication guards and export shaping. Regulatory calculations belong in explicit versioned rules rather than model-only judgments.

## AI/specialist path
AI/provider/specialist processing lives primarily under `src/litoral_trace/lacey_engine/` with application-side shadow/projection adapters under `src/litoral_trace/us_lacey/`. The output is candidate/inference state. It is not automatically canonical. Product Intelligence and Regulatory Rules do not add AI calls to their deterministic paths.

## Shadow path
Shadow capabilities must remain observational until an explicit promotion decision changes their authority. `ai_shadow.py`, `specialized_shadow.py`, specialized projections, multilingual snapshots and inference-cache code are not a shortcut around canonical truth.

## Evidence path
Source-backed values should flow from document identity/text/span through evidence/semantic structures into candidates and downstream decisions. Product Intelligence preserves document/table/sheet/row anchors from the existing parser. Regulatory assessments carry evidence references forward rather than creating a parallel provenance store.

## Human path
Unresolved identity, unsupported fields, ambiguous taxonomy, conflicting documents, missing required evidence or indeterminate regulatory calculations must surface to review. A reviewer decision is a separate authority event and should remain auditable.

## Publication path
Only current, permitted source-set generations should publish customer-facing canonical truth. Stale results, Product Intelligence observations, Regulatory Assessment work products, shadow outputs and unsupported candidates must not overwrite the current canonical state.

Product Intelligence and Regulatory Assessment run after canonical publication in the worker ordering, but this is an execution barrier only. Neither gains canonical authority or feeds PPQ505/LAWGS directly.

## Export path
`ppq505.py` and `us_lacey/exporters/` consume resolved/canonical application state. Export preparation is downstream of authority/review. Do not place raw AI extraction, raw Product Intelligence or Regulatory Assessment output directly into an export merely because the field/rule name matches.

## Customer hydration path
The full operation page may initially render while processing is still queued/running. HTMX polling replaces the processing panel until terminal state, then loads the operation workspace. That terminal workspace hydration reads/renders the current Product Intelligence and Regulatory Assessment snapshots so customers see newly completed composition evidence and rule-scoped assessments without manually reloading the page.

## Next insertion point

### Exception Review
Consume unresolved/blocked/review-required outputs from all upstream layers and present source-linked exceptions instead of forcing reviewers to inspect every document.
