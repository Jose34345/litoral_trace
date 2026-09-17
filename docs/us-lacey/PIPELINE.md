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
  -> future taxonomy/regulatory decisioning
```

The worker invokes Product Intelligence only when it owns source-set finalization, after canonical publication for that source set and before multilingual shadow/source-set finalization. A Product Intelligence snapshot is evidence about product composition, not canonical Lacey declaration truth.

## Source-set and supersession behavior
Product Intelligence is generation-fenced. The snapshot records the source-set revision/fingerprint used to compute it. When a newer source-set generation supersedes it, the old snapshot becomes `STALE` and the current read path must not present it as current composition evidence.

## Deterministic path
Deterministic logic includes operation/source-set identity, worker/job lifecycle, explicit normalization/reconciliation invariants, Product Intelligence BOM header/unit normalization, snapshot generation fencing, canonical publication guards and export shaping. New regulatory calculations such as future de minimis logic should join this path as explicit versioned rules rather than model-only judgments.

## AI/specialist path
AI/provider/specialist processing lives primarily under `src/litoral_trace/lacey_engine/` with application-side shadow/projection adapters under `src/litoral_trace/us_lacey/`. The output is candidate/inference state. It is not automatically canonical. Product Intelligence does not add AI calls in its current BOM foundation.

## Shadow path
Shadow capabilities must remain observational until an explicit promotion decision changes their authority. `ai_shadow.py`, `specialized_shadow.py`, specialized projections, multilingual snapshots and inference-cache code are not a shortcut around canonical truth.

## Evidence path
Source-backed values should flow from document identity/text/span through evidence/semantic structures into candidates and downstream decisions. Product Intelligence preserves document/table/sheet/row anchors from the existing parser. Future taxonomy/regulatory layers should link those anchors into the existing evidence chain rather than invent a parallel evidence store.

## Human path
Unresolved identity, unsupported fields, ambiguous taxonomy, conflicting documents, missing required evidence or indeterminate regulatory calculations must surface to review. A reviewer decision is a separate authority event and should remain auditable.

## Publication path
Only current, permitted source-set generations should publish customer-facing truth. Stale results, Product Intelligence observations, shadow outputs and unsupported candidates must not overwrite the current canonical state.

Product Intelligence runs after canonical publication in the worker ordering, but this is an execution barrier only. It does not acquire canonical authority and does not feed PPQ505/LAWGS directly.

## Export path
`ppq505.py` and `us_lacey/exporters/` consume resolved/canonical application state. Export preparation is downstream of authority/review. Do not place raw AI extraction or raw Product Intelligence output directly into an export merely because the field name matches.

## Customer hydration path
The full operation page may initially render while processing is still queued/running. HTMX polling replaces the processing panel until terminal state, then loads the operation workspace. That terminal workspace hydration must also read/render the current Product Intelligence snapshot so customers see newly completed BOM composition evidence without manually reloading the page.

## Next insertion points

### Taxonomy Resolver
Consume evidence-backed plant/material names plus relevant context. Produce candidates/status/confidence/provenance, with ambiguity explicitly reviewable. It must not auto-promote an uncertain common/commercial name to a scientific species.

### Regulatory Rules
Consume structured product composition, taxonomy and shipment context. Produce deterministic PASS/FAIL/INDETERMINATE-style assessments with ruleset version and calculation trace.

### Exception Review
Consume unresolved/blocked/review-required outputs from all upstream layers and present source-linked exceptions instead of forcing reviewers to inspect every document.
