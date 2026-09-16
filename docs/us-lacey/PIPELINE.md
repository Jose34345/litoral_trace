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
  -> evidence/snapshot persistence
  -> canonical publication support
  -> canonical shipment truth
  -> projection
  -> human review where required
  -> export/review output
```

## Deterministic path
Deterministic logic includes operation/source-set identity, worker/job lifecycle, explicit normalization/reconciliation invariants, canonical publication guards and export shaping. New regulatory calculations such as future de minimis logic should join this path as explicit versioned rules rather than model-only judgments.

## AI/specialist path
AI/provider/specialist processing lives primarily under `src/litoral_trace/lacey_engine/` with application-side shadow/projection adapters under `src/litoral_trace/us_lacey/`. The output is candidate/inference state. It is not automatically canonical.

## Shadow path
Shadow capabilities must remain observational until an explicit promotion decision changes their authority. `ai_shadow.py`, `specialized_shadow.py`, specialized projections and inference cache code are not a shortcut around canonical truth.

## Evidence path
Source-backed values should flow from document identity/text/span through evidence/semantic structures into candidates and downstream decisions. Future BOM/taxonomy/regulatory layers should attach to this chain so reviewers can navigate from a decision back to the original PDF page or spreadsheet location as the evidence model permits.

## Human path
Unresolved identity, unsupported fields, ambiguous taxonomy, conflicting documents, missing required evidence or indeterminate regulatory calculations must surface to review. A reviewer decision is a separate authority event and should remain auditable.

## Publication path
Only current, permitted source-set generations should publish customer-facing truth. Stale results, shadow outputs and unsupported candidates must not overwrite the current canonical state.

## Export path
`ppq505.py` and `us_lacey/exporters/` consume resolved/canonical application state. Export preparation is downstream of authority/review. Do not place raw AI extraction directly into an export merely because the field name matches.

## Future insertion points

### Product Composition / BOM
Insert after evidence-backed extraction/reconciliation and before regulatory decisioning. The product model should preserve component/material/weight/unit/source evidence rather than replace source evidence.

### Taxonomy Resolver
Consume evidence-backed plant/material names plus relevant context. Produce candidates/status/confidence/provenance, with ambiguity explicitly reviewable.

### Regulatory Rules
Consume structured product composition, taxonomy and shipment context. Produce deterministic PASS/FAIL/INDETERMINATE-style assessments with ruleset version and calculation trace.

### Exception Review
Consume unresolved/blocked/review-required outputs from all upstream layers and present source-linked exceptions instead of forcing reviewers to inspect every document.