# U.S. Lacey Field Judge V1 Design

## Objective

Reduce false-safe suggestions and false conflicts in the U.S. Lacey preparation workflow without allowing an LLM to invent, rewrite, or silently promote business values. The Field Judge is a bounded semantic gate between exact evidence/line binding and deterministic fusion.

## Audited baseline

The operational integration branch is `feature/us-lacey-pilot-platform` at `944f48c9dbe11b8f622a5826e54571cc55a80669`.

The specialized path already provides:

- document routing to four specialists;
- exact Engine 2 evidence verification;
- deterministic line binding;
- deterministic fusion;
- shadow persistence separated from legacy UI authority;
- latency and token telemetry.

Two defects materially distort current benchmark results:

1. Upload-first intake attaches every uploaded document as `document_role="UNKNOWN"`; `attach_document()` versions links by role, so later UNKNOWN uploads supersede earlier UNKNOWN uploads. A seven-document shipment can therefore collapse to one current document.
2. Upload-first intake always creates one plant line (`line_references=("1",)`), while specialized extraction can already derive distinct `line_item_key` values. Multi-line shipments therefore cannot yet be safely projected into distinct PPQ plant-line fields.

## Safety invariants

1. The Judge cannot create a business value.
2. The Judge can only reference the stable `candidate_identity()` of an existing candidate.
3. Exact evidence verification remains mandatory before judging.
4. The Judge runs after deterministic line binding and before deterministic fusion.
5. Invalid provider output, hallucinated candidate IDs, mismatched field keys, mismatched line keys, unknown reason codes, provider failures, or malformed JSON degrade to `NEEDS_REVIEW`; they never become `ACCEPT`.
6. `shadow` mode records Judge decisions but leaves current specialized fusion unchanged.
7. `enforce` mode removes `REJECT` candidates from fusion and withholds `NEEDS_REVIEW` candidates from safe fusion; only `ACCEPT` candidates may enter the safe fused set.
8. Default mode is `off`.
9. Human-reviewed values are never overwritten.
10. Specialized output remains non-authoritative until an explicit safe-projection gate is enabled.

## Field Judge contract

The closed decision vocabulary is:

- `ACCEPT`
- `REJECT`
- `NEEDS_REVIEW`

Closed reason codes:

- `EXACT_FIELD_CONTEXT`
- `SEMANTIC_FIELD_MATCH`
- `AUTHORITATIVE_SOURCE`
- `FIELD_MISMATCH`
- `SCOPE_MISMATCH`
- `AMBIGUOUS_CONTEXT`
- `INSUFFICIENT_CONTEXT`

Each request supplies only existing candidate metadata:

- `candidate_id`
- `field_key`
- `line_item_key`
- `normalized_value`
- `source_text`
- `document_type`
- `specialist`
- `page`
- `evidence_verified`
- deterministic document authority score

Each response must echo the exact candidate ID, field key, and line key. No output field exists for a new value.

## Judge modes

`LT_AI_FIELD_JUDGE_MODE` accepts:

- `off` — do not call the Judge; current specialized behavior remains unchanged;
- `shadow` — evaluate and persist decisions, but feed the pre-Judge candidate set to fusion;
- `enforce` — only `ACCEPT` candidates reach safe fusion; `REJECT` and `NEEDS_REVIEW` remain auditable in Judge telemetry.

Invalid values fail closed to `off` and emit a warning.

## Placement

```text
specialists
  -> exact Engine 2 evidence verification
  -> deterministic line binding
  -> Lacey Field Judge V1
  -> deterministic fusion
  -> isolated specialized persistence
  -> optional safe specialized projection (separate gate)
```

The existing conflict resolver remains downstream and separate; Field Judge V1 does not replace it.

## P0 multidocument fix

`UsLaceyOperationService.attach_document()` must preserve independent current links when the intake role is `UNKNOWN` (and conservatively `OTHER`). Exact duplicate assurance documents remain idempotent. Explicit semantic roles keep replacement/version semantics.

Required behavior:

- upload seven different UNKNOWN documents -> seven current links;
- upload the exact same UNKNOWN assurance document again -> return the existing current link, no duplicate;
- upload two different `BILL_OF_LADING` documents -> prior current BOL becomes historical and the newer BOL becomes current.

## Multi-line materialization boundary

Field Judge V1 does not ask the model to create lines. A deterministic materializer consumes existing specialized `line_item_key` values after safe fusion. It may create missing PPQ plant-line slots only from distinct, non-empty line keys that are supported by line-scoped candidates.

The mapping must be deterministic and idempotent. Existing human-created line references must not be deleted or reordered. New generated references use a stable derived reference, never a random value.

## Safe specialized projection

A separate gate `LT_AI_SPECIALIZED_PROJECTION_MODE` accepts `off|shadow|enforce`, default `off`.

- `off`: no specialized projection.
- `shadow`: calculate projection decisions and persist/log counts only.
- `enforce`: project only candidates that satisfy all of the following:
  - Judge decision is `ACCEPT` when Judge mode is `enforce`;
  - `evidence_verified` is true;
  - candidate evidence class is not inferred;
  - PPQ validation returns `VALID`;
  - field scope and line scope resolve unambiguously;
  - target field has not been human reviewed;
  - no conflicting accepted candidate exists for the same target.

Projection never labels values as human-confirmed. It creates `FOUND` suggestions that still require the existing customer confirmation flow.

## Persistence and telemetry

Reuse `UsLaceyEngineDocumentRun.resolution_json`; no database migration is required for Judge V1.

Specialized payload gains a `field_judge` object containing:

- version (`lacey_field_judge_v1`);
- mode;
- provider/model;
- latency_ms;
- input/output/total tokens when reported;
- accepted/rejected/needs_review counts;
- closed decision records.

The specialized engine identity incorporates Judge version and effective mode so stale pre-Judge results cannot be reused as Judge-evaluated results.

## Benchmark gates

The first promotion gate is correctness, not coverage.

1. False safe rate: 0 on deterministic/synthetic regression corpus.
2. Semantic field mismatch: 0 for known traps such as Vessel/POD/ETA/Gross Weight being offered as BOL.
3. True conflicts remain visible; the Judge must not erase intentionally contradictory authoritative evidence.
4. Distinct plant rows remain distinct after line binding/materialization.
5. Safe coverage may increase only after gates 1-4 pass.

## Test strategy

TDD cycles must cover:

- UNKNOWN multidocument preservation and duplicate idempotence;
- explicit-role replacement semantics unchanged;
- Judge mode parser and fail-safe default;
- closed decision validation;
- hallucinated candidate IDs -> NEEDS_REVIEW;
- mismatched field/line -> NEEDS_REVIEW;
- provider failure isolation;
- shadow mode does not alter fusion inputs;
- enforce mode passes ACCEPT only;
- operation telemetry serialization;
- line materialization idempotence;
- specialized projection refuses ambiguous/unverified/invalid/reviewed targets;
- worker operation remains successful when Judge fails in shadow mode.

## Rollout

1. Merge with Judge default `off` and specialized projection default `off`.
2. Enable `LT_AI_FIELD_JUDGE_MODE=shadow` for controlled benchmark runs.
3. Compare Judge decisions against Pack 1/2/3 ground truth.
4. Enable `enforce` only after false-safe and semantic-mismatch gates pass.
5. Enable specialized projection first in `shadow`, then `enforce` only after projection-specific tests and benchmark evidence are green.
