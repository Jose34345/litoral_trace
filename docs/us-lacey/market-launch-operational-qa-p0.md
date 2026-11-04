# Production first-use QA — synthetic only, 2026-10-16

## Scope and consent
Two **anonymous self-service sandbox test sessions** created by product QA through the public consent form using explicitly synthetic PDFs from `tests/fixtures/assurance_v2_pdf_e2e/pack2`. No real prospect, email outreach, credit card, supplier private file or protected customer document was involved. These are QA records; **exclude from prospect funnel interpretation** when analyzing anonymous traffic.

## Results (one-off observations, not percentiles)

| Step | Observed | Classification |
|---|---|---|
| GET sandbox/start and consent | HTTP 200, anonymous navigation to /operations/new without signup | PASS |
| First invoice-only upload | 1 synthetic PDF selected, POST created operation, no browser JS errors | PASS for intake |
| Pack 2 upload | 6 synthetic PDF documents accepted in one operation | PASS for intake |
| Initial overview | Displayed `17 items need review` while the operation still had no source-inspection controls | **FAIL UX**: premature/incomplete review interpretation |
| ~113-second repeated observations | Still displayed `17 pending` and 0 `data-source-inspect` in the sampled review before source processing finalized | **INCONCLUSIVE** for terminal presentation; **evidence of high first-use wait** |
| Worker logs | Jobs 137–142 sequentially marked COMPLETED, final operation status `REVIEW_REQUIRED`; deferred operation-level work until source set ready, no matching runtime exception in inspected subset | PASS for worker eventually processing, NOT a full evidence correctness check |
| PDF hover from this QA script | Not exercised (no inspector triggers in observed intermediate page) | NOT RUN |
| Exception accuracy / Pack 2 final filing gate | Not reviewed in authenticated completed session | NOT RUN |
| Tenant isolation / expiry on live anonymous session | Not probed across two tenants; no attempt to access another session's files | NOT RUN (security must be checked via dedicated tests and authorization) |

**Product decision:** During nonterminal analysis show document receipt/processing, not a provisional pending-action count. Disable the review CTA until a terminal valid source set exists. On error show the processing issue rather than offering review. Do not invent progress percentages or processing times.

## Fix implemented

`src/litoral_trace/templates/us_lacey/operation_detail.html`: conditional header, badge, dynamic counts and action according to `processing.terminal` and `processing.failed`; hide prior evidence-reuse and detailed readiness during processing. No backend processor, extractor, job timing, tenant isolation, export or authority state changed.

## Outstanding reliability work

- Instrument source upload → each child job → finalized source set → first actionable review, aggregated p50/p95 and per-document type, without raw document content.
- Investigate whether sequential jobs can be optimized within existing queue/resource budgets, without racing source revisions or weakening audit.
- Repeat first-use on an authenticated/anonymous held session to observe terminal Pack 2 correctness and PDF inspection. Do not classify the old first-load page as proven completed.
- Verify raw PDF purge after the defined 4-hour retention window using authorized SRE telemetry and sampled QA object; manual exercise did not wait four hours.
- No general paid pilot outreach until these P0 gates and U.S. customs-business legal review are satisfied.

## Precautions
Logs shown only for synthetic jobs and process statuses; no customer document text recorded. Source evidence authority and fail-closed regulatory gates remain unchanged. A completed child job is not evidence that all extracted fields are correct or that the overall package is filing-ready.
