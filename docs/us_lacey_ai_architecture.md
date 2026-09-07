# U.S. Lacey zero-entry AI preparation architecture

## Product boundary

Litoral Trace prepares and reconciles Lacey declaration data for human review and downstream ACE / LAWGS workflows. It does not automatically make a legal-compliance determination and does not file to ACE or LAWGS.

The authority boundary is intentionally asymmetric: software may extract, validate, reconcile, prefill and recommend; only an explicit human review decision can move a preparation value into the authoritative reviewed record.

## Customer experience target

1. Customer opens **Operations**.
2. Customer uploads the shipment/supplier files already available. No shipment metadata is required first.
3. Litoral Trace stores immutable originals and starts analysis.
4. Deterministic parsers extract explicit candidate values with page/locator provenance.
5. Engine 2 reconciles document evidence and rejects unsafe semantic substitutions.
6. Supported values are prefilled as `FOUND` suggestions, not silently treated as confirmed.
7. The customer may accept all unambiguous safe suggestions together.
8. Conflicts remain individually blocked and editable; AI may recommend one existing evidence-backed candidate in shadow mode but cannot resolve the issue itself.
9. Missing values are never invented. The UI identifies the likely source/owner of the missing fact so the customer is asked for the minimum additional information.
10. Only explicitly confirmed values become the authoritative preparation record.

## Intelligence pyramid

```text
                         GPT-5.6 Sol
                  bounded conflict adjudication
                              ^
                         GPT-5.6 Terra
                 cross-document reconciliation
                              ^
                         GPT-5.6 Luna
                high-volume structured extraction
                              ^
                 Engine 2 + deterministic parsers
              PDF / OCR / tables / spreadsheets
                              ^
                     immutable source files
```

## Tier 0 — deterministic ingestion, parsing and validation

Implemented responsibilities:

- preserve original bytes and SHA-256 evidence identity;
- validate all selected files before consuming an operation slot;
- extract PDF text/layout and spreadsheet cells;
- classify document structure where deterministic evidence is sufficient;
- keep page/locator provenance;
- extract explicit PPQ preparation labels, including shipment-party, entry, HTS, plant taxonomy, harvest-country and plant-material quantity fields where the source document actually states them;
- run regulatory field validators before a suggestion can enter review;
- reject unsafe substitutions such as country of origin -> country of harvest or gross/net shipment weight -> plant-material quantity.

This layer is cheap, reproducible and auditable and therefore runs before AI.

## Tier 1 — GPT-5.6 Luna extraction

Default high-volume hosted AI model: `gpt-5.6-luna`.

Implemented responsibilities:

- identify supported Lacey preparation candidates from document pages;
- return strict structured JSON;
- return exact source text and page for every candidate;
- label evidence as explicit, derived or inferred;
- omit absent fields rather than guessing;
- compare AI candidates against Engine 2 page text before they can be considered verified.

Safety boundary:

- unverified or inferred candidates do not become trusted suggestions;
- country of harvest cannot be inferred from country of origin, ports, exporter/manufacturer address or vessel route;
- gross shipment weight cannot become plant quantity without explicit support;
- HTS, MID and entry references cannot be invented;
- a verified Engine 2 + Luna agreement may prefill a `MISSING` review field as `FOUND`, never as `MATCHED`.

Implementation: `lacey_engine.ai_providers.OpenAIResponsesProvider` plus `us_lacey.ai_suggestions`. External egress remains disabled unless explicitly enabled through environment configuration.

## Tier 2 — GPT-5.6 Terra reconciliation

Default model: `gpt-5.6-terra`.

The routing policy reserves Terra for evidence that needs semantic cross-document reconciliation but is not a genuine blocking contradiction.

The bounded review layer can only choose among already persisted evidence-backed candidates or return `NEEDS_HUMAN`; it cannot create a new regulatory fact. Recommendation snapshots are stored inside reconciliation-issue evidence and remain non-authoritative.

Implementation: `lacey_engine.ai_routing` and `us_lacey.ai_review`.

## Tier 3 — GPT-5.6 Sol adjudication

Default model: `gpt-5.6-sol`.

Sol is reserved for genuine blocking contradictions after deterministic and lower-cost work. The current adjudicator is deliberately bounded:

- input is a finite list of existing candidate IDs and their bounded evidence metadata;
- output is either `SELECT(candidate_id)` or `NEEDS_HUMAN`;
- the server rejects any candidate ID that was not supplied;
- the recommendation cannot set `human_value`, change candidate decisions, resolve a conflict, mark a field `MATCHED`, or complete an operation;
- if evidence is insufficient, the correct result is `NEEDS_HUMAN`.

Implementation: `us_lacey.ai_review`, gated by `US_LACEY_AI_REVIEW_MODE=SHADOW`.

## Authority states

The customer-facing workflow distinguishes:

- `FOUND`: evidence-backed suggestion, not yet confirmed;
- `MATCHED`: human-confirmed value;
- `REVIEW`: conflict, ambiguity or low-confidence value requiring individual attention;
- `MISSING`: no supported value in supplied evidence;
- `NOT_REQUIRED`: explicitly resolved as not applicable under the implemented field rule.

`FOUND` values belong in the review queue, not the Confirmed counter. Direct completion is blocked while unconfirmed `FOUND` values remain.

## Bulk confirmation rule

The **Accept all safe suggestions** action may accept only unambiguous `FOUND` values that have a proposed value and no multiple-candidate conflict. It never bulk-accepts:

- multiple candidates;
- open conflicts;
- inferred or unverified AI evidence;
- invalid regulatory field values;
- missing data.

Conflicts can be prefilled to reduce typing, but they remain visibly warned and require an individual human decision.

## Missing-data policy

A stronger model is not called merely because evidence is absent. Missing facts remain missing and the review UI directs the customer to the likely documentary source, for example:

- importer/customs broker for filing reference, HTS and customs-entry data;
- supplier/manufacturer for genus, species, harvest country and plant-material quantity;
- invoice, packing list or B/L for merchandise/shipment facts.

This prevents cost escalation from turning into hallucination.

## Cost policy

Spend model budget only where additional intelligence can change the result:

- page-level extraction: Luna;
- one-engine-only or non-blocking semantic reconciliation: Terra;
- genuine blocking conflict/ambiguity: Sol;
- agreement: no additional model call;
- both missing / AI rejected: no stronger-model call — request the missing evidence instead.

The AI review pass is capped per operation (`US_LACEY_AI_REVIEW_MAX_ISSUES`, default 8, hard maximum 25) and is idempotent for the same candidate set and model.

## Runtime gates

All external AI remains opt-in and shadow-first. Relevant environment variables are:

```text
US_LACEY_ENGINE2_MODE=SHADOW
US_LACEY_AI_SHADOW_MODE=SHADOW
US_LACEY_AI_PROVIDER=openai
US_LACEY_AI_ALLOW_EXTERNAL=1
US_LACEY_AI_API_KEY=<secret environment value>
US_LACEY_AI_MODEL=gpt-5.6-luna
US_LACEY_AI_EXTRACT_MODEL=gpt-5.6-luna
US_LACEY_AI_RECONCILE_MODEL=gpt-5.6-terra
US_LACEY_AI_ADJUDICATE_MODEL=gpt-5.6-sol
US_LACEY_AI_REVIEW_MODE=SHADOW
US_LACEY_AI_REVIEW_MAX_ISSUES=8
```

Secrets must remain in the deployment secret/environment store, never in Git, issue bodies, logs or customer-visible output.

## Implemented release slices

1. **Upload-first intake** — implemented.
2. **Zero-required-metadata operation creation** — implemented.
3. **Deterministic PPQ explicit-field extraction expansion** — implemented.
4. **Engine 2 supported-evidence -> human `FOUND` suggestion bridge** — implemented conservatively.
5. **OpenAI Luna structured extraction shadow** — implemented behind explicit external-egress gates.
6. **Verified Engine 2 + Luna agreement -> `FOUND` bridge** — implemented.
7. **Terra/Sol bounded candidate recommendation shadow** — implemented; non-authoritative and not customer-accepted automatically.
8. **Exception-first review UX and safe bulk confirmation** — implemented.
9. **Missing-fact source guidance** — implemented in the review UI.
10. **Golden-case release gate on real complete/incomplete operations** — still required before increasing AI authority or exposing AI conflict recommendations as a normal customer action.

## Pre-production validation gate

Do not increase AI authority because a demo looks good. Before any stronger automation, benchmark representative real-world complete and intentionally incomplete operations and record at least:

- field precision and recall;
- false-supported rate;
- verified-evidence rate;
- unresolved-conflict rate;
- latency per operation;
- AI cost per operation;
- rate of manual edits after a suggestion;
- rate of unsafe inferences (target: zero for protected semantic substitutions).

Until those metrics are acceptable, AI conflict recommendations stay shadow-only and every final preparation value remains subject to explicit human review.
