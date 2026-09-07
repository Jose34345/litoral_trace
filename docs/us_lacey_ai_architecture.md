# U.S. Lacey zero-entry AI preparation architecture

## Product boundary

Litoral Trace prepares and reconciles Lacey declaration data for human review and downstream ACE / LAWGS workflows. It does not automatically make a legal-compliance determination and does not file to ACE or LAWGS.

## Customer experience target

1. Customer opens **Operations**.
2. Customer uploads the shipment/supplier files already available. No shipment metadata is required first.
3. Litoral Trace stores immutable originals and starts analysis.
4. The platform extracts candidate values and attaches exact evidence/provenance.
5. Deterministic rules validate field semantics and reject unsafe substitutions.
6. Cross-document reconciliation classifies values as supported, conflicting or missing.
7. Supported values are prefilled and may be accepted together by the customer.
8. Conflicting values are also prefilled as suggestions, visibly warned, and remain editable/blocked until a human decides.
9. Missing values are never invented. Future iterations classify the missing owner (supplier, importer or broker) and generate the minimum request needed to obtain it.
10. Only explicitly confirmed values become the authoritative preparation record.

## Intelligence pyramid

```text
                         GPT-5.6 Sol
                  true ambiguity / conflicts
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

### Tier 0 — deterministic ingestion and parsing

Responsibilities:

- preserve original bytes and SHA-256 evidence identity;
- validate file type and size;
- extract PDF text/layout and spreadsheet cells;
- classify document structure where deterministic evidence is sufficient;
- keep page/locator provenance;
- run stable normalizers and regulatory field validators.

This layer is cheap, reproducible and auditable. It should do work that does not require semantic judgment.

### Tier 1 — GPT-5.6 Luna extraction

Default high-volume hosted AI model: `gpt-5.6-luna`.

Responsibilities:

- identify Lacey preparation fields from a document;
- return strict structured JSON;
- return exact source text and page for every candidate;
- label evidence as explicit, derived or inferred;
- omit absent fields rather than guessing.

Safety boundary:

- AI source text must be verified against Engine 2 page text;
- unverified or inferred candidates do not become trusted suggestions;
- country of harvest cannot be inferred from country of origin, ports, exporter/manufacturer address or vessel route;
- gross shipment weight cannot become plant quantity without explicit support;
- HTS, MID and entry references cannot be invented.

Current implementation: `lacey_engine.ai_providers.OpenAIResponsesProvider`, initially `SHADOW` only.

### Tier 2 — GPT-5.6 Terra reconciliation

Default model: `gpt-5.6-terra`.

Use only when a field is supported by one extraction path but lacks independent agreement, or when evidence from multiple documents must be related semantically.

Responsibilities in the next integration slice:

- reconcile invoice / BOL / packing list / supplier declarations;
- distinguish country of manufacture/origin/export from country of harvest;
- associate component/species/country/quantity evidence across documents;
- identify the actor who can resolve a missing field;
- produce a recommendation, not an authoritative value.

### Tier 3 — GPT-5.6 Sol adjudication

Default model: `gpt-5.6-sol`.

Use only for genuine ambiguity or contradictions after deterministic and lower-cost passes. Routing is defined in `lacey_engine.ai_routing`.

Examples:

- two plausible species for the same component;
- supplier documents disagree about harvest country;
- several shipments appear in one report and evidence must be scoped to the current shipment;
- component/BOM language requires professional semantic reasoning.

Sol must never resolve a missing fact by invention. If evidence is insufficient, the correct output is a missing-data request.

## Authority states

The customer-facing workflow must distinguish at least:

- `FOUND`: evidence-backed suggestion, not yet confirmed;
- `MATCHED`: human-confirmed value;
- `REVIEW`: conflict, ambiguity or low-confidence value requiring individual attention;
- `MISSING`: no supported value in supplied evidence;
- `NOT_REQUIRED`: explicitly resolved as not applicable under the implemented field rule.

`FOUND` values belong in the review queue, not the Confirmed counter.

## Bulk confirmation rule

The **Accept all supported values** action may only accept unambiguous `FOUND` values. It must never bulk-accept:

- multiple candidates;
- open conflicts;
- inferred or unverified AI evidence;
- invalid regulatory field values;
- missing data.

Conflicts can be prefilled to reduce typing, but they remain visibly warned and require an individual human decision.

## Cost policy

Spend model budget only where additional intelligence changes the result:

- page-level extraction: Luna;
- one-engine-only evidence requiring semantic reconciliation: Terra;
- genuine conflict/ambiguity: Sol;
- agreement: no additional model call;
- both missing / AI rejected: no stronger-model call — request the missing evidence instead.

This keeps the intelligent workflow economically compatible with SaaS margins while reserving the strongest model for the small fraction of cases that need it.

## Next implementation slices

1. **Upload-first intake and supported-value review** — implemented on the feature branch.
2. **OpenAI Luna shadow benchmark** — configure external egress explicitly, run against real/ground-truth fixtures, measure precision/recall and cost.
3. **Engine 2 + Luna suggestion projector** — persist only verified suggestions into the human review queue; never directly into authoritative human values.
4. **Terra cross-document reconciler** — shipment/component scoped evidence graph and missing-owner classification.
5. **Sol conflict adjudicator** — bounded escalation with structured recommendation and explanation.
6. **Deterministic Lacey applicability/rule engine** — HTS schedule, entry type, disclaimer/SUD/unit/de-minimis rules, versioned separately from model prompts.
7. **Supplier/importer/broker request workflow** — ask only for unresolved information and track response/evidence.
8. **Golden-case release gate** — real complete and intentionally incomplete operations with measured precision, recall, false-supported rate, latency and AI cost per operation.
