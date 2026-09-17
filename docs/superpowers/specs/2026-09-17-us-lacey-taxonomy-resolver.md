# U.S. Lacey Taxonomy Resolver — Design

## Objective
Build a deterministic, versioned taxonomy resolver for U.S. Lacey Product Intelligence that converts source-backed material names into taxonomic candidates without fabricating species certainty.

## Authority boundary
The resolver is advisory/non-canonical. It must never write directly to `canonical_shipment_truth`, PPQ505, LAWGS, or a regulatory decision. A taxonomy result is evidence for later human/regulatory review, not a declaration fact.

## Placement
Create a focused package at `src/litoral_trace/us_lacey/regulatory/taxonomy/`.

The reusable BOM domain under `src/litoral_trace/product_intelligence/` remains regulation-agnostic. U.S.-specific taxonomy enrichment occurs only when U.S. Lacey Product Intelligence serializes material evidence.

## Resolution model
Inputs are literal material names from the BOM plus their existing source provenance.

The resolver normalizes case, surrounding whitespace, repeated whitespace, punctuation separators and Unicode accents only for matching. Raw source text is preserved unchanged.

Every result contains:
- catalog version;
- normalized query;
- status;
- zero or more ordered candidates;
- reason code;
- `review_required` boolean.

Candidate fields:
- scientific name;
- taxonomic rank (`SPECIES` or `GENUS` in v1);
- genus;
- species epithet when rank is species;
- match kind (`ACCEPTED_SCIENTIFIC_NAME`, `SYNONYM`, `COMMON_ALIAS`, `COMMERCIAL_ALIAS`, `GENUS_ALIAS`);
- confidence as a deterministic decimal string;
- authority source name;
- authority record URL;
- catalog record id.

## Status semantics
- `RESOLVED`: exact accepted scientific-name match. It may be used as a high-confidence candidate, but remains non-canonical.
- `REVIEW_REQUIRED`: a curated common/commercial/synonym alias points to a specific candidate, but the source text itself is not sufficient to publish a declaration fact automatically.
- `AMBIGUOUS`: the catalog deliberately records multiple plausible candidates for the same alias; no winner is selected.
- `NO_MATCH`: the catalog has no supported taxonomic candidate.

No fuzzy matching is allowed in v1. Near matches must return `NO_MATCH` rather than silently guessing.

## Versioned seed catalog
V1 ships a deliberately small curated catalog to prove the contract safely rather than pretending broad botanical coverage.

Seed records:
1. `Hevea brasiliensis` — accepted species; authority record in USDA NAL Agricultural Thesaurus.
2. `Siphonia brasiliensis` — curated synonym candidate to `Hevea brasiliensis`; because synonym input is not an accepted scientific name, result is `REVIEW_REQUIRED`.
3. `rubber tree`, `rubbertree`, `para rubber` — curated common aliases to `Hevea brasiliensis`; result is `REVIEW_REQUIRED`.
4. `rubberwood` / `rubber wood` — curated commercial aliases to `Hevea brasiliensis`; result is `REVIEW_REQUIRED`.
5. `hevea wood` — commercial/genus-context alias that does not prove a species. Return a genus-level `Hevea` candidate with `REVIEW_REQUIRED`, not `Hevea brasiliensis` automatically.
6. `oak` — genus-level `Quercus` candidate with `REVIEW_REQUIRED`.
7. `pine` — genus-level `Pinus` candidate with `REVIEW_REQUIRED`.

`plywood`, generic `wood`, typos such as `rubberwod`, and unknown material names must return `NO_MATCH`.

## Product Intelligence integration
When serializing each BOM `Material` inside `src/litoral_trace/us_lacey/product_intelligence_snapshot.py`, attach a `taxonomy` object produced by the resolver. Preserve the existing material fields and source anchor unchanged.

The Product Intelligence snapshot schema remains non-canonical. No database migration is required for Hito 7 because taxonomy enrichment is embedded in the immutable JSON payload already scoped to the exact source-set generation.

## Fail-closed invariants
- Never infer a species from a genus-only or generic trade name.
- Never fuzzy-match to force a result.
- Never convert `REVIEW_REQUIRED`, `AMBIGUOUS`, or `NO_MATCH` to `RESOLVED` for convenience.
- Preserve exact source provenance.
- Existing BOM readiness (`READY/PARTIAL/...`) is not upgraded or downgraded solely by taxonomy status in Hito 7.
- Existing canonical truth, review, PPQ505, LAWGS, auth, billing, tenant isolation, source-set fencing and worker completion ordering remain unchanged.

## Testing
Add deterministic unit tests for normalization, accepted-name resolution, synonym/commercial alias review requirements, genus-only safety, no-match behavior, ambiguity, and no fuzzy matching.

Add Product Intelligence integration tests to prove taxonomy enrichment is attached to BOM materials, provenance remains intact, and `hevea wood` does not become a species automatically.

Full CI and U.S. Lacey PostgreSQL Gate must remain green before merge.