# U.S. Lacey Taxonomy Resolver — Hito 7 Design

## Objective
Build a deterministic, versioned, fail-closed taxonomy resolver for U.S. Lacey Product Intelligence that converts source-backed plant/material names into supported taxonomic candidates without fabricating species certainty.

## Regulatory context
APHIS requires scientific genus/species information for covered plant material and points filers to authoritative scientific-name resources such as USDA GRIN, USDA PLANTS and ITIS. When the exact species varies and is unknown, the declaration must contain each species that may have been used rather than an invented single answer. APHIS Special Use Designations are separate regulatory constructs and must not be treated as exact species matches.

## Authority boundary
Taxonomy results are advisory/non-canonical. The resolver must never write directly to `canonical_shipment_truth`, PPQ505, LAWGS, or a regulatory decision. A taxonomy result is evidence for later human/regulatory review.

## Placement
Create a focused package at `src/litoral_trace/us_lacey/regulatory/taxonomy/`.

The reusable BOM domain under `src/litoral_trace/product_intelligence/` remains regulation-agnostic. U.S.-specific taxonomy enrichment occurs only when U.S. Lacey Product Intelligence serializes material evidence.

## Resolution model
Input is the literal material name from the BOM plus the existing source provenance retained by Product Intelligence.

Matching normalization may casefold, Unicode-normalize, trim/collapse whitespace, and normalize simple separators. Raw source text remains unchanged. V1 deliberately performs no fuzzy/edit-distance matching.

Every result contains:
- catalog version;
- normalized query;
- status;
- zero or more ordered candidates;
- reason code;
- `review_required` boolean.

Candidate fields:
- scientific name;
- rank (`SPECIES` or `GENUS` in v1);
- genus;
- species epithet when rank is species;
- match kind (`ACCEPTED_SCIENTIFIC_NAME`, `SYNONYM`, `COMMON_ALIAS`, `COMMERCIAL_ALIAS`, `GENUS_ALIAS`);
- deterministic confidence as a decimal string when serialized;
- authority source name;
- authority record URL;
- catalog record id.

## Status semantics
- `RESOLVED`: exact accepted scientific-name match in the curated catalog. Still non-canonical.
- `REVIEW_REQUIRED`: one supported candidate exists but input is a synonym/common/commercial/genus alias rather than an accepted species fact.
- `AMBIGUOUS`: multiple plausible supported candidates exist; no winner is selected.
- `NO_MATCH`: no supported taxonomic candidate exists.

`AMBIGUOUS` and `NO_MATCH` always require review. `REVIEW_REQUIRED` never auto-promotes to canonical truth.

## Versioned seed catalog
Catalog version: `2026-09-17-v1`.

Initial records are intentionally narrow and auditable:
1. `Hevea brasiliensis` — accepted species.
2. `Siphonia brasiliensis` — synonym candidate to `Hevea brasiliensis`; review required.
3. `rubber tree`, `rubbertree`, `para rubber` — USDA NAL aliases/synonyms for `Hevea brasiliensis`; review required.
4. `rubberwood`, `rubber wood` — curated commercial aliases to `Hevea brasiliensis`; review required and never canonical automatically.
5. `hevea wood` — genus-level `Hevea` candidate only; review required. It must not imply `H. brasiliensis` automatically.
6. `oak` — genus-level `Quercus` candidate; review required.
7. `pine` — genus-level `Pinus` candidate; review required.

`plywood`, generic `wood`, and unknown/near-match names return `NO_MATCH`.

The v1 Hevea species record and USDA aliases use the USDA National Agricultural Library Agricultural Thesaurus concept for `Hevea brasiliensis` as authority metadata. Commercial aliases are explicitly tagged as curated trade aliases rather than represented as USDA synonyms.

## Product Intelligence integration
When serializing each BOM `Material` in `src/litoral_trace/us_lacey/product_intelligence_snapshot.py`, attach a `taxonomy` object produced by the resolver. Preserve all existing material fields and source anchors unchanged.

No database migration is required in Hito 7. Taxonomy enrichment is embedded inside the already immutable, source-set-scoped Product Intelligence JSON snapshot.

## Fail-closed invariants
- Never infer a species from a genus-only or generic trade name.
- Never fuzzy-match to force a candidate.
- Never collapse multiple candidates into a winner.
- Never convert `REVIEW_REQUIRED`, `AMBIGUOUS`, or `NO_MATCH` to `RESOLVED` for convenience.
- Preserve exact BOM provenance.
- Existing BOM readiness (`READY/PARTIAL/FAILED/NOT_APPLICABLE/STALE`) is not upgraded/downgraded solely by taxonomy status in Hito 7.
- Existing canonical truth, review, PPQ505, LAWGS, auth, billing, tenant isolation, source-set fencing and worker completion ordering remain unchanged.

## Testing
Add deterministic unit tests for:
- accepted scientific-name resolution;
- synonym/common/commercial aliases requiring review;
- genus-only safety;
- ambiguity retaining all candidates;
- no-match behavior;
- no fuzzy matching;
- stable catalog/version/provenance metadata.

Extend Product Intelligence snapshot tests to prove taxonomy enrichment is attached to BOM materials while file/sheet/row provenance remains unchanged, and that `hevea wood` never becomes a species automatically.

Full CI, architecture-doc validation and U.S. Lacey PostgreSQL Gate must be green before the PR leaves draft status.
