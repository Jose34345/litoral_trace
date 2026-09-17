# Lacey Taxonomy Reference Layer — Design

## Goal

Add a deterministic, versioned taxonomy reference layer for the U.S. Lacey benchmark and future Engine 2 integration without making external HTTP services part of shipment processing. The first authority is USDA GRIN Taxonomy; APHIS Special Use Designations (SUDs) are modeled separately because they are filing designations, not botanical taxa.

Official references:
- APHIS Lacey filing resources identify USDA GRIN Taxonomy for Plants as a scientific-name lookup resource: https://www.aphis.usda.gov/plant-imports/file-lacey-act-declaration
- USDA GRIN Taxonomy search: https://npgsweb.ars-grin.gov/gringlobal/taxon/taxonomysearch
- GRIN-Global documentation confirms taxonomy search results can be exported as CSV/Excel: https://www.grin-global.org/help_pw2/Content/Taxonomy%20Browse.htm
- APHIS Special Use Designations: https://www.aphis.usda.gov/plant-imports/file-lacey-act-declaration/special-use-designations

## Non-goals

- Do not query GRIN or APHIS during customer shipment processing.
- Do not make a fuzzy match authoritative.
- Do not treat a valid scientific name as evidence that a shipment actually contains that species.
- Do not replace the Golden Corpus with master data.
- Do not change Engine 2 production behavior in this PR.

## Architecture

The new code remains under `src/litoral_trace/lacey_benchmark/` so the evaluator stays isolated from Engine 2, persistence, FastAPI, and workers.

New components:

1. `taxonomy_contracts.py`
   - `TaxonRecord`: stable source id, accepted name, genus, species, accepted/synonym status, accepted target id.
   - `TaxonomySnapshotManifest`: source, snapshot date, source URL, SHA-256, schema version.
   - `TaxonResolutionStatus`: `EXACT_ACCEPTED`, `EXACT_SYNONYM`, `GENUS_SPECIES`, `AMBIGUOUS`, `NOT_FOUND`, `SPECIAL_USE`.
   - `TaxonResolution`: canonical taxon identity plus evidence about the match.

2. `taxonomy_snapshot.py`
   - Reads an offline GRIN-style CSV export and validates required columns.
   - Builds immutable lookup indexes for accepted names, synonyms, and `(genus, species)` pairs.
   - Validates the file SHA-256 against the manifest.
   - No network calls.

3. `taxonomy_resolver.py`
   - Deterministic resolution only.
   - Exact accepted scientific name -> authoritative canonical id.
   - Exact synonym -> canonical accepted id.
   - Separate genus + species -> canonical accepted id when uniquely resolvable.
   - Species epithet alone is accepted only when genus is supplied and the pair resolves uniquely.
   - Abbreviations, fuzzy spelling, and multiple possible matches never become `AUTO_SUPPORTED`; they return `AMBIGUOUS` or `NOT_FOUND`.
   - APHIS SUD pairs (`SPECIAL/COMPOSITE`, `SPECIAL/RECYCLED`, `SPECIAL/RECLAIMED`, `SPECIAL/SPF`, `SPECIAL/PREAMENDMENT`, temporary/approved groupings) are resolved through a separate static snapshot with provenance.

4. `taxonomy_benchmark.py`
   - Normalizes `genus` + `species` into a benchmark taxon identity before comparing those fields.
   - If expected and actual resolve to the same canonical taxon id, differences such as `grandis` vs `Eucalyptus grandis` are not scored as extraction errors.
   - The base `SemanticEvaluator` remains generic and unchanged; taxonomy-aware comparison is an optional benchmark policy layer.

5. Reference data layout
   - `benchmarks/lacey/reference/grin/manifest.json`
   - `benchmarks/lacey/reference/grin/taxa.csv`
   - `benchmarks/lacey/reference/aphis/sud.json`
   - Initial repository fixture is intentionally small and contains only taxa required by benchmark tests. Production-sized GRIN exports are imported by the same schema later; the resolver must not contain pack/vendor-specific rules.

## Data flow

`Golden JSON + Actual JSON -> taxonomy policy -> canonical taxon ids -> existing semantic evaluator -> Scorecard`

Taxonomy master data answers whether two botanical names refer to the same accepted taxon. The Golden Corpus still answers whether that taxon belongs to the shipment and line being evaluated.

## Safety rules

- Exact normalized names only in the first authoritative implementation.
- No fuzzy auto-support.
- Snapshot checksum is mandatory.
- Duplicate source ids or conflicting accepted targets fail snapshot loading.
- Unknown or ambiguous taxa stay reviewable instead of being silently coerced.
- `WRONG_ENTITY_ASSOCIATION` still takes priority when the correct canonical taxon is found on another line.

## Testing strategy

TDD coverage will include:
- exact accepted name;
- exact synonym to accepted name;
- `genus=Eucalyptus` + `species=grandis` equals `species=Eucalyptus grandis`;
- same epithet with missing genus is not auto-resolved;
- ambiguous/conflicting records fail closed;
- APHIS SUD resolution remains distinct from botanical taxa;
- snapshot SHA mismatch fails closed;
- benchmark stops flagging a false conflict when expected and actual names canonicalize to the same taxon;
- wrong-line canonical taxon still produces `WRONG_ENTITY_ASSOCIATION`;
- no imports from Engine 2 runtime packages.

## Rollout

This PR remains benchmark-only. After the benchmark proves taxonomy normalization reduces false review/false conflict without increasing false-safe, a separate PR may introduce the resolver into Engine 2 shadow mode. No customer-facing authority cutover is part of this design.
