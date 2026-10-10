# Assurance V2 — Source-linked claims (Agent 1)

Contract: `assurance.claim.v1`. Module: `src/litoral_trace/lacey_engine/source_linked_claims.py`.

## Authority and compatibility

This is an **opt-in read-side adapter**, not a replacement for Engine 2, specialist
routing, the current QA benchmark, or the canonical publication path. No SQL,
workflows, regulatory decisions, human approval, OCR, regex-specific extractors,
or global worker behavior are changed.

Data boundaries:

1. `SourceEvidenceDocument` holds **original bytes** (in memory, not in serialized
   claims) plus parser-provided `ParsedLayout` or `SheetCell` records. The SHA-256
   of those exact bytes is embedded in every emitted claim. **Integration must
   derive the parsed evidence from those same bytes**; an arbitrary caller-supplied
   layout cannot cryptographically prove its own relationship to a PDF.
2. `ClaimContext` carries the organization, operation, source-set revision and
   extraction-run identities. **Application code must validate these identities
   against the current source-set generation**; this module has no DB access.
3. `from_specialist_envelope` re-checks the literal value and quote against an
   actual unique source layout block; provider `evidence_verified`, model
   coordinates, inferred value and fused key are not trusted as authority.
4. `from_engine2_candidate` preserves `RawCandidate.raw_text`, the deterministic
   normalized value *separately*, source coordinates, and source score as
   `extractor_score` (ranking points, **not** calibrated 0–1 confidence).
5. `from_spreadsheet_cell` preserves the exact sheet/row/column and source text;
   row identity is local to the document, not a packet-global product identity.
6. `claims_from_engine2_resolution` and `claims_from_specialist_result` carry
   **every candidate**, including contradictory, rejected and losing candidates.
   `group_claims` records corroboration/conflict without writing winners.
7. Source-local line/row keys cannot be used for cross-document joins.
   `EXPLICIT_SKU` is only emitted if the exact SKU is in the same verified block
   or cell; HTS, quantity, position, similarity and inferred keys never bind.
8. All claims have `candidate_state=CANDIDATE` and
   `interpretation_status=PROPOSED`. `SUPPORTED` means **literal source-anchor
   support only**, not semantic field correctness, human confirmation, or
   regulatory truth. Re-extraction does not count as independent corroboration.

## Contract / integration surface

Pure, JSON-safe functions:
`SourceLinkedClaim.to_dict()`, `serialize_claims(claims)`,
`verify_claim_anchor(claim, original_document)`, `group_claims(claims)`.
Wire root: `{"contract_version": "assurance.claim.v1", "claims": [...]}`.
All individual claims carry document SHA, identity, original value, optional
deterministic normalization and AI interpretation, source locator (block/page/
literal span/bbox OR sheet/row/column), subject, line key, confidence/extractor
metadata, support status and issues.

No dependency on any unmerged Agent 2 modules or schema changes. Integration
must provide **trusted original document bytes, parser output and source-set
identifiers**, persist claims under tenant/source-set fencing, and require explicit
review before promotion to canonical truth or reusable memory.

## Test corpus and limitations

Run:

```powershell
python -m pytest tests/lacey_engine/test_source_linked_claims.py -q -s
python -m pytest tests/lacey_engine tests/lacey_benchmark -q
```

The benchmark combines **26 labeled cases from**
`benchmarks/lacey/v1/field_truth.json` (using the corresponding parsed-text
packet fixture `tests/fixtures/lacey_router_packet_7_docs.json`) and **19
adversarial examples** in `adversarial_cases.json`: multiple SKU/species/countries,
invented values, historical documents, duplicate anchors, numerical-token
substrings, incompatible units, ES/PT/EN source text and conflicting cases.

**Important:** This checks source-linking given fixture text/layout; it is **not**
a real-PDF/OCR/image segmentation benchmark, an independent ground-truth review of
all document content, nor proof of accuracy in customer production documents.
The existing V2 Shadow-vs-Canonical benchmarks test a different contract.

## Field metrics (last local synthetic run)

Definitions:
- **Precision:** correctly source-supported / all marked `SUPPORTED`.
- **Coverage:** correctly source-supported / all labeled positive source claims.
- **Abstention:** not marked `SUPPORTED` / all positive and negative cases.
- Missing precision is N/A (no supported predictions), not 100%.

| Field | N | Precision | Coverage | Abstention |
|---|---:|---:|---:|---:|
| bill_of_lading | 1 | 100% | 100% | 0% |
| container_number | 1 | 100% | 100% | 0% |
| country_of_harvest | 6 | 100% | 100% | 16.7% |
| entered_value | 5 | 100% | 100% | 20.0% |
| estimated_arrival_date | 1 | N/A | **0%** | **100%** |
| filing_entry_reference | 1 | 100% | 100% | 0% |
| genus | 6 | 100% | 100% | 50.0% |
| hts_code | 5 | 100% | 100% | 20.0% |
| manufacturer_id | 1 | 100% | 100% | 0% |
| metric_unit | 5 | 100% | 100% | 20.0% |
| plant_quantity | 6 | 100% | 100% | 50.0% |
| species | 7 | 100% | 100% | 14.3% |

The unanchored `estimated_arrival_date` from baseline case `v1:24` is
intentionally **not** promoted. No false source-support case was observed in
this synthetic dataset; larger real document benchmarks are still required.
