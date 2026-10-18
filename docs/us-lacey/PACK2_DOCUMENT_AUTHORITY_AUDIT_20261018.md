# Pack 2: document role, source authority and review state audit (2026-10-18)

## Observed production facts (read-only queries, operation 610e78df-3aa3-46a0-9975-a9ebdb2cb17a)

- Six current documents; six durable jobs completed without error, source revision 1 finalized.
- All six operation document roles are UNKNOWN. Five of six legacy universal Assurance document types are INVOICE. These are **not** verified classifications.
- plant_quantity=288 and metric_unit=kg are REVIEW fields backed by packing list document 20/page 1, not the derived entry-preparation worksheet document 23.
- Both quantitative rows have two candidates with the same normalized value and no open reconciliation issue. Neither is human confirmed.
- The operation remains REVIEW_REQUIRED and NEEDS_HUMAN_REVIEW.

## Causes and downstream consumers

1. The self-service uploader defaults to document_role=UNKNOWN when no user role is provided. That role persists on us_lacey_operation_documents. Lacey Engine's role_hint is advisory and cached by role; do not silently rewrite historical roles or overwrite the source set.
2. Assurance V1's broad keyword classifier writes semantic_document_type=INVOICE when a document references an invoice. This legacy classification is **not** the Lacey Engine document classifier or regulatory source authority.
3. Document authority checks in review_source_authority, canonical_publication_support, and the regulatory applicability attestation independently consult current Vault + operation role + derived worksheet negative classification. An UNKNOWN source is not promoted into an independently verified original. The bulk supported-fields action correctly refuses unknown independent sources; this can create a mismatch between its generic UI count and actual eligible fields.
4. The multilingual shadow evidence snapshot previously repeated the legacy Assurance invoice label on U.S. Lacey documents without a matching commercial-invoice role. It now emits UNKNOWN in that specific mismatched case. This does **not** change canonical authority, the original records, export or stored source documents.
5. The processing HTMX poll swaps the operation workspace but not the shipment summary. The status synchronization previously relied on the possibly detached outerHTML target. It now inspects the attached terminal workspace and refreshes the summary from the authenticated server response.

## Exception-first review rule

Plant mass + unit may be explicitly confirmed in one click **only** if same line, same current original packing document, same page, source-specific immutable canonical locators, no conflicting candidate values, no open reconciliation issue, current finalized source set, no processing jobs and no V2 journaling. Both fields are audited through the established writer in one transaction. Failed/stale/conflicting calls roll back both, and leave original source and legal status unchanged.

The CBP entry number is a **later dependency** (only after actual entry issuance); until then it remains pending and blocks filing-ready output. It must never be invented or silently marked NOT_REQUIRED.

## GO/NO-GO gates

- Isolated tests for legacy semantic-type disagreement and derived worksheet authority.
- PostgreSQL atomic positive and negative tests for paired review; real Pack1/Pack2 and adversarial tests.
- Full U.S. Lacey unit tests, compiled Jinja/JS assets, CSS check and live deployment SHA verification.
- Live acceptance should verify source provenance, two separate human review audit records after explicit paired click, correct terminal summary and no regression in readiness/export authorization.
- No automatic historical role backfill and no activation of Assurance V2.
