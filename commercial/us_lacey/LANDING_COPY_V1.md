# Litoral Trace — U.S. Lacey Act Private Beta

## Category

**Source-document preparation for Lacey Act review**

Litoral Trace is not positioned as filing software, legal advice, or a replacement for ACE/LAWGS. The experiment tests whether U.S. customs brokers and import teams value a preparation layer that starts **before** a manually prepared spreadsheet: supplier/shipment documents → extraction → comparison → missing/conflicting data → evidence → structured export for human review.

## Hero

**Eyebrow:** Private Beta · Lacey Act Phase VII

# Stop manually preparing Lacey spreadsheets.

Start with the shipment documents you already receive. Litoral Trace extracts the available data, compares the files, keeps every value tied to its source and shows what is missing or inconsistent before filing.

**Primary CTA:** See a sample shipment

**Secondary CTA:** Test one completed shipment

**Trust line:** No ERP access. Historical shipment is enough. Sensitive data can be redacted.

## Core differentiation

### Manual preparation
1. Receive PDFs, spreadsheets and certificates.
2. Open each file and find the relevant values.
3. Copy values into a structured spreadsheet.
4. Validate and prepare the data for filing.

### With Litoral Trace
1. Upload the shipment documents already received.
2. Extract available values with source references.
3. Compare documents and surface conflicts or missing data.
4. Export reviewed structured data for the next step.

The commercial claim is deliberately narrow: **reduce the document-to-structured-data work that happens before filing.**

## Three benefits

### Extract
Read invoices, spreadsheets and supporting PDFs and create structured candidates only for values actually present.

### Compare
Check whether quantities, shipment references, species/origin data and other fields agree across available documents.

### Preserve evidence
Keep every candidate linked to its source so a reviewer can see where the information came from rather than trusting a black box.

## Synthetic demo

Route: `/lacey/demo`

The public demo is explicitly synthetic and illustrates intended behavior rather than claiming validated U.S. production accuracy.

Sample package:
- `commercial_invoice.pdf`
- `packing_list.pdf`
- `supplier_species.xlsx`
- `supplier_declaration.pdf`
- `bill_of_lading.pdf`

Demo result:
- 25 structured review fields
- 23 values found
- 2 missing values
- 1 document conflict
- source evidence shown for visible sample fields
- prepared XLSX downloadable for inspection

Deliberate exceptions:
- Plant Quantity: Invoice 5,000 kg vs Packing List 4,850 kg → REVIEW
- Country of Harvest: missing → MISSING
- Manufacturer ID: missing → MISSING

## Proof language

Use only capabilities already demonstrated in Litoral Trace or explicitly label them as synthetic demo behavior:

- PDF, Excel and CSV intake in the core product.
- OCR for scanned PDFs in the core product.
- Structured field extraction with confidence and provenance.
- Human review for uncertain/conflicting fields.
- Evidence Vault with SHA-256 integrity reference.
- Cross-document reconciliation and operational exceptions.
- Synthetic U.S. Lacey demo that shows the target field/output workflow.

Do **not** claim that Litoral Trace currently:

- files Lacey declarations;
- integrates with ACE or LAWGS;
- determines legal compliance;
- guarantees acceptance by CBP/APHIS;
- has a complete Lacey rules engine;
- has validated extraction accuracy on real U.S. Lacey shipment files.

## CTA section

# Test one completed historical shipment.

We are looking for a small number of U.S. customs brokers and import teams willing to show us one completed shipment and the source documents used to prepare its Lacey data.

Sensitive information can be redacted. No ERP credentials, live entry or sales call is required; the next step can be handled by email.

## Form fields

1. Work email
2. Role: Customs Broker / Import Compliance / Importer / Other
3. Approximate Lacey volume per month: 1–10 / 11–50 / 51–200 / 200+ / Not sure
4. Current workflow: Supplier spreadsheets / Email + PDFs / ERP / Broker portal / Mixed
5. Would you test one completed historical shipment? Yes / Maybe / No

## Footer disclaimer

Litoral Trace is preparation and evidence software. It does not file declarations, provide legal advice, certify Lacey Act compliance, or guarantee acceptance by APHIS, CBP, ACE or LAWGS.
