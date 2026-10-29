# DI-03 party-label parser acceptance

DI-03 changes only party-label parsing in `lacey_engine/pipeline.py`. It preserves
source blocks/pages, logical document identity and genuine disagreements. It does
not change address normalization, canonical authority, caches, DI-01 or DI-02.

## Address golden reference correction

The private real-PDF test previously expected the composite street/city/state/ZIP
body without a country. Inspection of page 5 confirms literal `Country Code US`
at `p5-l19`, repeated at `p5-l27`, in the same consignee group as the address
anchored at `p5-l15` / `p5-l23`. City, state and ZIP are adjacent labelled rows.
The existing parser deterministically assembles these components and marks the
candidate `DERIVED`. It does not infer the country from a port, shipper or another
document. `US` is source information; the semicolon is presentation punctuation.

The candidate's `raw_text` is already the assembled derived string, not a verbatim
single PDF line. Its normalized/effective values are identical in base 1a245702
and DI-03. The primary provenance still points to the street row; the regression
also verifies the contributing country anchors explicitly. No production address
code was changed, and no DI-04 unsupported-information defect is established.

The former golden omitted legitimate information and is obsolete. The test keeps
its exact former body in `_LEGACY_REAL_CONSIGNEE_ADDRESS`, verifies it unchanged,
and appends only the documented `; US`. This is a reference correction supported
by the document, not a relaxed matcher or suppression of an extraction error.
The old reference failure is preserved in the local audit evidence for both SHAs.

## Independent and integral contracts

`tests/lacey_engine/test_gate1_document_engine.py` checks:

- `test_real_import_info_consignee_name_contract`: full name, no spurious Name
  prefix, and original `p5-l14` evidence.
- `test_real_import_info_consignee_address_contract`: literal components, country
  rows, unchanged legacy body, derived raw/normalized value and source anchor/bbox.
- `test_real_import_info_gate1_regression_fixture`: the complete original golden
  assertions, through `process_bundle`, with logical source identities preserved.

These are separate tests; an integral failure must not be reported as an overall
pass merely because one field passes. The physical PDF is processed once per
module and its target detail is selected by its B/L evidence, not document order.

The private PDF is never included in Git. Local acceptance can read it in place:

```powershell
$env:LITORAL_TRACE_REAL_IMPORT_INFO_PDF = 'C:/path/to/private/source.pdf'
python -m pytest -q tests/lacey_engine/test_gate1_document_engine.py -k real_import_info
```

An explicitly provided missing path fails rather than silently skipping. Without
an external path, the previous optional fixture lookup remains available. The
40 synthetic cases in `test_inline_party_labels.py` require no private document.

## Comparison and limits

Against base, the 40 parser contracts demonstrate the known label failures;
consignee and integral PDF contracts remain red while the corrected address
contract passes independently. Against DI-03 all three PDF contracts and all
40 parser cases must pass. Pack1/Pack2 and CI/PostgreSQL gates remain required.

Classification of this web-print fixture remains heuristic and is unchanged.
Supported extraction is not human confirmation or federal filing authorization.
No private PDF, cropped image, customer evidence dump or local audit log belongs
in the PR. Merge still requires the repository's remote CI and human review.
