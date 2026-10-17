# P0 — First pending issue, Supplier Evidence and Pack 2 mass authority (2026-10-16)

## Problem established

A customer-facing menu link named `Documents` pointed to `/evidence`, which is actually the reusable verified supplier-product **evidence registry**. The shipment Overview button navigated to `#operation-workspace`, not the first pending field. The six-PDF synthetic Pack 2 packing list explicitly contains *Total bamboo net mass 288.00 KG*, *Net bamboo 3.84 KG per carton* and *75 cartons*, but the engine did not recognize the explicit total mass label. Its worksheet had previously remained the visible quantity/unit source.

## P0 implementation

1. Label the existing `/evidence` link and page **Supplier Evidence**; preserve the destination and underlying permissions. Search command matches the same name.
2. Overview's review CTA falls back to `#review-summary` without JS and, with JS, deep-links and focuses the first existing pending row `#review-field-ID`. Respect sticky header and reduced-motion preference. Preserve pending review input and source-inspection UI.
3. Add a narrow **packing-list plant net mass observation** in `lacey_engine/packing_net_material_mass.py`, emitted only if:
   - document is classified as packing list;
   - exactly one supported SKU row exists in a specific carton-and-line breakdown, optionally with a wrapped numeric carton-mark range;
   - explicit product material is solid bamboo, a single `Total bamboo net mass N KG` line exists, and `Net bamboo M KG per carton; gross G KG per carton` is explicit;
   - counts are positive, carton × sets-per-carton matches total sets, carton × net-per-carton exactly equals net total and gross-per-carton is not lower than net-per-carton.
   - Otherwise **abstain**; never substitute a gross mass, volume, cartons, shipment total from mixed lines or inferred quantity.
4. Plant quantity/unit retain original packing-list PDF page/block/source text and the exact single-SKU label; they do not manufacture cross-document component identity. Decimal comparison groups `288.0` and `288.00` to one comparison identity while the canonical display keeps an original source spelling; raw values and provenance remain unchanged. No canonical writes, DB updates, authority override or auto-filing logic in this patch.

## Acceptance / limitations

- Positive Golden Pack 2 original PDF: `plant_quantity=288.00`, `metric_unit=KG`, both with `Total bamboo net mass 288.00 KG` page-1 provenance.
- Six-document Golden Pack 2: 288.0 and 288.00 do not create a precision-only conflict, the original packing list is among supporting sources; overall shipment remains not-ready until all actual compliance requirements are met.
- Negative tests cover gross-only, mixed materials, conflicting net totals, wrong units, over-large gross contradictions, duplicate net mass labels and multiple SKU rows.
- **This is a narrow evidenced source parser, not general net-weight-to-plant-quantity conversion**. Additional source formats must first pass independent fixtures/identity checks.
- Historical operations do not retroactively update from a code deploy: a permitted reprocess under current source-set rules is necessary for new extraction. Original documents may already be deleted after their retention window; no backfilling from derivative worksheets.
- No anonymous sandbox feature reduction or V2 activation.

## Release gate

Focused extraction, semantic, Pack 2, UX and authorization tests; Python CI, PostgreSQL Gate, CSS Drift Gate, merge and verify Northflank web+worker exact SHAs, then public health/ready and JS source URL. A production one-shipment confirmation is only possible if its original documents remain authorized and available.
