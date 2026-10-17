Synthetic, fictional 2026 U.S. Lacey Act two-shipment original PDF fixtures. Do not replace with real customer documents. Exact original SHA-256 values in sha256.json. Tests must process PDF bytes, not manually transcribed text.

`pack1` (7 PDFs) and `pack2` (6 PDFs) are the untouched source corpus. The
additional `adv_supplier` and `adv_conflict` directories are versioned PDFs
derived reproducibly by `build_adversarial.py` from Pack 2. The former changes
the supplier name while keeping the same SKU. The latter adds a literal
contradictory species to the entry worksheet. They are test adversaries, not
new evidence for Pack 1 or Pack 2. All four packs are checked against their
SHA-256 manifest before the PostgreSQL golden test uploads their bytes.
