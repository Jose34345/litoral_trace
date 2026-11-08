"""Reproduce additional adversarial PDF bytes from the versioned Pack 2 corpus.

The original Pack 1 and Pack 2 files are never modified. The generated PDFs
remain synthetic test data and are not valid for any filing.
"""
from __future__ import annotations

from hashlib import sha256
import json
from pathlib import Path

from pypdf import PdfReader, PdfWriter
from pypdf.generic import DecodedStreamObject

ROOT = Path(__file__).resolve().parent
SOURCE = ROOT / "pack2"
REPLACEMENTS = {
    "adv_supplier": (
        (b"LONGSHENG", b"EASTSHORE"),
        (b"Longsheng", b"Eastshore"),
        (b"CNFUZLON88JIN", b"CNFUZEAS99JIN"),
    ),
    "adv_conflict": (
        (b"TO BE FILLED FROM REVIEWED SUPPLIER MEMORY", b"aurea"),
    ),
}


def main() -> None:
    manifest = json.loads((ROOT / "sha256.json").read_text(encoding="utf-8"))
    for pack, changes in REPLACEMENTS.items():
        target = ROOT / pack
        target.mkdir(exist_ok=True)
        hashes = {}
        for source in sorted(SOURCE.glob("*.pdf")):
            reader = PdfReader(str(source))
            writer = PdfWriter()
            for page in reader.pages:
                original = page.get_contents().get_data()
                modified = original
                for old, new in changes:
                    if pack == "adv_conflict" and "Entry_Preparation" not in source.name:
                        continue
                    if pack == "adv_conflict":
                        first = modified.find(old)
                        second = modified.find(old, first + len(old))
                        assert first >= 0 and second >= 0
                        modified = modified[:second] + new + modified[second + len(old):]
                    else:
                        modified = modified.replace(old, new)
                if pack == "adv_supplier" and b"LONGSHENG" in original:
                    assert b"EASTSHORE" in modified
                if pack == "adv_conflict" and "Entry_Preparation" in source.name:
                    assert b"TO BE FILLED FROM REVIEWED SUPPLIER MEMORY" in original
                    assert b"aurea" in modified
                stream = DecodedStreamObject()
                stream.set_data(modified)
                writer.add_page(page)
                writer.pages[-1].replace_contents(stream)
            output = target / source.name.replace("Shipment2", pack)
            with output.open("wb") as handle:
                writer.write(handle)
            hashes[output.name] = sha256(output.read_bytes()).hexdigest()
        manifest[pack] = hashes
    (ROOT / "sha256.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8",
    )


if __name__ == "__main__":
    main()
