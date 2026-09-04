"""Public Engine 2.0 entrypoint; deliberately free of database and HTTP dependencies."""
from __future__ import annotations
import re
from datetime import datetime

from .admission import admit
from .classifier import classify
from .domain import AdmittedCandidate, DocumentResolution, EvidenceClass, Provenance, RawCandidate
from .layout_parser import parse_layout
from .ranking import resolve
from .segmentation import segment

ENGINE_VERSION = "lacey-engine-2.0.0"
_FIELDS = ("estimated_arrival_date", "bill_of_lading", "container_number", "consignee_name", "consignee_address", "species", "genus", "filing_entry_reference", "manufacturer_id", "hts_code", "country_of_harvest", "plant_quantity", "metric_unit")


def _candidate(field: str, value: str, block, label: str, evidence=EvidenceClass.EXPLICIT, derived_from=None) -> RawCandidate:
    return RawCandidate(field, value, value, block, evidence, f"lacey.{field}", "2.0.0", derived_from, label)


def _normalized_date(value: str) -> str | None:
    for fmt in ("%m/%d/%Y", "%m-%d-%Y", "%B %d, %Y", "%b %d, %Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(value.strip(), fmt).date().isoformat()
        except ValueError:
            pass
    return None


def _extract(layout):
    found: dict[str, list[RawCandidate]] = {field: [] for field in _FIELDS}
    for block in layout.blocks:
        text = block.text
        label, value = (block.key_text, block.value_text) if block.key_text is not None else (None, None)
        pairs = [(label, value)] if label else []
        pairs.extend((match.group(1), match.group(2)) for match in re.finditer(r"(?im)^\s*([A-Za-z][A-Za-z /#.-]{1,45}?)\s*[:#]\s*([^\n]{1,120})$", text))
        for raw_label, raw_value in pairs:
            key, value = " ".join((raw_label or "").split()), " ".join((raw_value or "").split())
            if not value:
                continue
            lower = key.casefold()
            if re.search(r"estimated (?:arrival|date of arrival|time of arrival)|^eta$", lower):
                date = _normalized_date(value)
                if date: found["estimated_arrival_date"].append(_candidate("estimated_arrival_date", date, block, key))
            elif re.search(r"(?:master (?:bol|b/l)|house bol|bill of lading|b/l no\.?|bol)\b", lower):
                found["bill_of_lading"].append(_candidate("bill_of_lading", value.upper(), block, key))
            elif re.fullmatch(r"container(?: number| no\.?)?", lower):
                found["container_number"].append(_candidate("container_number", value.upper(), block, key))
            elif "consignee" in lower and "address" not in lower:
                found["consignee_name"].append(_candidate("consignee_name", value.upper(), block, key))
            elif re.search(r"country of harvest|harvest country|harvested in", lower):
                found["country_of_harvest"].append(_candidate("country_of_harvest", value, block, key))
        # Web-print PDFs often position labels and values on the same visual line
        # without a literal colon. These patterns remain label-bound and therefore
        # cannot turn generic identifiers into regulatory fields.
        for match in re.finditer(r"(?:Estimated (?:Arrival Date|Date of Arrival|Time of Arrival)|\bETA)\s*[:#-]?\s*([A-Za-z]+ \d{1,2}, \d{4}|\d{1,2}[/-]\d{1,2}[/-]\d{4}|\d{4}-\d{1,2}-\d{1,2})", text, re.I):
            date = _normalized_date(match.group(1))
            if date:
                found["estimated_arrival_date"].append(_candidate("estimated_arrival_date", date, block, "Estimated Arrival Date"))
        for match in re.finditer(r"(?:Master\s+(?:BOL|B/L)(?:\s*#)?|House\s+BOL|Bill of Lading|B/L\s*No\.?|\bBOL)\s*[:#-]?\s*([A-Z0-9-]{6,})", text, re.I):
            found["bill_of_lading"].append(_candidate("bill_of_lading", match.group(1).upper(), block, "Bill of Lading"))
        for match in re.finditer(r"\bContainer(?:\s+(?:Number|No\.?)?)?\s*[:#-]?\s*([A-Z]{4}\d{7})\b", text, re.I):
            found["container_number"].append(_candidate("container_number", match.group(1).upper(), block, "Container Number"))
        for match in re.finditer(r"\bConsignee(?:\s+Name)?\s*[:#-]?\s*([A-Z][A-Z &.'-]{3,80})", text):
            value = " ".join(match.group(1).split())
            if value:
                found["consignee_name"].append(_candidate("consignee_name", value, block, "Consignee Name"))
    genera = {"pinus", "eucalyptus", "quercus", "acer", "betula", "fagus", "fraxinus", "populus", "tectona"}
    for source in layout.blocks:
        taxon = next((match for match in re.finditer(r"\b([A-Za-z]{3,})\s+([A-Za-z]{3,})\b", source.text) if match.group(1).casefold() in genera), None)
        if taxon:
            found["species"].append(_candidate("species", taxon.group(2).lower(), source, "scientific taxon"))
            found["genus"].append(_candidate("genus", taxon.group(1).capitalize(), source, "scientific taxon", EvidenceClass.DERIVED, "species"))
    # Reconstruct a party address only from explicit, adjacent labelled
    # components in the consignee record. It is a deterministic DERIVED value;
    # isolated address labels elsewhere in a report are never a consignee.
    lines = [block for block in layout.blocks if block.block_type in {"TEXT_LINE", "OCR_LINE"}]
    for index, block in enumerate(lines):
        if not re.match(r"^Consignee(?: Name)?\s+", block.text, re.I):
            continue
        components: dict[str, tuple[str, object]] = {}
        for following in lines[index + 1 : index + 6]:
            if re.match(r"^Consignee(?: Name)?\s+", following.text, re.I):
                break
            match = re.match(r"^(Address Line 1|City|State Province|Zip Code)\s+(.+)$", following.text, re.I)
            if match:
                components[match.group(1).casefold()] = (" ".join(match.group(2).split()), following)
        if {"address line 1", "city", "state province", "zip code"}.issubset(components):
            address = components["address line 1"][0]
            city = components["city"][0]
            state = components["state province"][0]
            postal = components["zip code"][0]
            source = components["address line 1"][1]
            found["consignee_address"].append(_candidate(
                "consignee_address", f"{address}; {city}, {state} {postal}", source,
                "Consignee Address", EvidenceClass.DERIVED, "consignee_name",
            ))
    # The same visual row may be represented as a text line and a detected
    # table row. That is duplicate evidence, not a semantic disagreement.
    for field_key, candidates in found.items():
        unique: list[RawCandidate] = []
        seen: set[tuple[str, str]] = set()
        for candidate in candidates:
            identity = (candidate.normalized_value, candidate.source_block.block_id)
            if identity not in seen:
                seen.add(identity)
                unique.append(candidate)
        found[field_key] = unique
    return found


def process_document(*, filename: str, content: bytes, role_hint: str | None = None) -> DocumentResolution:
    layout = parse_layout(filename, content)
    document_type, confidence = classify(layout, role_hint)
    sections = segment(layout, document_type)
    section_type = {block_id: section.document_type for section in sections for block_id in section.block_ids}
    extracted = _extract(layout)

    def make(raw, source_score):
        provenance = Provenance(filename, raw.source_block.page, raw.source_block.bbox, raw.source_block.block_id, raw.source_block.text, raw.extractor_name, raw.extractor_version, raw.evidence_class)
        source_type = section_type.get(raw.source_block.block_id, document_type)
        return AdmittedCandidate(raw, provenance, 60 + source_score + (10 if raw.label else 0), source_type)
    make.document_type_for = lambda raw: section_type.get(raw.source_block.block_id, document_type)
    fields = {key: resolve(key, [raw for raw in candidates if admit(raw)], make) for key, candidates in extracted.items()}
    return DocumentResolution(filename, ENGINE_VERSION, document_type, confidence, layout, sections, fields)
