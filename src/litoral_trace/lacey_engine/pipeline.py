"""Public Engine 2.0 entrypoint; deliberately free of database and HTTP dependencies."""
from __future__ import annotations

from datetime import datetime
import re

from .admission import admit
from .classifier import classify
from .domain import (
    AdmittedCandidate,
    DocumentResolution,
    EvidenceClass,
    LayoutStructureType,
    Provenance,
    RawCandidate,
)
from .layout_parser import parse_layout
from .ranking import resolve
from .segmentation import segment
from .semantic_graph import (
    is_out_of_scope_context,
    party_address,
    party_core,
    valid_mid_value,
)

ENGINE_VERSION = "lacey-engine-2.1.0"
_FIELDS = (
    "estimated_arrival_date",
    "bill_of_lading",
    "container_number",
    "importer_name",
    "importer_address",
    "consignee_name",
    "consignee_address",
    "description",
    "species",
    "genus",
    "filing_entry_reference",
    "manufacturer_id",
    "hts_code",
    "entered_value",
    "article_component",
    "country_of_harvest",
    "plant_quantity",
    "metric_unit",
    "percent_recycled",
)
_MERCHANDISE_DESCRIPTION_LABEL = re.compile(
    r"(?:merchandise description|commodity description|cargo description(?:\s+\d+)?|description of goods|goods description)",
    re.I,
)
_IMPORTER_NAME_LABEL = re.compile(r"(?:importer|importer name|importer of record|importer of record name)", re.I)
_CONSIGNEE_NAME_LABEL = re.compile(r"(?:consignee|consignee name)", re.I)
_ENTRY_LABEL = re.compile(r"(?:entry number|entry no\.?|filing entry reference|filing entry number)", re.I)
_MID_LABEL = re.compile(r"(?:mid|manufacturer id|manufacturer identification|manufacturer identification code(?: \(mid\))?)", re.I)
_HTS_LABEL = re.compile(r"(?:hts|hts code|hts number|hts no\.?)", re.I)
_ENTERED_VALUE_LABEL = re.compile(r"(?:entered value|customs entered value)", re.I)
_ARTICLE_COMPONENT_LABEL = re.compile(r"(?:article\s*/\s*component|article component|component)", re.I)
_PLANT_QUANTITY_LABEL = re.compile(r"(?:quantity of plant material|plant material quantity|plant quantity)", re.I)
_PLANT_UNIT_LABEL = re.compile(r"(?:metric unit|plant unit|unit of plant material|plant material unit)", re.I)
_PERCENT_RECYCLED_LABEL = re.compile(r"(?:percent recycled|recycled percentage|% recycled)", re.I)


def _candidate(field: str, value: str, block, label: str, evidence=EvidenceClass.EXPLICIT, derived_from=None) -> RawCandidate:
    return RawCandidate(field, value, value, block, evidence, f"lacey.{field}", "2.1.0", derived_from, label)


def _normalized_date(value: str) -> str | None:
    for fmt in ("%m/%d/%Y", "%m-%d-%Y", "%B %d, %Y", "%b %d, %Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(value.strip(), fmt).date().isoformat()
        except ValueError:
            pass
    return None


def _explicit_number(value: str, *, allow_percent: bool = False) -> str | None:
    text = " ".join(str(value or "").split()).strip()
    pattern = (
        r"(?:USD\s*|\$\s*)?([0-9][0-9,]*(?:\.[0-9]+)?)(?:\s*%)?"
        if allow_percent
        else r"(?:USD\s*|\$\s*)?([0-9][0-9,]*(?:\.[0-9]+)?)"
    )
    match = re.fullmatch(pattern, text, re.I)
    return match.group(1).replace(",", "") if match else None


def _plant_quantity_parts(value: str) -> tuple[str | None, str | None]:
    match = re.fullmatch(
        r"\s*([0-9][0-9,]*(?:\.[0-9]+)?)\s*(kg|g|cg|mg|kl|l|ml|mm|mm2|mm3|cm|cm2|cm3|m|m2|m3|km|kilograms?|grams?|liters?|litres?|cubic meters?|cubic metres?)?\s*",
        str(value or ""),
        re.I,
    )
    if not match:
        return None, None
    return match.group(1).replace(",", ""), (match.group(2) or None)


def _table_context(block) -> bool:
    return block.structure_type in {LayoutStructureType.LINE_ITEM_TABLE, LayoutStructureType.MATRIX_TABLE}


def _append_party(found, *, target: str, address_target: str, value: str, block, label: str) -> None:
    name = party_core(value)
    if name:
        found[target].append(_candidate(target, name, block, label))
    address = party_address(value)
    if address:
        found[address_target].append(
            _candidate(address_target, address, block, f"{label} Address", EvidenceClass.DERIVED, target)
        )


def _extract(layout):
    found: dict[str, list[RawCandidate]] = {field: [] for field in _FIELDS}
    for block in layout.blocks:
        text = block.text
        # Explicit historical/reference-only prose belongs to another evidence
        # entity.  It must never compete with the current shipment.
        if is_out_of_scope_context(text):
            continue
        label, value = (block.key_text, block.value_text) if block.key_text is not None else (None, None)
        pairs = [(label, value)] if label else []
        pairs.extend(
            (match.group(1), match.group(2))
            for match in re.finditer(
                r"(?im)^\s*([A-Za-z][A-Za-z /#.%()'-]{1,70}?)\s*[:#]\s*([^\n]{1,240})$",
                text,
            )
        )
        for raw_label, raw_value in pairs:
            key = " ".join((raw_label or "").split())
            value = " ".join((raw_value or "").split())
            if not value:
                continue
            lower = key.casefold()
            if re.search(r"estimated (?:arrival|date of arrival|time of arrival)|^eta$", lower):
                date = _normalized_date(value)
                if date:
                    found["estimated_arrival_date"].append(_candidate("estimated_arrival_date", date, block, key))
            elif re.search(r"(?:master (?:bol|b/l)|house bol|bill of lading|b/l no\.?|bol)\b", lower):
                found["bill_of_lading"].append(_candidate("bill_of_lading", value.upper(), block, key))
            elif re.fullmatch(r"(?:current )?container(?: number| no\.?)?", lower):
                found["container_number"].append(_candidate("container_number", value.upper(), block, key))
            elif _IMPORTER_NAME_LABEL.fullmatch(key) and "address" not in lower:
                _append_party(found, target="importer_name", address_target="importer_address", value=value, block=block, label=key)
            elif re.fullmatch(r"importer(?:'s)? address|importer address", lower):
                found["importer_address"].append(_candidate("importer_address", value, block, key))
            elif _CONSIGNEE_NAME_LABEL.fullmatch(key) and "address" not in lower:
                _append_party(found, target="consignee_name", address_target="consignee_address", value=value, block=block, label=key)
            elif re.fullmatch(r"consignee(?:'s)? address|consignee address", lower):
                found["consignee_address"].append(_candidate("consignee_address", value, block, key))
            elif _MERCHANDISE_DESCRIPTION_LABEL.fullmatch(key):
                found["description"].append(_candidate("description", value, block, key))
            elif _ENTRY_LABEL.fullmatch(key):
                found["filing_entry_reference"].append(_candidate("filing_entry_reference", value.upper(), block, key))
            elif _MID_LABEL.fullmatch(key):
                candidate_value = value.upper().strip()
                if valid_mid_value(candidate_value):
                    found["manufacturer_id"].append(_candidate("manufacturer_id", candidate_value, block, key))
            elif _HTS_LABEL.fullmatch(key):
                found["hts_code"].append(_candidate("hts_code", value, block, key))
            elif _ENTERED_VALUE_LABEL.fullmatch(key):
                number = _explicit_number(value)
                if number:
                    found["entered_value"].append(_candidate("entered_value", number, block, key, EvidenceClass.DERIVED, "entered_value"))
            elif _ARTICLE_COMPONENT_LABEL.fullmatch(key) and (
                _table_context(block)
                or re.fullmatch(r"article\s*/\s*component|article component", key, re.I)
            ):
                # A fully explicit Lacey/PPQ "Article / Component" label is safe
                # outside a table.  The generic word "Component" remains table-only
                # so unrelated document prose cannot become plant-component evidence.
                found["article_component"].append(_candidate("article_component", value, block, key))
            elif re.fullmatch(r"genus|plant genus|scientific name genus", lower):
                found["genus"].append(_candidate("genus", value, block, key))
            elif re.fullmatch(r"species|plant species|scientific name species", lower):
                found["species"].append(_candidate("species", value, block, key))
            elif re.search(r"country of harvest|harvest country|harvested in", lower):
                found["country_of_harvest"].append(_candidate("country_of_harvest", value, block, key))
            elif _PLANT_QUANTITY_LABEL.fullmatch(key):
                amount, unit = _plant_quantity_parts(value)
                if amount:
                    found["plant_quantity"].append(_candidate("plant_quantity", amount, block, key, EvidenceClass.DERIVED, "plant_quantity"))
                if unit:
                    found["metric_unit"].append(_candidate("metric_unit", unit, block, key, EvidenceClass.DERIVED, "plant_quantity"))
            elif _PLANT_UNIT_LABEL.fullmatch(key) or (lower == "unit" and _table_context(block)):
                found["metric_unit"].append(_candidate("metric_unit", value, block, key))
            elif _PERCENT_RECYCLED_LABEL.fullmatch(key):
                number = _explicit_number(value, allow_percent=True)
                if number is not None:
                    found["percent_recycled"].append(_candidate("percent_recycled", number, block, key, EvidenceClass.DERIVED, "percent_recycled"))

        # Web-print PDFs often position labels and values on the same visual line.
        for match in re.finditer(r"(?:Estimated (?:Arrival Date|Date of Arrival|Time of Arrival)|\bETA)\s*[:#-]?\s*([A-Za-z]+ \d{1,2}, \d{4}|\d{1,2}[/-]\d{1,2}[/-]\d{4}|\d{4}-\d{1,2}-\d{1,2})", text, re.I):
            date = _normalized_date(match.group(1))
            if date:
                found["estimated_arrival_date"].append(_candidate("estimated_arrival_date", date, block, "Estimated Arrival Date"))
        for match in re.finditer(r"(?P<label>Master\s+(?:Bill of Lading|BOL|B/L)(?:\s*#)?|House\s+(?:Bill of Lading|BOL|B/L)(?:\s*#)?|Bill of Lading|B/L\s*No\.?|\bBOL)\s*[:#-]?\s*(?P<value>[A-Z0-9-]{6,})", text, re.I):
            found["bill_of_lading"].append(_candidate("bill_of_lading", match.group("value").upper(), block, match.group("label")))
        for match in re.finditer(r"\b(?:Current\s+)?Container(?:\s+(?:Number|No\.?)?)?\s*[:#-]?\s*([A-Z]{4}\d{7})\b", text, re.I):
            found["container_number"].append(_candidate("container_number", match.group(1).upper(), block, "Container Number"))
        for match in re.finditer(r"\bConsignee(?:\s+Name)?\s*[:#-]?\s*([A-Z][A-Z &.'-]{3,80})", text):
            name = party_core(match.group(1))
            if name:
                found["consignee_name"].append(_candidate("consignee_name", name, block, "Consignee Name"))
        for match in re.finditer(r"(?P<label>Commodity Description|Cargo Description\s+\d+|Description of Goods|Goods Description|Merchandise Description)\s*[:#-]?\s*(?P<value>[^\n]{1,240})", text, re.I):
            value = " ".join(match.group("value").split())
            if value:
                found["description"].append(_candidate("description", value, block, match.group("label")))
        for match in re.finditer(r"(?P<label>Entry (?:Number|No\.?)|Filing Entry (?:Reference|Number))\s*[:#-]?\s*(?P<value>[A-Z0-9-]{8,20})", text, re.I):
            found["filing_entry_reference"].append(_candidate("filing_entry_reference", match.group("value").upper(), block, match.group("label")))
        # Do not let the word "Code" from "Manufacturer Identification Code"
        # become a MID candidate.  Require the value after the complete label.
        for match in re.finditer(r"(?P<label>MID|Manufacturer Identification(?: Code)?(?: \(MID\))?|Manufacturer ID)\s*[:#-]?\s*(?P<value>[A-Z0-9][A-Z0-9 -]{4,24})\b", text, re.I):
            candidate_value = " ".join(match.group("value").split()).upper()
            if valid_mid_value(candidate_value):
                found["manufacturer_id"].append(_candidate("manufacturer_id", candidate_value, block, match.group("label")))
        for match in re.finditer(r"(?P<label>HTS(?:\s+(?:Code|Number|No\.?))?)\s*[:#-]?\s*(?P<value>\d{4,10}(?:[. -]\d{1,4})*)", text, re.I):
            found["hts_code"].append(_candidate("hts_code", match.group("value"), block, match.group("label")))

    genera = {"pinus", "eucalyptus", "quercus", "acer", "betula", "fagus", "fraxinus", "populus", "tectona", "hevea"}
    for source in layout.blocks:
        if is_out_of_scope_context(source.text):
            continue
        taxon = next((match for match in re.finditer(r"\b([A-Za-z]{3,})\s+([A-Za-z]{3,})\b", source.text) if match.group(1).casefold() in genera), None)
        if taxon:
            found["species"].append(_candidate("species", taxon.group(2).lower(), source, "scientific taxon"))
            found["genus"].append(_candidate("genus", taxon.group(1).capitalize(), source, "scientific taxon", EvidenceClass.DERIVED, "species"))

    # Reconstruct party addresses from adjacent explicitly labelled components.
    lines = [block for block in layout.blocks if block.block_type in {"TEXT_LINE", "OCR_LINE"} and not is_out_of_scope_context(block.text)]
    for party, target in (("Consignee", "consignee_address"), ("Importer", "importer_address")):
        for index, block in enumerate(lines):
            if not re.match(rf"^{party}(?: Name| of Record)?\s+", block.text, re.I):
                continue
            components: dict[str, tuple[str, object]] = {}
            for following in lines[index + 1 : index + 7]:
                if re.match(r"^(?:Consignee|Importer)(?: Name| of Record)?\s+", following.text, re.I):
                    break
                match = re.match(r"^(Address Line 1|Address|City|State Province|State|Zip Code|Postal Code|Country Code|Country)\s+(.+)$", following.text, re.I)
                if match:
                    components[match.group(1).casefold()] = (" ".join(match.group(2).split()), following)
            address_key = "address line 1" if "address line 1" in components else ("address" if "address" in components else None)
            city_key = "city" if "city" in components else None
            state_key = "state province" if "state province" in components else ("state" if "state" in components else None)
            postal_key = "zip code" if "zip code" in components else ("postal code" if "postal code" in components else None)
            if address_key and city_key and state_key and postal_key:
                address, city, state, postal = components[address_key][0], components[city_key][0], components[state_key][0], components[postal_key][0]
                country_key = "country" if "country" in components else ("country code" if "country code" in components else None)
                country = f"; {components[country_key][0]}" if country_key else ""
                source = components[address_key][1]
                found[target].append(_candidate(target, f"{address}; {city}, {state} {postal}{country}", source, f"{party} Address", EvidenceClass.DERIVED, f"{party.casefold()}_name"))

    # The same visual row may be represented as a text line and a table cell.  Keep
    # duplicate sources for corroboration only when the block differs; exact duplicate
    # candidate/block pairs are removed here.
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
        provenance = Provenance(
            filename,
            raw.source_block.page,
            raw.source_block.bbox,
            raw.source_block.block_id,
            raw.source_block.text,
            raw.extractor_name,
            raw.extractor_version,
            raw.evidence_class,
        )
        source_type = section_type.get(raw.source_block.block_id, document_type)
        return AdmittedCandidate(raw, provenance, 60 + source_score + (10 if raw.label else 0), source_type)

    make.document_type_for = lambda raw: section_type.get(raw.source_block.block_id, document_type)
    fields = {
        key: resolve(key, [raw for raw in candidates if admit(raw)], make)
        for key, candidates in extracted.items()
    }
    return DocumentResolution(filename, ENGINE_VERSION, document_type, confidence, layout, sections, fields)