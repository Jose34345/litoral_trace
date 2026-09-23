"""Public Engine 2.0 entrypoint; deliberately free of database and HTTP dependencies."""
from __future__ import annotations

from datetime import datetime
import re

from .admission import admit
from .classifier import classify
from .domain import (
    AdmittedCandidate,
    BundleResolution,
    DocumentResolution,
    DocumentSection,
    EvidenceClass,
    LayoutStructureType,
    LogicalDocumentResolution,
    ParsedLayout,
    Provenance,
    RawCandidate,
)
from .errors import LaceyEngineError
from .layout_parser import parse_layout
from .ranking import resolve
from .segmentation import segment
from .semantic_graph import (
    is_out_of_scope_context,
    party_address,
    party_core,
    valid_mid_value,
)
from .trade_validation import (
    is_valid_entity_name,
    normalize_invoice_total,
    normalize_iso6346_container,
    normalize_seal_number,
)

ENGINE_VERSION = "lacey-engine-2.5.0"
_FIELDS = (
    "estimated_arrival_date",
    "bill_of_lading",
    "container_number",
    "seal_number",
    "invoice_total",
    "importer_name",
    "importer_address",
    "consignee_name",
    "consignee_address",
    "supplier_name",
    "description",
    "species",
    "genus",
    "filing_entry_reference",
    "manufacturer_id",
    "hts_code",
    "entered_value",
    "currency",
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
_SUPPLIER_NAME_LABEL = re.compile(r"(?:supplier|supplier name|vendor|vendor name)", re.I)
_SEAL_LABEL = re.compile(r"(?:seal|seal number|seal no\.?)", re.I)
_INVOICE_TOTAL_LABEL = re.compile(
    r"(?:invoice total|total invoice|grand total|total amount|amount due)",
    re.I,
)
_ENTRY_LABEL = re.compile(r"(?:entry number|entry no\.?|filing entry reference|filing entry number)", re.I)
_MID_LABEL = re.compile(r"(?:mid|manufacturer id|manufacturer identification|manufacturer identification code(?: \(mid\))?)", re.I)
_HTS_LABEL = re.compile(r"(?:hts|hts code|hts number|hts no\.?)", re.I)
_ENTERED_VALUE_LABEL = re.compile(r"(?:entered value|customs entered value)", re.I)
_CURRENCY_LABEL = re.compile(r"(?:currency|currency code)", re.I)
_ARTICLE_COMPONENT_LABEL = re.compile(r"(?:article\s*/\s*component|article component|component)", re.I)
_PLANT_QUANTITY_LABEL = re.compile(r"(?:quantity of plant material|plant material quantity|plant quantity)", re.I)
_PLANT_UNIT_LABEL = re.compile(r"(?:metric unit|plant unit|unit of plant material|plant material unit)", re.I)
_PERCENT_RECYCLED_LABEL = re.compile(r"(?:percent recycled|recycled percentage|% recycled)", re.I)
_ISO_CURRENCY = re.compile(r"^(USD|EUR|CAD|GBP|AUD|JPY|CNY|BRL|MXN)\b", re.I)


def _candidate(field: str, value: str, block, label: str, evidence=EvidenceClass.EXPLICIT, derived_from=None) -> RawCandidate:
    return RawCandidate(field, value, value, block, evidence, f"lacey.{field}", "2.5.0", derived_from, label)


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


def _plant_declaration_table_ids(layout) -> frozenset[str]:
    headers_by_table: dict[str, set[str]] = {}
    for block in layout.blocks:
        if not _table_context(block) or not block.table_id or not block.table_header:
            continue
        headers_by_table.setdefault(str(block.table_id), set()).add(
            " ".join(str(block.table_header).split()).casefold()
        )
    qualified: set[str] = set()
    for table_id, headers in headers_by_table.items():
        has_quantity = any(_PLANT_QUANTITY_LABEL.fullmatch(header) for header in headers)
        has_unit = any(_PLANT_UNIT_LABEL.fullmatch(header) or header == "unit" for header in headers)
        has_genus = any(re.fullmatch(r"genus|plant genus|scientific name genus", header) for header in headers)
        has_species = any(re.fullmatch(r"species|plant species|scientific name species", header) for header in headers)
        has_harvest_or_component = any(
            re.search(r"country of harvest|harvest country", header)
            or _ARTICLE_COMPONENT_LABEL.fullmatch(header)
            for header in headers
        )
        if has_quantity and has_unit and has_genus and has_species and has_harvest_or_component:
            qualified.add(table_id)
    return frozenset(qualified)


def _entry_worksheet_table_ids(layout) -> frozenset[str]:
    """Identify customs line tables strongly enough to treat Qty as plant-line quantity."""
    headers_by_table: dict[str, set[str]] = {}
    for block in layout.blocks:
        if not _table_context(block) or not block.table_id or not block.table_header:
            continue
        headers_by_table.setdefault(str(block.table_id), set()).add(
            " ".join(str(block.table_header).split()).casefold()
        )
    qualified: set[str] = set()
    for table_id, headers in headers_by_table.items():
        has_line = any(header in {"line", "line no", "line number", "item"} for header in headers)
        has_hts = any(_HTS_LABEL.fullmatch(header) for header in headers)
        has_description = "description" in headers or any(_MERCHANDISE_DESCRIPTION_LABEL.fullmatch(header) for header in headers)
        has_qty = any(header in {"qty", "quantity"} for header in headers)
        has_value = any(_ENTERED_VALUE_LABEL.fullmatch(header) for header in headers)
        if has_line and has_hts and has_description and has_qty and has_value:
            qualified.add(table_id)
    return frozenset(qualified)


def _plant_quantity_row_keys(layout, table_ids: frozenset[str]) -> frozenset[tuple[str, int]]:
    rows: set[tuple[str, int]] = set()
    for block in layout.blocks:
        if not block.table_id or block.row_index is None or str(block.table_id) not in table_ids:
            continue
        key = " ".join(str(block.key_text or block.table_header or "").split())
        if not _PLANT_QUANTITY_LABEL.fullmatch(key):
            continue
        amount, _unit = _plant_quantity_parts(str(block.value_text or block.text or ""))
        if amount:
            rows.add((str(block.table_id), int(block.row_index)))
    return frozenset(rows)


def _append_party(found, *, target: str, address_target: str, value: str, block, label: str) -> None:
    name = party_core(value)
    if name and is_valid_entity_name(name):
        found[target].append(_candidate(target, name, block, label))
    address = party_address(value)
    if address:
        found[address_target].append(
            _candidate(address_target, address, block, f"{label} Address", EvidenceClass.DERIVED, target)
        )


def _extract(layout):
    found: dict[str, list[RawCandidate]] = {field: [] for field in _FIELDS}
    plant_declaration_tables = _plant_declaration_table_ids(layout)
    entry_worksheet_tables = _entry_worksheet_table_ids(layout)
    plant_quantity_rows = _plant_quantity_row_keys(layout, plant_declaration_tables)
    for block in layout.blocks:
        text = block.text
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
                container = normalize_iso6346_container(value)
                if container:
                    found["container_number"].append(
                        _candidate("container_number", container, block, key)
                    )
            elif _SEAL_LABEL.fullmatch(key):
                seal = normalize_seal_number(value)
                if seal:
                    found["seal_number"].append(
                        _candidate("seal_number", seal, block, key)
                    )
            elif _INVOICE_TOTAL_LABEL.fullmatch(key):
                total = normalize_invoice_total(value)
                if total is not None:
                    found["invoice_total"].append(
                        _candidate(
                            "invoice_total",
                            total,
                            block,
                            key,
                            EvidenceClass.DERIVED,
                            "invoice_total",
                        )
                    )
                    currency = _ISO_CURRENCY.match(value)
                    if currency:
                        found["currency"].append(
                            _candidate(
                                "currency",
                                currency.group(1).upper(),
                                block,
                                "Currency",
                                EvidenceClass.DERIVED,
                                "invoice_total",
                            )
                        )
            elif _IMPORTER_NAME_LABEL.fullmatch(key) and "address" not in lower:
                _append_party(found, target="importer_name", address_target="importer_address", value=value, block=block, label=key)
            elif re.fullmatch(r"importer(?:'s)? address|importer address", lower):
                found["importer_address"].append(_candidate("importer_address", value, block, key))
            elif _CONSIGNEE_NAME_LABEL.fullmatch(key) and "address" not in lower:
                _append_party(found, target="consignee_name", address_target="consignee_address", value=value, block=block, label=key)
            elif _SUPPLIER_NAME_LABEL.fullmatch(key):
                supplier = party_core(value)
                if supplier and is_valid_entity_name(supplier):
                    found["supplier_name"].append(
                        _candidate("supplier_name", supplier, block, key)
                    )
            elif re.fullmatch(r"consignee(?:'s)? address|consignee address", lower):
                found["consignee_address"].append(_candidate("consignee_address", value, block, key))
            elif _MERCHANDISE_DESCRIPTION_LABEL.fullmatch(key) or (_table_context(block) and lower == "description"):
                found["description"].append(_candidate("description", value, block, key))
            elif _ENTRY_LABEL.fullmatch(key):
                found["filing_entry_reference"].append(_candidate("filing_entry_reference", value.upper(), block, key))
            elif _MID_LABEL.fullmatch(key):
                candidate_value = value.upper().strip()
                if valid_mid_value(
                    candidate_value,
                    source_text=block.text,
                    label=key,
                ):
                    found["manufacturer_id"].append(
                        _candidate("manufacturer_id", candidate_value, block, key)
                    )
            elif _HTS_LABEL.fullmatch(key):
                found["hts_code"].append(_candidate("hts_code", value, block, key))
            elif _ENTERED_VALUE_LABEL.fullmatch(key):
                number = _explicit_number(value)
                if number:
                    found["entered_value"].append(_candidate("entered_value", number, block, key, EvidenceClass.DERIVED, "entered_value"))
                currency = _ISO_CURRENCY.match(value)
                if currency:
                    found["currency"].append(_candidate("currency", currency.group(1).upper(), block, "Currency", EvidenceClass.DERIVED, "entered_value"))
            elif _CURRENCY_LABEL.fullmatch(key) and _ISO_CURRENCY.fullmatch(value):
                found["currency"].append(_candidate("currency", value.upper(), block, key))
            elif _ARTICLE_COMPONENT_LABEL.fullmatch(key) and (
                _table_context(block) or re.fullmatch(r"article\s*/\s*component|article component", key, re.I)
            ):
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
            elif (
                lower in {"qty", "quantity"}
                and block.table_id is not None
                and str(block.table_id) in entry_worksheet_tables
            ):
                amount, unit = _plant_quantity_parts(value)
                if amount and unit:
                    found["plant_quantity"].append(_candidate("plant_quantity", amount, block, "Plant Quantity", EvidenceClass.DERIVED, "plant_quantity"))
                    found["metric_unit"].append(_candidate("metric_unit", unit, block, "Plant Quantity Unit", EvidenceClass.DERIVED, "plant_quantity"))
            elif _PLANT_UNIT_LABEL.fullmatch(key) or (
                lower == "unit" and block.table_id is not None and block.row_index is not None
                and (str(block.table_id), int(block.row_index)) in plant_quantity_rows
            ):
                found["metric_unit"].append(_candidate("metric_unit", value, block, key))
            elif _PERCENT_RECYCLED_LABEL.fullmatch(key):
                number = _explicit_number(value, allow_percent=True)
                if number is not None:
                    found["percent_recycled"].append(_candidate("percent_recycled", number, block, key, EvidenceClass.DERIVED, "percent_recycled"))

        for match in re.finditer(r"(?:Estimated (?:Arrival Date|Date of Arrival|Time of Arrival)|\bETA)\s*[:#-]?\s*([A-Za-z]+ \d{1,2}, \d{4}|\d{1,2}[/-]\d{1,2}[/-]\d{4}|\d{4}-\d{1,2}-\d{1,2})", text, re.I):
            date = _normalized_date(match.group(1))
            if date:
                found["estimated_arrival_date"].append(_candidate("estimated_arrival_date", date, block, "Estimated Arrival Date"))
        for match in re.finditer(r"(?P<label>Master\s+(?:Bill of Lading|BOL|B/L)(?:\s*#)?|House\s+(?:Bill of Lading|BOL|B/L)(?:\s*#)?|Bill of Lading|B/L\s*No\.?|\bBOL)\s*[:#-]?\s*(?P<value>[A-Z0-9-]{6,})", text, re.I):
            found["bill_of_lading"].append(_candidate("bill_of_lading", match.group("value").upper(), block, match.group("label")))
        for match in re.finditer(
            r"\b(?:Current\s+)?Container(?:\s+(?:Number|No\.?)?)?\s*[:#-]?\s*"
            r"([A-Z]{4}[\s-]?[0-9]{6}[\s-]?[0-9])\b",
            text,
            re.I,
        ):
            container = normalize_iso6346_container(match.group(1))
            if container:
                found["container_number"].append(
                    _candidate("container_number", container, block, "Container Number")
                )
        for match in re.finditer(
            r"\b(?P<label>Seal(?:\s+(?:Number|No\.?))?)\s*[:#-]?\s*"
            r"(?P<value>[A-Z0-9][A-Z0-9-]{2,23})\b",
            text,
            re.I,
        ):
            seal = normalize_seal_number(match.group("value"))
            if seal:
                found["seal_number"].append(
                    _candidate("seal_number", seal, block, match.group("label"))
                )
        for match in re.finditer(
            r"(?P<label>Invoice\s+Total|Total\s+Invoice|Grand\s+Total|Total\s+Amount|Amount\s+Due)"
            r"\s*[:#-]?\s*(?P<value>[^\n]{1,80})",
            text,
            re.I,
        ):
            total = normalize_invoice_total(match.group("value"))
            if total is not None:
                found["invoice_total"].append(
                    _candidate(
                        "invoice_total",
                        total,
                        block,
                        match.group("label"),
                        EvidenceClass.DERIVED,
                        "invoice_total",
                    )
                )
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
        for match in re.finditer(
            r"\b(?P<label>MID|Manufacturer Identification(?: Code)?(?: \(MID\))?|Manufacturer ID)"
            r"\b\s*[:#-]?\s*(?P<value>[A-Z0-9][A-Z0-9 -]{4,24})\b",
            text,
            re.I,
        ):
            candidate_value = " ".join(match.group("value").split()).upper()
            if valid_mid_value(
                candidate_value,
                source_text=text,
                label=match.group("label"),
            ):
                found["manufacturer_id"].append(
                    _candidate("manufacturer_id", candidate_value, block, match.group("label"))
                )
        for match in re.finditer(r"(?P<label>HTS(?:\s+(?:Code|Number|No\.?))?)\s*[:#-]?\s*(?P<value>\d{4,10}(?:[. -]\d{1,4})*)", text, re.I):
            found["hts_code"].append(_candidate("hts_code", match.group("value"), block, match.group("label")))
        for match in re.finditer(
            r"(?:country of harvest|harvest country|harvested in|pa[ií]s de colheita|pa[ií]s de cosecha)\s*[:#-]?\s*([A-Za-zÀ-ÿ][A-Za-zÀ-ÿ .'-]{1,60}?)(?=\s+-|\s+\||$)",
            text,
            re.I,
        ):
            country = " ".join(match.group(1).split()).strip(" .,-")
            if country:
                found["country_of_harvest"].append(_candidate("country_of_harvest", country, block, "Country of Harvest"))

    genera = {"pinus", "eucalyptus", "quercus", "acer", "betula", "fagus", "fraxinus", "populus", "tectona", "hevea"}
    for source in layout.blocks:
        if is_out_of_scope_context(source.text):
            continue
        taxon = next((match for match in re.finditer(r"\b([A-Za-z]{3,})\s+([A-Za-z]{3,})\b", source.text) if match.group(1).casefold() in genera), None)
        if taxon:
            found["species"].append(_candidate("species", taxon.group(2).lower(), source, "scientific taxon"))
            found["genus"].append(_candidate("genus", taxon.group(1).capitalize(), source, "scientific taxon", EvidenceClass.DERIVED, "species"))

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


def _slice_layout(
    layout: ParsedLayout,
    *,
    page_start: int,
    page_end: int,
) -> ParsedLayout:
    """Create a logical view while preserving physical PDF page coordinates."""

    if page_start < 1:
        raise ValueError("page_start must be >= 1.")
    if page_end < page_start:
        raise ValueError("page_end cannot precede page_start.")
    if page_end > layout.page_count:
        raise ValueError(
            "Logical page range exceeds the physical document."
        )

    blocks = tuple(
        block
        for block in layout.blocks
        if page_start <= block.page <= page_end
    )
    return ParsedLayout(
        blocks=blocks,
        # Physical page count is an immutable provenance property. LayoutBlock.page
        # values remain original source-page coordinates rather than being rebased.
        page_count=layout.page_count,
    )


def _resolve_logical_document(
    *,
    filename: str,
    layout: ParsedLayout,
    section: DocumentSection,
) -> DocumentResolution:
    """Extract and resolve candidates from exactly one logical document."""

    extracted = _extract(layout)

    def make(
        raw: RawCandidate,
        source_score: float,
    ) -> AdmittedCandidate:
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
        return AdmittedCandidate(
            raw,
            provenance,
            60 + source_score + (10 if raw.label else 0),
            section.document_type,
        )

    make.document_type_for = lambda _raw: section.document_type
    def compatible(field_key: str) -> bool:
        if field_key in {"container_number", "seal_number"}:
            return section.document_type.value == "BILL_OF_LADING"
        if field_key == "invoice_total":
            return section.document_type.value == "COMMERCIAL_INVOICE"
        return True

    fields = {
        key: resolve(
            key,
            [
                raw
                for raw in candidates
                if compatible(key) and admit(raw)
            ],
            make,
        )
        for key, candidates in extracted.items()
    }
    return DocumentResolution(
        filename,
        ENGINE_VERSION,
        section.document_type,
        section.confidence,
        layout,
        (section,),
        fields,
    )


def process_bundle(
    *,
    filename: str,
    content: bytes,
    role_hint: str | None = None,
) -> BundleResolution:
    """Resolve one immutable physical source into logical documents."""

    layout = parse_layout(filename, content)
    parent_type, _parent_confidence = classify(layout, role_hint)
    sections = segment(layout, parent_type)
    if not sections:
        raise LaceyEngineError(
            "Document segmentation produced no logical documents."
        )

    logical_documents: list[LogicalDocumentResolution] = []
    for index, section in enumerate(sections, start=1):
        logical_layout = _slice_layout(
            layout,
            page_start=section.page_start,
            page_end=section.page_end,
        )
        resolution = _resolve_logical_document(
            filename=filename,
            layout=logical_layout,
            section=section,
        )
        logical_documents.append(
            LogicalDocumentResolution(
                logical_document_id=f"logical-{index:03d}",
                parent_filename=filename,
                page_start=section.page_start,
                page_end=section.page_end,
                document_type=section.document_type,
                type_confidence=section.confidence,
                resolution=resolution,
            )
        )

    return BundleResolution(
        filename=filename,
        engine_version=ENGINE_VERSION,
        page_count=layout.page_count,
        documents=tuple(logical_documents),
    )


def process_document(
    *,
    filename: str,
    content: bytes,
    role_hint: str | None = None,
) -> DocumentResolution:
    """Compatibility seam for physical sources containing one logical document."""

    bundle = process_bundle(
        filename=filename,
        content=content,
        role_hint=role_hint,
    )
    if len(bundle.documents) != 1:
        raise LaceyEngineError(
            "Physical source contains multiple logical documents; "
            "caller must use process_bundle()."
        )
    return bundle.documents[0].resolution
