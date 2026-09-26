"""Conservative cross-document merchandise-line identity resolution.

Engine 2 line keys are document-local provenance identities.  They must not be
used as shipment-wide product identities because the same commercial line can
appear in an invoice, entry worksheet, packing list, and other documents with a
different local key in each source.

This module rewrites only *strongly* supported equivalence classes before the
canonical publication layer consumes a ShipmentResolution payload.  It fails
closed: ambiguous signatures remain separate and therefore continue to require
human review.
"""
from __future__ import annotations

from collections import defaultdict
from copy import deepcopy
import re
from typing import Mapping

from litoral_trace.us_lacey.ppq505 import PpqValidationStatus, normalize_hts


_MERCHANDISE_FIELDS = ("description", "hts_code", "entered_value")
_COMPONENT_FIELDS = (
    "article_component",
    "genus",
    "species",
    "country_of_harvest",
    "plant_quantity",
    "metric_unit",
    "percent_recycled",
)
_QUANTITATIVE_COMPONENT_FIELDS = frozenset({"plant_quantity", "metric_unit"})
_TAXON = re.compile(r"^taxon:([^:]+):([^:]+)$", re.IGNORECASE)
_EXPLICIT_SKU = re.compile(r"^SKU:[A-Z0-9][A-Z0-9._/-]*$", re.IGNORECASE)
_EXPLICIT_LINE = re.compile(r"^LINE:(\d{1,6})$", re.IGNORECASE)
_ROW = re.compile(r"^(?P<document>[^:]+):(?P<table>.+):row:(?P<row>\d+)$", re.IGNORECASE)
_HTS_SOURCE_VALUE = re.compile(
    r"^(?:\d{6,10}|\d{4}[.-]\d{2}(?:[.-]\d{2,4})?)$"
)
_PACKAGING_NOISE_TOKEN = re.compile(
    r"^(?:PAL(?:LET)?|BOX|CART(?:ON)?|AUX(?:-\d+)?|DUNNAGE|TRAY)$",
    re.IGNORECASE,
)
_PACKAGING_CONTEXT = re.compile(
    r"\b(?:pallet|packag(?:e|ing)?|dunnage|carton|box|corner\s+protector|auxiliary)\b",
    re.IGNORECASE,
)


def _candidate(row: Mapping) -> Mapping:
    value = row.get("candidate")
    return value if isinstance(value, Mapping) else {}


def _raw(row: Mapping) -> Mapping:
    value = _candidate(row).get("raw")
    return value if isinstance(value, Mapping) else {}


def _provenance(row: Mapping) -> Mapping:
    value = _candidate(row).get("provenance")
    return value if isinstance(value, Mapping) else {}


def _source_block(row: Mapping) -> Mapping:
    value = _raw(row).get("source_block")
    return value if isinstance(value, Mapping) else {}


def _rows(field_payload: object) -> list[dict]:
    if not isinstance(field_payload, Mapping):
        return []
    rows = field_payload.get("supporting_evidence")
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict)]


def _normalized(row: Mapping) -> str:
    return str(row.get("normalized_value") or _raw(row).get("normalized_value") or "").strip()


def _taxon_key(value: object) -> str | None:
    text = str(value or "").strip()
    match = _TAXON.fullmatch(text)
    if not match:
        return None
    return f"taxon:{match.group(1).casefold()}:{match.group(2).casefold()}"


def _line_document(line_key: str) -> str:
    return line_key.split(":", 1)[0]


def _line_ordinal(line_key: str) -> int | None:
    match = _ROW.fullmatch(line_key)
    return int(match.group("row")) if match else None


def _source_line_key(row: Mapping, known_line_keys: frozenset[str]) -> str | None:
    """Resolve a structured source cell back to its document-local line key."""
    block = _source_block(row)
    table_id = str(block.get("table_id") or "").strip()
    row_index = block.get("row_index")
    document_id = str(row.get("document_id") or "").strip()
    if not table_id or row_index is None or not document_id:
        return None
    try:
        candidate = f"{document_id}:{table_id}:row:{int(row_index)}"
    except (TypeError, ValueError):
        return None
    return candidate if candidate in known_line_keys else None


def _field_rows(fields: Mapping, field_name: str) -> list[dict]:
    return _rows(fields.get(field_name))


def _line_values(
    fields: Mapping,
    field_name: str,
    line_key: str,
) -> frozenset[str]:
    return frozenset(
        value
        for row in _field_rows(fields, field_name)
        if str(row.get("line_key") or "").strip() == line_key
        if (value := _normalized(row))
    )


def _hts_identity(value: str) -> str | None:
    """Return a canonical HTS only for a semantically valid source token.

    Source evidence must already look like an HTS/HTSUS value. This prevents SKU,
    packaging-code, or arbitrary alphanumeric cells from becoming line identity
    merely because they were emitted under an HTS-shaped field upstream.
    """

    text = str(value or "").strip()
    if not _HTS_SOURCE_VALUE.fullmatch(text):
        return None
    validation = normalize_hts(text)
    if validation.status is not PpqValidationStatus.VALID:
        return None
    return validation.normalized_value


def _is_packaging_noise_row(row: Mapping) -> bool:
    """Reject explicit package/admin rows before merchandise-line binding.

    Exact row/code tokens such as PAL, BOX, CART, or TRAY are noise. For free
    row text we additionally require packaging context so legitimate products
    such as "Acacia serving tray" are never discarded merely because they
    contain the word "tray".
    """

    block = _source_block(row)
    direct_values = (
        _normalized(row),
        str(block.get("value_text") or "").strip(),
        str(block.get("key_text") or "").strip(),
    )
    if any(_PACKAGING_NOISE_TOKEN.fullmatch(value) for value in direct_values if value):
        return True

    row_text = " ".join(
        str(value or "").strip()
        for value in (
            block.get("text"),
            _provenance(row).get("source_text"),
        )
        if str(value or "").strip()
    )
    if not row_text or not _PACKAGING_CONTEXT.search(row_text):
        return False
    first_token = re.split(r"\s+", row_text.strip(), maxsplit=1)[0].strip(":-")
    return bool(_PACKAGING_NOISE_TOKEN.fullmatch(first_token))


def _replace_supporting_rows(field_payload: dict, rows: list[dict]) -> None:
    field_payload["supporting_evidence"] = rows
    values = list(dict.fromkeys(_normalized(row) for row in rows if _normalized(row)))
    field_payload["values"] = [
        {"value": value, "evidence_ids": []}
        for value in values
    ]
    field_payload["state"] = (
        "MISSING"
        if not values
        else ("SUPPORTED" if len(values) == 1 else "SUPPORTED_MULTIPLE")
    )


def _filter_typed_merchandise_rows(fields: dict) -> None:
    """Apply type and row-domain guards before deriving shipment line keys."""

    for field_name in _MERCHANDISE_FIELDS:
        field_payload = fields.get(field_name)
        if not isinstance(field_payload, dict):
            continue
        rows = _field_rows(fields, field_name)
        filtered = [
            row
            for row in rows
            if not _is_packaging_noise_row(row)
            and (
                field_name != "hts_code"
                or _hts_identity(_normalized(row)) is not None
            )
        ]
        if len(filtered) != len(rows):
            _replace_supporting_rows(field_payload, filtered)


def _taxon_tokens(taxon: str) -> tuple[str, str] | None:
    match = _TAXON.fullmatch(taxon)
    if not match:
        return None
    return match.group(1).casefold(), match.group(2).casefold()


def _text_contains_taxon(text: object, taxon: str) -> bool:
    parts = _taxon_tokens(taxon)
    if parts is None:
        return False
    normalized = " ".join(
        re.sub(r"[^a-z0-9]+", " ", str(text or "").casefold()).split()
    )
    genus, species = parts
    return bool(
        re.search(rf"\b{re.escape(genus)}\b", normalized)
        and re.search(rf"\b{re.escape(species)}\b", normalized)
    )


def _known_component_taxa(fields: Mapping) -> frozenset[str]:
    return frozenset(
        taxon
        for field_name in ("genus", "species")
        for row in _field_rows(fields, field_name)
        if (taxon := _taxon_key(row.get("component_key"))) is not None
    )


def _anchored_taxa(fields: Mapping, known_line_keys: frozenset[str]) -> dict[str, frozenset[str]]:
    taxa: dict[str, set[str]] = defaultdict(set)
    for field_name in ("genus", "species"):
        for row in _field_rows(fields, field_name):
            taxon = _taxon_key(row.get("component_key"))
            if taxon is None:
                continue
            source_line = _source_line_key(row, known_line_keys)
            if source_line is not None:
                taxa[source_line].add(taxon)

    # A commercial description may contribute to the strong signature only when
    # it literally contains exactly one taxon already observed elsewhere in the
    # shipment. This is exact entity resolution, not taxonomy inference.
    known_taxa = _known_component_taxa(fields)
    if known_taxa:
        for line_key in known_line_keys:
            description_rows = [
                row
                for row in _field_rows(fields, "description")
                if str(row.get("line_key") or "").strip() == line_key
            ]
            matches = {
                taxon
                for row in description_rows
                for taxon in known_taxa
                if _text_contains_taxon(
                    " ".join(
                        value
                        for value in (
                            _normalized(row),
                            _provenance(row).get("source_text"),
                        )
                        if value
                    ),
                    taxon,
                )
            }
            if len(matches) == 1:
                taxa[line_key].update(matches)

    return {key: frozenset(values) for key, values in taxa.items()}


def _strong_signature(
    *,
    fields: Mapping,
    line_key: str,
    taxa_by_line: Mapping[str, frozenset[str]],
) -> tuple[str, str] | None:
    """Return an exact canonical HTS+taxon identity only when unambiguous."""
    hts_values = _line_values(fields, "hts_code", line_key)
    taxa = taxa_by_line.get(line_key, frozenset())
    if len(hts_values) != 1 or len(taxa) != 1:
        return None
    hts = _hts_identity(next(iter(hts_values)))
    if hts is None:
        return None
    return hts, next(iter(taxa))


def _equivalence_groups(
    *,
    fields: Mapping,
    line_keys: tuple[str, ...],
    taxa_by_line: Mapping[str, frozenset[str]],
) -> tuple[dict[str, str], dict[str, frozenset[str]], dict[str, str]]:
    """Return member->representative, representative->members and rep->taxon.

    A signature is collapsible only when each contributing document contributes
    at most one row.  This prevents two same-HTS/same-species rows in a single
    document from being silently fused.
    """
    by_signature: dict[tuple[str, str], list[str]] = defaultdict(list)
    for line_key in line_keys:
        signature = _strong_signature(
            fields=fields,
            line_key=line_key,
            taxa_by_line=taxa_by_line,
        )
        if signature is not None:
            by_signature[signature].append(line_key)

    member_to_rep: dict[str, str] = {key: key for key in line_keys}
    rep_to_members: dict[str, frozenset[str]] = {key: frozenset({key}) for key in line_keys}
    rep_to_taxon: dict[str, str] = {}

    for (_, taxon), members in by_signature.items():
        if len(members) < 2:
            if len(members) == 1:
                rep_to_taxon[members[0]] = taxon
            continue
        documents = [_line_document(member) for member in members]
        if len(documents) != len(set(documents)):
            # Ambiguous within one source document: fail closed.
            continue
        ordered = sorted(
            members,
            key=lambda key: (
                _line_ordinal(key) is None,
                _line_ordinal(key) or 10**9,
                key,
            ),
        )
        representative = ordered[0]
        member_set = frozenset(ordered)
        for member in ordered:
            member_to_rep[member] = representative
            if member != representative:
                rep_to_members.pop(member, None)
        rep_to_members[representative] = member_set
        rep_to_taxon[representative] = taxon

    # Preserve exact taxon identity for non-collapsed, otherwise unambiguous lines.
    for line_key in line_keys:
        representative = member_to_rep[line_key]
        if representative in rep_to_taxon:
            continue
        taxa = taxa_by_line.get(line_key, frozenset())
        if len(taxa) == 1:
            rep_to_taxon[representative] = next(iter(taxa))

    return member_to_rep, rep_to_members, rep_to_taxon


def _taxon_to_unique_rep(rep_to_taxon: Mapping[str, str]) -> dict[str, str]:
    reps_by_taxon: dict[str, list[str]] = defaultdict(list)
    for representative, taxon in rep_to_taxon.items():
        reps_by_taxon[taxon].append(representative)
    return {
        taxon: reps[0]
        for taxon, reps in reps_by_taxon.items()
        if len(reps) == 1
    }


def _line_documents(
    fields: Mapping,
    known_line_keys: frozenset[str],
) -> dict[str, frozenset[str]]:
    """Return source-document ownership for each merchandise line identity."""
    documents: dict[str, set[str]] = defaultdict(set)
    for field_name in _MERCHANDISE_FIELDS:
        for row in _field_rows(fields, field_name):
            line_key = str(row.get("line_key") or "").strip()
            document_id = str(row.get("document_id") or "").strip()
            if line_key in known_line_keys and document_id:
                documents[line_key].add(document_id)
    return {key: frozenset(values) for key, values in documents.items()}


def _line_signatures(
    *,
    fields: Mapping,
    line_keys: tuple[str, ...],
    taxa_by_line: Mapping[str, frozenset[str]],
) -> dict[str, tuple[str, str]]:
    """Freeze strong source-line signatures before any derived rewrite occurs."""
    signatures: dict[str, tuple[str, str]] = {}
    for line_key in line_keys:
        signature = _strong_signature(
            fields=fields,
            line_key=line_key,
            taxa_by_line=taxa_by_line,
        )
        if signature is not None:
            signatures[line_key] = signature
    return signatures


def _signature_to_unique_rep(
    *,
    line_signatures: Mapping[str, tuple[str, str]],
    member_to_rep: Mapping[str, str],
) -> dict[tuple[str, str], str]:
    reps_by_signature: dict[tuple[str, str], set[str]] = defaultdict(set)
    for line_key, signature in line_signatures.items():
        reps_by_signature[signature].add(member_to_rep[line_key])
    return {
        signature: next(iter(representatives))
        for signature, representatives in reps_by_signature.items()
        if len(representatives) == 1
    }


def _explicit_quantitative_rep(
    row: Mapping,
    *,
    known_line_keys: frozenset[str],
    member_to_rep: Mapping[str, str],
    line_documents: Mapping[str, frozenset[str]],
) -> str | None:
    """Resolve only explicit SKU or document-local line-number identities."""
    document_id = str(row.get("document_id") or "").strip()
    for raw in (row.get("line_key"), row.get("component_key")):
        identity = str(raw or "").strip()
        if not identity or identity not in known_line_keys:
            continue
        if _EXPLICIT_SKU.fullmatch(identity):
            return member_to_rep.get(identity)
        if _EXPLICIT_LINE.fullmatch(identity):
            owners = line_documents.get(identity, frozenset())
            if owners == frozenset({document_id}):
                return member_to_rep.get(identity)
    return None


def _strong_signature_quantitative_rep(
    row: Mapping,
    *,
    known_line_keys: frozenset[str],
    line_signatures: Mapping[str, tuple[str, str]],
    member_to_rep: Mapping[str, str],
    signature_to_rep: Mapping[tuple[str, str], str],
) -> str | None:
    """Resolve a quantitative row only through its frozen source-row signature."""
    source_line = _source_line_key(row, known_line_keys)
    if source_line is None:
        return None
    signature = line_signatures.get(source_line)
    if signature is None:
        return None
    representative = signature_to_rep.get(signature)
    if representative is None:
        return None
    return representative if member_to_rep[source_line] == representative else None


def _quantitative_rep(
    row: Mapping,
    *,
    known_line_keys: frozenset[str],
    line_signatures: Mapping[str, tuple[str, str]],
    member_to_rep: Mapping[str, str],
    signature_to_rep: Mapping[tuple[str, str], str],
    line_documents: Mapping[str, frozenset[str]],
) -> str | None:
    explicit = _explicit_quantitative_rep(
        row,
        known_line_keys=known_line_keys,
        member_to_rep=member_to_rep,
        line_documents=line_documents,
    )
    if explicit is not None:
        return explicit
    return _strong_signature_quantitative_rep(
        row,
        known_line_keys=known_line_keys,
        line_signatures=line_signatures,
        member_to_rep=member_to_rep,
        signature_to_rep=signature_to_rep,
    )


def _row_rep(
    row: Mapping,
    *,
    known_line_keys: frozenset[str],
    member_to_rep: Mapping[str, str],
    taxon_to_rep: Mapping[str, str],
) -> str | None:
    direct = str(row.get("component_key") or "").strip()
    if direct in member_to_rep:
        return member_to_rep[direct]

    source_line = _source_line_key(row, known_line_keys)
    if source_line is not None:
        return member_to_rep[source_line]

    taxon = _taxon_key(direct)
    if taxon is not None:
        return taxon_to_rep.get(taxon)
    return None


def _description_candidate(
    row: Mapping,
    *,
    representative: str,
) -> tuple[float, float, str, dict] | None:
    block = _source_block(row)
    label = str(block.get("key_text") or block.get("table_header") or "").strip().casefold()
    if label != "description":
        return None
    value = str(block.get("value_text") or "").strip()
    if not value:
        return None

    cloned = deepcopy(dict(row))
    cloned["field_key"] = "description"
    cloned["normalized_value"] = value
    cloned["line_key"] = representative
    cloned["component_key"] = None
    original_id = str(cloned.get("candidate_id") or "")
    cloned["candidate_id"] = f"line-identity-description:{original_id}"
    candidate = cloned.setdefault("candidate", {})
    raw = candidate.setdefault("raw", {})
    raw["field_key"] = "description"
    raw["raw_text"] = value
    raw["normalized_value"] = value
    provenance = candidate.setdefault("provenance", {})
    provenance["source_text"] = str(block.get("text") or value)

    authority = float(cloned.get("source_authority") or 0.0)
    score = float(cloned.get("candidate_score") or candidate.get("score") or 0.0)
    return authority, score, original_id, cloned


def _synthesized_descriptions(
    *,
    fields: Mapping,
    known_line_keys: frozenset[str],
    member_to_rep: Mapping[str, str],
    taxon_to_rep: Mapping[str, str],
) -> list[dict]:
    """Recover one explicit structured Description cell per canonical product.

    This is used only when Engine 2's shipment-level description aggregate has no
    evidence.  Choosing one best explicit source cell per product prevents normal
    multilingual/document wording differences from becoming false product-line
    conflicts while retaining provenance.
    """
    best: dict[str, tuple[float, float, str, dict]] = {}
    for field_name in ("genus", "species"):
        for row in _field_rows(fields, field_name):
            representative = _row_rep(
                row,
                known_line_keys=known_line_keys,
                member_to_rep=member_to_rep,
                taxon_to_rep=taxon_to_rep,
            )
            if representative is None:
                continue
            candidate = _description_candidate(row, representative=representative)
            if candidate is None:
                continue
            current = best.get(representative)
            if current is None or candidate[:3] > current[:3]:
                best[representative] = candidate
    return [
        best[key][3]
        for key in sorted(
            best,
            key=lambda value: (
                _line_ordinal(value) is None,
                _line_ordinal(value) or 10**9,
                value,
            ),
        )
    ]


def _rewrite_field_rows(
    field_payload: object,
    *,
    field_name: str,
    fields: Mapping,
    known_line_keys: frozenset[str],
    taxa_by_line: Mapping[str, frozenset[str]],
    line_signatures: Mapping[str, tuple[str, str]],
    member_to_rep: Mapping[str, str],
    rep_to_taxon: Mapping[str, str],
    taxon_to_rep: Mapping[str, str],
    signature_to_rep: Mapping[tuple[str, str], str],
    line_documents: Mapping[str, frozenset[str]],
) -> None:
    if not isinstance(field_payload, dict):
        return
    for row in _rows(field_payload):
        line_key = str(row.get("line_key") or "").strip()
        if line_key in member_to_rep:
            row["line_key"] = member_to_rep[line_key]

        if field_name == "hts_code":
            hts = _hts_identity(_normalized(row))
            if hts is not None:
                # Only the derived canonical view is normalized. Candidate/raw source
                # evidence remains byte-for-byte as extracted for audit/provenance.
                row["normalized_value"] = hts

        if field_name not in _COMPONENT_FIELDS:
            continue

        if field_name in _QUANTITATIVE_COMPONENT_FIELDS:
            representative = _quantitative_rep(
                row,
                known_line_keys=known_line_keys,
                line_signatures=line_signatures,
                member_to_rep=member_to_rep,
                signature_to_rep=signature_to_rep,
                line_documents=line_documents,
            )
            if representative is None:
                continue
            # This is a derived canonical binding proof only. Candidate/raw
            # evidence retains its original component/source identities.
            row["line_key"] = representative
            taxon = rep_to_taxon.get(representative)
            if taxon is not None:
                row["component_key"] = taxon
            continue

        representative = _row_rep(
            row,
            known_line_keys=known_line_keys,
            member_to_rep=member_to_rep,
            taxon_to_rep=taxon_to_rep,
        )
        if representative is None:
            continue
        taxon = rep_to_taxon.get(representative)
        if taxon is not None:
            row["component_key"] = taxon


def _set_description_field(field_payload: dict, rows: list[dict]) -> None:
    values = list(dict.fromkeys(_normalized(row) for row in rows if _normalized(row)))
    field_payload["field_key"] = "description"
    field_payload["state"] = (
        "MISSING"
        if not values
        else ("SUPPORTED" if len(values) == 1 else "SUPPORTED_MULTIPLE")
    )
    field_payload["values"] = [
        {"value": value, "evidence_ids": []}
        for value in values
    ]
    field_payload["supporting_evidence"] = rows


def _promote_exact_cross_document_consensus(fields: dict) -> None:
    """Promote aggregate state when every scoped value has independent support.

    This does not merge or invent evidence. It only removes an upstream review
    state when the same normalized value is repeated by at least two distinct
    physical/logical documents for each line/component scope represented.
    """

    for field_payload in fields.values():
        if not isinstance(field_payload, dict):
            continue
        rows = _rows(field_payload)
        if not rows:
            continue

        grouped: dict[tuple[str, str], list[dict]] = defaultdict(list)
        for row in rows:
            scope_key = (
                str(row.get("line_key") or "").strip(),
                str(row.get("component_key") or "").strip(),
            )
            grouped[scope_key].append(row)

        if not grouped:
            continue

        independently_supported = True
        for scoped_rows in grouped.values():
            values = {
                _normalized(row)
                for row in scoped_rows
                if _normalized(row)
            }
            documents = {
                str(row.get("document_id") or "").strip()
                for row in scoped_rows
                if str(row.get("document_id") or "").strip()
            }
            if len(values) != 1 or len(documents) < 2:
                independently_supported = False
                break

        if independently_supported:
            field_payload["state"] = "SUPPORTED_MULTIPLE"


def reconcile_cross_document_line_identity(payload: Mapping) -> dict:
    """Return a shipment payload with only strongly equivalent rows collapsed.

    The input is never mutated.  If the payload lacks the evidence required to
    establish exact HTS+taxon equivalence, the corresponding line keys are left
    untouched so downstream canonical publication remains fail-closed.
    """
    rewritten = deepcopy(dict(payload))
    fields = rewritten.get("canonical_fields")
    if not isinstance(fields, dict):
        return rewritten

    _filter_typed_merchandise_rows(fields)

    line_keys = tuple(
        sorted(
            {
                str(row.get("line_key")).strip()
                for field_name in _MERCHANDISE_FIELDS
                for row in _field_rows(fields, field_name)
                if row.get("line_key")
            },
            key=lambda value: (
                _line_ordinal(value) is None,
                _line_ordinal(value) or 10**9,
                value,
            ),
        )
    )
    if not line_keys:
        return rewritten

    known_line_keys = frozenset(line_keys)
    taxa_by_line = _anchored_taxa(fields, known_line_keys)
    member_to_rep, rep_to_members, rep_to_taxon = _equivalence_groups(
        fields=fields,
        line_keys=line_keys,
        taxa_by_line=taxa_by_line,
    )
    taxon_to_rep = _taxon_to_unique_rep(rep_to_taxon)
    line_signatures = _line_signatures(
        fields=fields,
        line_keys=line_keys,
        taxa_by_line=taxa_by_line,
    )
    signature_to_rep = _signature_to_unique_rep(
        line_signatures=line_signatures,
        member_to_rep=member_to_rep,
    )
    line_documents = _line_documents(fields, known_line_keys)

    description_payload = fields.get("description")
    original_description_rows = _field_rows(fields, "description")
    has_collapsed_equivalence = any(
        len(members) > 1 for members in rep_to_members.values()
    )
    synthesize_description = has_collapsed_equivalence and not original_description_rows
    synthesized = (
        _synthesized_descriptions(
            fields=fields,
            known_line_keys=known_line_keys,
            member_to_rep=member_to_rep,
            taxon_to_rep=taxon_to_rep,
        )
        if synthesize_description
        else []
    )

    for field_name, field_payload in fields.items():
        _rewrite_field_rows(
            field_payload,
            field_name=str(field_name),
            fields=fields,
            known_line_keys=known_line_keys,
            taxa_by_line=taxa_by_line,
            line_signatures=line_signatures,
            member_to_rep=member_to_rep,
            rep_to_taxon=rep_to_taxon,
            taxon_to_rep=taxon_to_rep,
            signature_to_rep=signature_to_rep,
            line_documents=line_documents,
        )

    if synthesize_description and synthesized:
        if not isinstance(description_payload, dict):
            description_payload = {}
            fields["description"] = description_payload
        _set_description_field(description_payload, synthesized)

    _promote_exact_cross_document_consensus(fields)

    return rewritten
