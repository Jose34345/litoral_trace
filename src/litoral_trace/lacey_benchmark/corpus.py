"""Versioned synthetic corpus materialization for the U.S. Lacey benchmark."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from litoral_trace.lacey_benchmark.contracts import (
    FieldTruth,
    FieldTruthCorpus,
    RouterCorpusDocument,
    RouterCorpusManifest,
    RouterCorpusVariant,
)
from litoral_trace.lacey_engine.multi_agent.contracts import DocumentType


_REPO_ROOT = Path(__file__).resolve().parents[3]


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def load_router_corpus_manifest(path: Path) -> RouterCorpusManifest:
    payload = _read_json(Path(path))
    variants = tuple(
        RouterCorpusVariant(
            variant_id=str(item["variant_id"]),
            language=str(item["language"]),
            modality=str(item["modality"]),
            transform=str(item["transform"]),
        )
        for item in payload["variants"]
    )
    manifest = RouterCorpusManifest(
        version=str(payload["version"]),
        corpus_kind=str(payload["corpus_kind"]),
        source_fixture=str(payload["source_fixture"]),
        documents_per_variant=int(payload["documents_per_variant"]),
        document_count=int(payload["document_count"]),
        variants=variants,
    )
    if manifest.corpus_kind != "synthetic_regression":
        raise ValueError("Router corpus v1 must remain explicitly synthetic.")
    if manifest.document_count != manifest.documents_per_variant * len(manifest.variants):
        raise ValueError("Router corpus manifest count does not match its variants.")
    return manifest


_LOCALIZED_SIGNALS: dict[str, dict[DocumentType, tuple[str, ...]]] = {
    "en": {
        DocumentType.COMMERCIAL_INVOICE: (
            "COMMERCIAL INVOICE",
            "Invoice Number MHW-INV-260915-77",
            "Commercial Line Items",
            "Entered Value USD 47,860.00",
        ),
        DocumentType.PACKING_LIST: (
            "PACKING LIST",
            "Package Detail",
            "Cartons Pieces",
            "Net Wt. Gross Wt.",
        ),
        DocumentType.BILL_OF_LADING: (
            "BILL OF LADING",
            "Port of Loading Cat Lai",
            "Port of Discharge Savannah",
            "Shipper Consignee",
        ),
        DocumentType.ENTRY_WORKSHEET: (
            "U.S. ENTRY WORKSHEET",
            "Entry / Filing Reference 123-4567890-1",
            "Entry Summary Lines",
            "Importer Number 88-7654321",
        ),
        DocumentType.BOTANICAL_DECLARATION: (
            "BOTANICAL LACEY DECLARATION",
            "Genus Species",
            "Country of Harvest",
            "Plant Quantity",
        ),
        DocumentType.SUPPLIER_ORIGIN: (
            "SUPPLIER MATERIAL ORIGIN STATEMENT",
            "Statement of Material Origin",
            "Scientific Name",
            "Harvest Country",
        ),
        DocumentType.ARRIVAL_NOTICE: (
            "ARRIVAL NOTICE",
            "Carrier Reference OOLU-TEST-260912-01",
            "Availability subject to customs release",
            "ETA October 3, 2026",
        ),
    },
    "es": {
        DocumentType.COMMERCIAL_INVOICE: (
            "FACTURA COMERCIAL",
            "Numero de factura MHW-INV-260915-77",
            "Partidas comerciales",
            "Valor declarado USD 47,860.00",
        ),
        DocumentType.PACKING_LIST: (
            "LISTA DE EMPAQUE",
            "Detalle de bultos",
            "Cajas Piezas",
            "Peso neto Peso bruto",
        ),
        DocumentType.BILL_OF_LADING: (
            "CONOCIMIENTO DE EMBARQUE",
            "Puerto de carga Cat Lai",
            "Puerto de descarga Savannah",
            "Cargador Consignatario",
        ),
        DocumentType.ENTRY_WORKSHEET: (
            "HOJA DE TRABAJO DE ENTRADA",
            "Referencia de entrada / presentacion 123-4567890-1",
            "Lineas de resumen de entrada",
            "Numero de importador 88-7654321",
        ),
        DocumentType.BOTANICAL_DECLARATION: (
            "DECLARACION BOTANICA",
            "Genero Especie",
            "Pais de cosecha",
            "Cantidad de material vegetal",
        ),
        DocumentType.SUPPLIER_ORIGIN: (
            "DECLARACION DE ORIGEN DEL PROVEEDOR",
            "Declaracion de origen del material",
            "Nombre cientifico",
            "Pais de cosecha",
        ),
        DocumentType.ARRIVAL_NOTICE: (
            "AVISO DE LLEGADA",
            "Referencia del transportista OOLU-TEST-260912-01",
            "Disponibilidad sujeta a liberacion aduanera",
            "ETA 3 de octubre de 2026",
        ),
    },
    "pt": {
        DocumentType.COMMERCIAL_INVOICE: (
            "FATURA COMERCIAL",
            "Numero da fatura MHW-INV-260915-77",
            "Itens comerciais",
            "Valor aduaneiro USD 47,860.00",
        ),
        DocumentType.PACKING_LIST: (
            "LISTA DE EMBALAGEM",
            "Detalhe dos volumes",
            "Caixas Pecas",
            "Peso liquido Peso bruto",
        ),
        DocumentType.BILL_OF_LADING: (
            "CONHECIMENTO DE EMBARQUE",
            "Porto de embarque Cat Lai",
            "Porto de descarga Savannah",
            "Embarcador Consignatario",
        ),
        DocumentType.ENTRY_WORKSHEET: (
            "PLANILHA DE ENTRADA",
            "Referencia de entrada / registro 123-4567890-1",
            "Linhas do resumo de entrada",
            "Numero do importador 88-7654321",
        ),
        DocumentType.BOTANICAL_DECLARATION: (
            "DECLARACAO BOTANICA",
            "Genero Especie",
            "Pais de colheita",
            "Quantidade de material vegetal",
        ),
        DocumentType.SUPPLIER_ORIGIN: (
            "DECLARACAO DE ORIGEM DO FORNECEDOR",
            "Declaracao de origem do material",
            "Nome cientifico",
            "Pais de colheita",
        ),
        DocumentType.ARRIVAL_NOTICE: (
            "AVISO DE CHEGADA",
            "Referencia do transportador OOLU-TEST-260912-01",
            "Disponibilidade sujeita a liberacao aduaneira",
            "ETA 3 de outubro de 2026",
        ),
    },
}


def _render_variant_text(
    *,
    expected_type: DocumentType,
    language: str,
    transform: str,
    source_text: str,
) -> str:
    try:
        signals = _LOCALIZED_SIGNALS[language][expected_type]
    except KeyError as exc:
        raise ValueError(
            f"Unsupported corpus localization: language={language} type={expected_type.value}"
        ) from exc

    source_tail = " ".join(source_text.split())[-360:]
    text = "\n".join(
        (
            "SYNTHETIC GOLDEN BENCHMARK DOCUMENT",
            *signals,
            "Shipment LT-TEST-2026-0912-A",
            source_tail,
        )
    )

    if transform == "identity":
        return text
    if transform == "whitespace":
        return "\n\n  ".join(line for line in text.splitlines())
    if transform == "uppercase_header":
        lines = text.splitlines()
        if len(lines) > 1:
            lines[1] = lines[1].upper()
        return "\n".join(lines)
    if transform == "ocr_layout":
        # Simulate layout fragmentation without corrupting the semantic label itself.
        return text.replace("\n", "\n  \n").replace("  ", "   ")
    if transform == "ocr_punctuation":
        # Simulate punctuation loss commonly seen in OCR while preserving word tokens.
        return text.replace(":", " ").replace(",", "").replace(".", " ")
    raise ValueError(f"Unsupported router corpus transform: {transform}")


def materialize_router_corpus(
    manifest: RouterCorpusManifest,
) -> tuple[RouterCorpusDocument, ...]:
    source_path = _REPO_ROOT / manifest.source_fixture
    source = _read_json(source_path)
    documents = source.get("documents", [])
    if len(documents) != manifest.documents_per_variant:
        raise ValueError(
            "Source fixture document count does not match manifest documents_per_variant."
        )

    materialized: list[RouterCorpusDocument] = []
    for variant in manifest.variants:
        for ordinal, item in enumerate(documents, start=1):
            expected_type = DocumentType(str(item["expected_type"]))
            source_pages = {
                int(page): str(text)
                for page, text in dict(item["pages"]).items()
            }
            rendered_pages = {
                page: _render_variant_text(
                    expected_type=expected_type,
                    language=variant.language,
                    transform=variant.transform,
                    source_text=text,
                )
                for page, text in source_pages.items()
            }
            stem = Path(str(item["filename"])).stem
            materialized.append(
                RouterCorpusDocument(
                    document_id=f"{manifest.version}:{variant.variant_id}:{ordinal:02d}",
                    filename=f"{stem}__{variant.variant_id}.pdf",
                    expected_type=expected_type,
                    language=variant.language,
                    modality=variant.modality,
                    pages=rendered_pages,
                )
            )

    if len(materialized) != manifest.document_count:
        raise ValueError("Materialized router corpus count does not match manifest.")
    return tuple(materialized)


def _normalize_span(value: str) -> str:
    return " ".join(str(value or "").split())


def load_field_truth(path: Path) -> FieldTruthCorpus:
    payload = _read_json(Path(path))
    source_fixture = str(payload["source_fixture"])
    source = _read_json(_REPO_ROOT / source_fixture)
    source_by_filename = {
        str(item["filename"]): item
        for item in source.get("documents", [])
    }

    fields: list[FieldTruth] = []
    for raw in payload["fields"]:
        field = FieldTruth(
            field_key=str(raw["field_key"]),
            value=str(raw["value"]),
            document_id=str(raw["document_id"]),
            page=int(raw["page"]),
            line_item_key=(
                None
                if raw.get("line_item_key") is None
                else str(raw["line_item_key"])
            ),
            evidence_text=str(raw["evidence_text"]),
        )

        document = source_by_filename.get(field.document_id)
        if document is None:
            raise ValueError(
                f"Field truth references unknown source document: {field.document_id}"
            )
        page_text = dict(document["pages"]).get(str(field.page))
        if page_text is None:
            raise ValueError(
                f"Field truth references missing page {field.page}: {field.document_id}"
            )
        if _normalize_span(field.evidence_text) not in _normalize_span(str(page_text)):
            raise ValueError(
                f"Field truth evidence is not present in source: "
                f"{field.document_id} page {field.page} {field.field_key}"
            )
        fields.append(field)

    return FieldTruthCorpus(
        version=str(payload["version"]),
        source_fixture=source_fixture,
        fields=tuple(fields),
    )
