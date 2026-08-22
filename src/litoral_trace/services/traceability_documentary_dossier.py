"""Buyer dossier enriched with the Litoral Trace documentary footprint.

P1E remains the stable genealogy/origin projection. UX10-E composes on top of
that contract: evidence is selected from the exact P1C graph, canonicalized as
buyer-safe references, included in the manifest integrity hash and rendered in
the PDF. Private Vault binaries are deliberately not embedded in the ZIP.
"""
from __future__ import annotations

from datetime import datetime, timezone
import io
import json
import re
from typing import Any
import zipfile

from litoral_trace.services.traceability_dossier import (
    DISCLAIMER,
    INTEGRITY_ALGORITHM,
    OriginDossierBundle,
    OriginDossierError,
    OriginDossierGenerationError,
    OriginDossierValidationError,
    build_canonical_manifest,
    build_geojson,
    manifest_sha256,
)


DOCUMENTARY_EXTENSION_SCHEMA = "litoral-trace.documentary-evidence.v1"
DOCUMENTARY_EXTENSION_NAME = "Huella Documental Litoral Trace"

_SUBJECT_LABELS = {
    "SOURCE_LOTE": "Origen",
    "TRACEABILITY_EVENT": "Movimiento",
    "TRACEABILITY_BATCH": "Lote industrial",
    "SHIPMENT": "Despacho",
}

_EVIDENCE_LABELS = {
    "ORIGIN_AUTHORIZATION": "Autorización de origen",
    "FOREST_GUIDE": "Guía forestal",
    "REMITO": "Remito",
    "INVOICE": "Factura / documento comercial",
    "CERTIFICATE": "Certificado",
    "TRANSPORT": "Documento de transporte",
    "GEOSPATIAL": "Evidencia geoespacial",
    "SUPPLIER_DECLARATION": "Declaración de proveedor",
    "OTHER": "Otra evidencia",
}


def _text(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _pretty_json_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def _clean_documentary_evidence(items: Any) -> list[dict[str, Any]]:
    """Canonicalize buyer-safe evidence and fail closed on invalid hashes."""

    allowed_subjects = frozenset(_SUBJECT_LABELS)
    cleaned: list[dict[str, Any]] = []

    for raw in items or []:
        item = raw or {}
        document = item.get("document") or {}
        subject_type = (_text(item.get("subject_type")) or "").upper()
        subject_reference = _text(item.get("subject_reference"))
        filename = _text(document.get("filename"))
        sha256 = (_text(document.get("sha256")) or "").lower()

        if subject_type not in allowed_subjects or not subject_reference:
            raise OriginDossierValidationError(
                "DOSSIER_EVIDENCE_SUBJECT_INVALID",
                "La evidencia documental contiene un eslabón no válido.",
            )
        if not filename or not re.fullmatch(r"[0-9a-f]{64}", sha256):
            raise OriginDossierValidationError(
                "DOSSIER_EVIDENCE_INTEGRITY_INVALID",
                "La evidencia documental no contiene un SHA-256 válido.",
            )

        size_bytes = document.get("size_bytes")
        try:
            size_bytes = int(size_bytes) if size_bytes is not None else None
        except (TypeError, ValueError) as exc:
            raise OriginDossierValidationError(
                "DOSSIER_EVIDENCE_SIZE_INVALID",
                "La evidencia documental contiene un tamaño inválido.",
            ) from exc
        if size_bytes is not None and size_bytes < 0:
            raise OriginDossierValidationError(
                "DOSSIER_EVIDENCE_SIZE_INVALID",
                "La evidencia documental contiene un tamaño inválido.",
            )

        # Deliberately omit operational notes and internal database identifiers.
        cleaned.append(
            {
                "subject_type": subject_type,
                "subject_reference": subject_reference,
                "evidence_type": _text(item.get("evidence_type")),
                "reference_number": _text(item.get("reference_number")),
                "issuer": _text(item.get("issuer")),
                "document_date": _text(item.get("document_date")),
                "valid_from": _text(item.get("valid_from")),
                "valid_until": _text(item.get("valid_until")),
                "document": {
                    "public_id": _text(document.get("public_id")),
                    "filename": filename,
                    "content_type": _text(document.get("content_type")),
                    "size_bytes": size_bytes,
                    "sha256": sha256,
                },
            }
        )

    cleaned.sort(
        key=lambda item: (
            item.get("subject_type") or "",
            item.get("subject_reference") or "",
            item.get("evidence_type") or "",
            (item.get("document") or {}).get("sha256") or "",
            (item.get("document") or {}).get("filename") or "",
        )
    )
    return cleaned


def _documentary_summary(items: list[dict[str, Any]]) -> dict[str, Any]:
    by_subject_type: dict[str, int] = {
        subject_type: 0 for subject_type in _SUBJECT_LABELS
    }
    unique_subjects: set[tuple[str, str]] = set()
    unique_documents: set[str] = set()

    for item in items:
        subject_type = str(item["subject_type"])
        by_subject_type[subject_type] += 1
        unique_subjects.add((subject_type, str(item["subject_reference"])))
        document = item.get("document") or {}
        if document.get("sha256"):
            unique_documents.add(str(document["sha256"]))

    return {
        "linked_evidence_count": len(items),
        "subjects_with_evidence": len(unique_subjects),
        "unique_document_hashes": len(unique_documents),
        "by_subject_type": by_subject_type,
        "statement": (
            "La Huella Documental Litoral Trace representa evidencia vinculada "
            "a los eslabones de esta genealogía. No constituye por sí sola una "
            "certificación ni una conclusión automática de cumplimiento regulatorio."
        ),
    }


def build_documentary_manifest(
    payload: dict[str, Any],
    *,
    documentary_evidence: Any = None,
) -> dict[str, Any]:
    """Extend the stable P1E manifest without changing the P1C lineage contract."""

    manifest = build_canonical_manifest(payload)
    evidence = _clean_documentary_evidence(documentary_evidence)
    manifest["extensions"] = {
        "documentary_evidence": {
            "schema_version": DOCUMENTARY_EXTENSION_SCHEMA,
            "name": DOCUMENTARY_EXTENSION_NAME,
        }
    }
    manifest["documentary_evidence_summary"] = _documentary_summary(evidence)
    manifest["documentary_evidence"] = evidence
    return manifest


def _pdf_safe(value: Any) -> str:
    text = "—" if value is None else str(value)
    return text.encode("latin-1", errors="replace").decode("latin-1")


def _unit_label(unit: Any) -> str:
    return {
        "M3": "m3",
        "TON": "t",
        "KG": "kg",
    }.get(str(unit or "").upper(), _pdf_safe(unit))


def build_documentary_pdf(manifest: dict[str, Any], digest: str) -> bytes:
    """Render one buyer-facing narrative: volume, origins, process and evidence."""

    try:
        from fpdf import FPDF

        class DocumentaryDossierPDF(FPDF):
            def footer(self) -> None:
                self.set_y(-14)
                self.set_x(self.l_margin)
                self.set_font("Helvetica", "", 7)
                self.set_text_color(90, 90, 90)
                self.cell(
                    self.epw,
                    4,
                    _pdf_safe(
                        f"Litoral Trace | SHA-256 manifest: {digest[:24]}... | Página {self.page_no()}"
                    ),
                    align="C",
                )

        pdf = DocumentaryDossierPDF()
        pdf.set_auto_page_break(auto=True, margin=18)
        if hasattr(pdf, "set_creation_date"):
            pdf.set_creation_date(datetime(2000, 1, 1, tzinfo=timezone.utc))
        if hasattr(pdf, "set_title"):
            pdf.set_title(
                _pdf_safe(
                    f"Dossier documental - {manifest['shipment']['shipment_code']}"
                )
            )
        if hasattr(pdf, "set_author"):
            pdf.set_author("Litoral Trace")
        if hasattr(pdf, "set_producer"):
            pdf.set_producer("Litoral Trace")

        def full_width_cell(
            height: float,
            text: Any,
            *,
            border: int = 0,
            align: str = "L",
        ) -> None:
            pdf.set_x(pdf.l_margin)
            pdf.cell(
                pdf.epw,
                height,
                _pdf_safe(text),
                border=border,
                align=align,
            )
            pdf.ln(height)
            pdf.set_x(pdf.l_margin)

        def full_width_multicell(height: float, text: Any) -> None:
            pdf.set_x(pdf.l_margin)
            pdf.multi_cell(pdf.epw, height, _pdf_safe(text))
            pdf.set_x(pdf.l_margin)

        pdf.add_page()
        pdf.set_font("Helvetica", "B", 14)
        pdf.set_text_color(15, 23, 42)
        full_width_cell(
            9,
            "LITORAL TRACE - DOSSIER DE ORIGEN, TRAZABILIDAD Y EVIDENCIA",
            align="C",
        )
        pdf.set_font("Helvetica", "I", 8)
        full_width_multicell(
            4,
            "Una sola lectura: origen geográfico → transformación → lote → despacho → respaldo documental.",
        )
        pdf.ln(2)

        shipment = manifest["shipment"]
        lineage = manifest["lineage"]
        evidence_summary = manifest.get("documentary_evidence_summary") or {}
        pdf.set_font("Helvetica", "B", 11)
        full_width_cell(7, f"Despacho: {shipment.get('shipment_code')}")
        pdf.set_font("Helvetica", "", 9)
        for label, value in (
            ("Venta / referencia", shipment.get("sale_reference")),
            ("Comprador", shipment.get("buyer_reference")),
            ("País destino", shipment.get("destination_country")),
            ("Fecha despacho", shipment.get("shipped_at")),
            ("Estado despacho", shipment.get("status")),
            (
                "Estado genealogía",
                "CERRADA" if lineage.get("complete") else "INCOMPLETA",
            ),
            ("Método atribución", lineage.get("allocation_method")),
            (
                "Referencias documentales",
                evidence_summary.get("linked_evidence_count", 0),
            ),
        ):
            full_width_multicell(5, f"{label}: {_pdf_safe(value)}")

        pdf.ln(2)
        pdf.set_font("Helvetica", "B", 10)
        full_width_cell(7, "1. RECONCILIACIÓN DE VOLUMEN", border=1)
        for total in manifest.get("unit_totals") or []:
            pdf.set_font("Helvetica", "", 9)
            unit = _unit_label(total.get("unit"))
            full_width_multicell(
                5,
                "Unidad {unit} | Despachado {shipped} | Atribuido {attributed} | "
                "Sin resolver {unresolved}".format(
                    unit=unit,
                    shipped=total.get("shipped_quantity"),
                    attributed=total.get("attributed_quantity"),
                    unresolved=total.get("unresolved_quantity"),
                ),
            )

        pdf.ln(2)
        pdf.set_font("Helvetica", "B", 10)
        full_width_cell(7, "2. ORÍGENES ATRIBUIDOS", border=1)
        origins = manifest.get("origins") or []
        if origins:
            for index, origin in enumerate(origins, start=1):
                pdf.set_font("Helvetica", "B", 9)
                full_width_multicell(
                    5,
                    f"{index}. {origin.get('identifier')} | Proveedor: {origin.get('producer_reference')}",
                )
                pdf.set_font("Helvetica", "", 8)
                geometry = origin.get("geometry") or {}
                full_width_multicell(
                    4,
                    "Producto: {product} | Superficie: {ha} ha | Atribución: {qty} {unit} | "
                    "Participación: {share} | Geometría: {geometry_type}".format(
                        product=origin.get("product"),
                        ha=origin.get("hectares"),
                        qty=origin.get("attributed_shipment_quantity"),
                        unit=_unit_label(origin.get("unit")),
                        share=origin.get("share_of_shipped_unit"),
                        geometry_type=geometry.get("type") or "no disponible",
                    ),
                )
                pdf.ln(1)
        else:
            pdf.set_font("Helvetica", "", 9)
            full_width_multicell(5, "No se pudieron atribuir orígenes a este despacho.")

        pdf.ln(2)
        pdf.set_font("Helvetica", "B", 10)
        full_width_cell(7, "3. CAMINO INDUSTRIAL", border=1)
        events = manifest.get("events") or []
        if events:
            for event in events:
                pdf.set_font("Helvetica", "B", 8)
                full_width_multicell(
                    4,
                    f"{event.get('event_code')} | {event.get('event_type')} | "
                    f"{event.get('occurred_at')} | {event.get('facility_reference')}",
                )
                reconciliation = event.get("reconciliation") or {}
                pdf.set_font("Helvetica", "", 8)
                full_width_multicell(
                    4,
                    "Input {input_q} {unit} | Output {output_q} {unit} | Merma/diferencia "
                    "{loss_q} {unit} | Rendimiento {yield_ratio}".format(
                        input_q=reconciliation.get("input_quantity"),
                        output_q=reconciliation.get("output_quantity"),
                        loss_q=reconciliation.get("loss_quantity"),
                        unit=_unit_label(reconciliation.get("unit")),
                        yield_ratio=reconciliation.get("yield_ratio"),
                    ),
                )
                pdf.ln(1)
        else:
            pdf.set_font("Helvetica", "", 9)
            full_width_multicell(5, "No hay eventos industriales recorribles.")

        pdf.ln(2)
        pdf.set_font("Helvetica", "B", 10)
        full_width_cell(7, "4. HUELLA DOCUMENTAL LITORAL TRACE", border=1)
        evidence = manifest.get("documentary_evidence") or []
        if evidence:
            pdf.set_font("Helvetica", "", 8)
            counts = evidence_summary.get("by_subject_type") or {}
            full_width_multicell(
                4,
                "Origen {origin} · Movimientos {events} · Lotes {batches} · Despachos {shipments}".format(
                    origin=counts.get("SOURCE_LOTE", 0),
                    events=counts.get("TRACEABILITY_EVENT", 0),
                    batches=counts.get("TRACEABILITY_BATCH", 0),
                    shipments=counts.get("SHIPMENT", 0),
                ),
            )
            pdf.ln(1)
            for item in evidence:
                document = item.get("document") or {}
                pdf.set_font("Helvetica", "B", 8)
                full_width_multicell(
                    4,
                    "{subject} · {reference} | {kind}".format(
                        subject=_SUBJECT_LABELS.get(
                            item.get("subject_type"),
                            item.get("subject_type"),
                        ),
                        reference=item.get("subject_reference"),
                        kind=_EVIDENCE_LABELS.get(
                            item.get("evidence_type"),
                            item.get("evidence_type"),
                        ),
                    ),
                )
                pdf.set_font("Helvetica", "", 8)
                full_width_multicell(
                    4,
                    "Ref. {doc_ref} | Emisor {issuer} | Fecha {date} | Archivo {filename}".format(
                        doc_ref=item.get("reference_number") or "—",
                        issuer=item.get("issuer") or "—",
                        date=item.get("document_date") or "—",
                        filename=document.get("filename"),
                    ),
                )
                full_width_multicell(
                    4,
                    f"SHA-256 del documento: {document.get('sha256')}",
                )
                pdf.ln(1)
        else:
            pdf.set_font("Helvetica", "", 9)
            full_width_multicell(
                5,
                "Esta genealogía no tiene referencias documentales vinculadas todavía.",
            )
        pdf.set_font("Helvetica", "I", 7)
        full_width_multicell(4, evidence_summary.get("statement"))

        issues = lineage.get("issues") or []
        warnings = manifest.get("artifact_warnings") or []
        if issues or warnings:
            pdf.ln(2)
            pdf.set_font("Helvetica", "B", 10)
            full_width_cell(7, "5. INCIDENCIAS Y ADVERTENCIAS", border=1)
            pdf.set_font("Helvetica", "", 8)
            for issue in [*issues, *warnings]:
                full_width_multicell(
                    4,
                    f"[{issue.get('code')}] {issue.get('message')}",
                )

        pdf.ln(4)
        pdf.set_font("Helvetica", "B", 9)
        full_width_multicell(5, f"SHA-256 del manifest canónico: {digest}")
        pdf.set_font("Helvetica", "", 8)
        full_width_multicell(4, manifest.get("disclaimer"))

        return bytes(pdf.output())
    except OriginDossierError:
        raise
    except Exception as exc:
        raise OriginDossierGenerationError(
            "DOSSIER_DOCUMENTARY_PDF_GENERATION_FAILED",
            "No fue posible generar el PDF documental del dossier.",
        ) from exc


def _zip_entry(name: str, content: bytes) -> tuple[zipfile.ZipInfo, bytes]:
    info = zipfile.ZipInfo(
        filename=name,
        date_time=(1980, 1, 1, 0, 0, 0),
    )
    info.compress_type = zipfile.ZIP_DEFLATED
    info.external_attr = 0o644 << 16
    return info, content


def build_documentary_zip(
    *,
    shipment_code: str,
    digest: str,
    manifest_json_bytes: bytes,
    geojson_bytes: bytes,
    pdf_bytes: bytes,
) -> bytes:
    readme = (
        "LITORAL TRACE - ORIGIN + DOCUMENTARY DOSSIER\n"
        f"Shipment: {shipment_code}\n"
        f"Integrity: {INTEGRITY_ALGORITHM} {digest}\n\n"
        "The SHA-256 value anchors the canonical manifest contained in manifest.json. "
        "PDF and GeoJSON are projections of that manifest.\n\n"
        "Documentary evidence is represented by buyer-safe references and content "
        "hashes. Private evidence binaries are not embedded in this ZIP.\n\n"
        f"{DISCLAIMER}\n"
    ).encode("utf-8")

    buffer = io.BytesIO()
    with zipfile.ZipFile(
        buffer,
        mode="w",
        compression=zipfile.ZIP_DEFLATED,
        compresslevel=9,
    ) as archive:
        for name, content in (
            ("manifest.json", manifest_json_bytes),
            ("origins.geojson", geojson_bytes),
            ("dossier.pdf", pdf_bytes),
            ("README.txt", readme),
        ):
            info, entry_payload = _zip_entry(name, content)
            archive.writestr(info, entry_payload)
    return buffer.getvalue()


def build_documentary_dossier_bundle(
    payload: dict[str, Any],
    *,
    documentary_evidence: Any = None,
) -> OriginDossierBundle:
    """Build a deterministic P1E+UX10-E dossier without embedding Vault binaries."""

    manifest = build_documentary_manifest(
        payload,
        documentary_evidence=documentary_evidence,
    )
    digest = manifest_sha256(manifest)
    manifest_document = {
        "integrity": {
            "algorithm": INTEGRITY_ALGORITHM,
            "canonical_manifest_sha256": digest,
        },
        "manifest": manifest,
    }
    manifest_bytes = _pretty_json_bytes(manifest_document)
    geojson = build_geojson(manifest, digest)
    geojson_bytes = _pretty_json_bytes(geojson)
    pdf_bytes = build_documentary_pdf(manifest, digest)
    shipment_code = str(manifest["shipment"]["shipment_code"])
    zip_bytes = build_documentary_zip(
        shipment_code=shipment_code,
        digest=digest,
        manifest_json_bytes=manifest_bytes,
        geojson_bytes=geojson_bytes,
        pdf_bytes=pdf_bytes,
    )
    return OriginDossierBundle(
        shipment_code=shipment_code,
        canonical_manifest=manifest,
        manifest_document=manifest_document,
        manifest_sha256=digest,
        manifest_json_bytes=manifest_bytes,
        geojson=geojson,
        geojson_bytes=geojson_bytes,
        pdf_bytes=pdf_bytes,
        zip_bytes=zip_bytes,
    )
