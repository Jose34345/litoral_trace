"""Buyer-facing origin dossier artifacts built from one P1C lineage payload.

P1E is intentionally a pure projection over the P1C reverse-lineage result.
It does not mutate genealogy, inventory, shipments, or source lots.

Integrity is anchored to a canonical JSON manifest. The SHA-256 digest is
computed from stable traceability content only; generation time and PDF byte
metadata are deliberately excluded so the same business evidence yields the
same manifest hash.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import io
import json
import re
from typing import Any
import zipfile


SCHEMA_VERSION = "litoral-trace.origin-dossier.v1"
DOSSIER_TYPE = "ORIGIN_CHAIN_OF_CUSTODY"
INTEGRITY_ALGORITHM = "SHA-256"
DISCLAIMER = (
    "Este dossier documenta origen y cadena de custodia según los registros "
    "disponibles en Litoral Trace. No constituye por sí solo una declaración "
    "regulatoria ni reemplaza la debida diligencia exigible al operador."
)


class OriginDossierError(RuntimeError):
    """Base dossier generation failure."""


class OriginDossierValidationError(OriginDossierError):
    """Invalid or incomplete P1C payload for dossier generation."""

    def __init__(self, code: str, detail: str) -> None:
        super().__init__(detail)
        self.code = code


class OriginDossierGenerationError(OriginDossierError):
    """Artifact rendering failed after a valid manifest was built."""

    def __init__(self, code: str, detail: str) -> None:
        super().__init__(detail)
        self.code = code


@dataclass(frozen=True)
class OriginDossierBundle:
    shipment_code: str
    canonical_manifest: dict[str, Any]
    manifest_document: dict[str, Any]
    manifest_sha256: str
    manifest_json_bytes: bytes
    geojson: dict[str, Any]
    geojson_bytes: bytes
    pdf_bytes: bytes
    zip_bytes: bytes


def _text(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _number(value: Any) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number or number in {float("inf"), float("-inf")}:
        return None
    return number


def _canonical_json_bytes(value: Any) -> bytes:
    try:
        rendered = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise OriginDossierValidationError(
            "DOSSIER_CANONICALIZATION_FAILED",
            "El contenido de trazabilidad no puede canonicalizarse de forma segura.",
        ) from exc
    return rendered.encode("utf-8")


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


def _geometry_from_lote(
    lote: dict[str, Any],
) -> tuple[dict[str, Any] | None, str | None]:
    polygon_wkt = _text(lote.get("polygon_wkt"))
    if polygon_wkt:
        try:
            from shapely import wkt
            from shapely.geometry import mapping

            geometry = mapping(wkt.loads(polygon_wkt))
            geometry_type = str(geometry.get("type") or "")
            if geometry_type in {"Polygon", "MultiPolygon"}:
                return dict(geometry), None
            return dict(geometry), "SOURCE_GEOMETRY_NON_POLYGON"
        except Exception:
            # Keep a visible warning and use only a real point if coordinates
            # exist. Never synthesize a polygon from invalid source evidence.
            pass

    latitude = _number(lote.get("latitud"))
    longitude = _number(lote.get("longitud"))
    if latitude is not None and longitude is not None:
        warning = (
            "SOURCE_GEOMETRY_POINT_FALLBACK"
            if polygon_wkt
            else "SOURCE_POLYGON_MISSING"
        )
        return {
            "type": "Point",
            "coordinates": [longitude, latitude],
        }, warning

    return None, "SOURCE_GEOMETRY_UNAVAILABLE"


def _clean_issue(issue: dict[str, Any]) -> dict[str, str | None]:
    return {
        "code": _text(issue.get("code")),
        "message": _text(issue.get("message")),
    }


def _clean_edge(edge: dict[str, Any]) -> dict[str, str | None]:
    return {
        "batch_code": _text(edge.get("batch_code")),
        "quantity": _text(edge.get("quantity")),
        "unit": _text(edge.get("unit")),
    }


def _clean_source_contribution(source: dict[str, Any]) -> dict[str, Any]:
    lote = source.get("lote") or {}
    return {
        "origin_identifier": _text(lote.get("identificador")),
        "producer_reference": _text(lote.get("productor_id")),
        "attributed_quantity": _text(source.get("attributed_shipment_quantity")),
        "unit": _text(source.get("unit")),
        "share": _text(source.get("share_of_shipment_item")),
    }


def build_canonical_manifest(payload: dict[str, Any]) -> dict[str, Any]:
    """Return stable buyer-facing traceability content with no internal DB ids."""

    if not isinstance(payload, dict):
        raise OriginDossierValidationError(
            "DOSSIER_PAYLOAD_INVALID",
            "Se requiere un resultado de genealogía válido.",
        )

    shipment = payload.get("shipment") or {}
    shipment_code = _text(shipment.get("shipment_code"))
    if not shipment_code:
        raise OriginDossierValidationError(
            "DOSSIER_SHIPMENT_CODE_REQUIRED",
            "El resultado de genealogía no contiene un código de despacho.",
        )

    artifact_warnings: list[dict[str, str | None]] = []
    origins: list[dict[str, Any]] = []
    for source in payload.get("source_lotes") or []:
        lote = source.get("lote") or {}
        geometry, geometry_warning = _geometry_from_lote(lote)
        origin = {
            "identifier": _text(lote.get("identificador")),
            "producer_reference": _text(lote.get("productor_id")),
            "product": _text(lote.get("producto_forestal")),
            "hectares": _number(lote.get("hectareas")),
            "source_status": _text(lote.get("estatus")),
            "attributed_shipment_quantity": _text(
                source.get("attributed_shipment_quantity")
            ),
            "unit": _text(source.get("unit")),
            "share_of_shipped_unit": _text(source.get("share_of_shipped_unit")),
            "geometry": geometry,
        }
        origins.append(origin)
        if geometry_warning:
            artifact_warnings.append(
                {
                    "code": geometry_warning,
                    "message": (
                        "El origen no aporta un polígono utilizable; el dossier conserva "
                        "la mejor georreferencia disponible sin inventar geometría."
                    ),
                    "origin_identifier": origin["identifier"],
                }
            )

    items: list[dict[str, Any]] = []
    for item in payload.get("items") or []:
        batch = item.get("batch") or {}
        contributions = [
            _clean_source_contribution(source)
            for source in item.get("source_contributions") or []
        ]
        contributions.sort(
            key=lambda value: (
                value.get("origin_identifier") or "",
                value.get("producer_reference") or "",
                value.get("unit") or "",
            )
        )
        items.append(
            {
                "batch_public_id": _text(batch.get("public_id")),
                "batch_code": _text(batch.get("code")),
                "product": _text(batch.get("product_name")),
                "stage": _text(batch.get("stage")),
                "batch_status": _text(batch.get("status")),
                "shipped_quantity": _text(item.get("shipped_quantity")),
                "attributed_quantity": _text(item.get("attributed_quantity")),
                "unresolved_quantity": _text(item.get("unresolved_quantity")),
                "unit": _text(item.get("unit")),
                "complete": bool(item.get("complete")),
                "issues": sorted(
                    [_clean_issue(issue) for issue in item.get("issues") or []],
                    key=lambda value: (
                        value.get("code") or "",
                        value.get("message") or "",
                    ),
                ),
                "source_contributions": contributions,
            }
        )

    events: list[dict[str, Any]] = []
    for event in payload.get("events") or []:
        reconciliation = event.get("reconciliation") or {}
        inputs = [_clean_edge(edge) for edge in event.get("inputs") or []]
        outputs = [_clean_edge(edge) for edge in event.get("outputs") or []]
        inputs.sort(
            key=lambda value: (
                value.get("batch_code") or "",
                value.get("unit") or "",
            )
        )
        outputs.sort(
            key=lambda value: (
                value.get("batch_code") or "",
                value.get("unit") or "",
            )
        )
        events.append(
            {
                "event_public_id": _text(event.get("public_id")),
                "event_code": _text(event.get("event_code")),
                "event_type": _text(event.get("event_type")),
                "status": _text(event.get("status")),
                "occurred_at": _text(event.get("occurred_at")),
                "facility_reference": _text(event.get("facility_reference")),
                "inputs": inputs,
                "outputs": outputs,
                "reconciliation": {
                    "unit": _text(reconciliation.get("unit")),
                    "input_quantity": _text(reconciliation.get("input_quantity")),
                    "output_quantity": _text(reconciliation.get("output_quantity")),
                    "loss_quantity": _text(reconciliation.get("loss_quantity")),
                    "yield_ratio": _text(reconciliation.get("yield_ratio")),
                },
            }
        )

    unit_totals = [
        {
            "unit": _text(total.get("unit")),
            "shipped_quantity": _text(total.get("shipped_quantity")),
            "attributed_quantity": _text(total.get("attributed_quantity")),
            "unresolved_quantity": _text(total.get("unresolved_quantity")),
        }
        for total in payload.get("unit_totals") or []
    ]

    origins.sort(
        key=lambda value: (
            value.get("identifier") or "",
            value.get("producer_reference") or "",
            value.get("unit") or "",
        )
    )
    items.sort(
        key=lambda value: (
            value.get("batch_code") or "",
            value.get("unit") or "",
        )
    )
    events.sort(
        key=lambda value: (
            value.get("occurred_at") or "",
            value.get("event_code") or "",
            value.get("event_public_id") or "",
        )
    )
    unit_totals.sort(key=lambda value: value.get("unit") or "")
    artifact_warnings.sort(
        key=lambda value: (
            value.get("code") or "",
            value.get("origin_identifier") or "",
        )
    )
    lineage_issues = sorted(
        [_clean_issue(issue) for issue in payload.get("issues") or []],
        key=lambda value: (
            value.get("code") or "",
            value.get("message") or "",
        ),
    )

    return {
        "schema_version": SCHEMA_VERSION,
        "dossier_type": DOSSIER_TYPE,
        "shipment": {
            "public_id": _text(shipment.get("public_id")),
            "shipment_code": shipment_code,
            "sale_reference": _text(shipment.get("sale_reference")),
            "buyer_reference": _text(shipment.get("buyer_reference")),
            "destination_country": _text(shipment.get("destination_country")),
            "shipped_at": _text(shipment.get("shipped_at")),
            "status": _text(shipment.get("status")),
            "lineage_state": _text(shipment.get("lineage_state")),
        },
        "lineage": {
            "allocation_method": _text(payload.get("allocation_method")),
            "complete": bool(payload.get("complete")),
            "issues": lineage_issues,
        },
        "unit_totals": unit_totals,
        "items": items,
        "events": events,
        "origins": origins,
        "artifact_warnings": artifact_warnings,
        "disclaimer": DISCLAIMER,
    }


def manifest_sha256(manifest: dict[str, Any]) -> str:
    return hashlib.sha256(_canonical_json_bytes(manifest)).hexdigest()


def build_manifest_document(manifest: dict[str, Any]) -> dict[str, Any]:
    digest = manifest_sha256(manifest)
    return {
        "integrity": {
            "algorithm": INTEGRITY_ALGORITHM,
            "canonical_manifest_sha256": digest,
        },
        "manifest": manifest,
    }


def build_geojson(manifest: dict[str, Any], digest: str) -> dict[str, Any]:
    features: list[dict[str, Any]] = []
    for origin in manifest.get("origins") or []:
        features.append(
            {
                "type": "Feature",
                "id": origin.get("identifier"),
                "geometry": origin.get("geometry"),
                "properties": {
                    "identifier": origin.get("identifier"),
                    "producer_reference": origin.get("producer_reference"),
                    "product": origin.get("product"),
                    "hectares": origin.get("hectares"),
                    "source_status": origin.get("source_status"),
                    "attributed_shipment_quantity": origin.get(
                        "attributed_shipment_quantity"
                    ),
                    "unit": origin.get("unit"),
                    "share_of_shipped_unit": origin.get("share_of_shipped_unit"),
                },
            }
        )
    return {
        "type": "FeatureCollection",
        "name": f"Litoral Trace origins - {manifest['shipment']['shipment_code']}",
        "litoral_trace_integrity": {
            "algorithm": INTEGRITY_ALGORITHM,
            "canonical_manifest_sha256": digest,
            "schema_version": SCHEMA_VERSION,
        },
        "features": features,
    }


def _pdf_safe(value: Any) -> str:
    text = "—" if value is None else str(value)
    return text.encode("latin-1", errors="replace").decode("latin-1")


def _unit_label(unit: Any) -> str:
    return {
        "M3": "m3",
        "TON": "t",
        "KG": "kg",
    }.get(str(unit or "").upper(), _pdf_safe(unit))


def build_pdf(manifest: dict[str, Any], digest: str) -> bytes:
    """Render a readable projection without relying on fpdf cursor defaults."""
    try:
        from fpdf import FPDF

        class OriginDossierPDF(FPDF):
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

        pdf = OriginDossierPDF()
        pdf.set_auto_page_break(auto=True, margin=18)
        if hasattr(pdf, "set_creation_date"):
            pdf.set_creation_date(datetime(2000, 1, 1, tzinfo=timezone.utc))
        if hasattr(pdf, "set_title"):
            pdf.set_title(
                _pdf_safe(
                    f"Dossier de origen - {manifest['shipment']['shipment_code']}"
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
            # fpdf2 2.8 defaults multi_cell new_x to RIGHT. Resetting before
            # and after every block makes the dossier stable across versions.
            pdf.set_x(pdf.l_margin)
            pdf.multi_cell(pdf.epw, height, _pdf_safe(text))
            pdf.set_x(pdf.l_margin)

        pdf.add_page()
        pdf.set_font("Helvetica", "B", 15)
        pdf.set_text_color(15, 23, 42)
        full_width_cell(
            9,
            "LITORAL TRACE - DOSSIER DE ORIGEN Y TRAZABILIDAD",
            align="C",
        )
        pdf.ln(3)

        shipment = manifest["shipment"]
        lineage = manifest["lineage"]
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
                    f"{index}. {origin.get('identifier')} | Proveedor: "
                    f"{origin.get('producer_reference')}",
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
            full_width_multicell(
                5,
                "No se pudieron atribuir orígenes a este despacho.",
            )

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

        issues = lineage.get("issues") or []
        warnings = manifest.get("artifact_warnings") or []
        if issues or warnings:
            pdf.ln(2)
            pdf.set_font("Helvetica", "B", 10)
            full_width_cell(7, "4. INCIDENCIAS Y ADVERTENCIAS", border=1)
            pdf.set_font("Helvetica", "", 8)
            for issue in [*issues, *warnings]:
                full_width_multicell(
                    4,
                    f"[{issue.get('code')}] {issue.get('message')}",
                )

        pdf.ln(4)
        pdf.set_font("Helvetica", "B", 9)
        full_width_multicell(
            5,
            f"SHA-256 del manifest canónico: {digest}",
        )
        pdf.set_font("Helvetica", "", 8)
        full_width_multicell(4, manifest.get("disclaimer"))

        return bytes(pdf.output())
    except OriginDossierError:
        raise
    except Exception as exc:
        raise OriginDossierGenerationError(
            "DOSSIER_PDF_GENERATION_FAILED",
            "No fue posible generar el PDF del dossier.",
        ) from exc


def _zip_entry(name: str, content: bytes) -> tuple[zipfile.ZipInfo, bytes]:
    info = zipfile.ZipInfo(
        filename=name,
        date_time=(1980, 1, 1, 0, 0, 0),
    )
    info.compress_type = zipfile.ZIP_DEFLATED
    info.external_attr = 0o644 << 16
    return info, content


def build_zip(
    *,
    shipment_code: str,
    digest: str,
    manifest_json_bytes: bytes,
    geojson_bytes: bytes,
    pdf_bytes: bytes,
) -> bytes:
    readme = (
        "LITORAL TRACE - ORIGIN DOSSIER\n"
        f"Shipment: {shipment_code}\n"
        f"Integrity: {INTEGRITY_ALGORITHM} {digest}\n\n"
        "The SHA-256 value anchors the canonical traceability manifest contained "
        "in manifest.json. PDF and GeoJSON are projections of that manifest.\n\n"
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


def safe_artifact_stem(shipment_code: str) -> str:
    normalized = re.sub(
        r"[^A-Za-z0-9._-]+",
        "-",
        shipment_code.strip(),
    )
    normalized = normalized.strip("-._")
    return normalized[:80] or "shipment"


def build_origin_dossier_bundle(
    payload: dict[str, Any],
) -> OriginDossierBundle:
    manifest = build_canonical_manifest(payload)
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
    pdf_bytes = build_pdf(manifest, digest)
    shipment_code = str(manifest["shipment"]["shipment_code"])
    zip_bytes = build_zip(
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
