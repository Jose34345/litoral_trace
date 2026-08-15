"""Secure XLSX ingestion primitives for Litoral Trace batch workflows."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import io
from pathlib import PurePosixPath
import re
import unicodedata
import zipfile

from openpyxl import load_workbook
import pandas as pd

from litoral_trace.services.compliance import (
    evaluar_compliance_lote,
    generar_dds_json_traces_nt,
)
from litoral_trace.services.reports import generar_pdf_reporte_bytes


BATCH_XLSX_MEDIA_TYPE = (
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)
BATCH_SHEET_NAME = "Plantilla_LitoralTrace"

BATCH_COLUMNAS = [
    "Identificador_Lote",
    "ID_Proveedor",
    "Producto_Forestal",
    "Hectareas",
    "Latitud",
    "Longitud",
    "Volumen_Ingresado_Ton",
    "Volumen_Exportar_Ton",
]

BATCH_FILA_EJEMPLO = [
    "Rodal_Norte_01",
    "CUIT-30123456789",
    "Madera Aserrada (Eucalipto)",
    120.0,
    -27.50,
    -58.90,
    500.0,
    200.0,
]

BATCH_MAX_FILE_BYTES = 10 * 1024 * 1024
BATCH_MAX_ROWS = 500
BATCH_MAX_SHEETS = 4
BATCH_MAX_ZIP_MEMBERS = 2048
BATCH_MAX_UNCOMPRESSED_BYTES = 64 * 1024 * 1024
BATCH_MAX_MEMBER_BYTES = 16 * 1024 * 1024

_REQUIRED_XLSX_MEMBERS = frozenset(
    {
        "[Content_Types].xml",
        "_rels/.rels",
        "xl/workbook.xml",
        "xl/_rels/workbook.xml.rels",
    }
)

_FORBIDDEN_MEMBER_PREFIXES = (
    "xl/embeddings/",
    "xl/externallinks/",
    "xl/activex/",
    "xl/ctrlprops/",
    "customui/",
)

_FORBIDDEN_MEMBER_FRAGMENTS = (
    "vbaproject.bin",
    "oleobject",
)

_CONTROL_CHARACTER_RE = re.compile(r"[\x00-\x1f\x7f]")


class BatchExcelError(ValueError):
    """Base class for safe, user-originated XLSX ingestion errors."""


class BatchExcelValidationError(BatchExcelError):
    """Fail-closed XLSX validation error with a stable public code."""

    def __init__(self, code: str, detail: str) -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail


@dataclass(frozen=True)
class BatchWorkbook:
    """Canonical, structurally validated workbook payload."""

    filename: str
    sha256: str
    sheet_name: str
    row_count: int
    dataframe: pd.DataFrame


def _raise_batch_error(code: str, detail: str) -> None:
    raise BatchExcelValidationError(code=code, detail=detail)


def normalizar_nombre_archivo_batch(filename: str | None) -> str:
    """Return a safe basename and require the modern XLSX container format."""

    raw = unicodedata.normalize("NFKC", str(filename or ""))
    raw = raw.replace("\\", "/")
    basename = PurePosixPath(raw).name.strip()

    if not basename:
        _raise_batch_error(
            "INVALID_FILENAME",
            "El archivo debe tener un nombre válido.",
        )

    if _CONTROL_CHARACTER_RE.search(basename):
        _raise_batch_error(
            "INVALID_FILENAME",
            "El nombre del archivo contiene caracteres no permitidos.",
        )

    if len(basename) > 255:
        _raise_batch_error(
            "INVALID_FILENAME",
            "El nombre del archivo excede el límite permitido.",
        )

    if not basename.lower().endswith(".xlsx"):
        _raise_batch_error(
            "UNSUPPORTED_FILE_TYPE",
            "Solo se admiten planillas Excel en formato .xlsx.",
        )

    return basename


def _validate_zip_member_name(name: str) -> None:
    normalized = name.replace("\\", "/")
    path = PurePosixPath(normalized)

    if (
        not normalized
        or normalized.startswith("/")
        or any(part in {"", ".", ".."} for part in path.parts)
        or ":" in path.parts[0]
    ):
        _raise_batch_error(
            "UNSAFE_XLSX_PATH",
            "La estructura interna del archivo XLSX no es válida.",
        )


def _read_member_bounded(
    archive: zipfile.ZipFile,
    info: zipfile.ZipInfo,
) -> bytes:
    if info.file_size > BATCH_MAX_MEMBER_BYTES:
        _raise_batch_error(
            "XLSX_MEMBER_TOO_LARGE",
            "La planilla contiene una parte interna demasiado grande.",
        )

    with archive.open(info, "r") as member:
        payload = member.read(BATCH_MAX_MEMBER_BYTES + 1)

    if len(payload) > BATCH_MAX_MEMBER_BYTES:
        _raise_batch_error(
            "XLSX_MEMBER_TOO_LARGE",
            "La planilla contiene una parte interna demasiado grande.",
        )

    return payload


def _preflight_xlsx_container(payload: bytes) -> None:
    """Validate the ZIP/XML container before handing bytes to openpyxl."""

    if not payload.startswith(b"PK"):
        _raise_batch_error(
            "INVALID_XLSX_CONTAINER",
            "El archivo no contiene una estructura XLSX válida.",
        )

    try:
        archive = zipfile.ZipFile(
            io.BytesIO(payload),
            mode="r",
            allowZip64=False,
        )
    except (zipfile.BadZipFile, zipfile.LargeZipFile):
        _raise_batch_error(
            "INVALID_XLSX_CONTAINER",
            "El archivo no contiene una estructura XLSX válida.",
        )

    with archive:
        infos = [info for info in archive.infolist() if not info.is_dir()]

        if not infos:
            _raise_batch_error(
                "INVALID_XLSX_CONTAINER",
                "El archivo XLSX no contiene datos internos.",
            )

        if len(infos) > BATCH_MAX_ZIP_MEMBERS:
            _raise_batch_error(
                "TOO_MANY_XLSX_PARTS",
                "La planilla excede la complejidad interna permitida.",
            )

        names: set[str] = set()
        total_uncompressed = 0

        for info in infos:
            _validate_zip_member_name(info.filename)

            normalized_name = info.filename.replace("\\", "/")
            lowered_name = normalized_name.lower()

            if normalized_name in names:
                _raise_batch_error(
                    "DUPLICATE_XLSX_PART",
                    "La estructura interna del archivo XLSX contiene duplicados.",
                )
            names.add(normalized_name)

            if info.flag_bits & 0x1:
                _raise_batch_error(
                    "ENCRYPTED_XLSX_PART",
                    "No se admiten planillas XLSX con partes cifradas.",
                )

            if any(
                lowered_name.startswith(prefix)
                for prefix in _FORBIDDEN_MEMBER_PREFIXES
            ) or any(
                fragment in lowered_name
                for fragment in _FORBIDDEN_MEMBER_FRAGMENTS
            ):
                _raise_batch_error(
                    "UNSUPPORTED_XLSX_FEATURE",
                    (
                        "La planilla contiene elementos embebidos, vínculos "
                        "externos o automatizaciones no permitidas."
                    ),
                )

            if info.file_size > BATCH_MAX_MEMBER_BYTES:
                _raise_batch_error(
                    "XLSX_MEMBER_TOO_LARGE",
                    "La planilla contiene una parte interna demasiado grande.",
                )

            total_uncompressed += info.file_size
            if total_uncompressed > BATCH_MAX_UNCOMPRESSED_BYTES:
                _raise_batch_error(
                    "XLSX_EXPANDED_TOO_LARGE",
                    "La planilla excede el tamaño interno permitido.",
                )

            if lowered_name.endswith((".xml", ".rels")):
                xml_payload = _read_member_bounded(archive, info)
                upper_xml = xml_payload.upper()

                if b"<!DOCTYPE" in upper_xml or b"<!ENTITY" in upper_xml:
                    _raise_batch_error(
                        "UNSAFE_XML",
                        "La planilla contiene XML no permitido.",
                    )

                if lowered_name.endswith(".rels") and (
                    b'TARGETMODE="EXTERNAL"' in upper_xml
                    or b"TARGETMODE='EXTERNAL'" in upper_xml
                ):
                    _raise_batch_error(
                        "EXTERNAL_RELATIONSHIP",
                        "No se admiten vínculos externos en la planilla.",
                    )

        missing = _REQUIRED_XLSX_MEMBERS.difference(names)
        if missing:
            _raise_batch_error(
                "INVALID_XLSX_STRUCTURE",
                "El archivo no contiene la estructura mínima de una planilla XLSX.",
            )


def _normalize_header(value: object) -> str:
    if value is None:
        return ""
    return unicodedata.normalize("NFKC", str(value)).strip()


def _cell_is_blank(value: object) -> bool:
    return value is None or (
        isinstance(value, str)
        and not value.strip()
    )


def parsear_excel_lotes(
    payload: bytes | bytearray | memoryview,
    *,
    filename: str,
) -> BatchWorkbook:
    """
    Parse a bounded XLSX workbook into the canonical batch schema.

    P2.4A validates only the workbook/container/schema contract. Business-level
    validation of each field is intentionally reserved for P2.4B.
    """

    safe_filename = normalizar_nombre_archivo_batch(filename)

    if not isinstance(payload, (bytes, bytearray, memoryview)):
        _raise_batch_error(
            "INVALID_UPLOAD_BODY",
            "El contenido recibido no es un archivo XLSX válido.",
        )

    data = bytes(payload)

    if not data:
        _raise_batch_error(
            "EMPTY_FILE",
            "El archivo Excel está vacío.",
        )

    if len(data) > BATCH_MAX_FILE_BYTES:
        _raise_batch_error(
            "FILE_TOO_LARGE",
            "El archivo Excel excede el tamaño máximo permitido.",
        )

    _preflight_xlsx_container(data)

    workbook = None
    try:
        workbook = load_workbook(
            io.BytesIO(data),
            read_only=True,
            data_only=False,
            keep_links=False,
        )
    except Exception:
        _raise_batch_error(
            "INVALID_WORKBOOK",
            "No fue posible interpretar la planilla XLSX.",
        )

    try:
        if len(workbook.sheetnames) > BATCH_MAX_SHEETS:
            _raise_batch_error(
                "TOO_MANY_SHEETS",
                "La planilla contiene demasiadas hojas.",
            )

        if BATCH_SHEET_NAME not in workbook.sheetnames:
            _raise_batch_error(
                "MISSING_REQUIRED_SHEET",
                f"La planilla debe contener la hoja '{BATCH_SHEET_NAME}'.",
            )

        worksheet = workbook[BATCH_SHEET_NAME]

        if worksheet.max_column > len(BATCH_COLUMNAS):
            _raise_batch_error(
                "TOO_MANY_COLUMNS",
                "La hoja de importación contiene columnas adicionales.",
            )

        if worksheet.max_row > BATCH_MAX_ROWS + 1:
            _raise_batch_error(
                "TOO_MANY_ROWS",
                (
                    f"La planilla excede el máximo de "
                    f"{BATCH_MAX_ROWS} filas de datos."
                ),
            )

        row_iterator = worksheet.iter_rows(
            min_row=1,
            max_row=BATCH_MAX_ROWS + 2,
            max_col=len(BATCH_COLUMNAS),
        )

        try:
            header_cells = next(row_iterator)
        except StopIteration:
            _raise_batch_error(
                "MISSING_HEADER",
                "La hoja de importación no contiene encabezados.",
            )

        if any(cell.data_type == "f" for cell in header_cells):
            _raise_batch_error(
                "FORMULA_NOT_ALLOWED",
                "No se admiten fórmulas en la planilla de importación.",
            )

        headers = [_normalize_header(cell.value) for cell in header_cells]

        if len(set(headers)) != len(headers):
            _raise_batch_error(
                "DUPLICATE_HEADERS",
                "La planilla contiene encabezados duplicados.",
            )

        if headers != BATCH_COLUMNAS:
            _raise_batch_error(
                "INVALID_HEADERS",
                (
                    "Los encabezados de la planilla no coinciden con "
                    "la plantilla oficial de Litoral Trace."
                ),
            )

        rows: list[dict[str, object]] = []

        for excel_row_number, cells in enumerate(row_iterator, start=2):
            if excel_row_number > BATCH_MAX_ROWS + 1:
                if any(not _cell_is_blank(cell.value) for cell in cells):
                    _raise_batch_error(
                        "TOO_MANY_ROWS",
                        (
                            f"La planilla excede el máximo de "
                            f"{BATCH_MAX_ROWS} filas de datos."
                        ),
                    )
                continue

            if any(cell.data_type == "f" for cell in cells):
                _raise_batch_error(
                    "FORMULA_NOT_ALLOWED",
                    (
                        "No se admiten fórmulas en la planilla "
                        f"de importación (fila {excel_row_number})."
                    ),
                )

            if any(cell.data_type == "e" for cell in cells):
                _raise_batch_error(
                    "CELL_ERROR",
                    (
                        "La planilla contiene una celda con error "
                        f"de Excel (fila {excel_row_number})."
                    ),
                )

            values = [cell.value for cell in cells]

            if all(_cell_is_blank(value) for value in values):
                continue

            rows.append(
                dict(zip(BATCH_COLUMNAS, values, strict=True))
            )

        if not rows:
            _raise_batch_error(
                "NO_DATA_ROWS",
                "La planilla no contiene filas de datos para importar.",
            )

        dataframe = pd.DataFrame(rows, columns=BATCH_COLUMNAS)

        return BatchWorkbook(
            filename=safe_filename,
            sha256=hashlib.sha256(data).hexdigest(),
            sheet_name=BATCH_SHEET_NAME,
            row_count=len(rows),
            dataframe=dataframe,
        )

    finally:
        if workbook is not None:
            workbook.close()


def generar_plantilla_excel() -> bytes:
    """Genera la plantilla XLSX oficial para la importación masiva de lotes."""

    df_template = pd.DataFrame(columns=BATCH_COLUMNAS)
    df_template.loc[0] = BATCH_FILA_EJEMPLO

    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df_template.to_excel(
            writer,
            index=False,
            sheet_name=BATCH_SHEET_NAME,
        )
    return buffer.getvalue()


def procesar_lote_masivo(
    df_upload: pd.DataFrame,
) -> tuple[pd.DataFrame, bytes]:
    """
    Legacy batch evidence processor retained during the P2.4 migration.

    P2.4A adds the safe XLSX parser without silently changing the existing
    evidence-generation contract. P2.4B/P2.4C will replace the permissive row
    defaults below with explicit semantic validation and atomic persistence.
    """

    resumen_filas = []
    zip_buffer = io.BytesIO()

    if df_upload is None or df_upload.empty:
        return pd.DataFrame(resumen_filas), zip_buffer.getvalue()

    with zipfile.ZipFile(
        zip_buffer,
        "w",
        zipfile.ZIP_DEFLATED,
    ) as zip_file:
        for idx, row in df_upload.iterrows():
            nombre = str(
                row.get("Identificador_Lote") or f"Lote_{idx + 1}"
            ).strip()
            proveedor = str(row.get("ID_Proveedor") or "N/A").strip()
            producto = str(
                row.get("Producto_Forestal")
                or "Madera Aserrada (Pino)"
            ).strip()

            try:
                hectareas = float(row.get("Hectareas") or 0.0)
                lat = float(row.get("Latitud") or -27.45)
                lon = float(row.get("Longitud") or -59.05)
                vol_in = float(
                    row.get("Volumen_Ingresado_Ton") or 0.0
                )
                vol_out = float(
                    row.get("Volumen_Exportar_Ton") or 0.0
                )
            except (ValueError, TypeError):
                hectareas = 0.0
                lat = -27.45
                lon = -58.90
                vol_in = 0.0
                vol_out = 0.0

            lote_data = {
                "identificador": nombre,
                "productor_id": proveedor,
                "producto_forestal": producto,
                "hectareas": hectareas,
                "latitud": lat,
                "longitud": lon,
                "polygon_wkt": (
                    "POLYGON(("
                    f"{lon - 0.01} {lat - 0.01}, "
                    f"{lon + 0.01} {lat - 0.01}, "
                    f"{lon + 0.01} {lat + 0.01}, "
                    f"{lon - 0.01} {lat + 0.01}, "
                    f"{lon - 0.01} {lat - 0.01}"
                    "))"
                ),
            }

            eval_res = evaluar_compliance_lote(
                lote_data,
                vol_in,
                vol_out,
            )
            dictamen = eval_res["dictamen"]
            obs = eval_res["observacion"]
            mb_result = eval_res["balance_masas"]

            resumen_filas.append(
                {
                    "Lote": nombre,
                    "Proveedor": proveedor,
                    "Producto": producto,
                    "Vol. Exportar (Ton)": vol_out,
                    "Dictamen": dictamen,
                    "Observación": obs,
                }
            )

            pdf_bytes = generar_pdf_reporte_bytes(
                lote_data,
                dictamen,
                obs,
                vol_in,
                vol_out,
                mb_result.coeficiente_rendimiento,
            )
            carpeta = f"{dictamen}_{proveedor}_{nombre}/"
            zip_file.writestr(
                f"{carpeta}AUDITORIA_{proveedor}.pdf",
                pdf_bytes,
            )

            if dictamen == "Verde":
                json_data = generar_dds_json_traces_nt(
                    lote_data,
                    vol_out,
                )
                zip_file.writestr(
                    f"{carpeta}DDS_TRACES_NT_{proveedor}.json",
                    json_data.encode("utf-8"),
                )

    return pd.DataFrame(resumen_filas), zip_buffer.getvalue()