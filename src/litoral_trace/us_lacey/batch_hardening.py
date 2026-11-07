"""Memory/complexity guardrails for one-shipment U.S. Lacey uploads."""
from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
import os
from pathlib import PurePath
from zipfile import BadZipFile, ZipFile
import re
from xml.etree import ElementTree
from typing import Iterable

from openpyxl import load_workbook

from litoral_trace.assurance.tabular_safety import TabularSafetyError, profile_csv_chunks


DATASET_TOO_LARGE = "DATASET_TOO_LARGE_FOR_SHIPMENT_PIPELINE"
DATASET_TOO_COMPLEX = "DATASET_TOO_COMPLEX_FOR_SHIPMENT_PIPELINE"


@dataclass(frozen=True, slots=True)
class ShipmentSpreadsheetLimits:
    max_bytes: int
    max_rows: int
    max_columns: int
    max_cells: int


@dataclass(frozen=True, slots=True)
class ShipmentSpreadsheetProfile:
    file_kind: str
    size_bytes: int
    rows: int
    columns: int
    cells: int


class ShipmentBatchRejected(ValueError):
    def __init__(self, code: str, safe_message: str):
        super().__init__(safe_message)
        self.code = code
        self.safe_message = safe_message


def _env_int(name: str, default: int, minimum: int, maximum: int) -> int:
    raw = str(os.environ.get(name, default)).strip()
    try:
        value = int(raw)
    except ValueError:
        value = default
    return max(minimum, min(maximum, value))


def shipment_spreadsheet_limits() -> ShipmentSpreadsheetLimits:
    return ShipmentSpreadsheetLimits(
        max_bytes=_env_int("US_LACEY_SHIPMENT_SPREADSHEET_MAX_BYTES", 5 * 1024 * 1024, 128 * 1024, 25 * 1024 * 1024),
        max_rows=_env_int("US_LACEY_SHIPMENT_SPREADSHEET_MAX_ROWS", 5000, 100, 100000),
        max_columns=_env_int("US_LACEY_SHIPMENT_SPREADSHEET_MAX_COLUMNS", 64, 4, 512),
        max_cells=_env_int("US_LACEY_SHIPMENT_SPREADSHEET_MAX_CELLS", 100000, 1000, 2_000_000),
    )


def _bulk_message() -> str:
    return (
        "This file contains multiple shipments. "
        "Litoral Trace processes one shipment per operation. "
        "Split this file so it contains only one shipment, then upload that file to this operation."
    )


def _reject_for_size(size_bytes: int, limits: ShipmentSpreadsheetLimits) -> None:
    if size_bytes > limits.max_bytes:
        raise ShipmentBatchRejected(DATASET_TOO_LARGE, _bulk_message())


def inspect_csv_chunks_for_shipment(
    chunks: Iterable[bytes],
    *,
    size_bytes: int,
    limits: ShipmentSpreadsheetLimits | None = None,
) -> ShipmentSpreadsheetProfile:
    budget = limits or shipment_spreadsheet_limits()
    _reject_for_size(size_bytes, budget)
    try:
        profile = profile_csv_chunks(
            chunks,
            max_rows=budget.max_rows,
            max_columns=budget.max_columns,
            max_cells=budget.max_cells,
        )
    except TabularSafetyError as exc:
        detail = str(exc)
        if detail.startswith(("CSV_ROW_LIMIT_EXCEEDED", "CSV_COLUMN_LIMIT_EXCEEDED", "CSV_CELL_LIMIT_EXCEEDED")):
            raise ShipmentBatchRejected(DATASET_TOO_COMPLEX, _bulk_message()) from exc
        raise ShipmentBatchRejected("INVALID_SHIPMENT_SPREADSHEET", "The CSV could not be safely interpreted as a shipment spreadsheet.") from exc
    return ShipmentSpreadsheetProfile("CSV", size_bytes, profile.row_count, profile.max_columns, profile.total_cells)


_CELL_REF = re.compile(r"^([A-Za-z]+)([1-9][0-9]*)$")


def _check_sheet_xml_bounds(archive: ZipFile, name: str, budget: ShipmentSpreadsheetLimits) -> None:
    """Check occupied cell addresses even when the XLSX omits dimensions.

    A bounded iter_rows(max_col=N+1) cannot detect cells beyond that window;
    inspect the compressed sheet's actual references before invoking openpyxl.
    """
    rows_seen = 0
    cells_seen = 0
    row_cells = 0
    with archive.open(name) as stream:
        for event, element in ElementTree.iterparse(stream, events=("start", "end")):
            tag = element.tag.rsplit("}", 1)[-1]
            if event == "start":
                if tag == "row":
                    rows_seen += 1
                    row_cells = 0
                    reference = element.attrib.get("r")
                    if rows_seen > budget.max_rows or (
                        reference is not None
                        and (not reference.isdecimal() or int(reference) > budget.max_rows)
                    ):
                        raise ShipmentBatchRejected(DATASET_TOO_COMPLEX, _bulk_message())
                elif tag == "c":
                    cells_seen += 1
                    row_cells += 1
                    if cells_seen > budget.max_cells or row_cells > budget.max_columns:
                        raise ShipmentBatchRejected(DATASET_TOO_COMPLEX, _bulk_message())
                    reference = element.attrib.get("r")
                    if reference:
                        match = _CELL_REF.fullmatch(reference)
                        if match is None or int(match.group(2)) > budget.max_rows:
                            raise ShipmentBatchRejected(DATASET_TOO_COMPLEX, _bulk_message())
                        col = 0
                        for letter in match.group(1).upper():
                            col = col * 26 + ord(letter) - ord("A") + 1
                            if col > budget.max_columns:
                                raise ShipmentBatchRejected(DATASET_TOO_COMPLEX, _bulk_message())
            else:
                element.clear()


def _preflight_xlsx_archive(content: bytes, budget: ShipmentSpreadsheetLimits) -> None:
    """Bound ZIP expansion before openpyxl allocates shared strings or worksheets."""
    expanded_limit = min(128 * 1024 * 1024, max(32 * 1024 * 1024, budget.max_bytes * 20))
    max_xml_member = 64 * 1024 * 1024
    expanded = 0
    try:
        with ZipFile(BytesIO(content)) as archive:
            members = archive.infolist()
            if len(members) > 256 or not members:
                raise ShipmentBatchRejected(DATASET_TOO_COMPLEX, _bulk_message())
            names = {member.filename for member in members}
            if "[Content_Types].xml" not in names or "xl/workbook.xml" not in names:
                raise ShipmentBatchRejected("INVALID_SHIPMENT_SPREADSHEET", "The XLSX is missing its required structure.")
            for member in members:
                name = member.filename.replace("\\", "/")
                if (
                    name.startswith("/")
                    or any(part == ".." for part in name.split("/"))
                    or member.flag_bits & 1
                    or member.file_size > max_xml_member
                    or (member.file_size and member.compress_size == 0)
                    or member.file_size > max(1, member.compress_size) * 400
                ):
                    raise ShipmentBatchRejected(DATASET_TOO_COMPLEX, _bulk_message())
                expanded += member.file_size
                if expanded > expanded_limit:
                    raise ShipmentBatchRejected(DATASET_TOO_COMPLEX, _bulk_message())
            for member in members:
                if member.filename.startswith("xl/worksheets/") and member.filename.endswith(".xml"):
                    _check_sheet_xml_bounds(archive, member.filename, budget)
    except ShipmentBatchRejected:
        raise
    except (BadZipFile, OSError, EOFError, ValueError, ElementTree.ParseError) as exc:
        raise ShipmentBatchRejected(
            "INVALID_SHIPMENT_SPREADSHEET", "The XLSX could not be safely opened."
        ) from exc


def _inspect_xlsx(content: bytes, budget: ShipmentSpreadsheetLimits) -> ShipmentSpreadsheetProfile:
    _preflight_xlsx_archive(content, budget)
    rows_total = 0
    max_columns = 0
    cells_total = 0
    try:
        workbook = load_workbook(BytesIO(content), read_only=True, data_only=True)
    except Exception as exc:
        raise ShipmentBatchRejected("INVALID_SHIPMENT_SPREADSHEET", "The XLSX could not be safely opened.") from exc
    try:
        for worksheet in workbook.worksheets:
            if (worksheet.max_row or 0) > budget.max_rows or (worksheet.max_column or 0) > budget.max_columns:
                raise ShipmentBatchRejected(DATASET_TOO_COMPLEX, _bulk_message())
            for row in worksheet.iter_rows(
                max_row=min(worksheet.max_row or budget.max_rows + 1, budget.max_rows + 1),
                max_col=min(worksheet.max_column or budget.max_columns + 1, budget.max_columns + 1),
                values_only=True,
            ):
                # openpyxl pads to max_col for dimensionless workbooks.
                # Count only occupied column positions, not padding cells.
                width = max(
                    (index for index, value in enumerate(row, start=1) if value not in (None, "")),
                    default=0,
                )
                if width > budget.max_columns:
                    raise ShipmentBatchRejected(DATASET_TOO_COMPLEX, _bulk_message())
                if width == 0:
                    continue
                max_columns = max(max_columns, width)
                rows_total += 1
                cells_total += width
                if rows_total > budget.max_rows or cells_total > budget.max_cells:
                    raise ShipmentBatchRejected(DATASET_TOO_COMPLEX, _bulk_message())
    finally:
        workbook.close()
    return ShipmentSpreadsheetProfile("XLSX", len(content), rows_total, max_columns, cells_total)


def _inspect_xls(content: bytes, budget: ShipmentSpreadsheetLimits) -> ShipmentSpreadsheetProfile:
    try:
        import xlrd
        workbook = xlrd.open_workbook(file_contents=content, on_demand=True)
    except Exception as exc:
        raise ShipmentBatchRejected("INVALID_SHIPMENT_SPREADSHEET", "The XLS file could not be safely opened.") from exc
    rows_total = 0
    max_columns = 0
    cells_total = 0
    try:
        for sheet_name in workbook.sheet_names():
            sheet = workbook.sheet_by_name(sheet_name)
            rows_total += int(sheet.nrows)
            max_columns = max(max_columns, int(sheet.ncols))
            cells_total += int(sheet.nrows) * int(sheet.ncols)
            if rows_total > budget.max_rows or max_columns > budget.max_columns or cells_total > budget.max_cells:
                raise ShipmentBatchRejected(DATASET_TOO_COMPLEX, _bulk_message())
    finally:
        try:
            workbook.release_resources()
        except Exception:
            pass
    return ShipmentSpreadsheetProfile("XLS", len(content), rows_total, max_columns, cells_total)


def enforce_shipment_document_budget(
    *,
    filename: str,
    content: bytes,
    limits: ShipmentSpreadsheetLimits | None = None,
) -> ShipmentSpreadsheetProfile | None:
    """Fail before Vault/queue work if a spreadsheet is a bulk dataset."""
    suffix = PurePath(str(filename or "")).suffix.lower()
    if suffix not in {".csv", ".xlsx", ".xls"}:
        return None
    budget = limits or shipment_spreadsheet_limits()
    _reject_for_size(len(content), budget)
    if suffix == ".csv":
        return inspect_csv_chunks_for_shipment((content,), size_bytes=len(content), limits=budget)
    if suffix == ".xlsx":
        return _inspect_xlsx(content, budget)
    return _inspect_xls(content, budget)


def enforce_streamed_csv_budget(
    *,
    chunks: Iterable[bytes],
    size_bytes: int,
    limits: ShipmentSpreadsheetLimits | None = None,
) -> ShipmentSpreadsheetProfile:
    """Worker-side guard for files already stored before this hardening shipped."""
    return inspect_csv_chunks_for_shipment(chunks, size_bytes=size_bytes, limits=limits)
