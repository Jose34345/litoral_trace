"""Bounded tabular inspection helpers used before expensive document processing.

These helpers intentionally avoid materializing an entire CSV as decoded text or
as a list of rows. They are suitable for upload validation, worker preflight and
bulk benchmark ingestion where constant-memory iteration matters.
"""
from __future__ import annotations

from dataclasses import dataclass
import codecs
import csv
from itertools import chain
from typing import Iterable, Iterator

from litoral_trace.assurance.parsers import ParsedDocument, ParsedTable, SourceLocation


CSV_SAMPLE_BYTES = 64 * 1024
_SUPPORTED_ENCODINGS = ("utf-8-sig", "cp1252", "latin-1")


class TabularSafetyError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class CsvProfile:
    encoding: str
    delimiter: str
    header_row: int
    headers: tuple[str, ...]
    row_count: int
    max_columns: int
    total_cells: int


def _detect_encoding(sample: bytes) -> str:
    for encoding in _SUPPORTED_ENCODINGS:
        try:
            decoded = sample.decode(encoding)
        except UnicodeDecodeError:
            continue
        if "\x00" not in decoded:
            return encoding
    raise TabularSafetyError("No se pudo detectar una codificacion CSV soportada.")


def _detect_delimiter(sample_text: str) -> str:
    try:
        return csv.Sniffer().sniff(sample_text[:8192], delimiters=",;\t|").delimiter
    except csv.Error:
        return ";" if sample_text.count(";") >= sample_text.count(",") else ","


def _candidate_header(row: list[str]) -> bool:
    nonempty = [str(value).strip() for value in row if str(value).strip()]
    if len(nonempty) < 2:
        return False
    unique = len({value.casefold() for value in nonempty})
    text_like = sum(any(character.isalpha() for character in value) for value in nonempty)
    return unique >= 2 and text_like >= 1


def validate_csv_header_sample(content: bytes, *, sample_bytes: int = CSV_SAMPLE_BYTES) -> tuple[str, str]:
    """Validate CSV shape from a bounded prefix only; never parse the full upload."""
    if not content:
        raise TabularSafetyError("El CSV esta vacio.")
    sample = content[: max(1024, int(sample_bytes))]
    encoding = _detect_encoding(sample)
    try:
        sample_text = sample.decode(encoding)
    except UnicodeDecodeError as exc:
        raise TabularSafetyError("No se pudo leer la muestra CSV.") from exc
    delimiter = _detect_delimiter(sample_text)
    reader = csv.reader(sample_text.splitlines(), delimiter=delimiter)
    for index, row in enumerate(reader):
        if index >= 25:
            break
        if _candidate_header(row):
            return encoding, delimiter
    raise TabularSafetyError("El CSV no contiene una cabecera tabular util en la muestra inicial.")


def _decoded_lines(chunks: Iterable[bytes], *, encoding: str) -> Iterator[str]:
    """Incrementally decode byte chunks into physical text lines for csv.reader."""
    decoder = codecs.getincrementaldecoder(encoding)(errors="strict")
    buffer = ""
    for chunk in chunks:
        if not chunk:
            continue
        try:
            buffer += decoder.decode(bytes(chunk), final=False)
        except UnicodeDecodeError as exc:
            raise TabularSafetyError("El CSV contiene bytes invalidos para su codificacion.") from exc
        while True:
            newline = buffer.find("\n")
            if newline < 0:
                break
            line = buffer[: newline + 1]
            buffer = buffer[newline + 1 :]
            yield line
    try:
        buffer += decoder.decode(b"", final=True)
    except UnicodeDecodeError as exc:
        raise TabularSafetyError("El CSV termina con una secuencia de caracteres invalida.") from exc
    if buffer:
        yield buffer


def iter_csv_rows_from_chunks(
    chunks: Iterable[bytes],
    *,
    sample_bytes: int = CSV_SAMPLE_BYTES,
) -> tuple[Iterator[list[str]], str, str]:
    """Return a constant-memory CSV row iterator plus detected encoding/delimiter."""
    source = iter(chunks)
    prefix_parts: list[bytes] = []
    prefix_size = 0
    while prefix_size < sample_bytes:
        try:
            chunk = next(source)
        except StopIteration:
            break
        if not chunk:
            continue
        prefix_parts.append(bytes(chunk))
        prefix_size += len(chunk)
    if not prefix_parts:
        raise TabularSafetyError("El CSV esta vacio.")
    prefix = b"".join(prefix_parts)
    encoding = _detect_encoding(prefix[:sample_bytes])
    sample_text = prefix[:sample_bytes].decode(encoding, errors="strict")
    delimiter = _detect_delimiter(sample_text)
    rows = csv.reader(
        _decoded_lines(chain(prefix_parts, source), encoding=encoding),
        delimiter=delimiter,
    )
    return rows, encoding, delimiter


def profile_csv_chunks(
    chunks: Iterable[bytes],
    *,
    max_rows: int | None = None,
    max_columns: int | None = None,
    max_cells: int | None = None,
) -> CsvProfile:
    """Profile a CSV incrementally and fail as soon as a configured budget is exceeded."""
    rows, encoding, delimiter = iter_csv_rows_from_chunks(chunks)
    header_row = 0
    headers: tuple[str, ...] = ()
    data_rows = 0
    max_seen_columns = 0
    total_cells = 0
    for physical_index, row in enumerate(rows, start=1):
        width = len(row)
        max_seen_columns = max(max_seen_columns, width)
        if max_columns is not None and width > max_columns:
            raise TabularSafetyError(f"CSV_COLUMN_LIMIT_EXCEEDED:{width}:{max_columns}")
        if not headers:
            if physical_index <= 25 and _candidate_header(row):
                header_row = physical_index
                headers = tuple((str(value).strip() or f"column_{index + 1}")[:255] for index, value in enumerate(row))
            elif physical_index >= 25:
                raise TabularSafetyError("CSV_HEADER_NOT_FOUND")
            continue
        if not any(str(value).strip() for value in row):
            continue
        data_rows += 1
        total_cells += width
        if max_rows is not None and data_rows > max_rows:
            raise TabularSafetyError(f"CSV_ROW_LIMIT_EXCEEDED:{data_rows}:{max_rows}")
        if max_cells is not None and total_cells > max_cells:
            raise TabularSafetyError(f"CSV_CELL_LIMIT_EXCEEDED:{total_cells}:{max_cells}")
    if not headers:
        raise TabularSafetyError("CSV_HEADER_NOT_FOUND")
    return CsvProfile(
        encoding=encoding,
        delimiter=delimiter,
        header_row=header_row,
        headers=headers,
        row_count=data_rows,
        max_columns=max_seen_columns,
        total_cells=total_cells,
    )


def parse_csv_incremental_bytes(content: bytes) -> ParsedDocument:
    """Parse a bounded shipment CSV row-by-row without a full decoded-string/rows copy."""
    rows, encoding, delimiter = iter_csv_rows_from_chunks((content,))
    header_index: int | None = None
    headers: tuple[str, ...] = ()
    records: list[dict[str, object]] = []
    for physical_index, row in enumerate(rows, start=1):
        if header_index is None:
            if physical_index <= 25 and _candidate_header(row):
                header_index = physical_index
                headers = tuple((str(value).strip() or f"column_{index + 1}")[:255] for index, value in enumerate(row))
            elif physical_index >= 25:
                raise TabularSafetyError("El CSV no contiene una cabecera util.")
            continue
        padded = list(row) + [""] * max(0, len(headers) - len(row))
        record = {
            header: (value.strip() if isinstance(value, str) and value.strip() else None)
            for header, value in zip(headers, padded[: len(headers)])
        }
        if any(value is not None for value in record.values()):
            first = str(next((value for value in record.values() if value is not None), "")).strip().casefold()
            if first in {"total", "subtotal", "totales", "total general", "observaciones", "observacion"}:
                continue
            records.append(record)
    if header_index is None or not headers:
        raise TabularSafetyError("El CSV no contiene una cabecera util.")
    table = ParsedTable(
        name="csv",
        headers=headers,
        rows=tuple(records),
        source=SourceLocation(row=header_index, locator=f"csv:header_row:{header_index}"),
    )
    return ParsedDocument(
        file_kind="CSV",
        tables=(table,),
        metadata={"encoding": encoding, "delimiter": delimiter, "row_count": len(records), "streaming_parser": True},
    )
