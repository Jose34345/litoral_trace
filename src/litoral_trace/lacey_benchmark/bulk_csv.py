"""Constant-memory CSV batching for DANE/CBP/FOIA benchmark datasets.

This module is intentionally separate from the customer shipment workflow. It
never creates one ExtractedDocumentField per raw cell and never assumes that a
bulk file represents one Lacey shipment.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import BinaryIO, Iterator

from litoral_trace.assurance.tabular_safety import iter_csv_rows_from_chunks


@dataclass(frozen=True, slots=True)
class BulkCsvBatch:
    headers: tuple[str, ...]
    start_row: int
    end_row: int
    rows: tuple[dict[str, str | None], ...]
    encoding: str
    delimiter: str


def _file_chunks(fileobj: BinaryIO, *, chunk_size: int = 256 * 1024):
    while True:
        chunk = fileobj.read(chunk_size)
        if not chunk:
            break
        yield chunk


def iter_bulk_csv_batches(
    fileobj: BinaryIO,
    *,
    batch_size: int = 500,
) -> Iterator[BulkCsvBatch]:
    if batch_size <= 0 or batch_size > 10000:
        raise ValueError("batch_size must be between 1 and 10000")
    reader, encoding, delimiter = iter_csv_rows_from_chunks(_file_chunks(fileobj))
    headers: tuple[str, ...] | None = None
    pending: list[dict[str, str | None]] = []
    data_row = 0
    start_row = 1
    for physical_index, row in enumerate(reader, start=1):
        if headers is None:
            nonempty = [str(value).strip() for value in row if str(value).strip()]
            if physical_index <= 25 and len(nonempty) >= 2:
                headers = tuple((str(value).strip() or f"column_{index + 1}")[:255] for index, value in enumerate(row))
                continue
            if physical_index >= 25:
                raise ValueError("CSV header not found")
            continue
        if not any(str(value).strip() for value in row):
            continue
        data_row += 1
        padded = list(row) + [""] * max(0, len(headers) - len(row))
        pending.append({header: (value.strip() or None) for header, value in zip(headers, padded[: len(headers)])})
        if len(pending) >= batch_size:
            yield BulkCsvBatch(headers, start_row, data_row, tuple(pending), encoding, delimiter)
            start_row = data_row + 1
            pending = []
    if headers is None:
        raise ValueError("CSV header not found")
    if pending:
        yield BulkCsvBatch(headers, start_row, data_row, tuple(pending), encoding, delimiter)
