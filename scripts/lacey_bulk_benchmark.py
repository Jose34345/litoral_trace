"""Stream a bulk CSV and emit a small ingestion manifest without loading it into RAM."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from litoral_trace.lacey_benchmark.bulk_csv import iter_bulk_csv_batches


def main() -> int:
    parser = argparse.ArgumentParser(description="Inspect a bulk benchmark CSV safely.")
    parser.add_argument("csv_path")
    parser.add_argument("--batch-size", type=int, default=500)
    args = parser.parse_args()
    path = Path(args.csv_path)
    digest = hashlib.sha256()
    with path.open("rb") as raw:
        while chunk := raw.read(1024 * 1024):
            digest.update(chunk)
    batches = 0
    rows = 0
    headers: tuple[str, ...] = ()
    encoding = ""
    delimiter = ""
    with path.open("rb") as raw:
        for batch in iter_bulk_csv_batches(raw, batch_size=args.batch_size):
            batches += 1
            rows = batch.end_row
            headers = batch.headers
            encoding = batch.encoding
            delimiter = batch.delimiter
    print(json.dumps({
        "filename": path.name,
        "size_bytes": path.stat().st_size,
        "sha256": digest.hexdigest(),
        "rows": rows,
        "columns": len(headers),
        "headers": headers,
        "encoding": encoding,
        "delimiter": delimiter,
        "batches": batches,
        "batch_size": args.batch_size,
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
