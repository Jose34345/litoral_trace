# U.S. Lacey Memory / Batch Hardening

This gate protects the customer shipment workflow from bulk customs datasets and prepares heavy processing for a dedicated service.

## Shipment spreadsheet budgets

Defaults are deliberately conservative for a *single shipment*:

- `US_LACEY_SHIPMENT_SPREADSHEET_MAX_BYTES=5242880`
- `US_LACEY_SHIPMENT_SPREADSHEET_MAX_ROWS=5000`
- `US_LACEY_SHIPMENT_SPREADSHEET_MAX_COLUMNS=64`
- `US_LACEY_SHIPMENT_SPREADSHEET_MAX_CELLS=100000`
- `LT_ASSURANCE_RAW_CELL_PERSIST_LIMIT=2000`

A spreadsheet that exceeds a shipment budget is rejected before Vault/queue work. Legacy files that were queued before this gate are re-checked by the worker and fail permanently with `DATASET_TOO_LARGE_FOR_SHIPMENT_PIPELINE` or `DATASET_TOO_COMPLEX_FOR_SHIPMENT_PIPELINE`; those failures are not retried.

## Bulk benchmark datasets

DANE, CBP and similar multi-shipment datasets do not belong in `/operations/{id}/upload`.

Use the constant-memory importer instead:

```bash
python scripts/lacey_bulk_benchmark.py Abril.csv --batch-size 500
```

The importer streams the file, batches rows and emits a manifest. It never creates one customer operation from a multi-shipment source and never persists one ORM row per raw source cell.

## Dedicated worker cutover

A separate ASGI entrypoint now exists:

```bash
uvicorn litoral_trace.web.us_lacey_worker_app:app --host 0.0.0.0 --port $PORT
```

Deploy it as a separate Render service with the same U.S. Lacey database, worker database and Evidence Vault credentials as the customer web service. After the worker health check is green, set `US_LACEY_INLINE_WORKER_ENABLED=0` on the customer web service. This removes heavy parsing/OCR/Engine work from the customer web process.

Do not disable the inline worker until the dedicated worker has the required secrets and is healthy, otherwise queued work will not be consumed.
