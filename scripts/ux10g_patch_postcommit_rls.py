"""One-shot UX10-G patch helper.

Moves construction of immutable operation results before COMMIT so SQLAlchemy
never needs to refresh tenant-scoped ORM rows after transaction-local RLS
context has been cleared.
"""
from __future__ import annotations

from pathlib import Path


TARGET = Path("src/litoral_trace/services/traceability_operations.py")

REPLACEMENTS = (
    (
        '''            session.commit()\n            return DraftEventResult(\n                event_id=int(event.id),\n                event_public_id=event.public_id,\n                event_code=event.event_code,\n                event_type=event.event_type,\n                status=event.status,\n                output_batch_public_ids=(batch.public_id,),\n            )''',
        '''            result = DraftEventResult(\n                event_id=int(event.id),\n                event_public_id=event.public_id,\n                event_code=event.event_code,\n                event_type=event.event_type,\n                status=event.status,\n                output_batch_public_ids=(batch.public_id,),\n            )\n            session.commit()\n            return result''',
    ),
    (
        '''            session.commit()\n            return DraftEventResult(\n                event_id=int(event.id),\n                event_public_id=event.public_id,\n                event_code=event.event_code,\n                event_type=event.event_type,\n                status=event.status,\n                output_batch_public_ids=tuple(batch.public_id for batch in output_batches),\n            )''',
        '''            result = DraftEventResult(\n                event_id=int(event.id),\n                event_public_id=event.public_id,\n                event_code=event.event_code,\n                event_type=event.event_type,\n                status=event.status,\n                output_batch_public_ids=tuple(batch.public_id for batch in output_batches),\n            )\n            session.commit()\n            return result''',
    ),
    (
        '''            session.commit()\n            return DraftShipmentResult(\n                shipment_id=int(shipment.id),\n                shipment_public_id=shipment.public_id,\n                shipment_code=shipment.shipment_code,\n                status=shipment.status,\n            )''',
        '''            result = DraftShipmentResult(\n                shipment_id=int(shipment.id),\n                shipment_public_id=shipment.public_id,\n                shipment_code=shipment.shipment_code,\n                status=shipment.status,\n            )\n            session.commit()\n            return result''',
    ),
)


def main() -> None:
    source = TARGET.read_text(encoding="utf-8")
    updated = source

    for old, new in REPLACEMENTS:
        count = updated.count(old)
        if count != 1:
            raise SystemExit(
                "UX10-G patch is fail-closed: expected exactly one match, "
                f"found {count}."
            )
        updated = updated.replace(old, new, 1)

    if updated == source:
        raise SystemExit("UX10-G patch made no changes.")

    TARGET.write_text(updated, encoding="utf-8")
    print("UX10-G patch applied to receipt, process and shipment drafts.")


if __name__ == "__main__":
    main()
