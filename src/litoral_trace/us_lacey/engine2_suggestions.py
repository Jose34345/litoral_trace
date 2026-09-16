"""Compatibility facade for canonical Engine 2 shipment publication.

The former suggestion projector independently interpreted shipment evidence and wrote
PPQ review fields. That created a second final representation beside
CanonicalShipmentTruth. Runtime callers keep this public function during the cutover,
but publication now has exactly one authority: the canonical shipment publisher.
"""
from __future__ import annotations

from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.us_lacey.canonical_shipment_truth import publish_canonical_shipment_truth
from litoral_trace.us_lacey.db import get_us_lacey_db_session


def project_engine2_supported_suggestions(*, organization_id: int, operation_id: int) -> int:
    """Publish the latest Engine 2 shipment run through CanonicalShipmentTruth only.

    The function name is retained temporarily as a compatibility seam for the worker
    and any internal callers. It no longer contains projection/reconciliation logic.
    Human-reviewed values and candidate audit history are preserved by the canonical
    publisher itself.
    """
    session = get_us_lacey_db_session()
    try:
        set_tenant_db_context(session, organization_id)
        result = publish_canonical_shipment_truth(
            session,
            organization_id=organization_id,
            operation_id=operation_id,
        )
        session.commit()
        return int(result.field_count)
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
