#!/usr/bin/env python
"""Idempotently discover U.S. Lacey supplier/product identities from history.

This backfill is intentionally non-authoritative. It may create DISCOVERED
supplier/product identities and exact structural operation/product links, but it
never calls the reusable-evidence promotion boundary and therefore never creates
VERIFIED/REUSABLE historical evidence from extraction alone.
"""
from __future__ import annotations

import argparse
import json
import logging
from typing import Any

from sqlalchemy import select

from litoral_trace.db.models import (
    UsLaceyOperation,
    UsLaceyOperationProductLink,
    UsLaceyProductIntelligenceSnapshot,
    UsLaceySourceSetRevision,
)
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.us_lacey.db import get_us_lacey_db_session
from litoral_trace.us_lacey.identity_memory import (
    resolve_operation_identity_memory,
)


LOGGER = logging.getLogger("backfill_us_lacey_identities")


def _current_snapshot(
    session,
    *,
    organization_id: int,
    operation_id: int,
):
    return session.execute(
        select(
            UsLaceySourceSetRevision,
            UsLaceyProductIntelligenceSnapshot,
        )
        .join(
            UsLaceyProductIntelligenceSnapshot,
            (
                UsLaceyProductIntelligenceSnapshot.source_set_revision_id
                == UsLaceySourceSetRevision.id
            )
            & (
                UsLaceyProductIntelligenceSnapshot.organization_id
                == UsLaceySourceSetRevision.organization_id
            ),
        )
        .where(
            UsLaceySourceSetRevision.organization_id
            == int(organization_id),
            UsLaceySourceSetRevision.operation_id == int(operation_id),
            UsLaceySourceSetRevision.is_current.is_(True),
            UsLaceyProductIntelligenceSnapshot.status != "STALE",
        )
        .order_by(UsLaceyProductIntelligenceSnapshot.id.desc())
        .limit(1)
    ).one_or_none()


def backfill(
    *,
    organization_id: int,
    operation_id: int | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Discover identity memory without promoting historical evidence."""
    org_id = int(organization_id)
    session = get_us_lacey_db_session()
    summary: dict[str, Any] = {
        "organization_id": org_id,
        "dry_run": bool(dry_run),
        "operations_scanned": 0,
        "operations_with_product_intelligence": 0,
        "suppliers_discovered": 0,
        "products_discovered": 0,
        "product_links_discovered": 0,
        "ambiguous_supplier_operations": 0,
        "errors": [],
    }
    try:
        set_tenant_db_context(session, org_id)
        query = (
            select(UsLaceyOperation)
            .where(UsLaceyOperation.organization_id == org_id)
            .order_by(UsLaceyOperation.id.asc())
        )
        if operation_id is not None:
            query = query.where(
                UsLaceyOperation.id == int(operation_id)
            )
        operations = session.scalars(query).all()

        for operation in operations:
            summary["operations_scanned"] += 1
            current = _current_snapshot(
                session,
                organization_id=org_id,
                operation_id=int(operation.id),
            )
            if current is None:
                continue
            revision, snapshot = current
            summary["operations_with_product_intelligence"] += 1

            try:
                with session.begin_nested():
                    result = resolve_operation_identity_memory(
                        session,
                        organization_id=org_id,
                        operation_id=int(operation.id),
                        product_payload=dict(snapshot.payload_json or {}),
                        source_set_revision_id=int(revision.id),
                        discovery_status="DISCOVERED",
                    )
                    direct_link_count = len(
                        session.scalars(
                            select(UsLaceyOperationProductLink.id).where(
                                UsLaceyOperationProductLink.organization_id
                                == org_id,
                                UsLaceyOperationProductLink.operation_id
                                == int(operation.id),
                                UsLaceyOperationProductLink.source_set_revision_id
                                == int(revision.id),
                            )
                        ).all()
                    )
                summary["suppliers_discovered"] += int(
                    result.supplier_count
                )
                summary["products_discovered"] += int(
                    result.product_count
                )
                summary["product_links_discovered"] += int(
                    direct_link_count
                )
                if result.ambiguous_supplier_count:
                    summary["ambiguous_supplier_operations"] += 1
            except Exception as exc:
                LOGGER.exception(
                    "Identity backfill failed closed for operation",
                    extra={
                        "organization_id": org_id,
                        "operation_id": int(operation.id),
                    },
                )
                summary["errors"].append(
                    {
                        "operation_id": int(operation.id),
                        "error": type(exc).__name__,
                    }
                )

        if dry_run:
            session.rollback()
        else:
            session.commit()
        return summary
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Discover non-authoritative U.S. Lacey supplier/product "
            "identities from existing operations."
        )
    )
    parser.add_argument(
        "--organization-id",
        required=True,
        type=int,
        help="Tenant organization id. Cross-tenant scans are intentionally disabled.",
    )
    parser.add_argument(
        "--operation-id",
        type=int,
        default=None,
        help="Optional single internal operation id.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Resolve and report but roll back all writes.",
    )
    return parser


def main() -> int:
    args = _parser().parse_args()
    logging.basicConfig(level=logging.INFO)
    result = backfill(
        organization_id=args.organization_id,
        operation_id=args.operation_id,
        dry_run=args.dry_run,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 1 if result["errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
