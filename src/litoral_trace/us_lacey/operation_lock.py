"""PostgreSQL operation locks for U.S. Lacey evidence projection.

Different documents for the same shipment can be processed by independent workers.
Plant-line materialization and projection must therefore converge under concurrency,
not race while deciding which declaration lines already exist.
"""
from __future__ import annotations

from contextlib import contextmanager

from sqlalchemy import text

from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.us_lacey.db import get_us_lacey_db_session


@contextmanager
def us_lacey_operation_projection_lock(*, organization_id: int, operation_id: int):
    """Hold one transaction-scoped advisory lock for a shipment operation.

    PostgreSQL's two-int advisory key gives us a stable ``(tenant, operation)``
    namespace without adding schema state.  The lock is released automatically
    when the guard transaction ends, including exception paths.  Non-PostgreSQL
    test runtimes are a no-op because they cannot exercise database concurrency.
    """
    org_id = int(organization_id)
    op_id = int(operation_id)
    session = get_us_lacey_db_session()
    try:
        set_tenant_db_context(session, org_id)
        bind = session.get_bind()
        if bind.dialect.name == "postgresql":
            session.execute(
                text("SELECT pg_advisory_xact_lock(:organization_id, :operation_id)"),
                {"organization_id": org_id, "operation_id": op_id},
            )
        yield
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
