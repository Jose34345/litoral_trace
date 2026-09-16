"""Persistent content-addressed cache lookup for specialized Lacey inference.

The cache stores/reuses computational output only. It never copies operation fields,
human decisions, review timestamps, operation status, or any other administrative
state. Tenant identity is part of the cache key and lookup boundary.
"""
from __future__ import annotations

from collections import Counter, defaultdict
import hashlib
import json
from typing import Iterable, Mapping

from sqlalchemy import select

from litoral_trace.db.models import AssuranceDocument, UsLaceyEngineDocumentRun
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.us_lacey.db import get_us_lacey_db_session


def specialized_computation_fingerprint(
    *,
    organization_id: int,
    documents: Iterable[Mapping[str, object]],
    engine_version: str,
    provider: str,
    model: str,
    max_pages: int,
    judge_mode: str,
    projection_mode: str,
    specialized_schema_version: str,
    field_judge_version: str,
    projection_version: str,
) -> str:
    """Return the immutable computational identity for one specialized source set.

    Unknown mapping keys are deliberately ignored. In particular operation IDs,
    operation-document IDs and assurance IDs never participate in this identity.
    """
    source_descriptors = sorted(
        (
            {
                "sha256": str(document.get("sha256") or ""),
                "role_hint": str(document.get("role_hint") or ""),
                "filename": str(document.get("filename") or ""),
            }
            for document in documents
        ),
        key=lambda item: (item["sha256"], item["role_hint"], item["filename"]),
    )
    payload = {
        "organization_id": int(organization_id),
        "documents": source_descriptors,
        "engine_version": str(engine_version),
        "provider": str(provider),
        "model": str(model),
        "max_pages": int(max_pages),
        "specialized_schema_version": str(specialized_schema_version),
        "field_judge_version": str(field_judge_version),
        "judge_mode": str(judge_mode),
        "projection_version": str(projection_version),
        "projection_mode": str(projection_mode),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _descriptor_from_document(document: object) -> tuple[str, str, str]:
    return (
        str(getattr(document, "source_sha256", "") or ""),
        str(getattr(document, "role_hint", "") or ""),
        str(getattr(document, "filename", "") or ""),
    )


def _descriptor_from_payload(row: UsLaceyEngineDocumentRun) -> tuple[str, str, str] | None:
    payload = row.resolution_json
    if not isinstance(payload, Mapping):
        return None
    cached = payload.get("cache_document")
    if not isinstance(cached, Mapping):
        return None
    return (
        str(cached.get("sha256") or ""),
        str(cached.get("role_hint") or ""),
        str(cached.get("filename") or ""),
    )


def find_cached_specialized_payloads(
    *,
    documents: tuple[object, ...],
    computation_fingerprint: str,
    schema_version: str,
) -> tuple[Mapping[str, object], ...] | None:
    """Return one complete prior source-set snapshot, or ``None`` fail-closed.

    A cache hit requires the exact multiset of content/role/filename descriptors. A
    partial run, duplicate ambiguity, different tenant, failed run, old schema, or
    malformed payload is a miss.
    """
    if not documents:
        return None
    assurance_ids = [int(getattr(document, "assurance_document_id")) for document in documents]
    session = get_us_lacey_db_session()
    try:
        organization_ids = set(
            session.scalars(
                select(AssuranceDocument.organization_id).where(
                    AssuranceDocument.id.in_(assurance_ids)
                )
            ).all()
        )
        if len(organization_ids) != 1:
            return None
        organization_id = int(next(iter(organization_ids)))
        set_tenant_db_context(session, organization_id)

        source_hashes = {str(getattr(document, "source_sha256", "")) for document in documents}
        rows = session.scalars(
            select(UsLaceyEngineDocumentRun)
            .where(
                UsLaceyEngineDocumentRun.organization_id == organization_id,
                UsLaceyEngineDocumentRun.schema_version == schema_version,
                UsLaceyEngineDocumentRun.status == "SUCCEEDED",
                UsLaceyEngineDocumentRun.source_sha256.in_(source_hashes),
            )
            .order_by(UsLaceyEngineDocumentRun.id.desc())
        ).all()

        groups: dict[int, list[UsLaceyEngineDocumentRun]] = defaultdict(list)
        for row in rows:
            payload = row.resolution_json
            if not isinstance(payload, Mapping):
                continue
            if str(payload.get("computation_fingerprint") or "") != computation_fingerprint:
                continue
            groups[int(row.operation_id)].append(row)

        expected = Counter(_descriptor_from_document(document) for document in documents)
        for _operation_id, group in sorted(groups.items(), reverse=True):
            if len(group) != len(documents):
                continue
            descriptors = [_descriptor_from_payload(row) for row in group]
            if any(descriptor is None for descriptor in descriptors):
                continue
            if Counter(descriptor for descriptor in descriptors if descriptor is not None) != expected:
                continue
            payloads = tuple(
                row.resolution_json
                for row in sorted(group, key=lambda item: item.id)
                if isinstance(row.resolution_json, Mapping)
            )
            if len(payloads) == len(documents):
                return payloads
        return None
    finally:
        session.close()
