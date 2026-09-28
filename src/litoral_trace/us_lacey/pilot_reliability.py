"""Passive pilot-quality snapshots and anomaly detection for U.S. Lacey.

This module deliberately observes durable processing outputs without becoming part
of regulatory truth. Raw document text and customs values may be inspected
transiently only to identify structural rows/totals; they are never persisted in
quality snapshots or incident manifests.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from hashlib import sha256
import re
from typing import Mapping
from uuid import UUID, uuid4

from sqlalchemy import func, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from litoral_trace.db.models.us_lacey import (
    UsLaceyEngineShipmentRun,
    UsLaceyOperation,
    UsLaceyOperationField,
    UsLaceyPpqPlantLine,
    UsLaceySourceSetRevision,
)
from litoral_trace.db.models.us_lacey_commercial import UsLaceyProcessingJob
from litoral_trace.db.models.us_lacey_pilot_reliability import (
    PilotIncidentSeverity,
    PilotIncidentStatus,
    PilotQualityTrigger,
    UsLaceyPilotIncident,
    UsLaceyPilotQualitySnapshot,
)
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.lacey_engine.domain import DocumentType
from litoral_trace.us_lacey.canonical_shipment_truth import CANONICAL_PUBLISHER_VERSION
from litoral_trace.us_lacey.db import get_us_lacey_db_session
from litoral_trace.us_lacey.worker_db import get_us_lacey_worker_db_session


COMMERCIAL_DOCUMENT_TYPES = frozenset(
    {
        DocumentType.COMMERCIAL_INVOICE.value,
        DocumentType.CUSTOMS_ENTRY_SUMMARY.value,
        DocumentType.PACKING_LIST.value,
    }
)
STRUCTURAL_COMMERCIAL_FIELDS = frozenset(
    {
        "description",
        "entered_value",
        "hts_code",
        "plant_quantity",
        "metric_unit",
    }
)
ACTION_REQUIRED_STATUSES = frozenset(
    {"MISSING", "CONFLICT", "REVIEW", "REVIEW_REQUIRED"}
)
AUTO_RESOLVED_STATUSES = frozenset(
    {"SUPPORTED", "FOUND", "SUPPORTED MULTIPLE", "SUPPORTED_MULTIPLE"}
)
CONFIRMED_STATUSES = frozenset({"MATCHED", "NOT_REQUIRED"})
TERMINAL_PROCESSING_STATUSES = frozenset(
    {"REVIEW_REQUIRED", "READY_FOR_REVIEW", "COMPLETED"}
)
TOTAL_MARKER = re.compile(r"\b(?:grand\s+total|sub\s*total|subtotal|total)\b", re.IGNORECASE)


@dataclass(frozen=True, slots=True)
class PilotQualitySnapshotData:
    organization_id: int
    operation_id: int
    operation_public_id: UUID
    attribution_session_id: UUID | None
    trigger: PilotQualityTrigger
    source_set_fingerprint: str | None
    engine_version: str | None
    canonical_publisher_version: str
    document_count: int
    valid_document_count: int
    logical_document_count: int
    document_type_counts: dict[str, int]
    commercial_line_count: int
    canonical_line_count: int
    auto_resolved_count: int
    action_required_count: int
    confirmed_count: int
    conflict_count: int
    total_field_count: int
    processing_duration_ms: int | None
    export_ready: bool


@dataclass(frozen=True, slots=True)
class PilotQualityAnomaly:
    detector_code: str
    severity: PilotIncidentSeverity
    reason: str


@dataclass(frozen=True, slots=True)
class PilotReliabilityCapture:
    snapshot: UsLaceyPilotQualitySnapshot
    incidents: tuple[UsLaceyPilotIncident, ...]


def _decimal(value: object) -> Decimal | None:
    text = str(value or "").strip().replace(",", "")
    if not text:
        return None
    try:
        return Decimal(text)
    except InvalidOperation:
        return None


def _safe_document_type(value: object) -> str:
    normalized = str(value or "").strip().upper()
    try:
        return DocumentType(normalized).value
    except ValueError:
        return DocumentType.UNKNOWN.value


def _document_catalog(payload: Mapping) -> tuple[dict[str, str], Counter[str], int]:
    by_id: dict[str, str] = {}
    counts: Counter[str] = Counter()
    valid = 0
    documents = payload.get("documents")
    if not isinstance(documents, list):
        return by_id, counts, valid

    for raw in documents:
        if not isinstance(raw, Mapping):
            continue
        document_id = str(raw.get("document_id") or "").strip()
        resolution = raw.get("resolution")
        if not document_id or not isinstance(resolution, Mapping):
            continue
        document_type = _safe_document_type(resolution.get("document_type"))
        by_id[document_id] = document_type
        counts[document_type] += 1

        confidence = resolution.get("type_confidence")
        try:
            confidence_value = float(confidence)
        except (TypeError, ValueError):
            confidence_value = 0.0
        if (
            document_type not in {DocumentType.UNKNOWN.value, DocumentType.OTHER.value}
            and confidence_value >= 0.5
        ):
            valid += 1
    return by_id, counts, valid


def _source_text(evidence: Mapping) -> str:
    candidate = evidence.get("candidate")
    if not isinstance(candidate, Mapping):
        return ""
    provenance = candidate.get("provenance")
    if not isinstance(provenance, Mapping):
        return ""
    return str(provenance.get("source_text") or "")


def _structural_row_key(evidence: Mapping) -> str | None:
    document_id = str(evidence.get("document_id") or "").strip()
    if not document_id:
        return None

    line_key = str(evidence.get("line_key") or "").strip()
    if line_key:
        return f"{document_id}|{line_key}"

    candidate = evidence.get("candidate")
    provenance = candidate.get("provenance") if isinstance(candidate, Mapping) else None
    source_block = (
        provenance.get("source_block") if isinstance(provenance, Mapping) else None
    )
    if not isinstance(source_block, Mapping):
        return None

    row_index = source_block.get("row_index")
    if row_index is None:
        return None
    table_id = str(source_block.get("table_id") or "").strip() or "table"
    return f"{document_id}|{table_id}|row:{row_index}"


def estimate_commercial_line_count(payload: Mapping) -> int:
    """Estimate real commercial rows independently of Canonical Truth.

    The estimator never requires HTS+taxon. It uses only recognized commercial
    document types and structural row identities produced by Engine 2. Aggregate
    rows are excluded by explicit total markers, shipment-total evidence, or a
    conservative mathematical rule: a non-HTS row equal to the sum of at least
    two other entered-value rows in the same document.
    """
    document_types, _counts, _valid = _document_catalog(payload)
    fields = payload.get("canonical_fields")
    if not isinstance(fields, Mapping):
        return 0

    rows_by_document: dict[str, set[str]] = defaultdict(set)
    hts_rows_by_document: dict[str, set[str]] = defaultdict(set)
    entered_values: dict[str, dict[str, set[Decimal]]] = defaultdict(
        lambda: defaultdict(set)
    )
    total_rows: dict[str, set[str]] = defaultdict(set)

    for field_name, raw_field in fields.items():
        if not isinstance(raw_field, Mapping):
            continue
        evidence_rows = raw_field.get("supporting_evidence")
        if not isinstance(evidence_rows, list):
            continue

        for evidence in evidence_rows:
            if not isinstance(evidence, Mapping):
                continue
            document_id = str(evidence.get("document_id") or "").strip()
            if document_types.get(document_id) not in COMMERCIAL_DOCUMENT_TYPES:
                continue
            row_key = _structural_row_key(evidence)
            if row_key is None:
                continue

            if field_name in STRUCTURAL_COMMERCIAL_FIELDS:
                rows_by_document[document_id].add(row_key)

            if field_name == "hts_code":
                hts_rows_by_document[document_id].add(row_key)

            if field_name in {"entered_value", "shipment_total_entered_value"}:
                number = _decimal(evidence.get("normalized_value"))
                if number is not None:
                    entered_values[document_id][row_key].add(number)

            if field_name == "shipment_total_entered_value" or TOTAL_MARKER.search(
                _source_text(evidence)
            ):
                total_rows[document_id].add(row_key)

    # Mathematical total suppression is intentionally conservative. A candidate
    # carrying HTS is treated as a real merchandise row even if arithmetic happens
    # to match other rows.
    for document_id, values_by_row in entered_values.items():
        single_values = {
            row_key: next(iter(values))
            for row_key, values in values_by_row.items()
            if len(values) == 1
        }
        if len(single_values) < 3:
            continue
        for row_key, value in single_values.items():
            if row_key in hts_rows_by_document.get(document_id, set()):
                continue
            others = [
                other_value
                for other_key, other_value in single_values.items()
                if other_key != row_key
            ]
            if len(others) >= 2 and value == sum(others, Decimal("0")):
                total_rows[document_id].add(row_key)

    counts = [
        len(rows - total_rows.get(document_id, set()))
        for document_id, rows in rows_by_document.items()
        if rows
    ]
    # Max is deliberately conservative for anomaly detection: a richer packing
    # list should raise the structural baseline rather than create a false P0.
    return max(counts, default=0)


def _processing_duration_ms(
    *,
    session: Session,
    organization_id: int,
    operation_id: int,
    engine_run: UsLaceyEngineShipmentRun | None,
    watchdog: bool = False,
) -> int | None:
    if engine_run is not None:
        revision = session.scalar(
            select(UsLaceySourceSetRevision)
            .where(
                UsLaceySourceSetRevision.organization_id == organization_id,
                UsLaceySourceSetRevision.operation_id == operation_id,
                UsLaceySourceSetRevision.source_set_fingerprint
                == engine_run.source_set_fingerprint,
            )
            .order_by(UsLaceySourceSetRevision.generation.desc())
            .limit(1)
        )
        if revision is not None and revision.created_at and engine_run.created_at:
            duration = engine_run.created_at - revision.created_at
            return max(0, int(duration.total_seconds() * 1000))

    jobs = tuple(
        session.scalars(
            select(UsLaceyProcessingJob).where(
                UsLaceyProcessingJob.organization_id == organization_id,
                UsLaceyProcessingJob.operation_id == operation_id,
            )
        ).all()
    )
    starts = [job.started_at for job in jobs if job.started_at is not None]
    ends = [job.completed_at for job in jobs if job.completed_at is not None]
    if not starts:
        return None
    if watchdog:
        ends.append(datetime.now(timezone.utc))
    if not ends:
        return None
    duration = max(ends) - min(starts)
    return max(0, int(duration.total_seconds() * 1000))


class PilotQualitySnapshotBuilder:
    """Build a privacy-bounded snapshot from one terminal operation."""

    @staticmethod
    def build(
        session: Session,
        *,
        organization_id: int,
        operation_id: int,
        trigger: PilotQualityTrigger,
        attribution_session_id: UUID | None = None,
    ) -> PilotQualitySnapshotData:
        operation = session.scalar(
            select(UsLaceyOperation).where(
                UsLaceyOperation.organization_id == int(organization_id),
                UsLaceyOperation.id == int(operation_id),
            )
        )
        if operation is None:
            raise ValueError("Pilot quality operation was not found.")
        operation_status = str(operation.status or "").upper()
        if trigger == PilotQualityTrigger.WATCHDOG:
            if operation_status != "PROCESSING":
                raise ValueError(
                    "WATCHDOG snapshots require an operation still in PROCESSING."
                )
        elif operation_status not in TERMINAL_PROCESSING_STATUSES:
            raise ValueError("Pilot quality snapshots require a terminal processing state.")

        engine_run = session.scalar(
            select(UsLaceyEngineShipmentRun)
            .where(
                UsLaceyEngineShipmentRun.organization_id == int(organization_id),
                UsLaceyEngineShipmentRun.operation_id == int(operation_id),
            )
            .order_by(UsLaceyEngineShipmentRun.id.desc())
            .limit(1)
        )
        if engine_run is None and trigger != PilotQualityTrigger.WATCHDOG:
            raise ValueError("Pilot quality snapshot requires an Engine 2 shipment run.")

        resolution = (
            engine_run.resolution_json
            if engine_run is not None and isinstance(engine_run.resolution_json, Mapping)
            else {}
        )
        if engine_run is not None and not resolution:
            raise ValueError("Pilot quality snapshot requires structured Engine 2 output.")

        _types_by_id, type_counts, valid_document_count = _document_catalog(resolution)
        logical_documents = resolution.get("documents")
        logical_document_count = (
            len(logical_documents) if isinstance(logical_documents, list) else 0
        )

        statuses = tuple(
            str(value or "").upper()
            for value in session.scalars(
                select(UsLaceyOperationField.field_status).where(
                    UsLaceyOperationField.organization_id == int(organization_id),
                    UsLaceyOperationField.operation_id == int(operation_id),
                )
            ).all()
        )
        total_field_count = len(statuses)
        auto_resolved_count = sum(status in AUTO_RESOLVED_STATUSES for status in statuses)
        action_required_count = sum(
            status in ACTION_REQUIRED_STATUSES for status in statuses
        )
        confirmed_count = sum(status in CONFIRMED_STATUSES for status in statuses)
        conflict_count = sum(status == "CONFLICT" for status in statuses)

        canonical_line_count = int(
            session.scalar(
                select(func.count(UsLaceyPpqPlantLine.id)).where(
                    UsLaceyPpqPlantLine.organization_id == int(organization_id),
                    UsLaceyPpqPlantLine.operation_id == int(operation_id),
                )
            )
            or 0
        )

        return PilotQualitySnapshotData(
            organization_id=int(organization_id),
            operation_id=int(operation_id),
            operation_public_id=operation.public_id,
            attribution_session_id=attribution_session_id,
            trigger=trigger,
            source_set_fingerprint=(
                engine_run.source_set_fingerprint if engine_run is not None else None
            ),
            engine_version=engine_run.engine_version if engine_run is not None else None,
            canonical_publisher_version=CANONICAL_PUBLISHER_VERSION,
            document_count=int(operation.document_count),
            valid_document_count=int(valid_document_count),
            logical_document_count=int(logical_document_count),
            document_type_counts=dict(sorted(type_counts.items())),
            commercial_line_count=estimate_commercial_line_count(resolution),
            canonical_line_count=canonical_line_count,
            auto_resolved_count=auto_resolved_count,
            action_required_count=action_required_count,
            confirmed_count=confirmed_count,
            conflict_count=conflict_count,
            total_field_count=total_field_count,
            processing_duration_ms=_processing_duration_ms(
                session=session,
                organization_id=int(organization_id),
                operation_id=int(operation_id),
                engine_run=engine_run,
                watchdog=trigger == PilotQualityTrigger.WATCHDOG,
            ),
            export_ready=action_required_count == 0,
        )

    @staticmethod
    def persist(
        session: Session,
        snapshot: PilotQualitySnapshotData,
    ) -> UsLaceyPilotQualitySnapshot:
        row = UsLaceyPilotQualitySnapshot(
            organization_id=snapshot.organization_id,
            operation_id=snapshot.operation_id,
            attribution_session_id=snapshot.attribution_session_id,
            trigger=snapshot.trigger.value,
            source_set_fingerprint=snapshot.source_set_fingerprint,
            engine_version=snapshot.engine_version,
            canonical_publisher_version=snapshot.canonical_publisher_version,
            document_count=snapshot.document_count,
            valid_document_count=snapshot.valid_document_count,
            logical_document_count=snapshot.logical_document_count,
            document_type_counts=snapshot.document_type_counts,
            commercial_line_count=snapshot.commercial_line_count,
            canonical_line_count=snapshot.canonical_line_count,
            auto_resolved_count=snapshot.auto_resolved_count,
            action_required_count=snapshot.action_required_count,
            confirmed_count=snapshot.confirmed_count,
            conflict_count=snapshot.conflict_count,
            total_field_count=snapshot.total_field_count,
            processing_duration_ms=snapshot.processing_duration_ms,
            export_ready=snapshot.export_ready,
        )
        session.add(row)
        session.flush()
        return row


class PilotQualityGuard:
    """Pure anomaly detector over privacy-bounded quality metrics."""

    ACTION_REQUIRED_RATIO = Decimal("0.60")

    @classmethod
    def evaluate(
        cls,
        snapshot: PilotQualitySnapshotData,
    ) -> tuple[PilotQualityAnomaly, ...]:
        anomalies: list[PilotQualityAnomaly] = []

        if snapshot.commercial_line_count > 0:
            delta = snapshot.canonical_line_count - snapshot.commercial_line_count
            ratio_spike = (
                snapshot.canonical_line_count
                > snapshot.commercial_line_count * 2
            )
            absolute_spike = delta >= 4
            if ratio_spike or absolute_spike:
                anomalies.append(
                    PilotQualityAnomaly(
                        detector_code="LINE_FRAGMENTATION_SPIKE",
                        severity=PilotIncidentSeverity.P0,
                        reason=(
                            "Canonical line count materially exceeds the structural "
                            "commercial-line baseline."
                        ),
                    )
                )

        if (
            snapshot.valid_document_count >= 3
            and snapshot.total_field_count > 0
            and (
                Decimal(snapshot.action_required_count)
                / Decimal(snapshot.total_field_count)
            )
            > cls.ACTION_REQUIRED_RATIO
        ):
            anomalies.append(
                PilotQualityAnomaly(
                    detector_code="ACTION_REQUIRED_SPIKE",
                    severity=PilotIncidentSeverity.P1,
                    reason="Action-required ratio exceeds the pilot quality threshold.",
                )
            )

        if (
            snapshot.valid_document_count >= 3
            and snapshot.auto_resolved_count == 0
        ):
            anomalies.append(
                PilotQualityAnomaly(
                    detector_code="ZERO_AUTOMATION",
                    severity=PilotIncidentSeverity.P1,
                    reason="No fields were auto-resolved across a multi-document packet.",
                )
            )

        return tuple(anomalies)


_MANIFEST_SCHEMA_VERSION = 1


def incident_fingerprint(
    *,
    operation_public_id: UUID,
    detector_code: str,
    engine_version: str | None,
) -> str:
    raw = (
        f"{operation_public_id}|{str(detector_code).strip().upper()}|"
        f"{str(engine_version or 'UNKNOWN').strip()}"
    )
    return sha256(raw.encode("utf-8")).hexdigest()


def build_diagnostic_manifest(
    snapshot: PilotQualitySnapshotData,
    anomaly: PilotQualityAnomaly,
) -> dict:
    """Create an allowlisted diagnostic payload containing no customer values."""
    return {
        "schema_version": _MANIFEST_SCHEMA_VERSION,
        "detector": {
            "code": anomaly.detector_code,
            "severity": anomaly.severity.value,
        },
        "operation_public_id": str(snapshot.operation_public_id),
        "trigger": snapshot.trigger.value,
        "versions": {
            "engine": snapshot.engine_version,
            "canonical_publisher": snapshot.canonical_publisher_version,
        },
        "documents": {
            "physical_count": snapshot.document_count,
            "valid_count": snapshot.valid_document_count,
            "logical_count": snapshot.logical_document_count,
            "by_type": dict(snapshot.document_type_counts),
        },
        "lines": {
            "commercial_structural": snapshot.commercial_line_count,
            "canonical": snapshot.canonical_line_count,
        },
        "fields": {
            "total": snapshot.total_field_count,
            "auto_resolved": snapshot.auto_resolved_count,
            "action_required": snapshot.action_required_count,
            "confirmed": snapshot.confirmed_count,
            "conflicts": snapshot.conflict_count,
        },
        "processing": {
            "duration_ms": snapshot.processing_duration_ms,
            "export_ready": snapshot.export_ready,
        },
    }


def _persist_incident(
    session: Session,
    *,
    snapshot_row: UsLaceyPilotQualitySnapshot,
    snapshot: PilotQualitySnapshotData,
    anomaly: PilotQualityAnomaly,
    fingerprint_version: str | None = None,
) -> UsLaceyPilotIncident:
    fingerprint = incident_fingerprint(
        operation_public_id=snapshot.operation_public_id,
        detector_code=anomaly.detector_code,
        engine_version=(
            fingerprint_version
            if fingerprint_version is not None
            else snapshot.engine_version
        ),
    )
    incident_public_id = uuid4()
    values = {
        "incident_public_id": incident_public_id,
        "organization_id": snapshot.organization_id,
        "operation_id": snapshot.operation_id,
        "quality_snapshot_id": snapshot_row.id,
        "attribution_session_id": snapshot.attribution_session_id,
        "detector_code": anomaly.detector_code,
        "severity": anomaly.severity.value,
        "status": PilotIncidentStatus.OPEN.value,
        "fingerprint": fingerprint,
        "diagnostic_manifest": build_diagnostic_manifest(snapshot, anomaly),
    }
    session.execute(
        pg_insert(UsLaceyPilotIncident)
        .values(**values)
        .on_conflict_do_nothing(
            index_elements=[UsLaceyPilotIncident.fingerprint]
        )
    )
    incident = session.scalar(
        select(UsLaceyPilotIncident).where(
            UsLaceyPilotIncident.fingerprint == fingerprint
        )
    )
    if incident is None:
        raise RuntimeError("Pilot incident could not be persisted.")
    return incident


def _operation_attribution_session_id(
    session: Session,
    *,
    organization_id: int,
    operation_id: int,
) -> UUID | None:
    return session.scalar(
        text(
            "SELECT public.us_lacey_pilot_operation_attribution("
            ":organization_id, :operation_id)"
        ),
        {
            "organization_id": int(organization_id),
            "operation_id": int(operation_id),
        },
    )


def _processing_quality_trigger(
    session: Session,
    *,
    organization_id: int,
    operation_id: int,
) -> PilotQualityTrigger:
    prior_processing_snapshot = session.scalar(
        select(UsLaceyPilotQualitySnapshot.id)
        .where(
            UsLaceyPilotQualitySnapshot.organization_id == int(organization_id),
            UsLaceyPilotQualitySnapshot.operation_id == int(operation_id),
            UsLaceyPilotQualitySnapshot.trigger.in_(
                (
                    PilotQualityTrigger.INITIAL_PROCESS.value,
                    PilotQualityTrigger.REPROCESS.value,
                )
            ),
        )
        .limit(1)
    )
    return (
        PilotQualityTrigger.REPROCESS
        if prior_processing_snapshot is not None
        else PilotQualityTrigger.INITIAL_PROCESS
    )


def _capture_with_sessions(
    *,
    read_session: Session,
    write_session: Session,
    organization_id: int,
    operation_id: int,
    trigger: PilotQualityTrigger,
    attribution_session_id: UUID | None,
    direct_anomaly: PilotQualityAnomaly | None = None,
    fingerprint_version: str | None = None,
) -> PilotReliabilityCapture:
    snapshot = PilotQualitySnapshotBuilder.build(
        read_session,
        organization_id=int(organization_id),
        operation_id=int(operation_id),
        trigger=trigger,
        attribution_session_id=attribution_session_id,
    )
    snapshot_row = PilotQualitySnapshotBuilder.persist(write_session, snapshot)
    anomalies = (
        (direct_anomaly,)
        if direct_anomaly is not None
        else PilotQualityGuard.evaluate(snapshot)
    )
    incidents = tuple(
        _persist_incident(
            write_session,
            snapshot_row=snapshot_row,
            snapshot=snapshot,
            anomaly=anomaly,
            fingerprint_version=fingerprint_version,
        )
        for anomaly in anomalies
    )
    return PilotReliabilityCapture(
        snapshot=snapshot_row,
        incidents=incidents,
    )


def capture_pilot_quality(
    *,
    organization_id: int,
    operation_id: int,
    trigger: PilotQualityTrigger,
    session: Session | None = None,
) -> PilotReliabilityCapture:
    """Persist one snapshot and idempotently open incidents for detected anomalies.

    When no session is supplied, the customer-runtime role reads only the known
    tenant's processing outputs while the dedicated worker role writes the internal
    observability tables. This preserves the least-privilege boundary introduced by
    the P1 schema.
    """
    if session is not None:
        capture = _capture_with_sessions(
            read_session=session,
            write_session=session,
            organization_id=int(organization_id),
            operation_id=int(operation_id),
            trigger=trigger,
            attribution_session_id=None,
        )
        return capture

    reader = get_us_lacey_db_session()
    writer = get_us_lacey_worker_db_session()
    try:
        set_tenant_db_context(reader, int(organization_id))
        attribution_session_id = _operation_attribution_session_id(
            writer,
            organization_id=int(organization_id),
            operation_id=int(operation_id),
        )
        capture = _capture_with_sessions(
            read_session=reader,
            write_session=writer,
            organization_id=int(organization_id),
            operation_id=int(operation_id),
            trigger=trigger,
            attribution_session_id=attribution_session_id,
        )
        writer.commit()
        return capture
    except Exception:
        writer.rollback()
        raise
    finally:
        reader.close()
        writer.close()


def capture_completed_pilot_quality(
    *,
    organization_id: int,
    operation_id: int,
) -> PilotReliabilityCapture:
    """Capture a successful finalization without letting WATCHDOG alter run semantics."""
    writer = get_us_lacey_worker_db_session()
    reader = get_us_lacey_db_session()
    try:
        trigger = _processing_quality_trigger(
            writer,
            organization_id=int(organization_id),
            operation_id=int(operation_id),
        )
        attribution_session_id = _operation_attribution_session_id(
            writer,
            organization_id=int(organization_id),
            operation_id=int(operation_id),
        )
        set_tenant_db_context(reader, int(organization_id))
        capture = _capture_with_sessions(
            read_session=reader,
            write_session=writer,
            organization_id=int(organization_id),
            operation_id=int(operation_id),
            trigger=trigger,
            attribution_session_id=attribution_session_id,
        )
        writer.commit()
        return capture
    except Exception:
        writer.rollback()
        raise
    finally:
        reader.close()
        writer.close()


def capture_stalled_pilot_quality(
    *,
    organization_id: int,
    operation_id: int,
    attribution_session_id: UUID,
) -> PilotReliabilityCapture:
    """Capture one stalled PROCESSING observation and idempotently open one P0."""
    writer = get_us_lacey_worker_db_session()
    reader = get_us_lacey_db_session()
    try:
        set_tenant_db_context(reader, int(organization_id))
        anomaly = PilotQualityAnomaly(
            detector_code="PROCESSING_STALLED",
            severity=PilotIncidentSeverity.P0,
            reason="Attributed pilot operation remained PROCESSING beyond the watchdog threshold.",
        )
        capture = _capture_with_sessions(
            read_session=reader,
            write_session=writer,
            organization_id=int(organization_id),
            operation_id=int(operation_id),
            trigger=PilotQualityTrigger.WATCHDOG,
            attribution_session_id=attribution_session_id,
            direct_anomaly=anomaly,
            # This is an operational detector rather than an Engine-2 detector.
            # A stable version namespace guarantees one incident per operation
            # even if Engine 2 materializes while the same stall is being observed.
            fingerprint_version="pilot-watchdog-v1",
        )
        writer.commit()
        return capture
    except Exception:
        writer.rollback()
        raise
    finally:
        reader.close()
        writer.close()
