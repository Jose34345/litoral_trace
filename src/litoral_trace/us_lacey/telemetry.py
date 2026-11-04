"""Anonymous Learning Plane capture for U.S. Lacey workflows."""
from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from uuid import UUID, uuid5

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from litoral_trace.db.models import (
    AssuranceDocument,
    DocumentExtractionRun,
    TelemetryFieldAction,
    TelemetryRun,
    UsLaceyOperationDocument,
    UsLaceyOperationField,
)
from litoral_trace.db.models.us_lacey_telemetry import TelemetryFieldActionType
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.us_lacey.db import get_us_lacey_db_session
from litoral_trace.us_lacey.worker_db import get_us_lacey_worker_db_session


TELEMETRY_SCHEMA_VERSION = 1
TELEMETRY_NAMESPACE = UUID("87f3185b-d233-44af-969a-78a34f4338f7")
CORPUS_FIELD_ALIASES = {
    "species": "species",
    "genus": "genus",
    "plant_quantity": "quantity",
    "quantity": "quantity",
    "metric_unit": "unit",
    "unit": "unit",
    "country_of_harvest": "country_of_harvest",
    "hts_code": "hts_code",
}
CORPUS_ALLOWED_FIELDS = frozenset(CORPUS_FIELD_ALIASES.values())
MAX_CORPUS_VALUE_CHARS = 512


class TelemetryService:
    @staticmethod
    def deterministic_run_id(*, origin: str, source_id: int) -> UUID:
        normalized_origin = str(origin or "").strip().upper()
        source = int(source_id)
        if normalized_origin not in {"SANDBOX", "PRODUCTION"} or source <= 0:
            raise ValueError("Telemetry run identity is invalid.")
        return uuid5(TELEMETRY_NAMESPACE, f"{normalized_origin.lower()}:{source}")

    @staticmethod
    def deterministic_field_instance_id(*, run_id: UUID, field_id: int) -> UUID:
        normalized_field_id = int(field_id)
        if normalized_field_id <= 0:
            raise ValueError("Telemetry field identity is invalid.")
        return uuid5(run_id, f"field:{normalized_field_id}")

    @staticmethod
    def _normalize_value(value: object) -> str | None:
        if value is None:
            return None
        normalized = " ".join(str(value).split())
        return normalized or None

    @staticmethod
    def classify_field_action(field: Mapping[str, object]) -> TelemetryFieldActionType:
        if field.get("reviewed_at") is None:
            return TelemetryFieldActionType.UNREVIEWED
        if str(field.get("field_status") or "").upper() == "NOT_REQUIRED":
            return TelemetryFieldActionType.REJECTED
        machine_value = TelemetryService._normalize_value(
            field.get("normalized_value") or field.get("original_value")
        )
        human_value = TelemetryService._normalize_value(field.get("human_value"))
        if machine_value is not None and human_value == machine_value:
            return TelemetryFieldActionType.CONFIRMED
        if human_value is not None:
            return TelemetryFieldActionType.CORRECTED
        return TelemetryFieldActionType.REJECTED

    @staticmethod
    def calculate_human_correction_rate(
        actions: Iterable[TelemetryFieldActionType | str],
    ) -> Decimal | None:
        normalized = [
            str(getattr(action, "value", action)).strip().upper()
            for action in actions
        ]
        reviewed = sum(
            action in {"CONFIRMED", "CORRECTED", "REJECTED"}
            for action in normalized
        )
        if reviewed == 0:
            return None
        corrected = sum(action == "CORRECTED" for action in normalized)
        return (Decimal(corrected) / Decimal(reviewed)).quantize(
            Decimal("0.00001"), rounding=ROUND_HALF_UP
        )

    @staticmethod
    def _bounded_corpus_value(value: object) -> str | None:
        normalized = TelemetryService._normalize_value(value)
        return None if normalized is None else normalized[:MAX_CORPUS_VALUE_CHARS]

    @staticmethod
    def _decimal_confidence(value: object) -> Decimal | None:
        if value is None:
            return None
        try:
            confidence = Decimal(str(value))
        except Exception:
            return None
        if confidence < 0 or confidence > 1:
            return None
        return confidence.quantize(Decimal("0.00001"), rounding=ROUND_HALF_UP)

    @classmethod
    def _load_sandbox_snapshot(cls, *, organization_id: int) -> dict[str, object]:
        org_id = int(organization_id)
        session = get_us_lacey_db_session()
        try:
            set_tenant_db_context(session, org_id)
            fields = session.execute(
                select(
                    UsLaceyOperationField.id,
                    UsLaceyOperationField.field_name,
                    UsLaceyOperationField.confidence,
                    UsLaceyOperationField.human_value,
                    UsLaceyOperationField.normalized_value,
                    UsLaceyOperationField.original_value,
                    UsLaceyOperationField.reviewed_at,
                    UsLaceyOperationField.field_status,
                    UsLaceyOperationField.extractor_version,
                    AssuranceDocument.semantic_document_type.label("document_type"),
                )
                .outerjoin(
                    AssuranceDocument,
                    (AssuranceDocument.organization_id == UsLaceyOperationField.organization_id)
                    & (AssuranceDocument.id == UsLaceyOperationField.source_assurance_document_id),
                )
                .where(UsLaceyOperationField.organization_id == org_id)
                .order_by(UsLaceyOperationField.id.asc())
            ).mappings().all()
            document_types = session.execute(
                select(AssuranceDocument.semantic_document_type)
                .join(
                    UsLaceyOperationDocument,
                    (UsLaceyOperationDocument.organization_id == AssuranceDocument.organization_id)
                    & (UsLaceyOperationDocument.assurance_document_id == AssuranceDocument.id),
                )
                .where(AssuranceDocument.organization_id == org_id)
            ).scalars().all()
            processing_ms = session.execute(
                select(
                    func.sum(
                        func.extract(
                            "epoch",
                            DocumentExtractionRun.completed_at - DocumentExtractionRun.started_at,
                        ) * 1000
                    )
                ).where(
                    DocumentExtractionRun.organization_id == org_id,
                    DocumentExtractionRun.started_at.is_not(None),
                    DocumentExtractionRun.completed_at.is_not(None),
                )
            ).scalar_one_or_none()
            return {
                "fields": [dict(row) for row in fields],
                "document_type_counts": dict(Counter(str(v or "UNKNOWN") for v in document_types)),
                "document_count": len(document_types),
                "processing_total_ms": None if processing_ms is None else max(0, int(processing_ms)),
            }
        finally:
            session.close()

    @classmethod
    def capture_sandbox_before_purge(
        cls,
        *,
        organization_id: int,
        purge_job_id: int,
        learning_opt_in: bool,
        sandbox_expires_at: datetime,
    ) -> UUID:
        snapshot = cls._load_sandbox_snapshot(organization_id=organization_id)
        run_id = cls.deterministic_run_id(origin="SANDBOX", source_id=purge_job_id)
        prepared_actions: list[dict[str, object]] = []
        action_types: list[TelemetryFieldActionType] = []
        confidences: list[Decimal] = []
        engine_versions: set[str] = set()

        for raw in snapshot["fields"]:
            field = dict(raw)
            action = cls.classify_field_action(field)
            action_types.append(action)
            confidence = cls._decimal_confidence(field.get("confidence"))
            if confidence is not None:
                confidences.append(confidence)
            extractor_version = cls._normalize_value(field.get("extractor_version"))
            if extractor_version:
                engine_versions.add(extractor_version)
            raw_name = str(field.get("field_name") or "").strip()
            canonical_name = CORPUS_FIELD_ALIASES.get(raw_name, raw_name)
            allowed = canonical_name in CORPUS_ALLOWED_FIELDS
            fragment = None
            target = None
            if bool(learning_opt_in) and action is TelemetryFieldActionType.CORRECTED and allowed:
                fragment = cls._bounded_corpus_value(
                    field.get("normalized_value") or field.get("original_value")
                )
                target = cls._bounded_corpus_value(field.get("human_value"))
            prepared_actions.append(
                {
                    "telemetry_run_id": run_id,
                    "field_instance_id": cls.deterministic_field_instance_id(
                        run_id=run_id, field_id=int(field["id"])
                    ),
                    "field_name": canonical_name[:128],
                    "document_type": cls._normalize_value(field.get("document_type")),
                    "prediction_confidence": confidence,
                    "action_taken": action.value,
                    "deidentified_fragment": fragment,
                    "target_value": target,
                }
            )

        counts = Counter(action.value for action in action_types)
        reviewed = counts["CONFIRMED"] + counts["CORRECTED"] + counts["REJECTED"]
        hcr = cls.calculate_human_correction_rate(action_types)
        mean_confidence = None
        if confidences:
            mean_confidence = (sum(confidences, Decimal("0")) / Decimal(len(confidences))).quantize(
                Decimal("0.00001"), rounding=ROUND_HALF_UP
            )

        started_at = sandbox_expires_at - timedelta(hours=4)
        if started_at.tzinfo is None:
            started_at = started_at.replace(tzinfo=timezone.utc)
        engine_version = ",".join(sorted(engine_versions))[:64] if engine_versions else None
        finalized_at = datetime.now(timezone.utc)

        worker_session = get_us_lacey_worker_db_session()
        try:
            run_values = {
                "run_id": run_id,
                "origin": "SANDBOX",
                "learning_opt_in": bool(learning_opt_in),
                "document_count": int(snapshot["document_count"]),
                "document_type_counts": snapshot["document_type_counts"],
                "processing_total_ms": snapshot["processing_total_ms"],
                "reviewed_field_count": reviewed,
                "confirmed_field_count": counts["CONFIRMED"],
                "corrected_field_count": counts["CORRECTED"],
                "rejected_field_count": counts["REJECTED"],
                "unreviewed_field_count": counts["UNREVIEWED"],
                "human_correction_rate": hcr,
                "mean_prediction_confidence": mean_confidence,
                "engine_version": engine_version,
                "telemetry_schema_version": TELEMETRY_SCHEMA_VERSION,
                "started_at": started_at,
                "finalized_at": finalized_at,
            }
            worker_session.execute(
                pg_insert(TelemetryRun).values(**run_values).on_conflict_do_update(
                    index_elements=[TelemetryRun.run_id],
                    set_={k: v for k, v in run_values.items() if k != "run_id"},
                )
            )
            for values in prepared_actions:
                worker_session.execute(
                    pg_insert(TelemetryFieldAction).values(**values).on_conflict_do_update(
                        constraint="uq_us_lacey_telemetry_field_instance",
                        set_={
                            k: v for k, v in values.items()
                            if k not in {"telemetry_run_id", "field_instance_id"}
                        },
                    )
                )
            worker_session.commit()
            return run_id
        except Exception:
            worker_session.rollback()
            raise
        finally:
            worker_session.close()
