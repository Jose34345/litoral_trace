"""Typed presentation and self-healing selection of Engine 2 shipment snapshots."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import StrEnum
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from litoral_trace.db.models import AssuranceDocument, UsLaceyEngineDocumentRun, UsLaceyEngineShipmentRun, UsLaceyOperation, UsLaceyOperationDocument, VaultDocument
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.lacey_engine.pipeline import ENGINE_VERSION
from litoral_trace.lacey_engine.serialization import BUNDLE_RESOLUTION_SCHEMA_VERSION, SHIPMENT_RESOLUTION_SCHEMA_VERSION, deserialize_shipment_resolution
from litoral_trace.lacey_engine.shipment import LaceyRuleset
from litoral_trace.us_lacey.db import get_us_lacey_db_session
from litoral_trace.services.vault import VaultService
from litoral_trace.us_lacey.engine2_suggestions import project_engine2_supported_suggestions
from litoral_trace.us_lacey.lacey_engine_service import (
    ENGINE2_SHADOW,
    UsLaceyEngine2Service,
    engine2_mode,
    source_set_fingerprint,
)
from litoral_trace.us_lacey.storage import (
    build_us_lacey_storage_settings,
    get_us_lacey_storage_client,
)

LOGGER = logging.getLogger(__name__)

class Engine2DossierAvailability(StrEnum):
    DISABLED = "DISABLED"; NOT_AVAILABLE = "NOT_AVAILABLE"; STALE = "STALE"; FAILED = "FAILED"; INVALID = "INVALID"; CURRENT = "CURRENT"

_LABELS = {"estimated_arrival_date": "Estimated Arrival Date", "master_bill_of_lading": "Master Bill of Lading", "house_bill_of_lading": "House Bill of Lading", "bill_of_lading": "Bill of Lading", "container_number": "Container Number", "importer_name": "Importer Name", "importer_address": "Importer Address", "consignee_name": "Consignee Name", "consignee_address": "Consignee Address", "shipper_name": "Shipper Name", "supplier_name": "Supplier Name", "manufacturer_name": "Manufacturer Name", "manufacturer_id": "Manufacturer ID", "filing_entry_reference": "Entry Number", "hts_code": "HTS Code", "description": "Description", "entered_value": "Entered Value", "article_component": "Article Component", "percent_recycled": "Percent Recycled", "genus": "Genus", "species": "Species", "country_of_origin": "Country of Origin", "country_of_harvest": "Country of Harvest", "plant_quantity": "Plant Quantity", "metric_unit": "Metric Unit"}

@dataclass(frozen=True, slots=True)
class Engine2DossierEvidenceView:
    source_filename: str; page: int; source_text: str; raw_text: str; normalized_value: str; evidence_class: str; candidate_score: float; source_authority: float; scope: str; line_key: str | None; component_key: str | None; bbox: tuple[float, float, float, float] | None
@dataclass(frozen=True, slots=True)
class Engine2DossierFieldView:
    field_key: str
    label: str
    state: str
    values: tuple[str, ...]
    evidence: tuple[Engine2DossierEvidenceView, ...]

    @property
    def line_evidence_groups(
        self,
    ) -> tuple[tuple[str, tuple[Engine2DossierEvidenceView, ...]], ...]:
        grouped: dict[str, list[Engine2DossierEvidenceView]] = {}
        for item in self.evidence:
            line_key = str(item.line_key or "").strip()
            if not line_key:
                continue
            grouped.setdefault(line_key, []).append(item)
        return tuple(
            (line_key, tuple(items))
            for line_key, items in grouped.items()
        )

    @property
    def global_evidence(self) -> tuple[Engine2DossierEvidenceView, ...]:
        return tuple(item for item in self.evidence if not str(item.line_key or "").strip())
@dataclass(frozen=True, slots=True)
class Engine2DossierIssueView:
    field_key: str; label: str; severity: str; issue_type: str; message: str; requires_human_review: bool; source_filenames: tuple[str, ...]; line_key: str | None; component_key: str | None
@dataclass(frozen=True, slots=True)
class Engine2DossierView:
    availability: Engine2DossierAvailability; readiness: str | None = None; engine_version: str | None = None; ruleset_version: str | None = None; schema_version: str | None = None; snapshot_created_at: object | None = None; document_count: int = 0; metrics: dict[str, int] | None = None; fields: tuple[Engine2DossierFieldView, ...] = (); issues: tuple[Engine2DossierIssueView, ...] = (); safe_status_message: str = ""

class UsLaceyEngineDossierService:
    def __init__(
        self,
        *,
        session_factory=get_us_lacey_db_session,
        recovery_service_factory=None,
        projection_publisher=project_engine2_supported_suggestions,
        auto_recover: bool = True,
    ) -> None:
        self._session_factory = session_factory
        self._recovery_service_factory = recovery_service_factory
        self._projection_publisher = projection_publisher
        self._auto_recover = bool(auto_recover)

    def _recovery_service(self) -> UsLaceyEngine2Service:
        if self._recovery_service_factory is not None:
            return self._recovery_service_factory()
        settings = build_us_lacey_storage_settings()
        vault = VaultService(
            storage_settings=settings,
            storage=get_us_lacey_storage_client(),
            session_factory=self._session_factory,
        )
        return UsLaceyEngine2Service(
            session_factory=self._session_factory,
            vault_service=vault,
        )

    def _recover_current_dossier(
        self,
        *,
        organization_id: int,
        operation_id: int,
    ) -> bool:
        """Regenerate the current immutable source-set snapshot and republish it.

        Historical snapshots remain untouched for auditability. Recovery succeeds
        only when Engine 2 produces a complete current shipment run.
        """
        try:
            result = self._recovery_service().resolve_operation_with_engine2(
                organization_id=organization_id,
                operation_id=operation_id,
            )
            if result.status != "SUCCEEDED" or result.shipment_run_id is None:
                LOGGER.warning(
                    "Engine 2 dossier auto-recovery did not produce a complete snapshot",
                    extra={
                        "organization_id": organization_id,
                        "operation_id": operation_id,
                        "engine2_status": result.status,
                    },
                )
                return False
            try:
                self._projection_publisher(
                    organization_id=organization_id,
                    operation_id=operation_id,
                )
            except Exception:
                # The current dossier is still valid and useful even if canonical
                # publication is deferred by a human-reviewed stale line. Preserve
                # the snapshot and surface the dossier rather than reverting to STALE.
                LOGGER.exception(
                    "Engine 2 dossier recovered but canonical publication was deferred",
                    extra={
                        "organization_id": organization_id,
                        "operation_id": operation_id,
                        "shipment_run_id": result.shipment_run_id,
                    },
                )
            return True
        except Exception:
            LOGGER.exception(
                "Engine 2 dossier auto-recovery failed",
                extra={
                    "organization_id": organization_id,
                    "operation_id": operation_id,
                },
            )
            return False

    def get_dossier(self, *, organization_id: int, operation_public_id: UUID | str) -> Engine2DossierView:
        if engine2_mode() != ENGINE2_SHADOW: return Engine2DossierView(Engine2DossierAvailability.DISABLED, safe_status_message="Enhanced evidence dossier is not enabled for this workspace/runtime.")
        session: Session = self._session_factory(); set_tenant_db_context(session, organization_id)
        try:
            operation = session.scalar(select(UsLaceyOperation).where(UsLaceyOperation.organization_id == organization_id, UsLaceyOperation.public_id == UUID(str(operation_public_id))))
            if operation is None: raise LookupError("Operation unavailable.")
            rows = session.execute(select(UsLaceyOperationDocument, AssuranceDocument, VaultDocument).join(AssuranceDocument, (AssuranceDocument.id == UsLaceyOperationDocument.assurance_document_id) & (AssuranceDocument.organization_id == UsLaceyOperationDocument.organization_id)).join(VaultDocument, (VaultDocument.id == AssuranceDocument.vault_document_id) & (VaultDocument.organization_id == AssuranceDocument.organization_id)).where(UsLaceyOperationDocument.organization_id == organization_id, UsLaceyOperationDocument.operation_id == operation.id, UsLaceyOperationDocument.is_current.is_(True))).all()
            if not rows: return Engine2DossierView(Engine2DossierAvailability.NOT_AVAILABLE, safe_status_message="Current Engine 2 dossier is not available yet.")
            pairs = [(row[0], row[2]) for row in rows]; ids = {str(row[0].id) for row in rows}
            fingerprint = source_set_fingerprint(organization_id=organization_id, operation_id=operation.id, documents=pairs, engine_version=ENGINE_VERSION, ruleset_version=LaceyRuleset().version, shipment_schema_version=SHIPMENT_RESOLUTION_SCHEMA_VERSION)
            snapshot = session.scalar(select(UsLaceyEngineShipmentRun).where(UsLaceyEngineShipmentRun.organization_id == organization_id, UsLaceyEngineShipmentRun.operation_id == operation.id, UsLaceyEngineShipmentRun.source_set_fingerprint == fingerprint, UsLaceyEngineShipmentRun.engine_version == ENGINE_VERSION, UsLaceyEngineShipmentRun.ruleset_version == LaceyRuleset().version, UsLaceyEngineShipmentRun.schema_version == SHIPMENT_RESOLUTION_SCHEMA_VERSION))
            if snapshot is None:
                failed = any(session.scalar(select(UsLaceyEngineDocumentRun.id).where(UsLaceyEngineDocumentRun.organization_id == organization_id, UsLaceyEngineDocumentRun.assurance_document_id == assurance.id, UsLaceyEngineDocumentRun.source_sha256 == vault.sha256, UsLaceyEngineDocumentRun.engine_version == ENGINE_VERSION, UsLaceyEngineDocumentRun.schema_version == BUNDLE_RESOLUTION_SCHEMA_VERSION, UsLaceyEngineDocumentRun.role_hint == link.document_role, UsLaceyEngineDocumentRun.status == "FAILED")) and not session.scalar(select(UsLaceyEngineDocumentRun.id).where(UsLaceyEngineDocumentRun.organization_id == organization_id, UsLaceyEngineDocumentRun.assurance_document_id == assurance.id, UsLaceyEngineDocumentRun.source_sha256 == vault.sha256, UsLaceyEngineDocumentRun.engine_version == ENGINE_VERSION, UsLaceyEngineDocumentRun.schema_version == BUNDLE_RESOLUTION_SCHEMA_VERSION, UsLaceyEngineDocumentRun.role_hint == link.document_role, UsLaceyEngineDocumentRun.status == "SUCCEEDED")) for link, assurance, vault in rows)
                historical = session.scalar(select(UsLaceyEngineShipmentRun.id).where(UsLaceyEngineShipmentRun.organization_id == organization_id, UsLaceyEngineShipmentRun.operation_id == operation.id))
                if (
                    historical
                    and not failed
                    and str(operation.status or "").upper() not in {"NEW", "PROCESSING"}
                    and self._auto_recover
                    and self._recover_current_dossier(
                        organization_id=organization_id,
                        operation_id=operation.id,
                    )
                ):
                    session.expire_all()
                    snapshot = session.scalar(select(UsLaceyEngineShipmentRun).where(
                        UsLaceyEngineShipmentRun.organization_id == organization_id,
                        UsLaceyEngineShipmentRun.operation_id == operation.id,
                        UsLaceyEngineShipmentRun.source_set_fingerprint == fingerprint,
                        UsLaceyEngineShipmentRun.engine_version == ENGINE_VERSION,
                        UsLaceyEngineShipmentRun.ruleset_version == LaceyRuleset().version,
                        UsLaceyEngineShipmentRun.schema_version == SHIPMENT_RESOLUTION_SCHEMA_VERSION,
                    ))
                if snapshot is None:
                    return Engine2DossierView(Engine2DossierAvailability.FAILED if failed else (Engine2DossierAvailability.STALE if historical else Engine2DossierAvailability.NOT_AVAILABLE), safe_status_message="Current shadow document processing did not produce a complete dossier." if failed else ("The dossier could not be refreshed for the current document set. Please retry processing." if historical else "Current Engine 2 dossier is not available yet."))
            try:
                resolution = deserialize_shipment_resolution(snapshot.resolution_json)
                logical_ids = {item.document_id for item in resolution.documents}
                evidence_ids = {
                    evidence.document_id
                    for result in resolution.canonical_fields.values()
                    for evidence in result.supporting_evidence
                }
                physical_ids = {
                    document_id.split(":", 1)[0]
                    for document_id in logical_ids
                }
                if (
                    resolution.engine_version != ENGINE_VERSION
                    or resolution.ruleset_version != LaceyRuleset().version
                    or snapshot.document_count != len(resolution.documents)
                    or not logical_ids
                    or physical_ids != ids
                    or not evidence_ids.issubset(logical_ids)
                ):
                    raise ValueError("snapshot consistency")
            except Exception:
                LOGGER.exception("Invalid Engine 2 dossier snapshot", extra={"organization_id": organization_id, "operation_id": operation.id})
                return Engine2DossierView(Engine2DossierAvailability.INVALID, safe_status_message="The stored dossier could not be safely read.")
            filenames = {
                item.document_id: item.filename
                for item in resolution.documents
            }
            fields = tuple(Engine2DossierFieldView(key, _LABELS.get(key, key.replace("_", " ").title()), result.state.value, tuple(value.value for value in result.values), tuple(Engine2DossierEvidenceView(filenames.get(e.document_id, "Source document"), e.candidate.provenance.page, e.candidate.provenance.source_text, e.candidate.raw.raw_text, e.normalized_value, e.candidate.raw.evidence_class.value, e.candidate_score, e.source_authority, e.scope.value, e.line_key, e.component_key, None if e.candidate.provenance.bbox is None else (e.candidate.provenance.bbox.x0, e.candidate.provenance.bbox.top, e.candidate.provenance.bbox.x1, e.candidate.provenance.bbox.bottom)) for e in result.supporting_evidence)) for key, result in resolution.canonical_fields.items())
            issues = tuple(Engine2DossierIssueView(issue.field_key, _LABELS.get(issue.field_key, issue.field_key.replace("_", " ").title()), issue.severity, issue.issue_type, issue.message, issue.requires_human_review, tuple(filenames.get(doc, "Source document") for doc in issue.document_ids), issue.line_key, issue.component_key) for issue in resolution.issues)
            return Engine2DossierView(Engine2DossierAvailability.CURRENT, resolution.readiness.value, resolution.engine_version, resolution.ruleset_version, snapshot.schema_version, snapshot.created_at, snapshot.document_count, resolution.metrics, fields, issues, "Read-only evidence preview generated from persisted shipment documents.")
        finally: session.close()
