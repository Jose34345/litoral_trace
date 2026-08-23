"""Idempotent ACCEPTANCE-only orchestration for EUDR DDS API V3.

The service deliberately separates deterministic preparation from the ephemeral
WS-Security envelope.  A persisted SENT or TRANSPORT_ERROR state is not retried
automatically because delivery may be ambiguous and a blind retry could create
a duplicate remote DDS.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session

from litoral_trace.config.eudr_acceptance import (
    EudrAcceptanceSettings,
    get_eudr_acceptance_settings,
)
from litoral_trace.db.models import EudrAcceptanceAttempt, EudrDdsCandidate
from litoral_trace.services.eudr_acceptance_contract import (
    EudrAcceptanceContractError,
    EudrV3PreparedBody,
    build_submit_dds_body,
)
from litoral_trace.services.eudr_acceptance_transport import (
    AcceptanceTransport,
    EudrAcceptanceTransportError,
    UrllibAcceptanceTransport,
    build_ws_security_envelope,
    parse_submit_response,
)
from litoral_trace.services.eudr_dds_candidate import EudrDdsCandidateService


class EudrAcceptanceSubmissionError(RuntimeError):
    def __init__(self, code: str, detail: str) -> None:
        self.code = code
        self.detail = detail
        super().__init__(detail)


class EudrAcceptanceSubmissionPersistenceError(EudrAcceptanceSubmissionError):
    pass


@dataclass(frozen=True)
class EudrAcceptanceAttemptView:
    public_id: UUID
    candidate_public_id: UUID
    shipment_code: str
    state: str
    environment: str
    operation: str
    operator_role: str
    country_of_activity: str
    border_cross_country: str | None
    internal_reference_number: str
    candidate_payload_sha256: str
    wire_contract_profile: str
    wire_contract_sha256: str
    request_body_sha256: str
    envelope_sha256: str | None
    response_sha256: str | None
    request_body_bytes: int
    remote_uuid: str | None
    remote_status: str | None
    http_status: int | None
    error_code: str | None
    error_summary: str | None
    sent_at: datetime | None
    completed_at: datetime | None
    created_at: datetime
    updated_at: datetime
    legal_effect: str = "NONE_NON_LEGAL_ACCEPTANCE"
    target_environment: str = "ACCEPTANCE"
    ledger_mutated: bool = False


@dataclass(frozen=True)
class EudrAcceptancePreparedView:
    attempt: EudrAcceptanceAttemptView
    created: bool
    network_ready: bool


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class EudrAcceptanceSubmissionService:
    def __init__(
        self,
        *,
        session: Session,
        organization_id: int,
        settings: EudrAcceptanceSettings | None = None,
        transport: AcceptanceTransport | None = None,
    ) -> None:
        self._session = session
        self._organization_id = int(organization_id)
        self._settings = settings or get_eudr_acceptance_settings()
        self._transport = transport or UrllibAcceptanceTransport()

    def _candidate_row(self, public_id: UUID) -> EudrDdsCandidate:
        row = self._session.scalar(
            select(EudrDdsCandidate).where(
                EudrDdsCandidate.organization_id == self._organization_id,
                EudrDdsCandidate.public_id == public_id,
            )
        )
        if row is None:
            raise EudrAcceptanceSubmissionError(
                "EUDR_CANDIDATE_NOT_FOUND",
                "No se encontró el candidato EUDR del tenant.",
            )
        return row

    @staticmethod
    def _view(
        row: EudrAcceptanceAttempt,
        *,
        candidate_public_id: UUID,
        shipment_code: str,
    ) -> EudrAcceptanceAttemptView:
        return EudrAcceptanceAttemptView(
            public_id=row.public_id,
            candidate_public_id=candidate_public_id,
            shipment_code=shipment_code,
            state=row.state,
            environment=row.environment,
            operation=row.operation,
            operator_role=row.operator_role,
            country_of_activity=row.country_of_activity,
            border_cross_country=row.border_cross_country,
            internal_reference_number=row.internal_reference_number,
            candidate_payload_sha256=row.candidate_payload_sha256,
            wire_contract_profile=row.wire_contract_profile,
            wire_contract_sha256=row.wire_contract_sha256,
            request_body_sha256=row.request_body_sha256,
            envelope_sha256=row.envelope_sha256,
            response_sha256=row.response_sha256,
            request_body_bytes=row.request_body_bytes,
            remote_uuid=row.remote_uuid,
            remote_status=row.remote_status,
            http_status=row.http_status,
            error_code=row.error_code,
            error_summary=row.error_summary,
            sent_at=row.sent_at,
            completed_at=row.completed_at,
            created_at=row.created_at,
            updated_at=row.updated_at,
        )

    def _prepared_body(
        self,
        *,
        shipment_code: str,
        operator_role: str,
        country_of_activity: str,
        border_cross_country: str | None,
        internal_reference_number: str | None,
        geo_location_confidential: bool,
    ) -> tuple[Any, EudrDdsCandidate, EudrV3PreparedBody]:
        conformance = EudrDdsCandidateService(
            session=self._session,
            organization_id=self._organization_id,
        ).conformance(shipment_code)
        if not conformance.ready or conformance.payload is None or conformance.payload_sha256 is None:
            missing = ", ".join(conformance.missing) if conformance.missing else "conformance"
            raise EudrAcceptanceSubmissionError(
                "EUDR_CANDIDATE_NOT_CONFORMANCE_READY",
                "El candidato no está CONFORMANCE_READY: " + missing,
            )
        if conformance.candidate is None:
            raise EudrAcceptanceSubmissionError(
                "EUDR_CANDIDATE_NOT_FOUND",
                "No existe un candidato local para preparar ACCEPTANCE.",
            )
        candidate_row = self._candidate_row(conformance.candidate.public_id)
        try:
            body = build_submit_dds_body(
                conformance.payload,
                operator_role=operator_role,
                country_of_activity=country_of_activity,
                border_cross_country=border_cross_country,
                internal_reference_number=internal_reference_number,
                geo_location_confidential=geo_location_confidential,
            )
        except EudrAcceptanceContractError as exc:
            raise EudrAcceptanceSubmissionError(exc.code, exc.detail) from exc
        return conformance, candidate_row, body

    def prepare(
        self,
        *,
        shipment_code: str,
        operator_role: str,
        country_of_activity: str,
        border_cross_country: str | None = None,
        internal_reference_number: str | None = None,
        geo_location_confidential: bool = False,
        actor_user_id: int | None = None,
    ) -> EudrAcceptancePreparedView:
        conformance, candidate, body = self._prepared_body(
            shipment_code=shipment_code,
            operator_role=operator_role,
            country_of_activity=country_of_activity,
            border_cross_country=border_cross_country,
            internal_reference_number=internal_reference_number,
            geo_location_confidential=geo_location_confidential,
        )
        existing = self._session.scalar(
            select(EudrAcceptanceAttempt).where(
                EudrAcceptanceAttempt.organization_id == self._organization_id,
                EudrAcceptanceAttempt.candidate_id == candidate.id,
                EudrAcceptanceAttempt.request_body_sha256 == body.sha256,
            )
        )
        if existing is not None:
            return EudrAcceptancePreparedView(
                attempt=self._view(
                    existing,
                    candidate_public_id=candidate.public_id,
                    shipment_code=conformance.shipment_code,
                ),
                created=False,
                network_ready=self._settings.network_ready,
            )

        row = EudrAcceptanceAttempt(
            organization_id=self._organization_id,
            candidate_id=candidate.id,
            environment="ACCEPTANCE",
            operation="SUBMIT_DDS",
            state="PREPARED",
            operator_role=body.operator_role,
            country_of_activity=body.country_of_activity,
            border_cross_country=body.border_cross_country,
            internal_reference_number=body.internal_reference_number,
            candidate_payload_sha256=conformance.payload_sha256,
            wire_contract_profile=body.wire_contract_profile,
            wire_contract_sha256=body.wire_contract_sha256,
            request_body_sha256=body.sha256,
            request_body_bytes=body.byte_length,
            created_by_user_id=actor_user_id,
        )
        self._session.add(row)
        try:
            self._session.commit()
            self._session.refresh(row)
        except IntegrityError:
            self._session.rollback()
            existing = self._session.scalar(
                select(EudrAcceptanceAttempt).where(
                    EudrAcceptanceAttempt.organization_id == self._organization_id,
                    EudrAcceptanceAttempt.candidate_id == candidate.id,
                    EudrAcceptanceAttempt.request_body_sha256 == body.sha256,
                )
            )
            if existing is None:
                raise EudrAcceptanceSubmissionPersistenceError(
                    "ACCEPTANCE_PREPARE_CONFLICT",
                    "No fue posible resolver el intento ACCEPTANCE idempotente.",
                )
            row = existing
            created = False
        except SQLAlchemyError as exc:
            self._session.rollback()
            raise EudrAcceptanceSubmissionPersistenceError(
                "ACCEPTANCE_PREPARE_PERSISTENCE_ERROR",
                "No fue posible persistir el intento ACCEPTANCE.",
            ) from exc
        else:
            created = True

        return EudrAcceptancePreparedView(
            attempt=self._view(
                row,
                candidate_public_id=candidate.public_id,
                shipment_code=conformance.shipment_code,
            ),
            created=created,
            network_ready=self._settings.network_ready,
        )

    def _attempt_with_candidate(
        self,
        attempt_public_id: UUID,
    ) -> tuple[EudrAcceptanceAttempt, EudrDdsCandidate]:
        row = self._session.scalar(
            select(EudrAcceptanceAttempt).where(
                EudrAcceptanceAttempt.organization_id == self._organization_id,
                EudrAcceptanceAttempt.public_id == attempt_public_id,
            )
        )
        if row is None:
            raise EudrAcceptanceSubmissionError(
                "ACCEPTANCE_ATTEMPT_NOT_FOUND",
                "No se encontró el intento ACCEPTANCE.",
            )
        candidate = self._session.scalar(
            select(EudrDdsCandidate).where(
                EudrDdsCandidate.organization_id == self._organization_id,
                EudrDdsCandidate.id == row.candidate_id,
            )
        )
        if candidate is None:
            raise EudrAcceptanceSubmissionPersistenceError(
                "ACCEPTANCE_CANDIDATE_REFERENCE_BROKEN",
                "El intento ACCEPTANCE no puede resolver su candidato.",
            )
        return row, candidate

    def get_attempt(self, attempt_public_id: UUID, *, shipment_code: str) -> EudrAcceptanceAttemptView:
        row, candidate = self._attempt_with_candidate(attempt_public_id)
        return self._view(row, candidate_public_id=candidate.public_id, shipment_code=shipment_code)

    def submit(
        self,
        *,
        attempt_public_id: UUID,
        shipment_code: str,
        actor_user_id: int | None = None,
        allow_retry_after_transport_error: bool = False,
    ) -> EudrAcceptanceAttemptView:
        # Validate deployment configuration before mutating the durable attempt.
        try:
            self._settings.require_network_ready()
        except (RuntimeError, ValueError) as exc:
            raise EudrAcceptanceSubmissionError(
                "ACCEPTANCE_NETWORK_NOT_READY",
                str(exc),
            ) from exc

        row, candidate = self._attempt_with_candidate(attempt_public_id)
        if row.state == "REMOTE_ACCEPTED" or row.state == "REMOTE_REJECTED":
            return self._view(row, candidate_public_id=candidate.public_id, shipment_code=shipment_code)
        if row.state == "SENT":
            raise EudrAcceptanceSubmissionError(
                "ACCEPTANCE_DELIVERY_UNCERTAIN",
                "El intento ya quedó SENT. No se reenvía automáticamente porque la entrega remota puede ser ambigua.",
            )
        if row.state == "TRANSPORT_ERROR" and not allow_retry_after_transport_error:
            raise EudrAcceptanceSubmissionError(
                "ACCEPTANCE_RETRY_REQUIRES_EXPLICIT_OVERRIDE",
                "Un TRANSPORT_ERROR puede haber ocurrido después de enviar bytes. Revise el sistema remoto antes de reintentar explícitamente.",
            )
        if row.state not in {"PREPARED", "TRANSPORT_ERROR"}:
            raise EudrAcceptanceSubmissionError(
                "ACCEPTANCE_ATTEMPT_NOT_SENDABLE",
                "El estado actual del intento no admite envío.",
            )

        conformance, current_candidate, body = self._prepared_body(
            shipment_code=shipment_code,
            operator_role=row.operator_role,
            country_of_activity=row.country_of_activity,
            border_cross_country=row.border_cross_country,
            internal_reference_number=row.internal_reference_number,
            geo_location_confidential=False,
        )
        if current_candidate.id != candidate.id:
            raise EudrAcceptanceSubmissionError(
                "ACCEPTANCE_CANDIDATE_MISMATCH",
                "El shipment ya no resuelve al candidato asociado al intento.",
            )
        if conformance.payload_sha256 != row.candidate_payload_sha256 or body.sha256 != row.request_body_sha256:
            raise EudrAcceptanceSubmissionError(
                "ACCEPTANCE_PREPARED_PAYLOAD_STALE",
                "El candidato o el wire body cambiaron desde PREPARED; cree un nuevo intento.",
            )

        assert self._settings.username is not None
        assert self._settings.authentication_key is not None
        assert self._settings.web_service_client_id is not None
        envelope = build_ws_security_envelope(
            body.xml,
            username=self._settings.username,
            authentication_key=self._settings.authentication_key,
            web_service_client_id=self._settings.web_service_client_id,
            validity_seconds=self._settings.timestamp_validity_seconds,
        )
        row.state = "SENT"
        row.envelope_sha256 = hashlib.sha256(envelope).hexdigest()
        row.sent_at = _utcnow()
        row.completed_at = None
        row.http_status = None
        row.response_sha256 = None
        row.remote_uuid = None
        row.remote_status = None
        row.error_code = None
        row.error_summary = None
        try:
            self._session.commit()
        except SQLAlchemyError as exc:
            self._session.rollback()
            raise EudrAcceptanceSubmissionPersistenceError(
                "ACCEPTANCE_SENT_PERSISTENCE_ERROR",
                "No fue posible persistir SENT antes del envío; no se llamó a ACCEPTANCE.",
            ) from exc

        try:
            response = self._transport.send(envelope=envelope, settings=self._settings)
            parsed = parse_submit_response(http_status=response.http_status, body=response.body)
        except EudrAcceptanceTransportError as exc:
            row.state = "TRANSPORT_ERROR"
            row.error_code = exc.code[:120]
            row.error_summary = exc.detail[:2000]
            row.http_status = exc.http_status
            row.completed_at = _utcnow()
            try:
                self._session.commit()
            except SQLAlchemyError as persist_exc:
                self._session.rollback()
                raise EudrAcceptanceSubmissionPersistenceError(
                    "ACCEPTANCE_TRANSPORT_ERROR_PERSISTENCE_ERROR",
                    "Ocurrió un error de transporte y no pudo persistirse su resultado.",
                ) from persist_exc
            return self._view(row, candidate_public_id=candidate.public_id, shipment_code=shipment_code)

        row.http_status = response.http_status
        row.response_sha256 = hashlib.sha256(response.body).hexdigest()
        row.completed_at = _utcnow()
        row.remote_uuid = parsed.remote_uuid
        row.remote_status = parsed.remote_status
        row.error_code = parsed.error_code
        row.error_summary = parsed.error_summary
        row.state = "REMOTE_ACCEPTED" if parsed.accepted else "REMOTE_REJECTED"
        try:
            self._session.commit()
        except SQLAlchemyError as exc:
            self._session.rollback()
            raise EudrAcceptanceSubmissionPersistenceError(
                "ACCEPTANCE_RESULT_PERSISTENCE_ERROR",
                "ACCEPTANCE respondió pero no fue posible persistir el resultado; requiere reconciliación manual.",
            ) from exc
        return self._view(row, candidate_public_id=candidate.public_id, shipment_code=shipment_code)
