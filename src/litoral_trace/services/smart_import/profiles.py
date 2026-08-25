"""Tenant-safe persistence and schema-drift matching for Smart Import profiles."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import unicodedata
from typing import Callable
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from litoral_trace.db.engine import get_db_session
from litoral_trace.db.models.smart_import_profile import SmartImportProfile
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.services.audit import (
    AuditAction,
    AuditOutcome,
    build_audit_actor,
    record_audit_event,
)
from litoral_trace.services.batch import BATCH_COLUMNAS

from .canonicalize import ConfirmedMapping
from .contracts import DatasetCandidate


SMART_PROFILE_SCHEMA_KIND = "lotes"
SMART_PROFILE_NAME_MAX_LENGTH = 120
SMART_PROFILE_MIN_OVERLAP = 0.45
_UNSAFE_PROFILE_NAME_CATEGORIES = frozenset({"Cc", "Cf", "Cs", "Zl", "Zp"})


class SmartImportProfileError(RuntimeError):
    """Safe base error for mapping-profile persistence."""


class SmartImportProfilePersistenceError(SmartImportProfileError):
    """Raised when profile storage is temporarily unavailable."""


class SmartImportProfileValidationError(SmartImportProfileError):
    """Raised for invalid user-confirmed profile metadata."""


@dataclass(frozen=True)
class SmartImportProfileMatch:
    """Resolved tenant profile against the current workbook candidate."""

    public_id: UUID
    name: str
    status: str
    similarity: float
    mappings: tuple[ConfirmedMapping, ...]
    missing_source_headers: tuple[str, ...] = ()


SessionFactory = Callable[[], Session | None]


def _contains_unsafe_profile_name_codepoint(value: str) -> bool:
    """Reject invisible/control direction changes in a user-visible profile name."""

    return any(
        unicodedata.category(character) in _UNSAFE_PROFILE_NAME_CATEGORIES
        for character in value
    )


def _normalize_profile_name(value: str | None, *, fallback: str) -> str:
    raw = str(value or fallback)
    if _contains_unsafe_profile_name_codepoint(raw):
        raise SmartImportProfileValidationError(
            "El nombre del formato contiene caracteres de control o dirección Unicode no permitidos."
        )

    normalized_unicode = unicodedata.normalize("NFKC", raw)
    if _contains_unsafe_profile_name_codepoint(normalized_unicode):
        raise SmartImportProfileValidationError(
            "El nombre del formato contiene caracteres de control o dirección Unicode no permitidos."
        )

    normalized = " ".join(normalized_unicode.strip().split())
    if not normalized:
        raise SmartImportProfileValidationError(
            "El nombre del formato recordado no puede estar vacío."
        )
    if len(normalized) > SMART_PROFILE_NAME_MAX_LENGTH:
        raise SmartImportProfileValidationError(
            f"El nombre del formato no puede superar {SMART_PROFILE_NAME_MAX_LENGTH} caracteres."
        )
    return normalized


def candidate_header_signature(candidate: DatasetCandidate) -> tuple[str, ...]:
    """Stable order-insensitive signature preserving duplicate normalized headers."""

    return tuple(
        sorted(
            mapping.normalized_source
            for mapping in candidate.mappings
            if mapping.normalized_source
        )
    )


def header_fingerprint(signature: tuple[str, ...] | list[str]) -> str:
    """Hash only schema/header metadata; never source business values."""

    payload = json.dumps(
        list(signature),
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _candidate_by_normalized_header(
    candidate: DatasetCandidate,
) -> dict[str, list]:
    result: dict[str, list] = {}
    for column in candidate.mappings:
        if not column.normalized_source:
            continue
        result.setdefault(column.normalized_source, []).append(column)
    return result


def profile_mapping_payload(
    candidate: DatasetCandidate,
    mappings: tuple[ConfirmedMapping, ...],
) -> dict[str, str]:
    """Persist canonical target -> normalized source header, never column data."""

    by_index = {item.source_index: item for item in candidate.mappings}
    payload: dict[str, str] = {}

    for mapping in mappings:
        if mapping.canonical_field not in BATCH_COLUMNAS:
            raise SmartImportProfileValidationError(
                "El mapping contiene un campo canónico no permitido."
            )
        source = by_index.get(mapping.source_index)
        if source is None or source.source_column != mapping.source_column:
            raise SmartImportProfileValidationError(
                "El mapping ya no coincide con las columnas analizadas."
            )
        if not source.normalized_source:
            raise SmartImportProfileValidationError(
                "No se puede recordar una columna sin encabezado estable."
            )
        payload[mapping.canonical_field] = source.normalized_source

    if set(payload) != set(BATCH_COLUMNAS):
        raise SmartImportProfileValidationError(
            "El formato sólo puede recordarse cuando los ocho campos obligatorios están confirmados."
        )

    if len(set(payload.values())) != len(payload):
        raise SmartImportProfileValidationError(
            "Un formato recordado no puede reutilizar la misma columna para dos campos canónicos."
        )

    return payload


def resolve_profile_mapping(
    profile_mapping: dict[str, str],
    candidate: DatasetCandidate,
) -> tuple[tuple[ConfirmedMapping, ...], tuple[str, ...]]:
    """Resolve a stored name-based mapping against fresh column positions."""

    by_header = _candidate_by_normalized_header(candidate)
    resolved: list[ConfirmedMapping] = []
    missing: list[str] = []

    for canonical_field in BATCH_COLUMNAS:
        source_header = str(profile_mapping.get(canonical_field, "")).strip()
        if not source_header:
            missing.append(canonical_field)
            continue
        matches = by_header.get(source_header, [])
        if len(matches) != 1:
            missing.append(source_header)
            continue
        source = matches[0]
        resolved.append(
            ConfirmedMapping(
                source_index=source.source_index,
                source_column=source.source_column,
                canonical_field=canonical_field,
            )
        )

    return tuple(resolved), tuple(missing)


def _signature_similarity(
    stored: list[str] | tuple[str, ...],
    current: tuple[str, ...],
) -> float:
    stored_set = set(str(value) for value in stored if str(value).strip())
    current_set = set(current)
    if not stored_set and not current_set:
        return 1.0
    union = stored_set | current_set
    if not union:
        return 0.0
    return len(stored_set & current_set) / len(union)


class SmartImportProfileService:
    """Read and remember per-tenant spreadsheet mappings."""

    def __init__(self, *, session_factory: SessionFactory | None = None) -> None:
        self._session_factory = session_factory or get_db_session

    def find_best_match(
        self,
        *,
        organization_id: int,
        candidate: DatasetCandidate,
    ) -> SmartImportProfileMatch | None:
        session = self._session_factory()
        if session is None:
            raise SmartImportProfilePersistenceError(
                "El almacenamiento de formatos recordados no está disponible."
            )

        try:
            set_tenant_db_context(session, organization_id)
            profiles = session.execute(
                select(SmartImportProfile).where(
                    SmartImportProfile.organization_id == int(organization_id),
                    SmartImportProfile.schema_kind == SMART_PROFILE_SCHEMA_KIND,
                    SmartImportProfile.active.is_(True),
                )
            ).scalars().all()

            signature = candidate_header_signature(candidate)
            fingerprint = header_fingerprint(signature)
            ranked: list[tuple[int, float, datetime, SmartImportProfileMatch]] = []

            for profile in profiles:
                raw_mapping = profile.mapping_json
                raw_signature = profile.header_signature
                if not isinstance(raw_mapping, dict) or not isinstance(raw_signature, list):
                    continue

                similarity = _signature_similarity(raw_signature, signature)
                if similarity < SMART_PROFILE_MIN_OVERLAP:
                    continue

                mappings, missing = resolve_profile_mapping(raw_mapping, candidate)
                if profile.header_fingerprint == fingerprint and not missing:
                    status = "EXACT"
                    rank = 3
                elif not missing and len(mappings) == len(BATCH_COLUMNAS):
                    status = "COMPATIBLE_DRIFT"
                    rank = 2
                else:
                    status = "BLOCKED_DRIFT"
                    rank = 1

                updated_at = profile.updated_at or profile.created_at
                ranked.append(
                    (
                        rank,
                        similarity,
                        updated_at,
                        SmartImportProfileMatch(
                            public_id=profile.public_id,
                            name=profile.name,
                            status=status,
                            similarity=similarity,
                            mappings=mappings,
                            missing_source_headers=missing,
                        ),
                    )
                )

            if not ranked:
                return None
            ranked.sort(key=lambda item: (item[0], item[1], item[2]), reverse=True)
            return ranked[0][3]
        except SmartImportProfileError:
            raise
        except SQLAlchemyError as exc:
            raise SmartImportProfilePersistenceError(
                "No fue posible consultar los formatos recordados."
            ) from exc
        finally:
            session.close()

    def remember(
        self,
        *,
        organization_id: int,
        user_id: int | None,
        candidate: DatasetCandidate,
        mappings: tuple[ConfirmedMapping, ...],
        name: str | None = None,
    ) -> SmartImportProfile:
        """Create/update a tenant mapping profile and its audit row atomically.

        Only structural metadata is audited. Raw workbook values, source samples
        and normalized source headers are deliberately excluded from the audit
        event so remembering an Excel format does not duplicate business data.
        """

        mapping_payload = profile_mapping_payload(candidate, mappings)
        signature = candidate_header_signature(candidate)
        fingerprint = header_fingerprint(signature)
        profile_name = _normalize_profile_name(
            name,
            fallback=candidate.sheet_name,
        )

        normalized_org_id = int(organization_id)
        if normalized_org_id <= 0:
            raise SmartImportProfileValidationError(
                "El tenant del formato recordado no es válido."
            )

        session = self._session_factory()
        if session is None:
            raise SmartImportProfilePersistenceError(
                "El almacenamiento de formatos recordados no está disponible."
            )

        now = datetime.now(timezone.utc)
        try:
            set_tenant_db_context(session, normalized_org_id)
            profile = session.execute(
                select(SmartImportProfile).where(
                    SmartImportProfile.organization_id == normalized_org_id,
                    SmartImportProfile.schema_kind == SMART_PROFILE_SCHEMA_KIND,
                    SmartImportProfile.name == profile_name,
                )
            ).scalar_one_or_none()

            created = profile is None
            before_state: dict[str, object] | None = None

            if profile is None:
                profile = SmartImportProfile(
                    organization_id=normalized_org_id,
                    created_by_user_id=user_id,
                    updated_by_user_id=user_id,
                    name=profile_name,
                    schema_kind=SMART_PROFILE_SCHEMA_KIND,
                    sheet_name=candidate.sheet_name,
                    header_fingerprint=fingerprint,
                    header_signature=list(signature),
                    mapping_json=mapping_payload,
                    version=1,
                    use_count=1,
                    active=True,
                    last_used_at=now,
                )
                session.add(profile)
            else:
                before_state = {
                    "version": int(profile.version),
                    "use_count": int(profile.use_count),
                    "active": bool(profile.active),
                    "header_fingerprint": profile.header_fingerprint,
                }
                profile.updated_by_user_id = user_id
                profile.sheet_name = candidate.sheet_name
                profile.header_fingerprint = fingerprint
                profile.header_signature = list(signature)
                profile.mapping_json = mapping_payload
                profile.version = int(profile.version) + 1
                profile.use_count = int(profile.use_count) + 1
                profile.active = True
                profile.last_used_at = now
                profile.updated_at = now

            # Assign DB-generated identity before writing the audit envelope. The
            # profile mutation and audit event remain inside the same transaction:
            # if audit persistence fails, neither state is committed.
            session.flush([profile])

            actor = build_audit_actor(
                organization_id=normalized_org_id,
                user_id=user_id,
                username=None,
                role=None,
            )
            after_state = {
                "version": int(profile.version),
                "use_count": int(profile.use_count),
                "active": bool(profile.active),
                "header_fingerprint": profile.header_fingerprint,
            }
            record_audit_event(
                session,
                actor=actor,
                action=(
                    AuditAction.SMART_IMPORT_PROFILE_CREATE
                    if created
                    else AuditAction.SMART_IMPORT_PROFILE_UPDATE
                ),
                entity_type="smart_import_profile",
                entity_id=profile.id,
                outcome=AuditOutcome.SUCCESS,
                metadata={
                    "schema_kind": SMART_PROFILE_SCHEMA_KIND,
                    "mapping_field_count": len(mapping_payload),
                    "profile_public_id": (
                        str(profile.public_id)
                        if profile.public_id is not None
                        else None
                    ),
                },
                before_data=before_state,
                after_data=after_state,
            )

            # Flush the audit row before detaching the profile so every DB write
            # still belongs to the same transaction. Detaching before commit avoids a
            # post-commit refresh: PostgreSQL transaction-local tenant context is cleared
            # by commit and FORCE RLS must never be bypassed merely to materialize the
            # return value. A failure in this flush or commit still rolls back atomically.
            session.flush()
            session.expunge(profile)
            session.commit()
            return profile
        except SmartImportProfileError:
            session.rollback()
            raise
        except (SQLAlchemyError, ValueError) as exc:
            session.rollback()
            raise SmartImportProfilePersistenceError(
                "No fue posible guardar el formato de importación."
            ) from exc
        finally:
            session.close()
