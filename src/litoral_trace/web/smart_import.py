"""Browser-safe Smart Import orchestration and preview ViewModels."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

import pandas as pd
from fastapi import Request

from litoral_trace.services.batch import BATCH_COLUMNAS, BatchWorkbook
from litoral_trace.services.smart_import import (
    ConfirmedMapping,
    DatasetCandidate,
    SmartImportEngine,
    SmartImportError,
    canonicalize_workbook,
    default_confirmed_mapping,
)
from litoral_trace.services.smart_import.profiles import (
    SmartImportProfileMatch,
    SmartImportProfilePersistenceError,
    SmartImportProfileService,
    SmartImportProfileValidationError,
    header_fingerprint,
)


SMART_MAPPING_FIELD_PREFIX = "smart_map__"
SMART_CONFIRM_FIELD = "smart_mapping_confirmed"
SMART_SHEET_FIELD = "smart_sheet_name"
SMART_HEADER_ROW_FIELD = "smart_header_row"
SMART_HEADER_FINGERPRINT_FIELD = "smart_header_fingerprint"
SMART_WORKBOOK_SHA_FIELD = "smart_workbook_sha256"
SMART_REMEMBER_FIELD = "smart_remember_mapping"
SMART_PROFILE_NAME_FIELD = "smart_profile_name"


@dataclass(frozen=True)
class SmartMappingOptionView:
    value: str
    label: str


@dataclass(frozen=True)
class SmartMappingRowView:
    canonical_field: str
    selected_source_index: str
    selected_source_column: str | None
    confidence_percent: int
    status: str
    reasons: tuple[str, ...]
    sample_values: tuple[str, ...]
    options: tuple[SmartMappingOptionView, ...]


@dataclass(frozen=True)
class SmartImportPreviewView:
    filename: str
    sheet_name: str
    header_row: int
    header_fingerprint: str
    workbook_sha256: str
    dataset_score_percent: int
    estimated_rows: int
    estimated_columns: int
    mapping_rows: tuple[SmartMappingRowView, ...]
    ignored_columns: tuple[str, ...]
    missing_required_fields: tuple[str, ...]
    profile_name: str | None
    profile_status: str | None
    profile_similarity_percent: int | None
    profile_missing_headers: tuple[str, ...]
    memory_warning: str | None
    remembered_profile_name: str | None
    mapping_confirmed: bool
    can_import: bool


@dataclass(frozen=True)
class SmartBatchWorkbook(BatchWorkbook):
    """Canonical workbook enriched only with browser presentation metadata."""

    smart_preview: SmartImportPreviewView


def _safe_text(value: Any, *, limit: int = 80) -> str:
    text = " ".join(str(value if value is not None else "").split())
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)] + "…"


def _form_text(form: Mapping[str, Any], key: str) -> str:
    value = form.get(key)
    return str(value or "").strip()


def _is_truthy_form_value(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "on", "yes", "si", "sí"}


def _candidate_fingerprint(candidate: DatasetCandidate) -> str:
    """Order-sensitive schema fingerprint for one browser confirmation.

    Persistent format profiles intentionally use an order-insensitive signature so
    they can survive harmless column reordering. A browser confirmation has a
    different security property: it maps by source index, so its fingerprint must
    change when columns are inserted, removed or reordered. The index is included
    explicitly to preserve gaps and duplicate normalized headers.
    """

    ordered_signature = tuple(
        f"{column.source_index}:{column.normalized_source}"
        for column in sorted(candidate.mappings, key=lambda item: item.source_index)
    )
    return header_fingerprint(ordered_signature)


def _select_candidate(
    analysis,
    form: Mapping[str, Any],
) -> DatasetCandidate:
    requested_sheet = _form_text(form, SMART_SHEET_FIELD)
    requested_header = _form_text(form, SMART_HEADER_ROW_FIELD)
    requested_fingerprint = _form_text(form, SMART_HEADER_FINGERPRINT_FIELD)
    requested_workbook_sha = _form_text(form, SMART_WORKBOOK_SHA_FIELD)
    mapping_confirmed = _is_truthy_form_value(form.get(SMART_CONFIRM_FIELD))

    if mapping_confirmed and (not requested_fingerprint or not requested_workbook_sha):
        raise SmartImportError(
            code="SMART_CANDIDATE_CHANGED",
            detail=(
                "La confirmación del mapping no incluye la identidad estructural completa del archivo. "
                "Volvé a analizarlo antes de importar."
            ),
        )

    if requested_workbook_sha and requested_workbook_sha != analysis.sha256:
        raise SmartImportError(
            code="SMART_CANDIDATE_CHANGED",
            detail=(
                "El archivo seleccionado no coincide exactamente con el que fue analizado. "
                "Volvé a analizar este Excel antes de confirmar o importar."
            ),
        )

    has_candidate_binding = bool(
        requested_sheet
        or requested_header
        or requested_fingerprint
        or requested_workbook_sha
    )
    if has_candidate_binding:
        if not requested_sheet or not requested_header:
            raise SmartImportError(
                code="SMART_INVALID_CANDIDATE",
                detail=(
                    "La selección de hoja/encabezado está incompleta. "
                    "Volvé a analizar el archivo."
                ),
            )
        try:
            header_row = int(requested_header)
        except (TypeError, ValueError):
            raise SmartImportError(
                code="SMART_INVALID_CANDIDATE",
                detail="La selección de hoja/encabezado ya no es válida. Volvé a analizar el archivo.",
            ) from None

        for candidate in analysis.candidates:
            if candidate.sheet_name != requested_sheet or candidate.header_row != header_row:
                continue
            if requested_fingerprint and _candidate_fingerprint(candidate) != requested_fingerprint:
                raise SmartImportError(
                    code="SMART_CANDIDATE_CHANGED",
                    detail=(
                        "La estructura de columnas cambió desde la vista previa. "
                        "Volvé a analizar el archivo antes de confirmar el mapping."
                    ),
                )
            return candidate

        raise SmartImportError(
            code="SMART_CANDIDATE_CHANGED",
            detail="La estructura del archivo cambió desde la vista previa. Volvé a validarlo.",
        )

    candidate = analysis.best_candidate
    if candidate is None:
        raise SmartImportError(
            code="SMART_DATASET_NOT_FOUND",
            detail=(
                "No encontramos una tabla suficientemente compatible con los campos de lotes de Litoral Trace. "
                "Podés usar la plantilla oficial o revisar los encabezados del archivo."
            ),
        )
    return candidate


def _explicit_mapping(
    form: Mapping[str, Any],
    candidate: DatasetCandidate,
) -> tuple[tuple[ConfirmedMapping, ...] | None, bool]:
    confirmed = _is_truthy_form_value(form.get(SMART_CONFIRM_FIELD))
    has_mapping_fields = any(
        _form_text(form, f"{SMART_MAPPING_FIELD_PREFIX}{field}")
        for field in BATCH_COLUMNAS
    )

    if not has_mapping_fields:
        return None, confirmed

    by_index = {column.source_index: column for column in candidate.mappings}
    mappings: list[ConfirmedMapping] = []
    used_indexes: set[int] = set()

    for canonical_field in BATCH_COLUMNAS:
        raw_index = _form_text(
            form,
            f"{SMART_MAPPING_FIELD_PREFIX}{canonical_field}",
        )
        if not raw_index:
            continue
        try:
            source_index = int(raw_index)
        except ValueError:
            raise SmartImportError(
                code="SMART_INVALID_MAPPING",
                detail="El mapping enviado contiene una columna inválida.",
            ) from None

        source = by_index.get(source_index)
        if source is None:
            raise SmartImportError(
                code="SMART_MAPPING_CHANGED",
                detail="Una columna seleccionada ya no existe en la hoja analizada.",
            )
        if source_index in used_indexes:
            raise SmartImportError(
                code="SMART_DUPLICATE_SOURCE_MAPPING",
                detail="Una misma columna del Excel no puede alimentar dos campos obligatorios en Smart Import V1.",
            )
        used_indexes.add(source_index)
        mappings.append(
            ConfirmedMapping(
                source_index=source.source_index,
                source_column=source.source_column,
                canonical_field=canonical_field,
            )
        )

    return tuple(mappings), confirmed


def _mapping_is_complete(mappings: tuple[ConfirmedMapping, ...]) -> bool:
    return {item.canonical_field for item in mappings} == set(BATCH_COLUMNAS)


def _profile_mapping_if_safe(
    match: SmartImportProfileMatch | None,
) -> tuple[ConfirmedMapping, ...] | None:
    if match is None:
        return None
    if match.status not in {"EXACT", "COMPATIBLE_DRIFT"}:
        return None
    if not _mapping_is_complete(match.mappings):
        return None
    return match.mappings


def _build_preview(
    *,
    analysis,
    candidate: DatasetCandidate,
    mappings: tuple[ConfirmedMapping, ...],
    mapping_confirmed: bool,
    profile_match: SmartImportProfileMatch | None,
    memory_warning: str | None,
    remembered_profile_name: str | None,
) -> SmartImportPreviewView:
    selected_by_target = {
        mapping.canonical_field: mapping
        for mapping in mappings
    }
    candidate_by_index = {
        column.source_index: column
        for column in candidate.mappings
    }
    options = tuple(
        SmartMappingOptionView(
            value=str(column.source_index),
            label=f"{column.source_column} · columna {column.source_index + 1}",
        )
        for column in sorted(candidate.mappings, key=lambda item: item.source_index)
    )

    rows: list[SmartMappingRowView] = []
    selected_indexes: set[int] = set()
    for canonical_field in BATCH_COLUMNAS:
        selected = selected_by_target.get(canonical_field)
        source = (
            candidate_by_index.get(selected.source_index)
            if selected is not None
            else None
        )
        if selected is not None:
            selected_indexes.add(selected.source_index)

        if source is not None and source.decision.canonical_field == canonical_field:
            confidence = int(round(source.decision.confidence * 100))
            status = source.decision.status.value
            reasons = source.decision.reasons
        elif source is not None:
            confidence = 0
            status = "USER"
            reasons = ("asignación elegida por el usuario o por un perfil recordado",)
        else:
            confidence = 0
            status = "MISSING"
            reasons = ("campo obligatorio sin columna asignada",)

        rows.append(
            SmartMappingRowView(
                canonical_field=canonical_field,
                selected_source_index=(str(selected.source_index) if selected is not None else ""),
                selected_source_column=(source.source_column if source is not None else None),
                confidence_percent=confidence,
                status=status,
                reasons=tuple(reasons),
                sample_values=tuple(
                    _safe_text(value)
                    for value in (source.sample_values if source is not None else ())[:3]
                ),
                options=options,
            )
        )

    ignored = tuple(
        column.source_column
        for column in sorted(candidate.mappings, key=lambda item: item.source_index)
        if column.source_index not in selected_indexes
    )
    missing = tuple(
        field
        for field in BATCH_COLUMNAS
        if field not in selected_by_target
    )

    return SmartImportPreviewView(
        filename=analysis.filename,
        sheet_name=candidate.sheet_name,
        header_row=candidate.header_row,
        header_fingerprint=_candidate_fingerprint(candidate),
        workbook_sha256=analysis.sha256,
        dataset_score_percent=int(round(candidate.score * 100)),
        estimated_rows=candidate.estimated_rows,
        estimated_columns=candidate.estimated_columns,
        mapping_rows=tuple(rows),
        ignored_columns=ignored,
        missing_required_fields=missing,
        profile_name=(profile_match.name if profile_match is not None else None),
        profile_status=(profile_match.status if profile_match is not None else None),
        profile_similarity_percent=(
            int(round(profile_match.similarity * 100))
            if profile_match is not None
            else None
        ),
        profile_missing_headers=(
            profile_match.missing_source_headers
            if profile_match is not None
            else ()
        ),
        memory_warning=memory_warning,
        remembered_profile_name=remembered_profile_name,
        mapping_confirmed=mapping_confirmed,
        can_import=_mapping_is_complete(mappings) and mapping_confirmed,
    )


def _preview_only_workbook(
    *,
    analysis,
    candidate: DatasetCandidate,
    preview: SmartImportPreviewView,
) -> SmartBatchWorkbook:
    return SmartBatchWorkbook(
        filename=analysis.filename,
        sha256=analysis.sha256,
        sheet_name=candidate.sheet_name,
        row_count=0,
        dataframe=pd.DataFrame(columns=BATCH_COLUMNAS),
        source_row_numbers=(),
        smart_preview=preview,
    )


async def parse_smart_browser_workbook(
    payload: bytes,
    *,
    filename: str,
    request: Request,
    organization_id: int | None,
    user_id: int | None,
) -> SmartBatchWorkbook:
    """Analyze, map and safely project a non-standard workbook for browser use."""

    form = await request.form()
    analysis = SmartImportEngine().analyze(payload, filename=filename)
    candidate = _select_candidate(analysis, form)

    explicit_mapping, mapping_confirmed = _explicit_mapping(form, candidate)
    profile_match: SmartImportProfileMatch | None = None
    memory_warning: str | None = None
    remembered_profile_name: str | None = None

    profile_service = SmartImportProfileService()
    if organization_id is not None:
        try:
            profile_match = profile_service.find_best_match(
                organization_id=organization_id,
                candidate=candidate,
            )
        except SmartImportProfilePersistenceError:
            memory_warning = (
                "El análisis funciona, pero los formatos recordados no pudieron consultarse en este momento."
            )

    if explicit_mapping is not None:
        mappings = explicit_mapping
    else:
        mappings = (
            _profile_mapping_if_safe(profile_match)
            or default_confirmed_mapping(candidate)
        )

    is_import_request = request.url.path.rstrip("/") == "/imports"
    if is_import_request and not mapping_confirmed:
        raise SmartImportError(
            code="SMART_MAPPING_CONFIRMATION_REQUIRED",
            detail=(
                "Este Excel no usa la plantilla oficial. Validalo primero, revisá el mapping sugerido y confirmalo antes de importar."
            ),
        )

    if mapping_confirmed and not _mapping_is_complete(mappings):
        raise SmartImportError(
            code="SMART_MAPPING_INCOMPLETE",
            detail="Asigná una columna a cada uno de los ocho campos obligatorios antes de continuar.",
        )

    if (
        mapping_confirmed
        and _mapping_is_complete(mappings)
        and organization_id is not None
        and _is_truthy_form_value(form.get(SMART_REMEMBER_FIELD))
    ):
        try:
            profile = profile_service.remember(
                organization_id=organization_id,
                user_id=user_id,
                candidate=candidate,
                mappings=mappings,
                name=_form_text(form, SMART_PROFILE_NAME_FIELD) or None,
            )
            remembered_profile_name = profile.name
            profile_match = SmartImportProfileMatch(
                public_id=profile.public_id,
                name=profile.name,
                status="EXACT",
                similarity=1.0,
                mappings=mappings,
            )
        except SmartImportProfileValidationError as exc:
            memory_warning = str(exc)
        except SmartImportProfilePersistenceError:
            memory_warning = (
                "El mapping puede usarse para esta carga, pero no pudo guardarse como formato recordado."
            )

    preview = _build_preview(
        analysis=analysis,
        candidate=candidate,
        mappings=mappings,
        mapping_confirmed=mapping_confirmed,
        profile_match=profile_match,
        memory_warning=memory_warning,
        remembered_profile_name=remembered_profile_name,
    )

    if not _mapping_is_complete(mappings):
        if is_import_request:
            raise SmartImportError(
                code="SMART_MAPPING_INCOMPLETE",
                detail="Faltan campos obligatorios para construir la importación canónica.",
            )
        return _preview_only_workbook(
            analysis=analysis,
            candidate=candidate,
            preview=preview,
        )

    canonical = canonicalize_workbook(
        payload,
        filename=filename,
        candidate=candidate,
        mappings=mappings,
    )
    return SmartBatchWorkbook(
        filename=canonical.filename,
        sha256=canonical.sha256,
        sheet_name=canonical.sheet_name,
        row_count=canonical.row_count,
        dataframe=canonical.dataframe,
        source_row_numbers=canonical.source_row_numbers,
        smart_preview=preview,
    )
